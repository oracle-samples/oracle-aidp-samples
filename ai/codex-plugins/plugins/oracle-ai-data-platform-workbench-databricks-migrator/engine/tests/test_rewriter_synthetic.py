"""Unit tests for catalog_ddl_rewriter against synthetic UC-API-shaped fixtures.

Validates that the rewriter handles edge-case Databricks UC features that we
couldn't exercise via the live Cerner workspace (warehouse provisioning broken).

Run: python -m pytest tests/test_rewriter_synthetic.py -v
"""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from catalog_ddl_rewriter import rewrite_table_ddl, UnsupportedDDL, RewriteResult

FIXTURES = Path(__file__).parent / "fixtures" / "catalog" / "synthetic"


def _load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _common_args():
    return dict(
        catalog_map={"main": "main"},
        bucket_map={"example-data-lake-prod": ("example-data-lake-prod", "exmplnspace2")},
        target_using="parquet",
        flatten_strategy="schema-prefix",
        location_strategy="drop",
    )


# ── Test 1: Liquid clustering ──
def test_liquid_clustered_table():
    """Liquid-clustered table → AIDP CREATE TABLE without CLUSTER BY (AIDP doesn't support it)."""
    t = _load("liquid_clustered.json")
    res = rewrite_table_ddl(dbx_table=t, **_common_args())
    assert res.aidp_full_name == "main_example_team.liquid_clustered_sales"
    sql = res.create_table_sql
    assert "CREATE TABLE IF NOT EXISTS main_example_team.liquid_clustered_sales" in sql
    assert "`customer_id` string NOT NULL" in sql
    assert "USING parquet" in sql
    assert "CLUSTER BY" not in sql, "Liquid clustering must be stripped"
    assert "clusteringColumns" not in sql, "clusteringColumns property must be dropped"
    # All 5 cols present
    assert "`order_id` bigint NOT NULL" in sql
    assert "`region` string" in sql
    assert "delta.feature.clustering" not in sql, "delta.feature.* must be dropped"
    # Comment preserved
    assert "Synthetic edge case: liquid clustering" in sql


# ── Test 2: Generated identity + generated expression ──
def test_generated_columns():
    """GENERATED ALWAYS AS IDENTITY → strip; column becomes plain type."""
    t = _load("generated_identity.json")
    res = rewrite_table_ddl(dbx_table=t, **_common_args())
    sql = res.create_table_sql
    # user_id keeps its type, no IDENTITY/GENERATED clause emitted as SQL keywords
    assert "`user_id` bigint NOT NULL" in sql
    # Look for SQL keywords (with space before), not substring matches against names/comments
    assert " GENERATED ALWAYS AS " not in sql, "GENERATED clause must be stripped"
    assert " IDENTITY" not in sql, "IDENTITY clause must be stripped"
    # tenant_id also becomes plain — generation expression goes through column metadata
    # (we drop it silently — the rewriter only sees columns[].name + columns[].type_text)
    assert "`tenant_id` string" in sql
    assert "delta.feature.identityColumns" not in sql, "delta.feature.* must be dropped"
    assert "delta.feature.generatedColumns" not in sql, "delta.feature.* must be dropped"
    assert "delta.identity" not in sql, "delta.identity.* must be dropped"
    assert "delta.generationExpression" not in sql, "delta.generationExpression must be dropped"


# ── Test 3: PARTITIONED BY + deletion vectors + CHECK constraints ──
def test_partitioned_with_check_constraints():
    """PARTITIONED BY preserved, deletion vectors stripped, CHECK constraints dropped."""
    t = _load("partitioned_with_check.json")
    res = rewrite_table_ddl(dbx_table=t, **_common_args())
    sql = res.create_table_sql
    # partition cols preserved
    assert "PARTITIONED BY (`event_date`, `region`)" in sql
    # data cols preserved
    assert "`event_id` string NOT NULL" in sql
    assert "`amount` decimal(10,2)" in sql
    # deletion vector + check constraint properties stripped
    assert "delta.feature.deletionVectors" not in sql
    assert "delta.feature.checkConstraints" not in sql
    assert "delta.enableDeletionVectors" not in sql
    assert "delta.constraints" not in sql
    # spark.sql.* stats dropped
    assert "spark.sql.statistics" not in sql
    # Comment preserved
    assert "Synthetic edge case: PARTITIONED BY + CHECK" in sql


# ── Test 4: External s3:// table → bucket-map lookup, preserved LOCATION ──
def test_external_s3_with_bucket_map():
    """EXTERNAL parquet table with s3:// LOCATION — bucket-map rewrite to oci://."""
    t = _load("external_s3_table.json")
    args = _common_args()
    args["location_strategy"] = "preserve"  # external tables typically keep LOCATION
    res = rewrite_table_ddl(dbx_table=t, **args)
    sql = res.create_table_sql
    # Location was rewritten via bucket_map
    assert "oci://example-data-lake-prod@exmplnspace2/landing/clinical_extracts/v2/" in sql
    assert "s3://" not in sql, "S3 prefix must be rewritten away"
    # Partition col preserved
    assert "PARTITIONED BY (`extract_dt`)" in sql
    # HMS-internal stats dropped
    assert "transient_lastDdlTime" not in sql
    # EXTERNAL=TRUE property dropped from TBLPROPERTIES — but allow EXTERNAL in table name / comment
    assert "'EXTERNAL'" not in sql, "EXTERNAL=TRUE property in TBLPROPERTIES must be dropped"
    # Comment preserved (includes the word EXTERNAL)
    assert "EXTERNAL parquet table backed by S3" in sql


# ── Test 5: External s3:// with unmapped bucket → preserve as-is + warning ──
def test_external_s3_unmapped_bucket():
    """If the source S3 bucket isn't in bucket_map, preserve URI + emit warn rule."""
    t = _load("external_s3_table.json")
    args = _common_args()
    args["bucket_map"] = {}  # no mappings
    args["location_strategy"] = "preserve"
    res = rewrite_table_ddl(dbx_table=t, **args)
    # Should produce a R02_BUCKET_UNMAPPED warning rule
    warn_rules = [r for r in res.applied_rules if r.severity == "warn"]
    assert any(r.rule_id == "R02_BUCKET_UNMAPPED" for r in warn_rules), \
        f"Expected R02_BUCKET_UNMAPPED warning; got {[r.rule_id for r in res.applied_rules]}"
    # Original s3:// preserved (passes through unchanged) so user can fix the map and rerun
    assert "s3://example-data-lake-prod" in res.create_table_sql


# ── Test 6: Materialized view → UnsupportedDDL ──
def test_materialized_view_raises_unsupported():
    """MV must raise UnsupportedDDL with R16_MV_UNSUPPORTED rule id."""
    t = _load("materialized_view.json")
    try:
        rewrite_table_ddl(dbx_table=t, **_common_args())
        assert False, "Should have raised UnsupportedDDL"
    except UnsupportedDDL as e:
        assert e.rule_id == "R16_MV_UNSUPPORTED"
        assert "MATERIALIZED VIEW" in str(e)


# ── Test 7: Streaming table → UnsupportedDDL ──
def test_streaming_table_raises_unsupported():
    """STREAMING_TABLE must raise UnsupportedDDL with R17 rule id."""
    t = _load("streaming_table.json")
    try:
        rewrite_table_ddl(dbx_table=t, **_common_args())
        assert False, "Should have raised UnsupportedDDL"
    except UnsupportedDDL as e:
        assert e.rule_id == "R17_STREAMING_UNSUPPORTED"


# ── Test 8: Comprehensive property scrubbing ──
def test_all_noise_properties_scrubbed():
    """Real samples.tpch.customer fixture had 96 properties — verify rewriter scrubs all."""
    # Use real fixture
    real_path = Path(__file__).parent / "fixtures" / "catalog" / "uc" / "samples.tpch.customer.json"
    t = json.loads(real_path.read_text(encoding="utf-8"))
    args = _common_args()
    args["catalog_map"] = {"samples": "samples"}
    res = rewrite_table_ddl(dbx_table=t, **args)
    assert "spark.sql.statistics" not in res.create_table_sql
    assert "delta.feature" not in res.create_table_sql
    assert "delta.minReaderVersion" not in res.create_table_sql
    assert "delta.lastCommit" not in res.create_table_sql
    assert "delta.parquet" not in res.create_table_sql
    assert "view.query" not in res.create_table_sql
    # We dropped 90+ noise properties
    assert len(res.dropped_properties) > 80, f"Should have dropped >80 noise props; got {len(res.dropped_properties)}"


# ── Test 9 (regression for code review 2026-06-05): partition_index ordering ──
def test_partition_columns_ordered_by_partition_index():
    """PARTITIONED BY must reflect partition_index order, NOT column position.

    Regression for code-review finding: `region` (position 1, partition_index 0)
    must appear BEFORE `year` (position 5, partition_index 1).
    """
    t = _load("partition_index_reverse_order.json")
    res = rewrite_table_ddl(dbx_table=t, **_common_args())
    sql = res.create_table_sql
    assert "PARTITIONED BY" in sql
    pb_start = sql.find("PARTITIONED BY (")
    pb_end = sql.find(")", pb_start)
    pb_clause = sql[pb_start:pb_end + 1]
    region_pos = pb_clause.find("`region`")
    year_pos = pb_clause.find("`year`")
    assert region_pos >= 0 and year_pos >= 0, f"Both partition cols must be present: {pb_clause!r}"
    assert region_pos < year_pos, (
        f"partition_index 0 (region) must appear before partition_index 1 (year). "
        f"Got: {pb_clause!r}"
    )


# ── Test 10 (regression for code review 2026-06-05): delta.* catch-all ──
def test_no_delta_property_survives():
    """Catch-all `delta.` prefix must scrub ALL delta.* properties.

    delta.columnMapping.mode='name' on a non-Delta target silently corrupts
    column resolution — none of the original explicit prefixes caught it.
    """
    t = _load("delta_misc_properties.json")
    res = rewrite_table_ddl(dbx_table=t, **_common_args())
    sql = res.create_table_sql
    for prop_key in t["properties"]:
        assert prop_key.startswith("delta."), f"fixture sanity: {prop_key}"
        assert f"'{prop_key}'" not in sql, f"Property {prop_key} leaked into output SQL"
    # Spot-check the most dangerous keys explicitly
    assert "columnMapping" not in sql, "delta.columnMapping.mode MUST be dropped"
    assert "autoOptimize" not in sql
    assert "targetFileSize" not in sql
    assert "logRetentionDuration" not in sql


# ── Test 11 (regression for code review 2026-06-05): Python <3.12 portability ──
def test_module_parses_on_python_3_10_3_11_syntax():
    """The rewriter must be importable on Python 3.10/3.11.

    An f-string with a backslash inside {} is a SyntaxError before 3.12 (PEP 701).
    The original `f"...{',\\n'.join(parts)}..."` would crash at import-time.
    """
    import ast, pathlib, re
    src = (Path(__file__).parent.parent / "scripts" / "catalog_ddl_rewriter.py").read_text(encoding="utf-8")
    # ast.parse uses THIS interpreter's grammar — on 3.10/3.11 it would raise on the bad pattern.
    ast.parse(src)
    # Belt-and-suspenders: scan for the anti-pattern (f-string with backslash inside braces)
    # on a single line.
    suspect = re.findall(r'''f["'][^"'\n]*\{[^}]*\\[^}]*\}[^"'\n]*["']''', src)
    assert not suspect, (
        f"Found f-string(s) with backslash inside {{}} braces "
        f"(SyntaxError on Py<3.12): {suspect}"
    )


if __name__ == "__main__":
    # Run all tests inline
    test_funcs = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    passed = 0
    for fn in test_funcs:
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {fn.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERR   {fn.__name__}: {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{passed}/{passed + failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
