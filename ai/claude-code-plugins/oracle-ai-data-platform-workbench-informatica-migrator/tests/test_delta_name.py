"""DeltaTable.forName needs a qualified name on AIDP once the session has
USEd an AIDP catalog: a one-part name raises IndexOutOfBoundsException
there (verified on an AIDP Spark 3.5.0 cluster, 2026-09-29), while
spark.catalog.tableExists accepts it -- so the failure only showed at the
MERGE. It broke every Sequence Generator (its counter table is one-part)
and every target the export names without an owner."""
from infa_compat import delta_name
from infa2aidp.generators.write_strategies import DeltaWriteStrategy
from infa2aidp.models import LoadStrategy


class _Catalog:
    def currentCatalog(self):  # noqa: N802
        return "default"

    def currentDatabase(self):  # noqa: N802
        return "sales_dm"


class _Spark:
    catalog = _Catalog()


def test_one_part_name_is_qualified_with_the_current_catalog_and_schema():
    assert delta_name(_Spark(), "_infa_compat_sequences") == "default.sales_dm._infa_compat_sequences"


def test_qualified_names_are_left_alone():
    for name in ("sales_dm.t", "default.sales_dm.t", "`a`.`b`"):
        assert delta_name(_Spark(), name) == name


def test_an_older_spark_without_current_catalog_keeps_the_name():
    class Old:
        class catalog:  # noqa: N801
            @staticmethod
            def currentCatalog():  # noqa: N802
                raise AttributeError("no currentCatalog")
    assert delta_name(Old(), "t") == "t"


def test_generated_merge_qualifies_only_one_part_targets():
    one = "\n".join(DeltaWriteStrategy().emit_write(None, None, "TGT", LoadStrategy.UPSERT, ["ID"], df_var="df"))
    two = "\n".join(DeltaWriteStrategy().emit_write(None, None, "SALES.TGT", LoadStrategy.UPSERT, ["ID"], df_var="df"))
    assert 'DeltaTable.forName(spark, infa_compat.delta_name(spark, "TGT"))' in one
    assert 'DeltaTable.forName(spark, "SALES.TGT")' in two
