"""Tests for infa2aidp.migrator.run_migration -- the library entrypoint
_cmd_migrate delegates to. Covers path selection (LLM vs. rule-based),
the comparison-report artifact, and the parallel batch-mode branch.

The LLM is always mocked here -- these tests never make a real API call.
"""
from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest

from infa2aidp.migrator import run_migration
from infa2aidp.models import (
    Mapping,
    MigrationResult,
    Transformation,
    TransformationField,
    TransformationType,
    DataFlowDirection,
)


def _mapping(name: str = "m_customers") -> Mapping:
    tx = Transformation(
        name="EXP_VALIDATE",
        type=TransformationType.EXPRESSION,
        fields=[
            TransformationField(
                name="CLEAN_EMAIL", datatype="string",
                direction=DataFlowDirection.OUTPUT, expression="LOWER(EMAIL)",
            ),
        ],
    )
    return Mapping(name=name, transformations=[tx])


def _migration_result(mapping: Mapping) -> MigrationResult:
    return MigrationResult(mappings=[mapping], sessions=[], workflows=[])


@pytest.fixture(autouse=True)
def _stub_xml_parser(monkeypatch):
    """Every test drives run_migration off a canned parse result -- no real
    XML files or fixtures needed; the mapping is set on the module dict.
    """
    monkeypatch.setattr(
        "infa2aidp.parsers.xml_parser.InformaticaXMLParser.parse",
        lambda self, path: _migration_result(_mapping()),
    )


def test_rule_based_path_selected_when_llm_disabled(tmp_path):
    result = run_migration(
        ["fake.xml"], str(tmp_path),
        use_llm=False, score_confidence=False, skip_lineage=True, skip_optimize=True,
    )
    assert result.notebooks == 1
    assert len(result.outcomes) == 1
    assert result.outcomes[0].path_used == "rule-based"
    assert result.outcomes[0].score == 0
    nb_dir = os.path.join(str(tmp_path), "Migrated")
    assert os.path.isfile(os.path.join(nb_dir, "nb_m_customers.ipynb"))


def test_rule_based_path_selected_when_llm_unavailable(monkeypatch, tmp_path):
    class _UnavailableLLM:
        claude_model = "n/a"

        def is_available(self):
            return False

    # The migrator selects its provider through handlers.make_llm_handler
    # now (this build defaults to OpenAI), so the factory is the seam to
    # stub -- patching the Anthropic class alone no longer reaches it.
    monkeypatch.setattr("infa2aidp.handlers.make_llm_handler",
                        lambda *a, **k: _UnavailableLLM())
    result = run_migration(
        ["fake.xml"], str(tmp_path),
        use_llm=True, score_confidence=False, skip_lineage=True, skip_optimize=True,
    )
    assert result.outcomes[0].path_used == "rule-based"


def test_llm_path_selected_when_available(monkeypatch, tmp_path):
    class _FakeLLMHandler:
        claude_model = "fake-claude"

        def is_available(self):
            return True

    class _FakeLLMGen:
        def __init__(self, llm_handler):
            self.llm = llm_handler
            self._last_score = 97
            self._last_fidelity = None

        def generate(self, mapping, session, output_format="ipynb", source_path=None):
            return '{"cells": []}'

    # The migrator selects its provider through handlers.make_llm_handler
    # now (this build defaults to OpenAI), so the factory is the seam to
    # stub -- patching the Anthropic class alone no longer reaches it.
    monkeypatch.setattr("infa2aidp.handlers.make_llm_handler",
                        lambda *a, **k: _FakeLLMHandler())
    monkeypatch.setattr(
        "infa2aidp.generators.llm_notebook_generator.LLMNotebookGenerator", _FakeLLMGen
    )

    result = run_migration(
        ["fake.xml"], str(tmp_path),
        use_llm=True, score_confidence=False, skip_lineage=True, skip_optimize=True,
    )
    assert result.outcomes[0].path_used == "llm"
    assert result.outcomes[0].score == 97
    assert result.notebooks == 1


def test_emit_comparison_produces_artifact(tmp_path):
    result = run_migration(
        ["fake.xml"], str(tmp_path),
        use_llm=False, emit_comparison=True,
        score_confidence=False, skip_lineage=True, skip_optimize=True,
    )
    outcome = result.outcomes[0]
    assert outcome.comparison_path
    assert os.path.isfile(outcome.comparison_path)
    html_path = os.path.join(
        str(tmp_path), "comparisons", "m_customers_comparison.html"
    )
    assert os.path.isfile(html_path)


def test_confidence_scoring_populates_summary(tmp_path):
    result = run_migration(
        ["fake.xml"], str(tmp_path),
        use_llm=False, score_confidence=True, skip_lineage=True, skip_optimize=True,
    )
    assert result.confidence_summary
    assert "auto_rate" in result.confidence_summary
    assert os.path.isfile(os.path.join(str(tmp_path), "reports", "confidence_report.md"))


def test_max_workers_greater_than_one_takes_batch_path(monkeypatch, tmp_path):
    calls = {}

    class _FakeLLMHandler:
        claude_model = "fake-claude"

        def is_available(self):
            return True

    class _FakeBatchSummary:
        success = 2
        fallback = 0
        failed = 0
        results = []

    class _FakeBatchMigrator:
        def __init__(self, llm_handler=None, max_workers=None):
            calls["max_workers"] = max_workers
            calls["llm_handler"] = llm_handler

        def migrate_folder(self, input_dir, output_dir):
            calls["input_dir"] = input_dir
            calls["output_dir"] = output_dir
            return _FakeBatchSummary()

    # The migrator selects its provider through handlers.make_llm_handler
    # now (this build defaults to OpenAI), so the factory is the seam to
    # stub -- patching the Anthropic class alone no longer reaches it.
    monkeypatch.setattr("infa2aidp.handlers.make_llm_handler",
                        lambda *a, **k: _FakeLLMHandler())
    monkeypatch.setattr("infa2aidp.batch.BatchMigrator", _FakeBatchMigrator)

    result = run_migration(
        ["dir/a.xml", "dir/b.xml"], str(tmp_path),
        use_llm=True, max_workers=4,
    )
    assert calls["max_workers"] == 4
    assert calls["output_dir"] == str(tmp_path)
    assert result.batch_summary is not None
    assert result.notebooks == 2


def test_single_input_does_not_take_batch_path_even_with_workers(monkeypatch, tmp_path):
    """max_workers>1 only pays off with >1 mapping to parallelize; a single
    input file should still take the serial per-mapping loop."""
    triggered = {"batch": False}

    class _FakeLLMHandler:
        claude_model = "fake-claude"

        def is_available(self):
            return True

    class _FakeBatchMigrator:
        def __init__(self, **kwargs):
            triggered["batch"] = True

        def migrate_folder(self, *a, **kw):
            triggered["batch"] = True

    # The migrator selects its provider through handlers.make_llm_handler
    # now (this build defaults to OpenAI), so the factory is the seam to
    # stub -- patching the Anthropic class alone no longer reaches it.
    monkeypatch.setattr("infa2aidp.handlers.make_llm_handler",
                        lambda *a, **k: _FakeLLMHandler())
    monkeypatch.setattr("infa2aidp.batch.BatchMigrator", _FakeBatchMigrator)

    result = run_migration(
        ["fake.xml"], str(tmp_path),
        use_llm=False, max_workers=4, score_confidence=False,
        skip_lineage=True, skip_optimize=True,
    )
    assert triggered["batch"] is False
    assert result.batch_summary is None


# ── Fix round 2: batch path must honor emit_comparison/score_confidence,
# and must never silently widen an explicit input list. ──────────────────

class _FakeLLMHandler:
    claude_model = "fake-claude"

    def is_available(self):
        return True


def _fake_batch_result(xml_path: str, mapping_name: str, notebook_path: str):
    class _BR:
        pass
    r = _BR()
    r.xml_path = xml_path
    r.mapping_name = mapping_name
    r.notebook_path = notebook_path
    r.score = 90
    r.status = "success"
    return r


def test_batch_path_honors_emit_comparison_and_confidence(monkeypatch, tmp_path):
    xml_a = tmp_path / "a.xml"
    xml_b = tmp_path / "b.xml"
    xml_a.write_text("<MAPPING/>", encoding="utf-8")
    xml_b.write_text("<MAPPING/>", encoding="utf-8")

    class _FakeSummary:
        success = 2
        fallback = 0
        failed = 0
        results = [
            _fake_batch_result(str(xml_a), "m_customers", str(tmp_path / "nb_a.ipynb")),
            _fake_batch_result(str(xml_b), "m_customers", str(tmp_path / "nb_b.ipynb")),
        ]

    class _FakeBatchMigrator:
        def __init__(self, **kwargs):
            pass

        def migrate_folder(self, input_dir, output_dir):
            return _FakeSummary()

    monkeypatch.setattr("infa2aidp.handlers.make_llm_handler",
                        lambda *a, **k: _FakeLLMHandler())
    monkeypatch.setattr("infa2aidp.batch.BatchMigrator", _FakeBatchMigrator)

    result = run_migration(
        [str(xml_a), str(xml_b)], str(tmp_path),
        use_llm=True, max_workers=4, emit_comparison=True, score_confidence=True,
        skip_optimize=True,
    )

    assert result.batch_summary is not None
    # Finding 1: the fidelity report must not silently vanish on the batch path.
    for outcome in result.outcomes:
        assert outcome.comparison_path, "batch-path outcome missing a comparison report"
        assert os.path.isfile(outcome.comparison_path)
    assert result.confidence_summary, "batch-path run produced no confidence summary"
    assert os.path.isfile(os.path.join(str(tmp_path), "reports", "confidence_report.md"))
    # Lineage defaults to on and must also survive the batch path.
    assert os.path.isfile(os.path.join(str(tmp_path), "lineage", "m_customers.md"))


def test_batch_path_with_custom_rules_raises_clear_error(monkeypatch, tmp_path):
    xml_a = tmp_path / "a.xml"
    xml_b = tmp_path / "b.xml"
    xml_a.write_text("<MAPPING/>", encoding="utf-8")
    xml_b.write_text("<MAPPING/>", encoding="utf-8")

    monkeypatch.setattr("infa2aidp.handlers.make_llm_handler",
                        lambda *a, **k: _FakeLLMHandler())

    with pytest.raises(ValueError, match="--custom-rules"):
        run_migration(
            [str(xml_a), str(xml_b)], str(tmp_path / "out"),
            use_llm=True, max_workers=4, custom_rules_path="some_rules.yaml",
        )


def test_batch_path_disabled_when_ancestor_dir_has_unrequested_files(monkeypatch, tmp_path):
    """Finding 2 regression: run_migration is a public entrypoint now, so a
    caller passing a scattered file list must never have extra files in the
    computed ancestor directory silently swept into the batch run."""
    xml_a = tmp_path / "a.xml"
    xml_b = tmp_path / "b.xml"
    xml_extra = tmp_path / "c.xml"  # deliberately NOT in the requested inputs
    xml_a.write_text("<MAPPING/>", encoding="utf-8")
    xml_b.write_text("<MAPPING/>", encoding="utf-8")
    xml_extra.write_text("<MAPPING/>", encoding="utf-8")

    batch_invoked = {"flag": False}

    class _FakeBatchMigrator:
        def __init__(self, **kwargs):
            batch_invoked["flag"] = True

        def migrate_folder(self, *a, **kw):
            batch_invoked["flag"] = True

    monkeypatch.setattr("infa2aidp.handlers.make_llm_handler",
                        lambda *a, **k: _FakeLLMHandler())
    monkeypatch.setattr("infa2aidp.batch.BatchMigrator", _FakeBatchMigrator)

    result = run_migration(
        [str(xml_a), str(xml_b)], str(tmp_path / "out"),
        use_llm=True, max_workers=4, score_confidence=False,
        skip_lineage=True, skip_optimize=True,
    )

    assert batch_invoked["flag"] is False, "BatchMigrator must not run when the ancestor dir has extra files"
    assert result.batch_summary is None
    # Only the two requested files were migrated -- c.xml was never touched.
    assert result.notebooks == 2
    assert {o.xml_path for o in result.outcomes} == {str(xml_a), str(xml_b)}


# ── Fix round 3 ────────────────────────────────────────────────────────

def test_unrequested_files_under_normalises_paths_consistently():
    """Finding 1 regression: requested paths were compared via
    os.path.abspath (does not resolve symlinks) against found paths via
    Path.resolve() (does). On macOS /tmp -> /private/tmp and
    /var/folders/... -> /private/var/folders/..., so every requested file
    used to come out looking "unrequested".

    pytest's tmp_path fixture already hands back a fully-resolved path, so
    it never exercises this mismatch -- this test uses tempfile.mkdtemp()
    directly (the same repro the coordinator used) to go through the
    unresolved form.
    """
    import shutil
    import tempfile

    from infa2aidp.migrator import _unrequested_files_under

    d = tempfile.mkdtemp()
    try:
        a = os.path.join(d, "a.xml")
        b = os.path.join(d, "b.xml")
        for p in (a, b):
            with open(p, "w", encoding="utf-8") as f:
                f.write("<MAPPING/>")

        assert _unrequested_files_under([a, b], d) == []
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_batch_path_routes_json_input_through_detect_and_parse_file(monkeypatch, tmp_path):
    """Finding 2 regression: BatchMigrator's own rglob picks up both
    PowerCenter XML and IICS JSON and routes each through
    detect_and_parse_file (batch.py:_migrate_single). The post-batch
    comparison/confidence/lineage re-parse must go through the same
    boundary -- not a hardcoded InformaticaXMLParser -- or every IICS/JSON
    mapping silently loses those artifacts.

    Asserts on the routing decision itself (detect_and_parse_file's call
    log) rather than relying on the module's autouse InformaticaXMLParser
    stub, which -- per the coordinator's note -- ignores its `path`
    argument entirely and would mask this exact bug if used here: a
    hardcoded, wrong parser call would still "succeed" against that stub
    without ever touching detect_and_parse_file.
    """
    xml_path = tmp_path / "a.xml"
    json_path = tmp_path / "b.json"
    xml_path.write_text("<MAPPING/>", encoding="utf-8")
    json_path.write_text("{}", encoding="utf-8")

    class _FakeSummary:
        success = 2
        fallback = 0
        failed = 0
        results = [
            _fake_batch_result(str(xml_path), "m_xml", str(tmp_path / "nb_xml.ipynb")),
            _fake_batch_result(str(json_path), "m_iics", str(tmp_path / "nb_iics.ipynb")),
        ]

    class _FakeBatchMigrator:
        def __init__(self, **kwargs):
            pass

        def migrate_folder(self, input_dir, output_dir):
            return _FakeSummary()

    monkeypatch.setattr("infa2aidp.handlers.make_llm_handler",
                        lambda *a, **k: _FakeLLMHandler())
    monkeypatch.setattr("infa2aidp.batch.BatchMigrator", _FakeBatchMigrator)

    detect_calls: list[str] = []

    def _fake_detect_and_parse_file(path):
        detect_calls.append(path)
        return _migration_result(_mapping("m_from_detect"))

    monkeypatch.setattr(
        "infa2aidp.parsers.format_detector.detect_and_parse_file",
        _fake_detect_and_parse_file,
    )

    result = run_migration(
        [str(xml_path), str(json_path)], str(tmp_path),
        use_llm=True, max_workers=4, emit_comparison=True,
        score_confidence=False, skip_optimize=True,
    )

    # Both the .xml and the .json result were routed through
    # detect_and_parse_file -- not a hardcoded XML-only parser -- so a
    # direct InformaticaXMLParser.parse(json_path) call (which would raise
    # on JSON content) is never made and detect_calls records both.
    assert set(detect_calls) == {str(xml_path), str(json_path)}
    assert all(o.comparison_path and os.path.isfile(o.comparison_path) for o in result.outcomes)


# ---------------------------------------------------------------------------
# source_fidelity wiring: the independent, non-circular
# check exists (generators/source_fidelity.py) and generate() already
# accepts a source_path -- but nothing on the run_migration path ever
# passed one, so the check never actually ran outside of unit tests that
# drive LLMNotebookGenerator.generate() directly. These tests drive the
# real run_migration() library entrypoint end-to-end (only the Anthropic
# client is faked -- LLMNotebookGenerator itself is NOT mocked) and prove
# a fidelity result reaches the returned MigrationRunResult / MappingOutcome
# and the on-disk report, not just that source_fidelity() works standalone.
# ---------------------------------------------------------------------------

_FIDELITY_XML_FIXTURE = """<MAPPING NAME="m_customers">
  <TRANSFORMATION NAME="EXP_VALIDATE">
    <TRANSFORMFIELD NAME="CLEAN_EMAIL" EXPRESSION="LOWER(EMAIL)"/>
  </TRANSFORMATION>
  <TRANSFORMATION NAME="EXP_AUDIT_ONLY">
    <TRANSFORMFIELD NAME="AUDIT_TS" EXPRESSION="SYSDATE"/>
  </TRANSFORMATION>
</MAPPING>
"""

# The generated notebook only ever mentions EXP_VALIDATE/CLEAN_EMAIL --
# EXP_AUDIT_ONLY/AUDIT_TS is a construct in the raw XML above that never
# made it into the notebook, exactly the kind of parser/generator-level
# loss source_fidelity exists to name. The (circular) validator response
# below still scores it 100/100, since its spec never carried
# EXP_AUDIT_ONLY either -- mirroring the historical variable-port defect.
_FIDELITY_NOTEBOOK_RESPONSE = json.dumps([
    {"cell_type": "markdown", "source": "# m_customers"},
    {"cell_type": "code", "source": "df = spark.table('CUSTOMERS')"},
    {"cell_type": "code", "source": "df = df.withColumn('CLEAN_EMAIL', F.lower(F.col('EMAIL')))"},
    {"cell_type": "code", "source": "df.write.format('delta').mode('append').saveAsTable('t')"},
])

_FIDELITY_VALIDATION_RESPONSE = json.dumps({
    "score": 100,
    "critical_issues": [],
    "warnings": [],
    "info": [],
    "correct_steps": ["EXP_VALIDATE converted"],
})


class _FidelityFakeStream:
    """Stands in for the context manager ``messages.stream()`` returns --
    the generator does ``with ...stream(...) as s: s.get_final_message()``
    as of this release (Opus 5's default max_tokens exceeds the non-streaming
    ceiling)."""

    def __init__(self, message):
        self._message = message

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def get_final_message(self):
        return self._message


class _FidelityFakeMessages:
    def __init__(self, responses: list[str]):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def _next_message(self, kwargs):
        self.calls.append(kwargs)
        text = self._responses.pop(0) if self._responses else self._responses[-1]
        # type="text" matters -- _extract_text() only accepts a "text"
        # block (Opus 5 leads with "thinking" by default).
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])

    def create(self, **kwargs):
        return self._next_message(kwargs)

    def stream(self, **kwargs):
        return _FidelityFakeStream(self._next_message(kwargs))


class _FidelityFakeClaudeClient:
    def __init__(self, responses: list[str]):
        self.messages = _FidelityFakeMessages(responses)

    def with_options(self, **_kwargs):
        return self


class _FidelityFakeLLMHandler:
    """Real LLMNotebookGenerator + real LLMMigrationValidator run against
    this -- only the Anthropic client is faked, so this test exercises the
    genuine generate() -> source_fidelity() wiring, not a mocked stand-in
    for it."""

    def __init__(self):
        self.claude_model = "fake-claude-for-tests"
        self._claude_client = _FidelityFakeClaudeClient(
            [_FIDELITY_NOTEBOOK_RESPONSE, _FIDELITY_VALIDATION_RESPONSE]
        )

    def is_available(self):
        return True


def test_run_migration_surfaces_a_real_source_fidelity_result(monkeypatch, tmp_path):
    """The centerpiece proof for: drive the real,
    public run_migration() entrypoint (not LLMNotebookGenerator.generate()
    directly) over a real XML file on disk, with only the Anthropic client
    faked, and confirm the fidelity gap comes back on both the per-mapping
    outcome and the aggregate result -- and gets written to a report a user
    would actually see, not just logged.
    """
    xml_path = tmp_path / "m_customers.xml"
    xml_path.write_text(_FIDELITY_XML_FIXTURE, encoding="utf-8")

    # The migrator selects its provider through handlers.make_llm_handler
    # now (this build defaults to OpenAI), so the factory is the seam to
    # stub -- patching the Anthropic class alone no longer reaches it.
    monkeypatch.setattr("infa2aidp.handlers.make_llm_handler",
                        lambda *a, **k: _FidelityFakeLLMHandler())

    result = run_migration(
        [str(xml_path)], str(tmp_path),
        use_llm=True, score_confidence=False, skip_lineage=True, skip_optimize=True,
    )

    assert result.notebooks == 1
    outcome = result.outcomes[0]
    assert outcome.path_used == "llm"

    # Per-mapping: the gap is visible on the outcome the caller gets back,
    # not just something that was logged and discarded.
    assert outcome.fidelity_checked is True
    assert outcome.fidelity_has_gaps is True
    assert "EXP_AUDIT_ONLY" in outcome.fidelity_summary

    # Aggregate: surfaced on the MigrationRunResult the same way
    # confidence_summary is, plus a report file a user would actually open.
    assert result.fidelity_summary
    assert result.fidelity_summary["checked"] == 1
    assert result.fidelity_summary["with_gaps"] == 1
    report_path = result.fidelity_summary["report_path"]
    assert os.path.isfile(report_path)
    report_text = open(report_path, encoding="utf-8").read()
    assert "EXP_AUDIT_ONLY" in report_text


# ---------------------------------------------------------------------------
# migrate reads back what it wrote
# ---------------------------------------------------------------------------

def test_a_broken_notebook_is_reported_not_delivered_silently(tmp_path):
    """A notebook that parses, satisfies every string assertion, and
    still raises NameError on its first cell.

    The dataflow bookkeeping can name a variable it never assigns. Nothing
    else in a run reports that: the summary counts notebooks produced and
    the confidence score describes conversions attempted, so a notebook
    that cannot run is indistinguishable from one that can.
    """
    from infa2aidp.migrator import _validate_generated
    import json

    out = tmp_path / "nb"
    (out / "F").mkdir(parents=True)
    bad = {"cells": [{"cell_type": "code", "source": [
        "from pyspark.sql import functions as F\n",
        "df_in_x = _rename_cols(df, [('A', 'B')])\n",   # df never assigned
    ]}], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    (out / "F" / "nb_bad.ipynb").write_text(json.dumps(bad))

    good = {"cells": [{"cell_type": "code", "source": [
        "from pyspark.sql import SparkSession, functions as F\n",
        "spark = SparkSession.builder.getOrCreate()\n",
        'df_source = spark.table("t")\n',
        "df_final = df_source\n",
    ]}], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    (out / "F" / "nb_good.ipynb").write_text(json.dumps(good))

    broken = _validate_generated(str(out))
    names = {os.path.basename(p) for p, _ in broken}
    assert names == {"nb_bad.ipynb"}, broken
    problems = broken[0][1]
    assert any("never assigned" in p and "df" in p for p in problems), problems


def test_a_notebook_that_does_not_parse_is_reported(tmp_path):
    from infa2aidp.migrator import _validate_generated
    import json
    out = tmp_path / "nb"
    out.mkdir()
    nb = {"cells": [{"cell_type": "code", "source": ["df = (\n"]}],
          "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    (out / "nb_x.ipynb").write_text(json.dumps(nb))
    broken = _validate_generated(str(out))
    assert len(broken) == 1
    assert "does not parse" in broken[0][1][0]


def test_the_corpus_notebooks_pass_their_own_check(tmp_path):
    """Proof the check is not vacuous: the shipped corpus must come out
    clean, or every run would report noise and get ignored."""
    from infa2aidp.migrator import _validate_generated, run_migration
    out = tmp_path / "out"
    import glob as _glob
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    inputs = sorted(_glob.glob(os.path.join(repo, "tests", "fixtures", "corpus", "*")))
    assert inputs, "corpus fixtures not found"
    run_migration(inputs, str(out),
                  use_llm=False, skip_lineage=True, skip_optimize=True,
                  score_confidence=False)
    assert _validate_generated(str(out)) == []


def test_two_mappings_with_the_same_folder_and_name_are_both_kept(tmp_path):
    """Silent overwrite was the worst defect the corpus exposed.

    The run reported one notebook per mapping and delivered fewer, with no
    mention of which were lost. Informatica allows the same mapping name in
    different folders, which the folder directory already handles; this is
    the same folder AND the same name, which means two exports disagree.
    """
    from infa2aidp.migrator import run_migration
    import shutil

    src = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "tests", "fixtures", "corpus", "orders_transform.xml")
    a = tmp_path / "a.xml"
    b = tmp_path / "b.xml"
    shutil.copy(src, a)
    shutil.copy(src, b)          # same mapping name, same folder, twice

    out = tmp_path / "out"
    result = run_migration([str(a), str(b)], str(out), use_llm=False,
                           skip_lineage=True, skip_optimize=True,
                           score_confidence=False)

    import glob as _glob
    files = _glob.glob(os.path.join(str(out), "**", "*.ipynb"), recursive=True)
    assert len(files) == result.notebooks, (
        f"reported {result.notebooks} notebooks, wrote {len(files)}"
    )
    assert result.notebook_collisions, "the clash was not reported"
    assert any("__2" in a for _m, _f, a in result.notebook_collisions)
