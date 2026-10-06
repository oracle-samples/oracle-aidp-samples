"""An export with no source/target wiring must degrade visibly.

This behaviour used to be covered by accident. Eight fixtures in the demo
corpus were flattened copies of complete mappings sitting in
``tests/fixtures/powercenter/``, so the whole corpus exercised the
no-metadata path and none of it exercised the read/write path. Restoring
the complete copies fixed the measurement and would have deleted this
coverage, so it is now carried deliberately by one fixture that is stripped
on purpose: ``flattened_no_instances.xml``.

What matters here is that the generator does not invent a table name. A
fabricated source or target produces a notebook that runs, reads the wrong
data, and reports nothing -- strictly worse than a notebook that refuses to
guess.
"""

import json
import pathlib

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.models import Session
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

FIXTURE = str(
    pathlib.Path(__file__).parent / "fixtures" / "powercenter"
    / "flattened_no_instances.xml"
)


def _notebook():
    """Generate exactly as the CLI does, so the test sees real output."""
    mapping = InformaticaXMLParser().parse(FIXTURE).mappings[0]
    session = Session(name=f"s_{mapping.name}")
    converter = TransformationConverter()
    conversion = {"transformations": {}, "source_reads": [], "target_write": ""}
    for tx in mapping.transformations:
        conversion["transformations"][tx.name] = "\n".join(converter.convert(tx))
    data = json.loads(
        NotebookGenerator().generate(mapping, session, conversion,
                                     output_format="ipynb")
    )
    return "\n".join(
        "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
        for c in data["cells"]
    )


class TestDegradesVisibly:
    def test_the_fixture_really_has_no_wiring(self):
        """Guards the fixture itself. If someone completes it, this fails
        before the behavioural tests below start passing vacuously."""
        raw = pathlib.Path(FIXTURE).read_text()
        assert "<SOURCE " not in raw
        assert "<TARGET " not in raw
        assert "<INSTANCE" not in raw

    def test_mapping_parses_with_no_sources_or_targets(self):
        mapping = InformaticaXMLParser().parse(FIXTURE).mappings[0]
        assert mapping.sources == []
        assert mapping.targets == []
        assert len(mapping.transformations) == 2

    def test_source_read_falls_back_to_a_named_placeholder(self):
        code = _notebook()
        assert "placeholder" in code.lower(), (
            "a missing source must produce an obvious placeholder, not a "
            "silently invented table"
        )

    def test_no_table_name_is_invented(self):
        """The mapping names no table anywhere. Any concrete spark.table()
        target here would be fabricated."""
        code = _notebook()
        import re
        reads = re.findall(r'spark\.table\(["\']([^"\']+)["\']\)', code)
        assert not reads, f"invented table name(s): {reads}"

    def test_transformation_logic_still_converts(self):
        """Missing wiring must not suppress the body. The expressions and
        the filter are present in the export and convert normally."""
        code = _notebook()
        assert "withColumn" in code
        assert "0.9" in code, "the NET_AMOUNT expression should convert"
        assert "upper" in code.lower(), "UPPER(STATUS) should convert"
        assert ".filter(" in code or ".where(" in code

    def test_the_gap_is_reported_not_hidden(self):
        code = _notebook()
        assert "REVIEW REQUIRED" in code or "TODO" in code or "No target" in code
