"""Generated notebooks pin the SQL semantics they were written against.

AIDP runs Spark 3.5 today and moves to Spark 4. The two releases disagree
on what a failed cast, a divide by zero and a numeric overflow do: 3.5
returns NULL, 4 raises, because ANSI mode flips from off to on by default.

Informatica is permissive — NULL or the port default, plus a row error,
never an aborted run — so migrated logic was written against the 3.5
behaviour. A notebook that inherits the cluster default therefore changes
behaviour the day the runtime is upgraded, with nothing in the notebook to
explain it: rows that were NULL become a failed job, or reconciliation
totals move.

These tests pin the settings so that upgrade is a non-event. They are
about *reproducibility across the upgrade* as much as about which value is
correct.
"""

import json
import pathlib

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.models import Session
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

FIXTURE = str(
    pathlib.Path(__file__).parent / "fixtures" / "powercenter" / "order_to_cash.xml"
)


def _notebook_code():
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


class TestAnsiIsPinnedOff:
    def test_ansi_mode_is_explicitly_disabled(self):
        code = _notebook_code()
        assert 'spark.conf.set("spark.sql.ansi.enabled", "false")' in code, (
            "ANSI must be pinned, not inherited -- Spark 4 defaults it on and "
            "would silently turn NULL-producing rows into an aborted job"
        )

    def test_store_assignment_policy_is_pinned(self):
        """Spark 4 defaults this to ANSI, which raises when a value does not
        fit the target column. Informatica truncates or nulls."""
        code = _notebook_code()
        assert 'spark.conf.set("spark.sql.storeAssignmentPolicy", "LEGACY")' in code

    def test_the_setting_is_explained_not_just_set(self):
        """A bare conf.set reads as arbitrary and gets removed by the next
        person tidying up. The reason has to travel with it."""
        code = _notebook_code()
        assert "Informatica" in code
        assert "ANSI" in code

    def test_both_settings_precede_any_dataframe_work(self):
        """Set after a read, the conf change may not apply to work already
        planned."""
        code = _notebook_code()
        ansi = code.index('spark.sql.ansi.enabled')
        for later in ("spark.table(", "spark.read", "df_source"):
            if later in code:
                assert ansi < code.index(later), (
                    f"ANSI is pinned after {later}, which is too late"
                )
