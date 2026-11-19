"""Informatica parameter-file (.par/.prm) parsing and the code/report it emits.

A parameter file decides which dates, paths and connections a session runs
with, so the pinned behaviour is: every key lands in the right scope,
``$PM``/``$$`` references resolve to the value *in that scope*, comments and
blank lines are ignored, and the generated Python is valid and carries the
values unchanged (Windows paths and quotes included).
"""
from __future__ import annotations

import pytest

from infa2aidp.parsers.parameter_parser import (
    ParameterFileParser,
    ParameterFileResult,
    ParameterScope,
)


PAR = r"""# nightly load
// also a comment
$PMRootDir=/infa/server
$PMSessionLogDir=$PMRootDir/SessLogs

[SALES.WF:wf_nightly.ST:s_m_orders]
$$LAST_EXTRACT_DATE=01/01/2024 00:00:00
$$DB_CONNECTION=ORACLE_PROD
$$SRC_DIR=$PMRootDir/src
$$SRC_FILE=$$SRC_DIR/orders.csv
$$FILTER=STATUS = 'A'
$$EMPTY=

[HR]
$$BATCH_ID=42
not a parameter line
"""


def _write(tmp_path, text, name="nightly.par"):
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return str(p)


@pytest.fixture
def parsed(tmp_path):
    return ParameterFileParser().parse(_write(tmp_path, PAR))


class TestParse:
    def test_global_parameters_and_references(self, parsed):
        assert parsed.global_params == {
            "$PMRootDir": "/infa/server",
            "$PMSessionLogDir": "/infa/server/SessLogs",
        }

    def test_scopes_folder_and_session_names(self, parsed):
        assert [s.scope for s in parsed.scopes] == ["SALES.WF:wf_nightly.ST:s_m_orders", "HR"]
        sales, hr = parsed.scopes
        assert (sales.folder, sales.session) == ("SALES", "s_m_orders")
        assert (hr.folder, hr.session) == ("HR", "")

    def test_scoped_values_resolve_chains_and_keep_equals_signs(self, parsed):
        sales = parsed.scopes[0].parameters
        assert sales["$$SRC_DIR"] == "/infa/server/src"
        assert sales["$$SRC_FILE"] == "/infa/server/src/orders.csv"
        assert sales["$$FILTER"] == "STATUS = 'A'"
        assert sales["$$EMPTY"] == ""
        assert sales["$$LAST_EXTRACT_DATE"] == "01/01/2024 00:00:00"
        assert parsed.scopes[1].parameters == {"$$BATCH_ID": "42"}

    def test_flattened_view_contains_everything(self, parsed):
        assert parsed.all_params["$$SRC_FILE"] == "/infa/server/src/orders.csv"
        assert "$$BATCH_ID" in parsed.all_params
        assert len(parsed.all_params) == 9

    def test_unknown_reference_is_left_verbatim(self, tmp_path):
        r = ParameterFileParser().parse(_write(tmp_path, "$$A=$$NOT_DEFINED/x\n"))
        assert r.all_params["$$A"] == "$$NOT_DEFINED/x"

    def test_self_reference_does_not_loop(self, tmp_path):
        r = ParameterFileParser().parse(_write(tmp_path, "$$A=$$A-suffix\n"))
        assert r.all_params["$$A"] == "$$A-suffix"

    def test_missing_file_is_an_empty_result(self, tmp_path):
        r = ParameterFileParser().parse(str(tmp_path / "nope.par"))
        assert (r.scopes, r.global_params, r.all_params) == ([], {}, {})

    def test_directory_parses_only_par_files_in_name_order(self, tmp_path):
        _write(tmp_path, "$$A=1\n", "b.par")
        _write(tmp_path, "$$B=2\n", "a.PAR")
        _write(tmp_path, "$$C=3\n", "c.txt")
        results = ParameterFileParser().parse_directory(str(tmp_path))
        assert [r.all_params for r in results] == [{"$$B": "2"}, {"$$A": "1"}]
        assert ParameterFileParser().parse_directory(str(tmp_path / "missing")) == []

    def test_references_resolve_within_their_own_scope(self, tmp_path):
        text = (
            "[F.s_one]\n$$DIR=/one\n$$FILE=$$DIR/a.csv\n"
            "[F.s_two]\n$$DIR=/two\n$$FILE=$$DIR/b.csv\n"
        )
        r = ParameterFileParser().parse(_write(tmp_path, text))
        one, two = (s.parameters for s in r.scopes)
        assert two["$$FILE"] == "/two/b.csv"
        assert one["$$FILE"] == "/one/a.csv"


class TestCodeGeneration:
    def test_spark_conf_is_valid_python_and_round_trips_values(self, tmp_path):
        text = "$PMRootDir=C:\\infa\\new\n[F.s]\n$$Q=say \"hi\"\n"
        r = ParameterFileParser().parse(_write(tmp_path, text))
        code = ParameterFileParser().to_spark_conf(r)
        assert "# Source: nightly.par" in code
        assert "# Global parameters" in code and "# Scope: F.s" in code

        class Conf:
            def __init__(self):
                self.values = {}

            def set(self, k, v):
                self.values[k] = v

        spark = type("S", (), {"conf": Conf()})()
        exec(compile(code, "<conf>", "exec"), {"spark": spark})  # noqa: S102
        assert spark.conf.values == {
            "migration.pmroot_dir": "C:\\infa\\new",
            "migration.q": 'say "hi"',
        }

    def test_widgets_define_and_read_every_parameter(self, parsed):
        code = ParameterFileParser().to_notebook_widgets(parsed)
        created, read = {}, []

        class Widgets:
            @staticmethod
            def text(name, default, label):
                created[name] = (default, label)

            @staticmethod
            def get(name):
                read.append(name)
                return created[name][0]

        ns = {"dbutils": type("D", (), {"widgets": Widgets})}
        exec(compile(code, "<widgets>", "exec"), ns)  # noqa: S102
        assert created["last_extract_date"] == ("01/01/2024 00:00:00", "$$LAST_EXTRACT_DATE")
        assert created["pmrootdir"] == ("/infa/server", "$PMRootDir")
        assert ns["SRC_FILE"] == "/infa/server/src/orders.csv"
        assert len(read) == len(parsed.all_params)

    def test_widget_defaults_survive_backslashes_and_quotes(self, tmp_path):
        r = ParameterFileParser().parse(_write(tmp_path, '$$DIR=C:\\temp\\new\n$$Q=a "b"\n'))
        code = ParameterFileParser().to_notebook_widgets(r)
        created = {}

        class Widgets:
            @staticmethod
            def text(name, default, label):
                created[name] = default

            @staticmethod
            def get(name):
                return created[name]

        try:
            exec(compile(code, "<widgets>", "exec"),  # noqa: S102
                 {"dbutils": type("D", (), {"widgets": Widgets})})
        except SyntaxError as exc:
            raise AssertionError(f"generated widget code does not compile: {exc}") from exc
        assert created == {"dir": "C:\\temp\\new", "q": 'a "b"'}

    def test_env_mapping_classifies_parameters(self, parsed):
        m = ParameterFileParser().to_env_mapping(parsed)
        assert m["$$LAST_EXTRACT_DATE"] == {
            "original_value": "01/01/2024 00:00:00",
            "aidp_equivalent": "migration.last_extract_date",
            "aidp_method": "spark.conf.get()",
            "description": "Incremental extraction date — used for change data capture",
        }
        assert m["$PMRootDir"]["aidp_method"] == "environment/config"
        assert "secret scope" in m["$$DB_CONNECTION"]["description"]
        assert m["$$BATCH_ID"]["description"] == "ETL batch identifier"
        assert "workspace path" in m["$PMRootDir"]["description"]
        assert "Object Storage" in m["$$SRC_FILE"]["description"]
        assert m["$$FILTER"]["description"] == "Application-specific parameter"

    @pytest.mark.parametrize("key,expected", [
        ("$$LAST_EXTRACT_DATE", "migration.last_extract_date"),
        ("$$lastRunDate", "migration.last_run_date"),
        ("$PMSessionLogDir", "migration.pmsession_log_dir"),
    ])
    def test_conf_key(self, key, expected):
        assert ParameterFileParser._to_conf_key(key) == expected

    def test_conf_key_matches_its_documented_example(self):
        # The docstring now documents what the key has always been.
        assert ParameterFileParser._to_conf_key("$PMRootDir") == "migration.pmroot_dir"
        assert "migration.pmroot_dir" in ParameterFileParser._to_conf_key.__doc__


class TestReport:
    def test_report_lists_files_scopes_and_equivalents(self, tmp_path, parsed):
        empty = ParameterFileResult(file_path="empty.par",
                                    scopes=[ParameterScope(scope="X")])
        out = tmp_path / "reports" / "params.md"
        ParameterFileParser().generate_report([parsed, empty], str(out))
        md = out.read_text(encoding="utf-8")
        assert "**Files parsed:** 2" in md
        assert f"**Total parameters:** {len(parsed.all_params)}" in md
        assert "## nightly.par" in md and "## empty.par" in md
        assert "### Global Parameters" in md
        assert "| `$PMRootDir` | `/infa/server` | `spark.conf.get(\"migration.pmroot_dir\")` |" in md
        assert "### Scope: SALES.WF:wf_nightly.ST:s_m_orders" in md
        assert "### Scope: X" not in md          # empty scopes are omitted
