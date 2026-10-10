"""The generated-code gate refuses credential literals (SEC-AIDP-SAMPLES-002).

The generators write every credential as a runtime lookup --
``password=os.environ["ADW_PASSWORD"]`` -- but a notebook is a file that is
reviewed, committed and deployed, and nothing in the read-back validation
would have noticed a literal in that place: it parses, every name
resolves, the write succeeds. ``hardcoded_credentials`` is the rule;
``_validate_generated`` applies it to every notebook a run wrote;
``run_migration`` splits the hits into ``credential_leaks`` and ``migrate``
exits non-zero on them.

Three kinds of coverage:
1. Unit tests against ``hardcoded_credentials()`` -- each credential shape
   the gate must catch (including the "make it run" edits a first version
   missed: ``os.environ["ADW_PASSWORD"] = "..."``, tuple unpacking,
   camelCase and ``PASSWORD_PROD`` names, concatenated literals,
   ``.option(key=, value=)``), each legitimate shape it must leave alone
   (SQL on a column called TOKEN, a port rename, names ABOUT a credential),
   and the rule that a finding names the line and the shape but never the
   value. ``credential_literals_in_text()`` is the tree-less form used for
   markdown and for code that does not parse.
2. The gate: ``_validate_generated`` lists the notebook (even a declared
   REVIEW REQUIRED refusal, even one that does not parse, even when the
   secret is in a markdown cell), the summary says SECURITY, ``migrate``
   exits 1 and ``broken_notebooks.md`` carries the finding without the
   secret -- and a SQL override on a TOKEN column does NOT fail the run.
3. The corpus, for both target catalog families, comes out clean -- the
   rule must not cry wolf on the generators' own ADW credential cells.

Usernames and hostnames are deliberately not a failing rule (configuration,
not secrets); that scope decision is pinned here so it is visible.
"""
from __future__ import annotations

import glob
import json
import os

import pytest

from infa2aidp.cli import main
from infa2aidp.generators.code_validation import (
    credential_literals_in_text,
    hardcoded_credentials,
    is_secret_name,
)
from infa2aidp.migrator import (
    CREDENTIAL_PROBLEM,
    MigrationRunResult,
    _validate_generated,
    format_run_summary,
    run_migration,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS = sorted(glob.glob(os.path.join(ROOT, "tests", "fixtures", "corpus", "*")))
ORDERS = os.path.join(ROOT, "tests", "fixtures", "corpus", "orders_transform.xml")

VALUE = "tiger-hunter2-9f8e7d"   # the literal that must never be echoed


# ── 1. the rule ───────────────────────────────────────────────────────

@pytest.mark.parametrize("code", [
    f'password = "{VALUE}"',
    f'ADW_PASSWORD = "{VALUE}"',
    f'cfg.password = "{VALUE}"',
    f'pwd: str = "{VALUE}"',
    f'df.write.format("jdbc").option("password", "{VALUE}").save()',
    f'spark.conf.set("spark.sql.catalog.adw.password", "{VALUE}")',
    f'os.environ.setdefault("ADW_PASSWORD", "{VALUE}")',
    f'conn = dict(user="scott", password="{VALUE}")',
    f'connect(user="scott", wallet_password="{VALUE}")',
    f'props = {{"user": "scott", "password": "{VALUE}"}}',
    f'token = "{VALUE}"',
    f'client_secret = "{VALUE}"',
    f'api_key = "{VALUE}"',
    f'url = "jdbc:postgresql://scott:{VALUE}@db:5432/sales"',
    f'url = "jdbc:oracle:thin:scott/{VALUE}@db:1521/svc"',
    f'url = "jdbc:sqlserver://db;user=scott;password={VALUE}"',
    f'url = "jdbc:mysql://db/x?user=scott&pwd={VALUE}"',
    f'u = "https://api.example.com/data?api_key={VALUE}"',
    f'u = "https://api.example.com/data?access_token={VALUE}"',
    f'headers = {{"Authorization": "Bearer {VALUE}"}}',
    f'headers = {{"authorization": "Basic c2NvdHQ6{VALUE}"}}',
    f'req.add_header("Authorization", "Bearer {VALUE}")',
    f'h = "Authorization: Bearer {VALUE}"',
    f'h = "Authorization=Basic c2NvdHQ6{VALUE}"',
    # the shapes a "make it run" edit actually takes (review of the first
    # version: each of these slipped past)
    f'os.environ["ADW_PASSWORD"] = "{VALUE}"',      # the generators read exactly this variable
    f'props["password"] = "{VALUE}"',
    f'user, password = "scott", "{VALUE}"',
    f'(user, pwd) = ("scott", "{VALUE}")',
    f'dbPassword = "{VALUE}"',
    f'oauthToken = "{VALUE}"',
    f'PASSWORD_PROD = "{VALUE}"',
    f'aws_secret_access_key = "{VALUE}"',
    f'password = "tig" + "er-hunter2"',
    f'password += "{VALUE}"',
    f'password = b"{VALUE}"',
    f'df.write.option(key="password", value="{VALUE}")',
    f'df.write.option("password", value="{VALUE}")',
    f'spark.conf.set(key="spark.sql.catalog.adw.password", value="{VALUE}")',
    f'os.environ.update(ADW_PASSWORD="{VALUE}")',
    f'session.headers["Authorization"] = "Bearer {VALUE}"',
    f'url = "jdbc:postgresql://scott:tig" + "er@db/x"',
    f'conn = "Data Source=db;User Id=scott;Password={VALUE}"',
])
def test_a_credential_literal_is_caught(code):
    findings = hardcoded_credentials(code)
    assert findings, code
    assert all(f.startswith("line 1:") for f in findings), findings
    assert VALUE not in " ".join(findings), "the finding must not echo the secret"
    assert "er-hunter2" not in " ".join(findings)


@pytest.mark.parametrize("code", [
    # what the generators actually emit
    'conn = connect(user=os.environ["ADW_USER"], password=os.environ["ADW_PASSWORD"])',
    'connect(wallet_password=os.environ.get("ADW_WALLET_PASSWORD", os.environ["ADW_PASSWORD"]))',
    'props = {"user": os.environ["ADW_USER"], "password": os.environ["ADW_PASSWORD"]}',
    'url = f"jdbc:oracle:thin:@{os.environ[\'ADW_TNS_SERVICE\']}"',
    'url = f"jdbc:sqlserver://db;user={u};password={os.environ[\'P\']}"',
    'df.write.format("jdbc").option("password", dbutils.secrets.get("scope", "adw-pw")).save()',
    'spark.conf.set("spark.sql.catalog.adw.password", os.environ["ADW_PASSWORD"])',
    # names that hold a name or an address, not a credential
    'password_env = "ADW_PASSWORD"',
    'token_url = "https://idcs.example.com/oauth2/v1/token"',
    'secret_name = "adw-password"',
    'password_file = "/run/secrets/adw"',
    # empty and placeholder values
    'password = ""',
    'password = None',
    'msg = "set password=<your password> in the environment"',
    'tmpl = "password=${ADW_PASSWORD}"',
    'tmpl = "user=%s;password=%s"',
    # URLs without credentials
    '.option("url", "jdbc:oracle:thin:@db:1521/svc")',
    'u = "https://objectstorage.us-ashburn-1.oraclecloud.com/n/ns/b/bucket/o/x"',
    'header_name = "Authorization"',
    # ordinary generated code
    'df = spark.table("cat.sch.tbl").filter(F.col("TOKEN_TYPE") == "X")',
    'df_final = df.withColumn("PASSWORD_HASH", F.sha2(F.col("PWD"), 256))',
    'spark.sql("SELECT PASSWORD_HASH FROM users WHERE token_id = 1")',
    # SQL overrides and filters on a column that happens to be called TOKEN
    # or PWD -- the first version failed a whole migration on these
    "df_source = df_source.filter(F.expr('TOKEN = 1'))",
    'spark.sql("SELECT * FROM t WHERE PWD = 0")',
    'spark.sql("UPDATE t SET token=NULL WHERE id=1")',
    'spark.sql("UPDATE t SET a=1; UPDATE t SET token=NULL")',
    'df.filter("pwd = 0")',
    # a port named PASSWORD/TOKEN renamed by the generator is a column
    'df = df.withColumnRenamed("PASSWORD", "PWD")',
    'df = df.withColumnRenamed("TOKEN", "AUTH_TOKEN")',
    # prose and names ABOUT a credential
    '"""Credentials: pass password=os.environ[ADW_PASSWORD] at runtime."""',
    'help_text = "Set token=<value> or api_key=YOURKEY in .env"',
    'secret_ocid = "ocid1.vaultsecret.oc1..x"',
    'password_hash = "5f4dcc3b"',
    'TOKEN_TYPE = "bearer"',
    'pwd_column = "PWD"',
    'os.environ["ADW_USER"] = "scott"',
])
def test_runtime_lookups_and_look_alikes_are_not_flagged(code):
    code = code.lstrip(".")
    if code.startswith("option("):
        code = "df.write." + code
    assert hardcoded_credentials(code) == [], code


@pytest.mark.parametrize("code", ['username = "scott"', 'host = "adw.prod.oraclecloud.com"',
                                  'connect(user="scott", password=os.environ["ADW_PASSWORD"])'])
def test_usernames_and_hosts_are_configuration_not_secrets(code):
    """A deliberate scope decision, recorded here and in the CHANGELOG: the
    gate fails a run on credential literals only. A username or hostname
    is configuration -- failing a migration on `user="ADMIN"` would make
    the gate cry wolf, and a gate that cries wolf gets switched off."""
    assert hardcoded_credentials(code) == []


@pytest.mark.parametrize("name, secret", [
    ("password", True), ("ADW_PASSWORD", True), ("dbPassword", True), ("PASSWORD_PROD", True),
    ("spark.sql.catalog.adw.password", True), ("api_key", True), ("apiKey", True),
    ("client_secret", True), ("secret_key", True), ("access_token", True), ("passphrase", True),
    ("password_env", False), ("token_url", False), ("secret_name", False), ("password_file", False),
    ("PASSWORD_HASH", False), ("TOKEN_TYPE", False), ("api_key_env", False), ("secret_ocid", False),
    ("token_count", False), ("tokenizer", False), ("header_name", False), ("user", False),
])
def test_is_secret_name_reads_segments_not_suffixes(name, secret):
    assert is_secret_name(name) is secret, name


# ── 1b. the text rule, for markdown and code that does not parse ─────

@pytest.mark.parametrize("text", [
    f'password = "{VALUE}"\ndef broken(:\n',
    f"Connect with password={VALUE}",
    f'"password": "{VALUE}"',
    f'os.environ["ADW_PASSWORD"] = "{VALUE}"',
    f"export ADW_PASSWORD={VALUE}",
    f'url = "jdbc:postgresql://scott:{VALUE}@db/x"',
    f"Authorization: Bearer {VALUE}",
])
def test_text_rule_catches_a_credential_without_a_syntax_tree(text):
    findings = credential_literals_in_text(text)
    assert findings, text
    assert VALUE not in " ".join(findings)


@pytest.mark.parametrize("text", [
    "WHERE TOKEN = 1", "SET token=NULL",
    "the notebook reads password=os.environ['ADW_PASSWORD'] at runtime",
    "set ADW_PASSWORD=<your password> in the environment",
    "password = ${ADW_PASSWORD}",
    "| TOKEN_TYPE = 'bearer' |",
    "password_env = ADW_PASSWORD",
    "token_url: https://idcs.example.com/oauth2/v1/token",
    "# Credentials come from ADW_USER / ADW_PASSWORD",
])
def test_text_rule_leaves_prose_and_sql_alone(text):
    assert credential_literals_in_text(text) == [], text


def test_findings_name_each_line_once_and_sort_by_line():
    code = (
        'import os\n'
        f'password = "{VALUE}"\n'
        'user = os.environ["U"]\n'
        f'url = "jdbc:postgresql://scott:{VALUE}@db/x"\n'
        f'url = "jdbc:postgresql://scott:{VALUE}@db/x"\n'
    )
    findings = hardcoded_credentials(code)
    lines = [int(f.split(":")[0].split()[1]) for f in findings]
    assert lines == sorted(lines) and lines[0] == 2 and 4 in lines
    assert VALUE not in " ".join(findings)


# ── 2. the gate ───────────────────────────────────────────────────────

def _nb(*source: str) -> dict:
    return {"cells": [{"cell_type": "code", "source": [s + "\n" for s in source]}],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5}


CLEAN = (
    "import os",
    "from pyspark.sql import SparkSession, functions as F",
    "spark = SparkSession.builder.getOrCreate()",
    'df_source = spark.table("cat.sch.t")',
    'df_final = df_source.write.format("jdbc").option("password", os.environ["ADW_PASSWORD"])',
)
LEAKY = CLEAN[:-1] + (f'df_final = df_source.write.format("jdbc").option("password", "{VALUE}")',)


def test_validate_generated_reports_a_credential_literal_without_the_value(tmp_path):
    out = tmp_path / "nb"
    out.mkdir()
    (out / "nb_clean.ipynb").write_text(json.dumps(_nb(*CLEAN)))
    (out / "nb_leaky.ipynb").write_text(json.dumps(_nb(*LEAKY)))
    broken = _validate_generated(str(out))
    assert {os.path.basename(p) for p, _ in broken} == {"nb_leaky.ipynb"}, broken
    problems = broken[0][1]
    assert any(p.startswith(CREDENTIAL_PROBLEM) for p in problems), problems
    assert VALUE not in " ".join(problems)


def test_a_declared_refusal_is_still_reported_when_it_embeds_a_credential(tmp_path):
    """REVIEW REQUIRED stubs are exempt from the cannot-run rules, which is
    right -- but a stub that also carries a password is a leaked password."""
    out = tmp_path / "nb"
    out.mkdir()
    (out / "nb_stub.ipynb").write_text(json.dumps(_nb(
        f'password = "{VALUE}"',
        'raise NotImplementedError("REVIEW REQUIRED: unconnected lookup")',
    )))
    (out / "nb_stub_clean.ipynb").write_text(json.dumps(_nb(
        'raise NotImplementedError("REVIEW REQUIRED: unconnected lookup")',
        "df_final = df",            # unresolved, but a declared refusal: exempt
    )))
    names = {os.path.basename(p) for p, _ in _validate_generated(str(out))}
    assert names == {"nb_stub.ipynb"}


def test_summary_and_exit_code_fail_on_a_leak(tmp_path, monkeypatch, capsys):
    import infa2aidp.migrator as migrator

    leak = [(str(tmp_path / "nb_x.ipynb"), [f"{CREDENTIAL_PROBLEM}: line 5: password= is a string literal"])]

    def fake_run(*a, **k):
        return MigrationRunResult(output_dir=str(tmp_path), notebooks=1,
                                  broken_notebooks=leak, credential_leaks=leak)

    monkeypatch.setattr(migrator, "run_migration", fake_run)
    assert main(["migrate", "-i", ORDERS, "-o", str(tmp_path)]) == 1
    out = capsys.readouterr().out
    assert "SECURITY" in out and "credential literal" in out

    def clean_run(*a, **k):
        return MigrationRunResult(output_dir=str(tmp_path), notebooks=1)

    monkeypatch.setattr(migrator, "run_migration", clean_run)
    assert main(["migrate", "-i", ORDERS, "-o", str(tmp_path)]) == 0
    assert "SECURITY" not in capsys.readouterr().out


def test_a_notebook_that_does_not_parse_is_still_credential_scanned(tmp_path):
    """"does not parse" used to end the check: a password beside a typo
    shipped, and -- since the parse failure is the only problem the
    cannot-run rules see -- a declared refusal with both was dropped."""
    out = tmp_path / "nb"
    out.mkdir()
    (out / "nb_typo.ipynb").write_text(json.dumps(_nb(f'password = "{VALUE}"', "def broken(:")))
    (out / "nb_stub_typo.ipynb").write_text(json.dumps(_nb(
        f'password = "{VALUE}"', "def broken(:",
        'raise NotImplementedError("REVIEW REQUIRED: unconnected lookup")',
    )))
    broken = _validate_generated(str(out))
    assert {os.path.basename(p) for p, _ in broken} == {"nb_typo.ipynb", "nb_stub_typo.ipynb"}
    for _, problems in broken:
        assert any(p.startswith("does not parse") for p in problems)
        assert any(p.startswith(CREDENTIAL_PROBLEM) for p in problems), problems
        assert VALUE not in " ".join(problems)


def test_markdown_cells_are_credential_scanned_too(tmp_path):
    out = tmp_path / "nb"
    out.mkdir()
    nb = _nb("import os")
    nb["cells"].insert(0, {"cell_type": "markdown", "source": ["# Orders\n", f"Connect with password={VALUE}\n"]})
    (out / "nb_md.ipynb").write_text(json.dumps(nb))
    broken = _validate_generated(str(out))
    assert [os.path.basename(p) for p, _ in broken] == ["nb_md.ipynb"]
    problems = broken[0][1]
    assert any(p.startswith(CREDENTIAL_PROBLEM) and "markdown cell 1" in p for p in problems), problems
    assert VALUE not in " ".join(problems)


def test_a_sql_override_on_a_column_called_token_does_not_fail_migrate(tmp_path, capsys):
    """Through the real run: the first version read `WHERE TOKEN = 1` in a
    source-qualifier override as a connection string and exited 1 with a
    SECURITY line -- a gate that cries wolf on a column name."""
    src = tmp_path / "in" / "scd_type1.xml"
    src.parent.mkdir()
    xml = open(os.path.join(ROOT, "tests", "fixtures", "corpus", "scd_type1.xml"), encoding="utf-8").read()
    assert 'FROM CUSTOMERS_STAGE"' in xml
    src.write_text(xml.replace('FROM CUSTOMERS_STAGE"', 'FROM CUSTOMERS_STAGE WHERE TOKEN = 1 AND PWD = 0"'),
                   encoding="utf-8")
    out = tmp_path / "out"
    rc = main(["migrate", "-i", str(src), "-o", str(out), "--skip-lineage", "--skip-optimize"])
    assert rc == 0
    assert "SECURITY" not in capsys.readouterr().out
    assert not (out / "reports" / "broken_notebooks.md").exists()
    code = "\n".join(
        "".join(c["source"]) for p in glob.glob(str(out / "**" / "*.ipynb"), recursive=True)
        for c in json.load(open(p, encoding="utf-8"))["cells"] if c["cell_type"] == "code"
    )
    assert "TOKEN = 1" in code, "the override must have reached the notebook for this to prove anything"


def test_end_to_end_a_leaky_notebook_in_the_output_fails_migrate(tmp_path, capsys):
    """Through the real run: a notebook with a credential literal sitting in
    the output tree is read back, listed in broken_notebooks.md without its
    secret, counted as a leak, and the command exits 1."""
    out = tmp_path / "out"
    (out / "F").mkdir(parents=True)
    (out / "F" / "nb_leaky.ipynb").write_text(json.dumps(_nb(*LEAKY)))
    rc = main(["migrate", "-i", ORDERS, "-o", str(out), "--skip-lineage", "--skip-optimize"])
    assert rc == 1
    assert "SECURITY: 1 generated notebook(s) embed a credential literal" in capsys.readouterr().out
    report = (out / "reports" / "broken_notebooks.md").read_text()
    assert "nb_leaky.ipynb" in report and CREDENTIAL_PROBLEM in report
    assert VALUE not in report


# ── 3. the corpus is clean, for both target families ─────────────────

@pytest.mark.parametrize("target", ["delta", "adw"])
def test_the_corpus_has_no_credential_literals(tmp_path, target):
    """Proof the rule is not vacuous and does not cry wolf: the generators'
    own ADW cells read credentials from the environment, and every notebook
    the shipped corpus produces must pass."""
    assert CORPUS, "corpus fixtures not found"
    result = run_migration(CORPUS, str(tmp_path / target), use_llm=False, skip_lineage=True,
                           skip_optimize=True, score_confidence=False, target_catalog_type=target)
    assert result.notebooks > 0
    assert result.credential_leaks == []
    for path in glob.glob(os.path.join(str(tmp_path / target), "**", "*.ipynb"), recursive=True):
        nb = json.load(open(path, encoding="utf-8"))
        code = "\n".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code")
        assert hardcoded_credentials(code) == [], path
    assert "SECURITY" not in format_run_summary(result)
