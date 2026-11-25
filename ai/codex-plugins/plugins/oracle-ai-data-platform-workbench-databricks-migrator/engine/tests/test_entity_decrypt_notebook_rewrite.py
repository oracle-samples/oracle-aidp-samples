"""Regression tests for notebook-scoped entity-crypto entity crypto conversion."""

from scripts import job_migrate as jm


def _cell(source):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.splitlines(keepends=True),
    }


def test_python_entity_decrypt_gets_scala_setup_and_input_wrapper(monkeypatch):
    monkeypatch.setattr(
        jm,
        "_ENTITY_CRYPTO_ARN",
        "arn:aws:sts::000000000000:assumed-role/test/session",
    )
    python_source = '''exp_scrub = exp_scrub.withColumn(
    "CUSTOMER_ID",
    F.expr("entity_decrypt(CUSTOMER_ID,'NFS','COL','')"),
)
'''

    cells = jm._ensure_entity_crypto_setup_cells([_cell(python_source)])

    assert len(cells) == 3
    registrar = "".join(cells[0]["source"])
    arn_setup = "".join(cells[1]["source"])
    converted = "".join(cells[2]["source"])
    assert "import com.example.dp.UdfRegistrar" in registrar
    assert "UdfRegistrar.registerUdf(spark)" in registrar
    assert 'spark.udf.register("ensure_arn"' in arn_setup
    assert "entity_decrypt(ensure_arn(CUSTOMER_ID),'NFS','COL','')" in converted


def test_entity_crypto_notebook_rewrite_is_idempotent(monkeypatch):
    monkeypatch.setattr(
        jm,
        "_ENTITY_CRYPTO_ARN",
        "arn:aws:sts::000000000000:assumed-role/test/session",
    )
    source = (
        'exp_scrub = exp_scrub.withColumn("CUSTOMER_ID", '
        'F.expr("entity_decrypt(CUSTOMER_ID,\'NFS\',\'COL\',\'\')"))\n'
    )

    converted = jm._ensure_entity_crypto_setup_cells([_cell(source)])

    assert jm._ensure_entity_crypto_setup_cells(converted) == converted


def test_existing_crypto_setup_is_reused_without_environment_arn(monkeypatch):
    monkeypatch.setattr(jm, "_ENTITY_CRYPTO_ARN", "")
    registrar = _cell(
        "%scala\nimport com.example.dp.UdfRegistrar\n"
        "UdfRegistrar.registerUdf(spark)\n"
    )
    arn_setup = _cell(
        '%scala\nSystem.setProperty("ENTITY_CRYPTO_ARN", arn)\n'
        'spark.udf.register("ensure_arn", udf((data: String) => data))\n'
    )
    crypto = _cell(
        'exp_scrub = exp_scrub.withColumn("CUSTOMER_ID", '
        'F.expr("entity_decrypt(CUSTOMER_ID,\'NFS\',\'COL\',\'\')"))\n'
    )

    converted = jm._ensure_entity_crypto_setup_cells(
        [registrar, arn_setup, crypto]
    )

    assert len(converted) == 3
    assert "entity_decrypt(ensure_arn(CUSTOMER_ID)" in "".join(
        converted[2]["source"]
    )


def test_python_cell_rewriter_wraps_entity_crypto_with_configured_arn(monkeypatch):
    monkeypatch.setattr(
        jm,
        "_ENTITY_CRYPTO_ARN",
        "arn:aws:sts::000000000000:assumed-role/test/session",
    )
    source = (
        'F.expr("entity_decrypt(INVOICE_ID,\'NFS\',\'COL\',\'\')")\n'
    )

    converted = jm._apply_field_learned_swaps(source)

    assert "entity_decrypt(ensure_arn(INVOICE_ID),'NFS','COL','')" in converted


def test_scala_entity_crypto_is_self_contained_without_notebook_setup(monkeypatch):
    monkeypatch.setattr(
        jm,
        "_ENTITY_CRYPTO_ARN",
        "arn:aws:sts::000000000000:assumed-role/test/session",
    )
    source = "%scala\nval decoded = entity_decrypt(CUSTOMER_ID, \"NFS\", \"COL\", \"\")\n"

    prepared = jm._ensure_entity_crypto_setup_cells([_cell(source)])

    assert len(prepared) == 1
    assert "ensure_arn(CUSTOMER_ID)" not in "".join(prepared[0]["source"])

    converted = jm._apply_field_learned_swaps("".join(prepared[0]["source"]))

    assert converted.count('spark.udf.register("ensure_arn"') == 1
    assert "UdfRegistrar.registerUdf(spark)" in converted
    assert "entity_decrypt(ensure_arn(CUSTOMER_ID)" in converted


def test_child_python_crypto_cells_receive_notebook_setup(monkeypatch):
    monkeypatch.setattr(
        jm,
        "_ENTITY_CRYPTO_ARN",
        "arn:aws:sts::000000000000:assumed-role/test/session",
    )
    child_cells = [
        _cell('F.expr("entity_decrypt(CUSTOMER_ID,\'NFS\',\'COL\',\'\')")\n')
    ]

    prepared = jm._ensure_entity_crypto_setup_cells(child_cells)

    assert len(prepared) == 3
    assert "UdfRegistrar.registerUdf(spark)" in "".join(prepared[0]["source"])
    assert "entity_decrypt(ensure_arn(CUSTOMER_ID)" in "".join(prepared[2]["source"])
