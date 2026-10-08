"""Every transformation type on both platforms is recognised and named.

Recognition is not conversion. These tests assert the weaker but
load-bearing property: no transformation type from either platform can pass
through the tool anonymously. It either resolves to an enum member, or it
reaches the generator as UNKNOWN *carrying its original type string*, which
the unsupported handler then prints.

The failure this guards against is the silent one. Before this, a CDI
Hierarchy Processor resolved to UNKNOWN and generated the comment
"Unsupported transformation type: Unknown" -- true, useless, and
indistinguishable from any other unrecognised type. A migrator reading that
notebook could not tell what had been dropped.
"""

import pytest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.models import Transformation, TransformationType
from infa2aidp.parsers.xml_parser import _resolve_tx_type
from infa2aidp.transformation_types import resolve

# The 39 Cloud Data Integration transformation types.
CDI_TRANSFORMATIONS = [
    "Source", "Target", "Access Policy", "Aggregator", "B2B", "Chunking",
    "Cleanse", "Data Masking", "Data Services", "Deduplicate", "Expression",
    "Filter", "Hierarchy Builder", "Hierarchy Parser", "Hierarchy Processor",
    "Input", "Java", "Joiner", "Labeler", "Lookup", "Machine Learning",
    "Mapplet", "Normalizer", "Output", "Parse", "Python", "Rank", "Router",
    "Rule Specification", "Sequence", "Sorter", "SQL", "Structure Parser",
    "Transaction Control", "Union", "Vector Embedding", "Velocity",
    "Verifier", "Web Services",
]

# PowerCenter transformation types, including the spellings that differ from
# the canonical enum value (Lookup Procedure, Union Transformation, Sequence).
POWERCENTER_TRANSFORMATIONS = [
    "Source Definition", "Target Definition", "Source Qualifier",
    "Application Source Qualifier", "MQ Source Qualifier",
    "XML Source Qualifier", "Expression", "Filter", "Joiner",
    "Lookup Procedure", "Aggregator", "Router", "Sequence",
    "Update Strategy", "Stored Procedure", "Normalizer", "Rank", "Sorter",
    "Union Transformation", "Custom Transformation", "Java Transformation",
    "SQL Transformation", "HTTP Transformation", "Transaction Control",
    "XML Parser", "XML Generator", "External Procedure", "Unstructured Data",
    "Web Services Consumer", "Input Transformation", "Output Transformation",
    "Data Masking", "Mapplet",
]


@pytest.mark.parametrize("type_name", CDI_TRANSFORMATIONS)
def test_every_cdi_transformation_resolves(type_name):
    assert resolve(type_name) is not TransformationType.UNKNOWN, (
        f"CDI transformation {type_name!r} is not recognised"
    )


@pytest.mark.parametrize("type_name", POWERCENTER_TRANSFORMATIONS)
def test_every_powercenter_transformation_resolves(type_name):
    assert resolve(type_name) is not TransformationType.UNKNOWN, (
        f"PowerCenter transformation {type_name!r} is not recognised"
    )


def test_cdi_count_is_thirty_nine():
    """Pins the list itself. If a CDI type is added, this fails first."""
    assert len(CDI_TRANSFORMATIONS) == 39


class TestNoSubstringMisresolution:
    """The removed substring fallback resolved these to the wrong member.

    Each pair below is a real collision in the expanded enum: the first
    string is a substring of the second type's name. Under the old
    ``if k in key or key in k`` fallback these silently produced confident,
    wrong code.
    """

    @pytest.mark.parametrize(
        "type_name,must_not_be",
        [
            ("Parse", TransformationType.XML_PARSER),
            ("Input", TransformationType.UNSTRUCTURED_DATA),
            ("Union", TransformationType.UNKNOWN),
            ("Java", TransformationType.UNKNOWN),
            ("Source", TransformationType.SOURCE_QUALIFIER),
        ],
    )
    def test_resolves_to_its_own_type(self, type_name, must_not_be):
        assert resolve(type_name) is not must_not_be

    def test_parse_is_the_cdq_parse_not_xml_parser(self):
        assert resolve("Parse") is TransformationType.PARSE

    def test_source_is_not_source_qualifier(self):
        """Distinct concepts: Source is the definition, Source Qualifier the
        PowerCenter read operator. Conflating them loses the SQ's SQL
        override."""
        assert resolve("Source") is TransformationType.SOURCE
        assert resolve("Source Qualifier") is TransformationType.SOURCE_QUALIFIER


class TestNormalisation:
    """Spacing, case, underscores and hyphens vary between exports."""

    @pytest.mark.parametrize(
        "variant",
        ["Hierarchy Processor", "hierarchy_processor", "HIERARCHY-PROCESSOR",
         "hierarchyprocessor", "  Hierarchy Processor  "],
    )
    def test_variants_resolve_the_same(self, variant):
        assert resolve(variant) is TransformationType.HIERARCHY_PROCESSOR

    def test_empty_and_none_are_unknown(self):
        assert resolve("") is TransformationType.UNKNOWN
        assert resolve(None) is TransformationType.UNKNOWN

    def test_genuinely_unknown_stays_unknown(self):
        """No fuzzy matching: an invented type must not be guessed at."""
        assert resolve("Quantum Flux Capacitor") is TransformationType.UNKNOWN


class TestRawTypeIsReported:
    """An unrecognised transformation must be named, not called 'Unknown'."""

    def test_unsupported_handler_prints_the_raw_type(self):
        tx = Transformation(
            name="hp_flatten_orders",
            type=TransformationType.UNKNOWN,
            raw_type="HIERARCHY_PROCESSOR_V2",
        )
        lines = TransformationConverter().convert(tx, "df_in", "df_out")
        body = "\n".join(lines)
        assert "HIERARCHY_PROCESSOR_V2" in body, (
            "the export's own type string must appear in the notebook"
        )
        assert "hp_flatten_orders" in body

    def test_unsupported_handler_says_logic_is_not_applied(self):
        """The pass-through is the dangerous part: rows keep flowing as if
        nothing were missing. That has to be stated, not implied."""
        tx = Transformation(name="t1", type=TransformationType.UNKNOWN,
                            raw_type="Verifier")
        body = "\n".join(TransformationConverter().convert(tx, "df_in", "df_out"))
        assert "NOT applied" in body
        assert "df_out = df_in" in body

    def test_recognised_but_unconverted_type_still_names_itself(self):
        tx = Transformation(name="t1", type=TransformationType.VERIFIER,
                            raw_type="Verifier")
        body = "\n".join(TransformationConverter().convert(tx, "df_in", "df_out"))
        assert "Verifier" in body

    def test_marker_is_emitted_so_the_metric_counts_it(self):
        """A dropped transformation must not read as a clean conversion --
        the conversion metric keys off these markers."""
        tx = Transformation(name="t1", type=TransformationType.UNKNOWN,
                            raw_type="Cleanse")
        body = "\n".join(TransformationConverter().convert(tx, "df_in", "df_out"))
        assert "TODO" in body or "REVIEW REQUIRED" in body


class TestParsersCaptureRawType:
    def test_xml_parser_resolver_is_the_shared_one(self):
        assert _resolve_tx_type("Lookup Procedure") is TransformationType.LOOKUP
        assert _resolve_tx_type("Hierarchy Processor") is (
            TransformationType.HIERARCHY_PROCESSOR
        )

    def test_transformation_defaults_raw_type_to_empty(self):
        assert Transformation(name="t").raw_type == ""


class TestPhase2Converters:
    """Input/Output, Deduplicate and Transaction Control now convert."""

    def _body(self, tx):
        return "\n".join(TransformationConverter().convert(tx, "df_in", "df_out"))

    def test_mapplet_input_passes_through_without_a_todo(self):
        tx = Transformation(name="mplt_in", type=TransformationType.INPUT)
        body = self._body(tx)
        assert "df_out = df_in" in body
        assert "TODO" not in body, "a mapplet port is not a manual-conversion gap"

    def test_mapplet_port_applies_a_renamed_port(self):
        """A renamed port changes the column name crossing the mapplet
        boundary; dropping the rename loses the column downstream."""
        from infa2aidp.models import TransformationField
        fld = TransformationField(name="CUST_KEY")
        fld.source_field = "CUSTOMER_ID"
        tx = Transformation(name="mplt_out", type=TransformationType.OUTPUT,
                            fields=[fld])
        body = self._body(tx)
        assert 'withColumnRenamed("CUSTOMER_ID", "CUST_KEY")' in body

    def test_deduplicate_uses_the_key_set(self):
        tx = Transformation(name="dd1", type=TransformationType.DEDUPLICATE,
                            group_by_fields=["CUSTOMER_ID", "ORDER_DT"])
        body = self._body(tx)
        assert 'dropDuplicates(["CUSTOMER_ID", "ORDER_DT"])' in body

    def test_deduplicate_flags_the_nondeterministic_survivor(self):
        """Informatica keeps the first row in input order; Spark keeps an
        arbitrary one. That is a real behavioural difference."""
        tx = Transformation(name="dd1", type=TransformationType.DEDUPLICATE,
                            group_by_fields=["ID"])
        assert "REVIEW REQUIRED" in self._body(tx)

    def test_deduplicate_without_keys_says_so(self):
        tx = Transformation(name="dd1", type=TransformationType.DEDUPLICATE)
        body = self._body(tx)
        assert "dropDuplicates()" in body
        assert "REVIEW REQUIRED" in body

    def test_transaction_control_names_the_lost_contract(self):
        """Rows of a rolled-back transaction are removed; what cannot be
        kept -- mid-run visibility -- is stated. With no condition every row
        continues the transaction."""
        tx = Transformation(name="tc1",
                            type=TransformationType.TRANSACTION_CONTROL)
        assert "df_out = df_in" in self._body(tx)
        tx.properties["tc_expression"] = "IIF(BAD = 1, TC_ROLLBACK_AFTER, TC_CONTINUE_TRANSACTION)"
        body = self._body(tx)
        assert "commits mid-stream" in body
        assert 'how="left_anti"' in body and "F.lit(4)" in body


class TestPhase3Hierarchy:
    """Hierarchy family + Structure Parser.

    No real IDMC export has been parsed, so these converters are written
    against plausible property spellings. The tests therefore pin two
    things: the Spark that IS emitted when metadata is present, and -- more
    important -- that absent metadata produces a named review item rather
    than an invented schema.
    """

    def _body(self, tx, extra=None):
        return "\n".join(
            TransformationConverter().convert(tx, "df_in", "df_out", extra or {})
        )

    def _field(self, name, path, direction=None):
        from infa2aidp.models import DataFlowDirection, TransformationField
        f = TransformationField(name=name)
        f.expression = path
        f.direction = direction or DataFlowDirection.OUTPUT
        return f

    # -- Hierarchy Parser

    def test_parser_uses_explode_outer_not_explode(self):
        """Gotcha #15. Informatica emits a row for a parent whose child
        array is empty; explode drops it, explode_outer keeps it. Silent
        row loss otherwise."""
        tx = Transformation(
            name="hp1", type=TransformationType.HIERARCHY_PARSER,
            fields=[self._field("sku", "order.lines[].sku")],
        )
        tx.properties["input field"] = "payload"
        body = self._body(tx)
        assert "explode_outer" in body
        assert "F.explode(" not in body, "plain explode silently drops rows"

    def test_parser_without_metadata_reports_rather_than_guesses(self):
        tx = Transformation(name="hp1", type=TransformationType.HIERARCHY_PARSER)
        body = self._body(tx)
        assert "REVIEW REQUIRED" in body
        assert "df_out = df_in" in body

    # -- Hierarchy Builder

    def test_builder_emits_a_struct(self):
        tx = Transformation(
            name="hb1", type=TransformationType.HIERARCHY_BUILDER,
            fields=[self._field("id", "ORDER_ID")],
        )
        tx.properties["output field"] = "order_doc"
        body = self._body(tx)
        assert "F.struct(" in body and "order_doc" in body

    def test_builder_flags_the_unrecoverable_grouping_key(self):
        """One document per row is only right when the source did not group
        rows into a repeating element -- and the grouping key is not in the
        export."""
        tx = Transformation(
            name="hb1", type=TransformationType.HIERARCHY_BUILDER,
            fields=[self._field("id", "ORDER_ID")],
        )
        assert "collect_list" in self._body(tx)

    # -- Hierarchy Processor

    def test_processor_flattens_when_told_to(self):
        tx = Transformation(
            name="hpr1", type=TransformationType.HIERARCHY_PROCESSOR,
            fields=[self._field("sku", "order.lines[].sku")],
        )
        tx.properties["output type"] = "relational"
        assert "explode_outer" in self._body(tx)

    def test_processor_nests_when_told_to(self):
        tx = Transformation(
            name="hpr1", type=TransformationType.HIERARCHY_PROCESSOR,
            fields=[self._field("id", "ORDER_ID")],
        )
        tx.properties["output type"] = "hierarchical"
        assert "F.struct(" in self._body(tx)

    def test_processor_refuses_to_guess_direction(self):
        """Flattening when the mapping meant to nest yields a notebook that
        runs and is wrong. Neither direction is emitted."""
        tx = Transformation(name="hpr1",
                            type=TransformationType.HIERARCHY_PROCESSOR)
        body = self._body(tx)
        assert "REVIEW REQUIRED" in body
        assert "explode_outer" not in body
        assert "F.struct(" not in body
        assert "df_out = df_in" in body

    def test_processor_reports_multiple_output_groups(self):
        tx = Transformation(name="hpr1",
                            type=TransformationType.HIERARCHY_PROCESSOR)
        tx.properties["output_groups"] = ["g1", "g2", "g3"]
        body = self._body(tx)
        assert "3 output groups" in body

    # -- Structure Parser

    def test_structure_parser_explains_why_there_is_no_equivalent(self):
        """The parsing logic lives in an IDMC-held intelligent structure
        model that is not in the export. Nothing to translate -- which is a
        different problem from a missing converter."""
        tx = Transformation(name="sp1",
                            type=TransformationType.STRUCTURE_PARSER)
        tx.properties["structure model"] = "ism_apache_log"
        body = self._body(tx)
        assert "ism_apache_log" in body
        assert "REVIEW REQUIRED" in body
        assert "NOT parsed" in body
        assert "df_out = df_in" in body

    # -- all four emit a marker so the metric never scores them clean

    @pytest.mark.parametrize("ttype", [
        TransformationType.HIERARCHY_PARSER,
        TransformationType.HIERARCHY_BUILDER,
        TransformationType.HIERARCHY_PROCESSOR,
        TransformationType.STRUCTURE_PARSER,
    ])
    def test_bare_transformation_is_never_scored_as_clean(self, ttype):
        body = self._body(Transformation(name="t", type=ttype))
        assert "REVIEW REQUIRED" in body or "TODO" in body


class TestPhase4CodeCarrying:
    """Embedded source code is preserved verbatim, never paraphrased.

    The body IS the logic. Dropping it loses the only copy the migrator
    has; rewriting it would be a guess at semantics nobody can check.
    """

    def _body(self, tx):
        return "\n".join(TransformationConverter().convert(tx, "df_in", "df_out"))

    @pytest.mark.parametrize("ttype,prop", [
        (TransformationType.JAVA, "java code"),
        (TransformationType.PYTHON, "python code"),
        (TransformationType.VELOCITY, "template"),
        (TransformationType.CUSTOM, "module identifier"),
        (TransformationType.EXTERNAL_PROCEDURE, "module identifier"),
    ])
    def test_body_is_preserved_verbatim(self, ttype, prop):
        marker = "ROW_HASH = sha256(CUSTOMER_ID + SALT)"
        tx = Transformation(name="t1", type=ttype)
        tx.properties[prop] = marker
        body = self._body(tx)
        assert marker in body, "the original body must survive into the notebook"
        assert "REVIEW REQUIRED" in body
        assert "df_out = df_in" in body

    def test_multiline_body_survives_intact(self):
        code = "int x = 1;\nint y = 2;\nreturn x + y;"
        tx = Transformation(name="t1", type=TransformationType.JAVA)
        tx.properties["java code"] = code
        body = self._body(tx)
        for line in code.splitlines():
            assert line in body

    def test_absent_body_says_it_lives_outside_the_export(self):
        tx = Transformation(name="t1",
                            type=TransformationType.EXTERNAL_PROCEDURE)
        body = self._body(tx)
        assert "not present in this export" in body

    def test_guidance_is_specific_per_language(self):
        """Generic 'unsupported' leaves no next step. Each type says what
        to actually do."""
        j = Transformation(name="t", type=TransformationType.JAVA)
        p = Transformation(name="t", type=TransformationType.PYTHON)
        assert "UDF" in self._body(j)
        assert "pandas_udf" in self._body(p)


class TestPhase5AidpNative:
    def _body(self, tx):
        return "\n".join(TransformationConverter().convert(tx, "df_in", "df_out"))

    def test_chunking_emits_real_spark(self):
        tx = Transformation(name="ch1", type=TransformationType.CHUNKING)
        tx.properties["input field"] = "DOC_TEXT"
        tx.properties["chunk size"] = "500"
        body = self._body(tx)
        assert "F.substring" in body and "explode_outer" in body
        assert "500" in body

    def test_chunking_flags_the_unrecoverable_strategy(self):
        """Sentence/paragraph/token-aware boundaries are not in the export,
        and boundaries change retrieval quality downstream."""
        tx = Transformation(name="ch1", type=TransformationType.CHUNKING)
        tx.properties["input field"] = "DOC_TEXT"
        assert "REVIEW REQUIRED" in self._body(tx)

    def test_chunking_without_a_field_does_not_invent_one(self):
        tx = Transformation(name="ch1", type=TransformationType.CHUNKING)
        body = self._body(tx)
        assert "F.substring" not in body
        assert "df_out = df_in" in body

    def test_embedding_warns_about_vector_dimension(self):
        """A dimension mismatch fails at query time, not at generation."""
        tx = Transformation(name="ve1",
                            type=TransformationType.VECTOR_EMBEDDING)
        tx.properties["model"] = "infa-embed-v2"
        body = self._body(tx)
        assert "infa-embed-v2" in body
        assert "dimension" in body

    def test_ml_says_the_model_must_be_migrated_first(self):
        tx = Transformation(name="ml1",
                            type=TransformationType.MACHINE_LEARNING)
        tx.properties["model name"] = "churn_v3"
        body = self._body(tx)
        assert "churn_v3" in body
        assert "MLOps" in body or "mlflow" in body


class TestPhase6:
    def _body(self, tx):
        return "\n".join(TransformationConverter().convert(tx, "df_in", "df_out"))

    def test_data_masking_hashes_named_fields(self):
        from infa2aidp.models import TransformationField
        tx = Transformation(name="dm1", type=TransformationType.DATA_MASKING,
                            fields=[TransformationField(name="SSN")])
        body = self._body(tx)
        assert "F.sha2" in body and "SSN" in body

    def test_format_preserving_masking_is_refused_not_hashed(self):
        """A hash satisfies the column type and breaks downstream format
        validation. Emitting one would be worse than emitting nothing."""
        from infa2aidp.models import TransformationField
        tx = Transformation(name="dm1", type=TransformationType.DATA_MASKING,
                            fields=[TransformationField(name="CARD")])
        tx.properties["masking technique"] = "format-preserving"
        body = self._body(tx)
        assert "F.sha2" not in body
        assert "REVIEW REQUIRED" in body
        assert "df_out = df_in" in body

    @pytest.mark.parametrize("ttype", [
        TransformationType.CLEANSE, TransformationType.LABELER,
        TransformationType.PARSE, TransformationType.RULE_SPECIFICATION,
        TransformationType.VERIFIER, TransformationType.B2B,
        TransformationType.DATA_SERVICES, TransformationType.WEB_SERVICES,
        TransformationType.WEB_SERVICES_CONSUMER,
        TransformationType.ACCESS_POLICY,
        TransformationType.UNSTRUCTURED_DATA, TransformationType.HTTP,
    ])
    def test_no_equivalent_types_state_why_and_what_instead(self, ttype):
        """A refusal is only coverage if it leaves the migrator a next
        step. Every entry names both the reason and the alternative."""
        body = self._body(Transformation(name="t1", type=ttype))
        assert ttype.value in body
        assert "REVIEW REQUIRED" in body
        assert "df_out = df_in" in body
        assert len(body) > 260, "a bare refusal is not guidance"

    def test_referenced_asset_name_is_surfaced(self):
        """The asset name is the migrator's only lead on what to export
        from the source system next."""
        tx = Transformation(name="rs1",
                            type=TransformationType.RULE_SPECIFICATION)
        tx.properties["rule name"] = "rs_validate_postcode"
        assert "rs_validate_postcode" in self._body(tx)

    def test_per_row_http_is_called_out_as_a_scaling_trap(self):
        for t in (TransformationType.HTTP, TransformationType.WEB_SERVICES):
            assert "batched" in self._body(Transformation(name="t", type=t))


def test_no_transformation_reaches_the_generic_unsupported_handler():
    """The completion check for Phases 1-6.

    Every type on both platforms must have a deliberate outcome -- a
    converter, a preserved body, or a specific refusal. Falling through to
    the generic handler means a type was added to the enum and forgotten.
    """
    generic = "manual conversion required"
    missed = []
    for name in CDI_TRANSFORMATIONS + POWERCENTER_TRANSFORMATIONS:
        t = resolve(name)
        # Source and Target are instances, not transformations: the
        # notebook generator's read/write paths handle them, so they have no
        # dispatch entry by design. Everything else must have one.
        #
        # This exclusion list used to also carry the Source Qualifier
        # variants and XML Parser/Generator, which meant the test asserted
        # completeness while skipping the five types that were not complete.
        # An exclusion list is only legitimate when it names things handled
        # elsewhere, never things not handled at all.
        if t in (TransformationType.SOURCE, TransformationType.TARGET):
            continue
        body = "\n".join(
            TransformationConverter().convert(
                Transformation(name="t", type=t, raw_type=name), "df_in", "df_out"
            )
        )
        if generic in body:
            missed.append(name)
    assert not missed, f"fell through to the generic handler: {missed}"
