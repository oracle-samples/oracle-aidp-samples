"""Tests for InformaticaXMLParser against a three-stage warehouse-load export."""

import os
import tempfile
import unittest

from infa2aidp.models import (
    InfaVersion,
    TransformationType,
)
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

FIXTURES_PC = os.path.join(os.path.dirname(__file__), "fixtures", "powercenter")
FIXTURES_CORPUS = os.path.join(os.path.dirname(__file__), "fixtures", "corpus")
MULTI_STAGE_XML = os.path.join(FIXTURES_PC, "multi_stage_invoice_dw.xml")
JOINER_XML = os.path.join(FIXTURES_PC, "joiner_no_master_flag.xml")


class TestMultiStageParser(unittest.TestCase):
    """Parse multi_stage_invoice_dw.xml and verify structure."""

    @classmethod
    def setUpClass(cls):
        parser = InformaticaXMLParser()
        cls.result = parser.parse(MULTI_STAGE_XML)

    # ---- Version detection ----

    def test_version_is_v10(self):
        """This fixture is a 10.x export (repository version 188.x)."""
        self.assertEqual(self.result.version, InfaVersion.V10)

    def test_version_detail_contains_188(self):
        self.assertIn("188", self.result.version_detail)

    # ---- Repository / folder metadata ----

    def test_repository_name(self):
        self.assertEqual(self.result.repository_name, "REP_FIN_DW")

    def test_folder_name(self):
        self.assertEqual(self.result.folder_name, "FINANCE_DW")

    # ---- Mapping counts ----

    def test_mapping_count(self):
        self.assertEqual(len(self.result.mappings), 3)

    def test_session_count(self):
        self.assertEqual(len(self.result.sessions), 3)

    def test_workflow_count(self):
        self.assertEqual(len(self.result.workflows), 1)

    # ---- Mapping names ----

    def test_mapping_names(self):
        names = [m.name for m in self.result.mappings]
        self.assertIn("m_stage_invoice_txn", names)
        self.assertIn("m_load_invoice_fact", names)
        self.assertIn("m_rollup_revenue", names)

    # ---- SDE mapping transformations ----

    def test_sde_has_source_qualifier(self):
        sde = self._mapping("m_stage_invoice_txn")
        sq = [t for t in sde.transformations if t.type == TransformationType.SOURCE_QUALIFIER]
        self.assertTrue(len(sq) >= 1)

    def test_sde_source_qualifier_has_sql_override(self):
        sde = self._mapping("m_stage_invoice_txn")
        sq = [t for t in sde.transformations if t.type == TransformationType.SOURCE_QUALIFIER][0]
        self.assertIn("LAST_UPDATE_DATE", sq.sql_override)

    def test_sde_has_expression(self):
        sde = self._mapping("m_stage_invoice_txn")
        exps = [t for t in sde.transformations if t.type == TransformationType.EXPRESSION]
        self.assertTrue(len(exps) >= 1)

    def test_sde_has_filter(self):
        sde = self._mapping("m_stage_invoice_txn")
        fils = [t for t in sde.transformations if t.type == TransformationType.FILTER]
        self.assertTrue(len(fils) >= 1)

    def test_sde_has_lookup(self):
        sde = self._mapping("m_stage_invoice_txn")
        lkps = [t for t in sde.transformations if t.type == TransformationType.LOOKUP]
        self.assertTrue(len(lkps) >= 1)

    def test_sde_lookup_table(self):
        sde = self._mapping("m_stage_invoice_txn")
        lkp = [t for t in sde.transformations if t.type == TransformationType.LOOKUP][0]
        self.assertEqual(lkp.lookup_table, "FIN.party_master")

    def test_sde_filter_condition(self):
        sde = self._mapping("m_stage_invoice_txn")
        fil = [t for t in sde.transformations if t.type == TransformationType.FILTER][0]
        self.assertIn("UNIT_ID", fil.filter_condition)

    def test_sde_has_sources(self):
        sde = self._mapping("m_stage_invoice_txn")
        self.assertTrue(len(sde.sources) >= 1)
        src_names = [s.name for s in sde.sources]
        self.assertIn("invoice_txn", src_names)

    def test_sde_has_targets(self):
        sde = self._mapping("m_stage_invoice_txn")
        self.assertTrue(len(sde.targets) >= 1)
        tgt_names = [t.name for t in sde.targets]
        self.assertIn("stg_invoice_txn", tgt_names)

    def test_sde_has_connectors(self):
        sde = self._mapping("m_stage_invoice_txn")
        self.assertTrue(len(sde.connectors) > 0)

    def test_sde_has_mapping_variables(self):
        sde = self._mapping("m_stage_invoice_txn")
        self.assertIn("$$LAST_EXTRACT_DATE", sde.parameters)

    # ---- SIL mapping transformations ----

    def test_sil_has_lookups(self):
        sil = self._mapping("m_load_invoice_fact")
        lkps = [t for t in sil.transformations if t.type == TransformationType.LOOKUP]
        self.assertEqual(len(lkps), 2, "SIL should have 2 lookups (customer + org)")

    def test_sil_has_update_strategy(self):
        sil = self._mapping("m_load_invoice_fact")
        upds = [t for t in sil.transformations if t.type == TransformationType.UPDATE_STRATEGY]
        self.assertEqual(len(upds), 1)

    def test_sil_update_strategy_expression(self):
        sil = self._mapping("m_load_invoice_fact")
        upd = [t for t in sil.transformations if t.type == TransformationType.UPDATE_STRATEGY][0]
        self.assertIn("DD_INSERT", upd.update_strategy_expression)
        self.assertIn("DD_UPDATE", upd.update_strategy_expression)

    def test_sil_reads_staging_table(self):
        sil = self._mapping("m_load_invoice_fact")
        src_names = [s.name for s in sil.sources]
        self.assertIn("stg_invoice_txn", src_names)

    def test_sil_writes_fact_table(self):
        sil = self._mapping("m_load_invoice_fact")
        tgt_names = [t.name for t in sil.targets]
        self.assertIn("fct_invoice_txn", tgt_names)

    # ---- PLP mapping transformations ----

    def test_plp_has_aggregator(self):
        plp = self._mapping("m_rollup_revenue")
        aggs = [t for t in plp.transformations if t.type == TransformationType.AGGREGATOR]
        self.assertEqual(len(aggs), 1)

    def test_plp_aggregator_group_by(self):
        plp = self._mapping("m_rollup_revenue")
        agg = [t for t in plp.transformations if t.type == TransformationType.AGGREGATOR][0]
        self.assertIn("PARTY_SK", agg.group_by_fields)

    # ---- Transformation field details ----

    def test_expression_fields_have_expressions(self):
        sde = self._mapping("m_stage_invoice_txn")
        exp = [t for t in sde.transformations if t.type == TransformationType.EXPRESSION][0]
        # The IIF expression on INVOICED_AMOUNT_OUT
        iif_field = [f for f in exp.fields if f.name == "INVOICED_AMOUNT_OUT"]
        self.assertTrue(len(iif_field) == 1)
        self.assertIn("IIF", iif_field[0].expression)

    # ---- Workflow ----

    def test_workflow_name(self):
        wf = self.result.workflows[0]
        self.assertEqual(wf.name, "wf_finance_analytics")

    def test_workflow_sessions(self):
        wf = self.result.workflows[0]
        self.assertIn("s_m_stage_invoice_txn", wf.sessions)
        self.assertIn("s_m_load_invoice_fact", wf.sessions)
        self.assertIn("s_m_rollup_revenue", wf.sessions)

    def test_workflow_execution_order(self):
        wf = self.result.workflows[0]
        order = wf.execution_order
        # SDE must come before SIL, SIL before PLP
        if "s_m_stage_invoice_txn" in order and "s_m_load_invoice_fact" in order:
            sde_idx = order.index("s_m_stage_invoice_txn")
            sil_idx = order.index("s_m_load_invoice_fact")
            self.assertLess(sde_idx, sil_idx, "SDE should execute before SIL")
        if "s_m_load_invoice_fact" in order and "s_m_rollup_revenue" in order:
            sil_idx = order.index("s_m_load_invoice_fact")
            plp_idx = order.index("s_m_rollup_revenue")
            self.assertLess(sil_idx, plp_idx, "SIL should execute before PLP")

    def test_workflow_dependencies(self):
        wf = self.result.workflows[0]
        self.assertTrue(len(wf.dependencies) >= 3, "Should have at least 3 workflow links")

    # ---- Session details ----

    def test_session_mapping_names(self):
        session_map = {s.name: s.mapping_name for s in self.result.sessions}
        self.assertEqual(session_map["s_m_stage_invoice_txn"], "m_stage_invoice_txn")
        self.assertEqual(session_map["s_m_load_invoice_fact"], "m_load_invoice_fact")
        self.assertEqual(session_map["s_m_rollup_revenue"], "m_rollup_revenue")

    # ---- Helper ----

    def _mapping(self, name):
        for m in self.result.mappings:
            if m.name == name:
                return m
        self.fail(f"Mapping {name} not found")


class TestGenericParser(unittest.TestCase):
    """Parse joiner_no_master_flag.xml and verify a source-neutral structure.

    joiner_no_master_flag.xml is HAND-AUTHORED, like every other fixture in
    this repo, and carries REPOSITORY_VERSION="186.95".

    It was previously described here as "a genuine 9.x export". It is not,
    and the distinction matters: that sentence was the only basis anyone
    had for believing 186 means 9.x, and it made a typed number look like
    evidence. The file shares a CREATION_DATE with another sample carrying
    a different REPOSITORY_VERSION, which is an authoring artifact rather
    than two real exports.

    What 186/187 actually correspond to is still unresolved -- see
    references/conversion-hazards.md. Informatica publishes no
    REPOSITORY_VERSION table, and KB 516223, which was expected to settle
    it, documents SerializationSpecVersion for Developer exports instead.

    The file remains valuable as the fullest-shape PowerCenter structure
    available: real SOURCE/TARGET/INSTANCE metadata, a SESSION and a
    WORKFLOW. That is a statement about its shape, not its provenance.
    """

    @classmethod
    def setUpClass(cls):
        import os
        cls._env_patch = os.environ.get("INFA_ALLOW_UNSUPPORTED_VERSION")
        os.environ["INFA_ALLOW_UNSUPPORTED_VERSION"] = "1"
        parser = InformaticaXMLParser()
        cls.result = parser.parse(JOINER_XML)

    @classmethod
    def tearDownClass(cls):
        import os
        if cls._env_patch is None:
            os.environ.pop("INFA_ALLOW_UNSUPPORTED_VERSION", None)
        else:
            os.environ["INFA_ALLOW_UNSUPPORTED_VERSION"] = cls._env_patch

    def test_version_is_v10_5(self):
        """This fixture is a 10.5 export (repository version 189.x)."""
        self.assertEqual(self.result.version, InfaVersion.V10_5)

    def test_mapping_count(self):
        self.assertEqual(len(self.result.mappings), 1)

    def test_has_joiner(self):
        m = self.result.mappings[0]
        joiners = [t for t in m.transformations if t.type == TransformationType.JOINER]
        self.assertEqual(len(joiners), 1)

    def test_joiner_condition(self):
        m = self.result.mappings[0]
        jnr = [t for t in m.transformations if t.type == TransformationType.JOINER][0]
        self.assertIn("TEAM_ID", jnr.join_condition)

    def test_has_aggregator(self):
        m = self.result.mappings[0]
        aggs = [t for t in m.transformations if t.type == TransformationType.AGGREGATOR]
        self.assertEqual(len(aggs), 1)

    def test_sources_count(self):
        m = self.result.mappings[0]
        self.assertEqual(len(m.sources), 2, "Should have employees + departments")

    def test_targets_count(self):
        m = self.result.mappings[0]
        self.assertEqual(len(m.targets), 1)


class TestRouterGroupAttributeSpelling(unittest.TestCase):
    """Router GROUP elements carry their condition as either EXPRESSION
    (what we believe is the canonical PowerCenter DTD spelling) or
    CONDITION (seen in the wild, and used by our own
    tests/fixtures/powercenter/data_quality_route.xml). We have no licensed
    PowerCenter install to produce a genuine Router export and settle
    which is authoritative, so the parser must accept both rather than
    silently dropping whichever one it doesn't recognize -- a dropped
    condition means every row goes to every downstream group, which is a
    data-correctness bug, not just an incomplete conversion.
    """

    ROUTER_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MAPPING NAME="m_router_attr_test">
    <TRANSFORMATION NAME="RTR_TEST" TYPE="Router">
        <GROUP NAME="via_expression" EXPRESSION="AMOUNT &gt; 100"/>
        <GROUP NAME="via_condition" CONDITION="AMOUNT &lt;= 100"/>
        <GROUP NAME="default_group"/>
        <TRANSFORMFIELD NAME="AMOUNT" DATATYPE="decimal" PORTTYPE="INPUT/OUTPUT"/>
    </TRANSFORMATION>
</MAPPING>"""

    @classmethod
    def setUpClass(cls):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".xml", delete=False
        ) as f:
            f.write(cls.ROUTER_XML)
            cls.tmp_path = f.name
        parser = InformaticaXMLParser()
        cls.result = parser.parse(cls.tmp_path)

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.tmp_path)

    def _router_tx(self):
        m = self.result.mappings[0]
        return next(
            t for t in m.transformations if t.type == TransformationType.ROUTER
        )

    def test_group_count(self):
        self.assertEqual(len(self._router_tx().router_groups), 3)

    def test_expression_attribute_is_read(self):
        groups = {g["name"]: g["condition"] for g in self._router_tx().router_groups}
        self.assertEqual(groups["via_expression"], "AMOUNT > 100")

    def test_condition_attribute_is_also_read(self):
        """This is the fix: CONDITION used to be silently dropped (parser
        only looked for EXPRESSION), leaving this group's condition empty
        -- which the generator then treated as an extra, ungoverned
        default group."""
        groups = {g["name"]: g["condition"] for g in self._router_tx().router_groups}
        self.assertEqual(groups["via_condition"], "AMOUNT <= 100")

    def test_true_default_group_has_no_condition(self):
        groups = {g["name"]: g["condition"] for g in self._router_tx().router_groups}
        self.assertEqual(groups["default_group"], "")


if __name__ == "__main__":
    unittest.main()
