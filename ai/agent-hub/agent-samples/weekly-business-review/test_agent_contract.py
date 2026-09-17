from __future__ import annotations

import csv
import json
from pathlib import Path
import unittest
from unittest import mock

import agent

try:
    import jsonschema  # noqa: F401
    import referencing  # noqa: F401
except ModuleNotFoundError:
    HAS_SCHEMA_DEPENDENCIES = False
else:
    HAS_SCHEMA_DEPENDENCIES = True


class WeeklyBusinessReviewContractTests(unittest.TestCase):
    def _base_plan(self) -> dict:
        return {
            "mode": "a2ui",
            "message": "Pipeline is concentrated in later stages.",
            "screen": {
                "type": "overview",
                "title": "Weekly Business Review",
                "filters": dict(agent.DEFAULT_FILTERS),
                "metrics": [{"label": "Pipeline", "value": "$10.8M"}],
                "actions": [
                    {
                        "label": "Review renewal risk",
                        "prompt": "Show renewal risk for the same date range, region, and segment",
                    }
                ],
                "sources": ["get_weekly_business_summary"],
            },
        }

    def _render(self) -> list[dict]:
        if HAS_SCHEMA_DEPENDENCIES:
            return agent.render_operations(self._base_plan())
        with mock.patch.object(agent, "validate_operations"):
            return agent.render_operations(self._base_plan())

    @unittest.skipUnless(HAS_SCHEMA_DEPENDENCIES, "schema dependencies are not installed")
    def test_schema_validation_rejects_invalid_text_type(self) -> None:
        operations = agent.render_operations(self._base_plan())
        components = operations[2]["updateComponents"]["components"]
        next(item for item in components if item["id"] == "screen_title")["text"] = 12345

        with self.assertRaises(ValueError):
            agent.validate_operations(operations)

    def test_schema_validation_rejects_old_dependency_version(self) -> None:
        operations = self._render()

        def installed_version(package: str) -> str:
            return "4.17.3" if package == "jsonschema" else "0.30.0"

        with mock.patch.object(agent.importlib_metadata, "version", side_effect=installed_version):
            with self.assertRaisesRegex(RuntimeError, "jsonschema>=4.18.0; found 4.17.3"):
                agent.validate_operations(operations)

    def test_schema_validation_fails_explicitly_when_unavailable(self) -> None:
        operations = self._render()

        with mock.patch.object(
            agent.importlib_metadata,
            "version",
            side_effect=agent.importlib_metadata.PackageNotFoundError("jsonschema"),
        ):
            with self.assertRaisesRegex(RuntimeError, "package is not installed"):
                agent.validate_operations(operations)

    def test_serializer_has_one_sdk_independent_wire_shape(self) -> None:
        operations = self._render()
        content = agent.response_text(agent.serialize_operations(operations))

        self.assertTrue(content.startswith("A2UI\n"))
        self.assertEqual(
            json.loads(content.removeprefix("A2UI\n")),
            [
                {
                    "root": {
                        "kind": "data",
                        "data": operation,
                        "metadata": {"mimeType": "application/json+a2ui"},
                    }
                }
                for operation in operations
            ],
        )

    def test_old_tool_failure_does_not_poison_new_turn(self) -> None:
        result = {
            "messages": [
                {
                    "role": "tool",
                    "name": "get_pipeline_by_stage",
                    "status": "error",
                    "content": "Error: old request failed",
                },
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": '{"mode":"text","message":"Hello"}'},
            ]
        }

        self.assertEqual(agent.extract_tool_failures(result), [])

    def test_current_turn_tool_failure_is_reported(self) -> None:
        result = {
            "messages": [
                {"role": "user", "content": "Show pipeline"},
                {
                    "role": "tool",
                    "name": "get_pipeline_by_stage",
                    "content": '{"code":500,"message":"query failed"}',
                },
            ]
        }

        failures = agent.extract_tool_failures(result)
        self.assertEqual(failures[0][0], "get_pipeline_by_stage")

    def test_sql_tools_select_latest_dated_snapshot(self) -> None:
        captured: dict[str, str] = {}

        def capture_tool(name: str, description: str, query: str, params: list[dict]) -> dict:
            captured[name] = query
            return {"name": name, "description": description, "params": params}

        with mock.patch.object(agent, "CATALOG_KEY", "catalog"), \
                mock.patch.object(agent, "SCHEMA_KEY", "schema"), \
                mock.patch.object(agent, "_sql_tool", side_effect=capture_tool):
            tools = agent.build_tools()

        self.assertEqual(len(tools), 6)
        summary = captured["get_weekly_business_summary"]
        self.assertNotIn("SUM(active_accounts)", summary)
        self.assertNotIn("SUM(pipeline_usd)", summary)
        self.assertIn("MAX(latest_row.week_start)", summary)
        self.assertIn("FETCH FIRST 1 ROW ONLY", summary)

        for name in (
            "get_pipeline_by_stage",
            "get_renewal_risk_accounts",
            "get_product_usage_by_account",
            "get_support_health_by_account",
        ):
            query = captured[name]
            self.assertIn("MAX(latest_row.week_start)", query)
            self.assertIn(".week_start", query)
            self.assertIn(".week_end", query)
            self.assertIn("= {{region}}", query)
            self.assertIn("= {{segment}}", query)

    def test_follow_up_actions_include_explicit_scope(self) -> None:
        operations = self._render()
        components = operations[2]["updateComponents"]["components"]
        button = next(item for item in components if item["id"] == "action_1_button")
        context = button["action"]["event"]["context"]

        for key, value in agent.DEFAULT_FILTERS.items():
            self.assertEqual(context[key], value)
            self.assertIn(value, context["prompt"])
        self.assertNotIn("same date range", context["prompt"].lower())

        request = agent.normalize_request(
            {"userAction": {"name": "ask_wbr_question", "context": context}},
            {},
        )
        for value in agent.DEFAULT_FILTERS.values():
            self.assertIn(value, request)

    def test_multi_week_fixture_resolves_to_latest_snapshot(self) -> None:
        data_dir = Path(__file__).parent / "sample_data"

        def rows_for(filename: str) -> list[dict[str, str]]:
            with (data_dir / filename).open(encoding="utf-8", newline="") as source:
                rows = list(csv.DictReader(source))
            matching = [
                row
                for row in rows
                if row["region"] == "North America"
                and row["segment"] == "Enterprise"
                and row["week_start"] >= "2026-07-27"
                and row["week_end"] <= "2026-08-09"
            ]
            latest_week = max(row["week_start"] for row in matching)
            return [row for row in matching if row["week_start"] == latest_week]

        summary = rows_for("weekly_business_metrics.csv")
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["active_accounts"], "5")
        self.assertEqual(summary[0]["pipeline_usd"], "11240000")

        pipeline = rows_for("pipeline_by_stage.csv")
        self.assertEqual(sum(int(row["pipeline_usd"]) for row in pipeline), 11240000)

        account_b_risk = [
            row
            for row in rows_for("renewal_risk_accounts.csv")
            if row["account_name"] == "Account B"
        ]
        self.assertEqual(len(account_b_risk), 1)
        self.assertEqual(account_b_risk[0]["risk_level"], "Medium")
        self.assertEqual(account_b_risk[0]["week_start"], "2026-08-03")

        account_b_usage = [
            row
            for row in rows_for("product_usage_by_account.csv")
            if row["account_name"] == "Account B"
        ]
        self.assertEqual(len(account_b_usage), 1)
        self.assertEqual(account_b_usage[0]["active_users"], "2505")
        self.assertEqual(account_b_usage[0]["week_end"], "2026-08-09")


if __name__ == "__main__":
    unittest.main()
