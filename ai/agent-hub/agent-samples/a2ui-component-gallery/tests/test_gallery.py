from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path


SAMPLE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SAMPLE_ROOT))

from a2ui_sdk.manager import A2uiSchemaManager, A2uiValidator  # noqa: E402
from a2ui_sdk.compat import (  # noqa: E402
    V08SurfaceBuilderAdapter,
    V08_UNSUPPORTED_COMPONENTS,
    capability_candidates,
    catalog_id_matches_version,
    convert_operations_to_v08,
)
from a2ui_sdk.parser import (  # noqa: E402
    build_text_response_from_llm_text_with_repair,
    parse_llm_text_to_a2a_parts,
)
from templates.template_selection import TemplateSelectionService  # noqa: E402
from templates.template_utils import (  # noqa: E402
    COMPONENT_OPTIONS,
    SUPPORTED_COMPONENTS,
    SurfaceBuilder,
    action_ui_response,
    extract_requested_component_from_action,
    is_greeting_query,
    is_help_query,
    wants_ui,
)


class GalleryTemplateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manager = A2uiSchemaManager(version="0.9")
        cls.catalog = cls.manager.get_selected_catalog()
        cls.validator = A2uiValidator(cls.catalog)
        cls.selection = TemplateSelectionService(
            supported_components=SUPPORTED_COMPONENTS,
            default_component="Text",
        )
        cls.builder = SurfaceBuilder(cls.selection, COMPONENT_OPTIONS)

    def test_gallery_index_and_all_30_catalog_components_are_valid(self):
        # The supplied demo has 31 previews: one gallery/index surface plus the
        # 30 component types declared by the v0.9 complete catalog.
        self.assertEqual(30, len(SUPPORTED_COMPONENTS))
        self.assertEqual(31, len(("Gallery",) + SUPPORTED_COMPONENTS))
        self.assertEqual(30, len(set(SUPPORTED_COMPONENTS)))

        for component in SUPPORTED_COMPONENTS:
            with self.subTest(component=component):
                operations = self.builder.build_surface_operations(
                    surface_id=f"test-{component.lower()}",
                    catalog_id=self.catalog.catalog_id,
                    selected_component=component,
                    include_begin=True,
                )
                self.validator.validate(operations)

    def test_unknown_component_falls_back_to_text(self):
        operations = self.builder.build_surface_operations(
            surface_id="test-fallback",
            catalog_id=self.catalog.catalog_id,
            selected_component="NotAComponent",
            include_begin=True,
        )
        self.validator.validate(operations)
        serialized = str(operations)
        self.assertIn("Text", serialized)

    def test_common_gallery_requests_skip_llm_selection(self):
        self.assertEqual(
            "Text",
            self.selection.deterministic_component_for_query("Show the component gallery"),
        )
        self.assertEqual(
            "OADataGrid",
            self.selection.deterministic_component_for_query("Preview the data grid"),
        )
        self.assertEqual(
            "TextField",
            self.selection.deterministic_component_for_query("Show a text field"),
        )
        self.assertIsNone(
            self.selection.deterministic_component_for_query(
                "Show something that accepts a bounded numeric value"
            )
        )
        self.assertEqual(
            "ChoicePicker",
            self.selection.deterministic_component_for_query("show multiple choice"),
        )

    def test_conversation_and_demo_intents_are_distinct(self):
        self.assertTrue(is_help_query("what can i ask you"))
        self.assertTrue(is_help_query("What can you help me with?"))
        self.assertTrue(is_greeting_query("Hello!"))
        self.assertFalse(wants_ui("what can i ask you"))
        self.assertFalse(wants_ui("hello"))
        self.assertFalse(wants_ui("help me build something"))
        self.assertTrue(wants_ui("show me a demo"))
        self.assertTrue(wants_ui("open the component gallery"))
        self.assertTrue(wants_ui("show me a ChoicePicker"))
        self.assertEqual(
            "ChoicePicker",
            self.selection.deterministic_component_for_query("show me a choice picker"),
        )

    def test_v08_compatibility_operations_are_valid(self):
        manager = A2uiSchemaManager(version="0.8")
        catalog = manager.get_selected_catalog()
        validator = A2uiValidator(catalog)
        component_schemas = catalog.catalog_schema["components"]

        for component in SUPPORTED_COMPONENTS:
            if component in V08_UNSUPPORTED_COMPONENTS:
                continue
            with self.subTest(component=component):
                operations_v09 = self.builder.build_surface_operations(
                    surface_id=f"compat-{component.lower()}",
                    catalog_id=self.catalog.catalog_id,
                    selected_component=component,
                    include_begin=True,
                )
                operations_v08 = convert_operations_to_v08(
                    operations_v09,
                    catalog_id=catalog.catalog_id,
                    component_schemas=component_schemas,
                )
                validator.validate(operations_v08)

    def test_capability_negotiation_prefers_v09_then_v08(self):
        versioned = capability_candidates(
            {
                "a2uiClientCapabilities": {
                    "v0.8": {"supportedCatalogIds": ["agent-hub-catalog-v1-v08"]},
                    "v0.9": {"supportedCatalogIds": ["catalog-v09"]},
                }
            }
        )
        self.assertEqual(["0.9", "0.8"], [version for version, _ in versioned])
        self.assertEqual(
            ["0.9", "0.8"],
            [
                version
                for version, _ in capability_candidates(
                    {"a2uiClientCapabilities": {"supportedCatalogIds": ["legacy"]}}
                )
            ],
        )
        self.assertEqual(
            [],
            capability_candidates(
                {"a2uiClientCapabilities": {"v1.0": {"supportedCatalogIds": ["future"]}}}
            ),
        )
        # Major-only future spellings (v2, v10) must also be recognized as
        # version keys and fall through to the text fallback, not be
        # misread as flat capabilities and served v0.9 operations.
        for future_key in ("v2", "v10", "2"):
            with self.subTest(future_key=future_key):
                self.assertEqual(
                    [],
                    capability_candidates(
                        {
                            "a2uiClientCapabilities": {
                                future_key: {"supportedCatalogIds": ["future"]}
                            }
                        }
                    ),
                )

    def test_catalog_alias_matching_uses_an_exact_allowlist(self):
        self.assertTrue(
            catalog_id_matches_version(
                "/a2ui_specification/2.0.0/agent_hub_a2ui_custom_component_catalog.json",
                "0.9",
            )
        )
        self.assertTrue(catalog_id_matches_version("agent-hub-catalog-v1-v08", "0.8"))
        self.assertFalse(
            catalog_id_matches_version(
                "https://foreign.example/2.0.0/unrelated-catalog.json",
                "0.9",
            )
        )
        # The basic catalog is a real separate catalog served by direct match,
        # NOT an alias of the bundled complete catalog. If this ever returns
        # True again, the alias fallback in negotiate_a2ui will emit complete-
        # catalog components stamped with the basic-catalog ID.
        self.assertFalse(
            catalog_id_matches_version(
                "https://a2ui.org/specification/v0_9/catalogs/basic/catalog.json",
                "0.9",
            )
        )

    def test_validator_registry_uses_platform_independent_schema_urls(self):
        registry_uris = {str(uri) for uri in self.validator._registry}
        self.assertIn(
            "https://a2ui.org/specification/v0_9/catalog.json",
            registry_uris,
        )
        self.assertIn(
            "https://a2ui.org/specification/v0_9/common_types.json",
            registry_uris,
        )
        self.assertFalse(any("\\" in uri for uri in registry_uris))

    def test_manager_star_import_exports_only_defined_names(self):
        namespace: dict[str, object] = {}
        exec("from a2ui_sdk.manager import *", namespace)
        self.assertIn("A2uiSchemaManager", namespace)

    def test_invalid_a2ui_after_text_raises_instead_of_being_dropped(self):
        with self.assertLogs("a2ui_sdk.parser", level="WARNING"):
            with self.assertRaises(ValueError):
                parse_llm_text_to_a2a_parts(
                    "Here is the preview. <a2ui-json>{INVALID}</a2ui-json>",
                    validator=self.validator,
                )

    def test_invalid_a2ui_after_text_triggers_one_repair_attempt(self):
        operations = self.builder.build_surface_operations(
            surface_id="repair-test",
            catalog_id=self.catalog.catalog_id,
            selected_component="Text",
            include_begin=True,
        )
        repair_calls: list[str] = []

        async def repair_async(prompt: str) -> str:
            repair_calls.append(prompt)
            import json

            return f"<a2ui-json>{json.dumps(operations)}</a2ui-json>"

        with self.assertLogs("a2ui_sdk.parser", level="WARNING"):
            result = asyncio.run(
                build_text_response_from_llm_text_with_repair(
                    "Here is the preview. <a2ui-json>{INVALID}</a2ui-json>",
                    user_query="show text",
                    validator=self.validator,
                    repair_async=repair_async,
                    max_repair_attempts=1,
                )
            )
        self.assertEqual(1, len(repair_calls))
        self.assertTrue(result.get("messages"))

    def test_v08_preserves_path_backed_audio_data(self):
        manager = A2uiSchemaManager(version="0.8")
        catalog = manager.get_selected_catalog()
        validator = A2uiValidator(catalog)
        operations_v09 = self.builder.build_surface_operations(
            surface_id="compat-audio-data",
            catalog_id=self.catalog.catalog_id,
            selected_component="AudioPlayer",
            include_begin=True,
        )
        operations_v08 = convert_operations_to_v08(
            operations_v09,
            catalog_id=catalog.catalog_id,
            component_schemas=catalog.catalog_schema["components"],
        )
        validator.validate(operations_v08)

        audio_updates = [
            operation["dataModelUpdate"]
            for operation in operations_v08
            if isinstance(operation.get("dataModelUpdate"), dict)
            and operation["dataModelUpdate"].get("path") == "/demo/audio"
        ]
        self.assertEqual(1, len(audio_updates))
        values = {
            entry["key"]: entry.get("valueString")
            for entry in audio_updates[0]["contents"]
        }
        self.assertEqual(
            "https://samplelib.com/lib/preview/mp3/sample-3s.mp3",
            values["primaryUrl"],
        )
        self.assertEqual(
            "Card embedded path backed audio player",
            values["secondaryDescription"],
        )

    def test_v08_choice_picker_uses_strict_path_binding(self):
        manager = A2uiSchemaManager(version="0.8")
        catalog = manager.get_selected_catalog()
        validator = A2uiValidator(catalog)
        operations_v09 = self.builder.build_surface_operations(
            surface_id="compat-choice-selection",
            catalog_id=self.catalog.catalog_id,
            selected_component="ChoicePicker",
            include_begin=True,
        )
        operations_v08 = convert_operations_to_v08(
            operations_v09,
            catalog_id=catalog.catalog_id,
            component_schemas=catalog.catalog_schema["components"],
        )
        validator.validate(operations_v08)

        converted_components = next(
            operation["surfaceUpdate"]["components"]
            for operation in operations_v08
            if isinstance(operation.get("surfaceUpdate"), dict)
        )
        gallery_picker = next(
            component["component"]["MultipleChoice"]
            for component in converted_components
            if component.get("id") == "componentChoice1"
        )
        self.assertEqual(
            {"path": "/demo/selections"},
            gallery_picker["selections"],
        )

        allowed_client_properties = {
            "options",
            "selections",
            "maxAllowedSelections",
        }
        for component in converted_components:
            multiple_choice = component.get("component", {}).get("MultipleChoice")
            if not isinstance(multiple_choice, dict):
                continue
            self.assertFalse(
                set(multiple_choice) - allowed_client_properties,
                msg=f"Unsupported Agent Hub v0.8 MultipleChoice properties: {multiple_choice}",
            )

        labels = {
            component["id"]: component.get("component", {}).get("Text", {}).get("text")
            for component in converted_components
            if component.get("id") in {
                "tpl_variant_header_v1",
                "tpl_variant_header_v2",
                "tpl_variant_header_v4",
            }
        }
        self.assertEqual(
            {"literalString": "MultipleChoice literal selection (single-select)"},
            labels["tpl_variant_header_v1"],
        )

    def test_v08_choice_picker_label_overrides_do_not_leak_to_text(self):
        manager = A2uiSchemaManager(version="0.8")
        catalog = manager.get_selected_catalog()
        operations_v08 = convert_operations_to_v08(
            self.builder.build_surface_operations(
                surface_id="compat-text-labels",
                catalog_id=self.catalog.catalog_id,
                selected_component="Text",
                include_begin=True,
            ),
            catalog_id=catalog.catalog_id,
            component_schemas=catalog.catalog_schema["components"],
        )
        converted_components = next(
            operation["surfaceUpdate"]["components"]
            for operation in operations_v08
            if isinstance(operation.get("surfaceUpdate"), dict)
        )
        header = next(
            component["component"]["Text"]["text"]
            for component in converted_components
            if component.get("id") == "tpl_variant_header_v1"
        )
        self.assertEqual({"literalString": "Text literal (no usageHint)"}, header)

    def test_v08_choice_picker_action_value_is_normalized(self):
        for action in (
            {"selectedComponent": ["ChoicePicker"]},
            {"selectedComponent": {"literalArray": ["ChoicePicker"]}},
            {
                "context": [
                    {
                        "key": "selectedComponent",
                        "value": {"literalArray": ["ChoicePicker"]},
                    }
                ]
            },
        ):
            with self.subTest(action=action):
                self.assertEqual(
                    "ChoicePicker",
                    extract_requested_component_from_action(action, SUPPORTED_COMPONENTS),
                )


class GalleryActionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        manager = A2uiSchemaManager(version="0.9")
        cls.catalog = manager.get_selected_catalog()
        cls.validator = A2uiValidator(cls.catalog)
        cls.selection = TemplateSelectionService(SUPPORTED_COMPONENTS, "Text")
        cls.builder = SurfaceBuilder(cls.selection, COMPONENT_OPTIONS)

    async def _select(self, _query, action, requested):
        if requested in SUPPORTED_COMPONENTS:
            return requested
        if isinstance(action, dict) and action.get("selectedComponent") in SUPPORTED_COMPONENTS:
            return action["selectedComponent"]
        return "Text"

    def _invoke(self, action):
        return asyncio.run(
            action_ui_response(
                template_selection=self.selection,
                surface_builder=self.builder,
                user_query="test action",
                action=action,
                select_component_with_llm=self._select,
                catalog_id=self.catalog.catalog_id,
                schema_validate=self.validator.validate,
            )
        )

    def test_supported_action_paths_return_messages(self):
        actions = [
            {"name": "render_preview", "surfaceId": "surface-1", "selectedComponent": "OAChart"},
            {"name": "update_chart_preview", "surfaceId": "surface-1", "selectedChartType": "bar"},
            {"name": "update_chart_reference_controls", "surfaceId": "surface-1"},
            {"name": "update_grid_preview", "surfaceId": "surface-1", "selectedDataGridMaxRows": "3"},
            {"name": "preview_button_action", "surfaceId": "surface-1"},
            {"name": "preview_oaactioncard_action", "surfaceId": "surface-1"},
        ]
        for action in actions:
            with self.subTest(action=action["name"]):
                result = self._invoke(action)
                self.assertTrue(result.get("messages"))

    def test_v08_choice_picker_render_action_returns_valid_multiple_choice(self):
        manager = A2uiSchemaManager(version="0.8")
        catalog = manager.get_selected_catalog()
        validator = A2uiValidator(catalog)
        supported_components = tuple(
            component
            for component in SUPPORTED_COMPONENTS
            if component not in V08_UNSUPPORTED_COMPONENTS
        )
        selection = TemplateSelectionService(supported_components, "Text")
        options = [
            option
            for option in COMPONENT_OPTIONS
            if option.get("value") not in V08_UNSUPPORTED_COMPONENTS
        ]
        builder = V08SurfaceBuilderAdapter(
            SurfaceBuilder(selection, options),
            catalog_id=catalog.catalog_id,
            component_schemas=catalog.catalog_schema["components"],
        )
        result = asyncio.run(
            action_ui_response(
                template_selection=selection,
                surface_builder=builder,
                user_query="render ChoicePicker",
                action={
                    "name": "render_preview",
                    "surfaceId": "surface-1",
                    "selectedComponent": ["ChoicePicker"],
                },
                select_component_with_llm=self._select,
                catalog_id=catalog.catalog_id,
                schema_validate=validator.validate,
            )
        )
        self.assertIn("MultipleChoice", str(result))

    def test_malformed_actions_return_readable_messages(self):
        for action in (
            {"name": "not_supported", "surfaceId": "surface-1"},
            {"name": "render_preview"},
        ):
            result = self._invoke(action)
            self.assertTrue(result.get("messages"))


if __name__ == "__main__":
    unittest.main()
