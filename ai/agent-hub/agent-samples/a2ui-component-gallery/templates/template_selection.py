from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

class TemplateSelectionService:
    def __init__(
        self,
        supported_components: tuple[str, ...],
        default_component: str,
    ) -> None:
        self._template_dir = Path(__file__).resolve().parent
        self._supported_components = supported_components
        self._default_component = default_component
        self._templates_cache: dict[str, Any] = {}

    @property
    def supported_components(self) -> tuple[str, ...]:
        return self._supported_components

    @property
    def default_component(self) -> str:
        return self._default_component

    def deterministic_component_for_query(self, user_query: str) -> str | None:
        """Resolve explicit component and generic-gallery requests without an LLM.

        The gallery's common entry prompt should be immediate and deterministic.
        The LLM remains available for genuinely ambiguous natural-language
        requests such as "show something that accepts a bounded numeric value."
        """
        text = " ".join(str(user_query or "").lower().split())
        if not text:
            return self._default_component

        aliases: dict[str, tuple[str, ...]] = {
            "AudioPlayer": ("audio player", "audioplayer"),
            "CheckBox": ("check box", "checkbox"),
            "ChoicePicker": ("choice picker", "choicepicker", "multiple choice"),
            "DateTimeInput": ("date time input", "datetime input", "datetimeinput"),
            "TextField": ("text field", "textfield"),
            "OAActionCard": ("oa action card", "action card", "oaactioncard"),
            "OAChart": ("oa chart", "chart", "oachart"),
            "OACollapsible": ("oa collapsible", "collapsible", "oacollapsible"),
            "OACombobox": ("oa combobox", "combo box", "combobox", "oacombobox"),
            "OADataGrid": ("oa data grid", "data grid", "datagrid", "oadatagrid"),
            "OAListView": ("oa list view", "list view", "oalistview"),
            "OAPopup": ("oa popup", "popup", "oapopup"),
            "OAProgressBar": ("oa progress bar", "progress bar", "oaprogressbar"),
            "OAProgressCircle": ("oa progress circle", "progress circle", "oaprogresscircle"),
            "OARadioSet": ("oa radio set", "radio set", "oaradioset"),
            "OASwitch": ("oa switch", "switch", "oaswitch"),
            "OATruncatingText": ("oa truncating text", "truncating text", "oatruncatingtext"),
        }
        candidates: list[tuple[str, str]] = []
        for component in self._supported_components:
            component_aliases = aliases.get(component, (component.lower(),))
            candidates.extend((alias, component) for alias in component_aliases)

        for alias, component in sorted(candidates, key=lambda item: len(item[0]), reverse=True):
            if re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", text):
                return component

        if any(
            phrase in text
            for phrase in (
                "component gallery",
                "component picker",
                "show the gallery",
                "show gallery",
                "show ui",
                "a2ui demo",
                "a2ui gallery",
            )
        ):
            return self._default_component

        return None

    def _load_json(self, file_name: str) -> Any:
        full_path = self._template_dir / file_name
        with full_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def load_component_template_file(self, component_name: str) -> list[dict[str, Any]]:
        if component_name in self._templates_cache:
            cached = self._templates_cache[component_name]
            if isinstance(cached, list):
                return cached

        loaded = self._load_json(f"{component_name}.json")
        if not isinstance(loaded, list):
            raise ValueError(f"Template '{component_name}.json' must be a top-level operations array.")
        operations = [operation for operation in loaded if isinstance(operation, dict)]
        self._templates_cache[component_name] = operations
        return operations

    def required_operations_for_component(self, component_name: str) -> list[dict[str, Any]]:
        operations = self.load_component_template_file(component_name)
        if not operations:
            raise ValueError(f"No required operations found for component '{component_name}'")
        return [deepcopy(operation) for operation in operations]

    def preview_components_for_component(self, component_name: str) -> tuple[str, list[dict[str, Any]]]:
        operations = self.load_component_template_file(component_name)
        for operation in operations:
            # v0.9 native templates use updateComponents.
            update_components = operation.get("updateComponents")
            if isinstance(update_components, dict):
                components = update_components.get("components")
            else:
                # Backward-compatible fallback for older template shape.
                surface_update = operation.get("surfaceUpdate")
                if not isinstance(surface_update, dict):
                    continue
                components = surface_update.get("components")
            if not isinstance(components, list):
                continue

            normalized = [deepcopy(component) for component in components if isinstance(component, dict)]
            if not normalized:
                continue

            root_id = "templatePreviewVariants"
            if not any(component.get("id") == root_id for component in normalized):
                first_id = normalized[0].get("id")
                if isinstance(first_id, str) and first_id:
                    root_id = first_id

            return root_id, normalized

        raise ValueError(f"No updateComponents components found for component '{component_name}'")

    def template_catalog_summary(self) -> dict[str, list[dict[str, str]]]:
        return {
            component_name: [
                {
                    "id": f"{component_name.lower()}_template",
                    "label": f"{component_name} template preview",
                }
            ]
            for component_name in self._supported_components
        }
