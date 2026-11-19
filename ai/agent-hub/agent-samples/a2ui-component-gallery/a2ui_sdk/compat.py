"""A2UI v0.9-to-v0.8 compatibility helpers for Agent Hub clients."""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any


V08_COMPONENT_RENAMES = {"ChoicePicker": "MultipleChoice"}
V08_UNSUPPORTED_COMPONENTS = {"OAPopup"}

KNOWN_CATALOG_IDS_BY_VERSION = {
    "0.9": frozenset(
        {
            # The basic catalog URL is intentionally NOT listed here: it is a
            # real, separate agent-supported catalog (matched directly by
            # get_selected_catalog), not an alias of the bundled complete
            # catalog. Listing it would let the alias fallback serve complete-
            # catalog components stamped with the basic-catalog ID.
            "/a2ui_specification/2.0.0/agent_hub_a2ui_custom_component_catalog.json",
        }
    ),
    "0.8": frozenset(
        {
            "/a2ui_specification/1.0.0/agent_hub_a2ui_custom_component_catalog.json",
            "agent-hub-catalog-v1-v08",
        }
    ),
}

_VERSION_KEY_PATTERN = re.compile(r"^v?\d+(?:[._]\d+)*$")

V08_PROPERTY_RENAMES: dict[str, dict[str, str]] = {
    "Text": {"variant": "usageHint"},
    "Image": {"description": "altText", "variant": "usageHint"},
    "Row": {"align": "alignment", "justify": "distribution"},
    "Column": {"align": "alignment", "justify": "distribution"},
    "List": {"align": "alignment"},
    "Tabs": {"tabs": "tabItems"},
    "Modal": {"content": "contentChild", "trigger": "entryPointChild"},
    "TextField": {"value": "text", "variant": "textFieldType"},
    "Slider": {"min": "minValue", "max": "maxValue"},
    "OATruncatingText": {"fontWeight": "weight"},
}

V08_DROPPED_PROPERTIES: dict[str, set[str]] = {
    # Agent Hub's deployed v0.8 catalog accepts the core MultipleChoice
    # contract but rejects the newer presentation hints as additional
    # properties. Keep those richer previews in v0.9 only.
    "ChoicePicker": {"displayStyle", "filterable"},
    "OAChart": {"valueFormats"},
    "OADataGrid": {"formatting"},
}

V08_TEXT_OVERRIDES = {
    "tpl_variant_header_v1": "MultipleChoice literal selection (single-select)",
    "tpl_variant_header_v2": "MultipleChoice literal selection (multi-select)",
    "tpl_variant_header_v4": "MultipleChoice literal empty selection",
}


def capability_candidates(metadata: object) -> list[tuple[str, dict[str, Any] | None]]:
    """Return advertised protocol capabilities in newest-compatible-first order."""
    if not isinstance(metadata, dict):
        return [("0.9", None), ("0.8", None)]
    raw = metadata.get("a2uiClientCapabilities", metadata)
    if not isinstance(raw, dict):
        return [("0.9", None), ("0.8", None)]

    version_keys = {
        "0.9": ("v0.9", "0.9", "v0_9"),
        "0.8": ("v0.8", "0.8", "v0_8"),
    }
    has_known_version = any(key in raw for keys in version_keys.values() for key in keys)
    has_explicit_version = any(
        isinstance(key, str) and _VERSION_KEY_PATTERN.fullmatch(key)
        for key in raw
    )
    if has_known_version or has_explicit_version:
        candidates: list[tuple[str, dict[str, Any] | None]] = []
        for version in ("0.9", "0.8"):
            for key in version_keys[version]:
                value = raw.get(key)
                if isinstance(value, dict):
                    candidates.append((version, value))
                    break
        return candidates
    return [("0.9", raw), ("0.8", raw)]


def advertised_catalog_ids(capabilities: object) -> list[str]:
    if not isinstance(capabilities, dict):
        return []
    values = capabilities.get("supportedCatalogIds")
    if not isinstance(values, list):
        return []
    return [value for value in values if isinstance(value, str) and value]


def catalog_id_matches_version(catalog_id: str, version: str) -> bool:
    return catalog_id in KNOWN_CATALOG_IDS_BY_VERSION.get(version, frozenset())


def v08_component_name(component_name: str) -> str:
    return V08_COMPONENT_RENAMES.get(component_name, component_name)


def _json_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    if value is None:
        return "null"
    return "unknown"


def _matching_schema(value: Any, schema: dict[str, Any]) -> dict[str, Any]:
    value_type = _json_type(value)
    for keyword in ("oneOf", "anyOf"):
        choices = schema.get(keyword)
        if not isinstance(choices, list):
            continue
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            choice_type = choice.get("type")
            if choice_type == value_type or (value_type == "number" and choice_type == "integer"):
                return choice
        for choice in choices:
            if isinstance(choice, dict) and choice.get("type") == "object":
                return choice
    return schema


def _literal_binding(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        if "path" in value:
            return {"path": value["path"]}
        for key in ("literalString", "literalNumber", "literalBoolean", "literalArray"):
            if key in value:
                return {key: deepcopy(value[key])}
    if isinstance(value, bool):
        return {"literalBoolean": value}
    if isinstance(value, str):
        return {"literalString": value}
    if isinstance(value, (int, float)):
        return {"literalNumber": value}
    if isinstance(value, list):
        return {"literalArray": deepcopy(value)}
    return {"literalString": str(value)}


def _convert_action(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    action = value.get("event") if isinstance(value.get("event"), dict) else value
    name = action.get("name")
    if not isinstance(name, str):
        return value
    converted: dict[str, Any] = {"name": name}
    context = action.get("context")
    if isinstance(context, dict):
        converted["context"] = [
            {"key": key, "value": _literal_binding(context_value)}
            for key, context_value in context.items()
        ]
    elif isinstance(context, list):
        converted["context"] = deepcopy(context)
    return converted


def _coerce_to_v08_schema(
    value: Any,
    schema: dict[str, Any],
) -> Any:
    schema = _matching_schema(value, schema)
    schema_type = schema.get("type")
    properties = schema.get("properties")

    if schema_type == "object" and isinstance(properties, dict):
        if isinstance(value, list) and "explicitList" in properties:
            return {"explicitList": deepcopy(value)}
        if (
            isinstance(value, dict)
            and "template" in properties
            and isinstance(value.get("componentId"), str)
            and isinstance(value.get("path"), str)
        ):
            return {
                "template": {
                    "componentId": value["componentId"],
                    "dataBinding": value["path"],
                }
            }
        if not isinstance(value, dict):
            for literal_key in (
                "literalString",
                "literalNumber",
                "literalBoolean",
                "literalArray",
            ):
                if literal_key in properties:
                    return _literal_binding(value)
            return value

        converted: dict[str, Any] = {}
        allow_extra = schema.get("additionalProperties", True) is not False
        for key, item in value.items():
            child_schema = properties.get(key)
            if isinstance(child_schema, dict):
                converted[key] = _coerce_to_v08_schema(item, child_schema)
            elif allow_extra:
                converted[key] = deepcopy(item)
        return converted

    if schema_type == "array" and isinstance(value, list):
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            return [_coerce_to_v08_schema(item, item_schema) for item in value]
    return deepcopy(value)


def convert_component_to_v08(
    component: dict[str, Any],
    component_schemas: dict[str, Any],
) -> dict[str, Any] | None:
    source_name = component.get("component")
    if not isinstance(source_name, str) or source_name in V08_UNSUPPORTED_COMPONENTS:
        return None

    target_name = v08_component_name(source_name)
    component_schema = component_schemas.get(target_name)
    if not isinstance(component_schema, dict):
        return None

    property_schemas = component_schema.get("properties", {})
    if not isinstance(property_schemas, dict):
        property_schemas = {}

    renames = V08_PROPERTY_RENAMES.get(source_name, {})
    dropped = V08_DROPPED_PROPERTIES.get(source_name, set())
    target_properties: dict[str, Any] = {}
    for source_key, source_value in component.items():
        if source_key in {"id", "component", "weight"} or source_key in dropped:
            continue
        target_key = renames.get(source_key, source_key)

        if source_name == "Button" and source_key == "variant":
            target_key = "primary"
            source_value = source_value == "primary"
        elif source_name == "ChoicePicker":
            if source_key == "value":
                target_key = "selections"
            elif source_key == "displayStyle":
                target_key = "variant"
            elif source_key == "variant":
                if source_value == "mutuallyExclusive":
                    target_properties["maxAllowedSelections"] = 1
                continue

        if target_key not in property_schemas:
            continue
        if (
            source_name == "Text"
            and target_key == "text"
            and isinstance(source_value, str)
        ):
            component_id = component.get("id")
            if component_id == "title1":
                source_value = source_value.replace("v0.9", "v0.8")
        if target_key == "children" and isinstance(source_value, list):
            source_value = {"explicitList": source_value}
        if target_key.endswith("Action") or target_key == "action":
            source_value = _convert_action(source_value)
        target_properties[target_key] = _coerce_to_v08_schema(
            source_value,
            property_schemas[target_key],
        )

    converted: dict[str, Any] = {
        "id": component.get("id"),
        "component": {target_name: target_properties},
    }
    if isinstance(component.get("weight"), (int, float)):
        converted["weight"] = component["weight"]
    return converted


def _v08_data_entry(key: str, value: Any) -> dict[str, Any] | None:
    if isinstance(value, bool):
        return {"key": key, "valueBoolean": value}
    if isinstance(value, str):
        return {"key": key, "valueString": value}
    if isinstance(value, (int, float)):
        return {"key": key, "valueNumber": value}
    return None


def _join_json_pointer(base_path: str, key: str) -> str:
    escaped_key = key.replace("~", "~0").replace("/", "~1")
    normalized_base = base_path.rstrip("/")
    if not normalized_base:
        return f"/{escaped_key}"
    return f"{normalized_base}/{escaped_key}"


def _convert_data_value_to_v08(
    *,
    surface_id: str,
    path: str,
    value: Any,
) -> list[dict[str, Any]]:
    """Convert scalar maps into v0.8 adjacency-list data-model updates.

    The v0.8 schema can represent scalar object properties but has no native
    array value entry. Array-only updates remain client-owned UI state.
    """
    operations: list[dict[str, Any]] = []
    if isinstance(value, dict):
        scalar_entries = [
            entry
            for key, item in value.items()
            for entry in [_v08_data_entry(key, item)]
            if entry is not None
        ]
        if scalar_entries:
            operations.append(
                {
                    "dataModelUpdate": {
                        "surfaceId": surface_id,
                        "path": path,
                        "contents": scalar_entries,
                    }
                }
            )
        for key, item in value.items():
            if isinstance(item, dict):
                operations.extend(
                    _convert_data_value_to_v08(
                        surface_id=surface_id,
                        path=_join_json_pointer(path, key),
                        value=item,
                    )
                )
        return operations

    entry_key = path.rstrip("/").rsplit("/", 1)[-1]
    parent_path = path.rstrip("/").rsplit("/", 1)[0] or "/"
    entry = _v08_data_entry(entry_key, value)
    if entry is not None:
        operations.append(
            {
                "dataModelUpdate": {
                    "surfaceId": surface_id,
                    "path": parent_path,
                    "contents": [entry],
                }
            }
        )
    return operations


def convert_operations_to_v08(
    operations: list[dict[str, Any]],
    *,
    catalog_id: str,
    component_schemas: dict[str, Any],
) -> list[dict[str, Any]]:
    """Convert deterministic v0.9 gallery operations to Agent Hub v0.8."""
    converted: list[dict[str, Any]] = []
    begin_by_surface: dict[str, dict[str, Any]] = {}
    root_by_surface: dict[str, str] = {}

    for operation in operations:
        create_surface = operation.get("createSurface")
        if isinstance(create_surface, dict):
            surface_id = create_surface.get("surfaceId")
            if isinstance(surface_id, str):
                begin_by_surface[surface_id] = {
                    "surfaceId": surface_id,
                    "catalogId": catalog_id,
                }
            continue

        update_components = operation.get("updateComponents")
        if isinstance(update_components, dict):
            surface_id = update_components.get("surfaceId")
            components = update_components.get("components")
            if not isinstance(surface_id, str) or not isinstance(components, list):
                continue
            converted_components = [
                converted_component
                for component in components
                if isinstance(component, dict)
                for converted_component in [
                    convert_component_to_v08(
                        component,
                        component_schemas,
                    )
                ]
                if converted_component is not None
            ]
            if not converted_components:
                continue
            if any(
                str(component.get("id", "")).startswith("tpl_previewMultipleChoice")
                for component in converted_components
            ):
                for converted_component in converted_components:
                    override = V08_TEXT_OVERRIDES.get(converted_component.get("id"))
                    text_component = converted_component.get("component", {}).get("Text")
                    if override and isinstance(text_component, dict):
                        text_component["text"] = {"literalString": override}
            first_id = converted_components[0].get("id")
            if isinstance(first_id, str):
                root_by_surface[surface_id] = first_id
            converted.append(
                {
                    "surfaceUpdate": {
                        "surfaceId": surface_id,
                        "components": converted_components,
                    }
                }
            )
            continue

        update_data_model = operation.get("updateDataModel")
        if isinstance(update_data_model, dict):
            surface_id = update_data_model.get("surfaceId")
            path = update_data_model.get("path", "/")
            if isinstance(surface_id, str) and isinstance(path, str):
                converted.extend(
                    _convert_data_value_to_v08(
                        surface_id=surface_id,
                        path=path or "/",
                        value=update_data_model.get("value"),
                    )
                )
            continue

        delete_surface = operation.get("deleteSurface")
        if isinstance(delete_surface, dict):
            converted.append({"deleteSurface": deepcopy(delete_surface)})

    for surface_id, begin_rendering in begin_by_surface.items():
        begin_rendering["root"] = root_by_surface.get(surface_id, "root")
        converted.append({"beginRendering": begin_rendering})
    return converted


class V08SurfaceBuilderAdapter:
    """Adapt every operation-producing SurfaceBuilder method to v0.8."""

    def __init__(self, builder: Any, *, catalog_id: str, component_schemas: dict[str, Any]):
        self._builder = builder
        self._catalog_id = catalog_id
        self._component_schemas = component_schemas

    def _convert(self, operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return convert_operations_to_v08(
            operations,
            catalog_id=self._catalog_id,
            component_schemas=self._component_schemas,
        )

    def build_surface_operations(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self._convert(self._builder.build_surface_operations(**kwargs))

    def build_chart_reference_controls_operations(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self._convert(
            self._builder.build_chart_reference_controls_operations(**kwargs)
        )
