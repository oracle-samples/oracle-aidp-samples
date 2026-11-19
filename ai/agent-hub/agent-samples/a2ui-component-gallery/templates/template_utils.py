from __future__ import annotations

import json
import math
import re
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any, Awaitable, Callable

from a2ui_sdk.parser import (
    A2UI_CLOSE_TAG,
    A2UI_OPEN_TAG,
    build_text_message,
    build_text_response_from_llm_text_with_repair,
)

from templates.template_selection import TemplateSelectionService

SUPPORTED_COMPONENTS = (
    "Text",
    "Image",
    "Icon",
    "Video",
    "AudioPlayer",
    "Modal",
    "Button",
    "CheckBox",
    "Slider",
    "Divider",
    "DateTimeInput",
    "Tabs",
    "ChoicePicker",
    "TextField",
    "Card",
    "List",
    "Row",
    "Column",
    "OAChart",
    "OACombobox",
    "OAListView",
    "OAActionCard",
    "OADataGrid",
    "OACollapsible",
    "OAProgressBar",
    "OAProgressCircle",
    "OAPopup",
    "OARadioSet",
    "OASwitch",
    "OATruncatingText",
)
CHART_TYPES = ("line", "bar", "area", "bubble", "funnel", "pie", "pyramid", "scatter")
CHART_ORIENTATIONS = ("horizontal", "vertical")
CHART_STACK_MODES = ("on", "off")
CHART_REFERENCE_OBJECT_TYPES = ("line", "bar", "area", "bubble", "scatter")
CHART_VERTICAL_REFERENCE_OBJECT_OPTIONS = ("line", "area", "none")
CHART_HORIZONTAL_REFERENCE_OBJECT_OPTIONS = ("line", "area", "variedLine", "variedArea", "none")
CHART_REFERENCE_LINE_STYLES = ("solid", "dashed", "dotted")
CHART_CATEGORICAL_X_AXIS_VALUES = {
    "line": ("Jan", "Feb", "Mar", "Apr"),
    "bar": ("Q1", "Q2"),
    "area": ("Week 1", "Week 2", "Week 3", "Week 4"),
}
CHART_NUMERIC_X_AXIS_TYPES = ("bubble", "scatter")

CHART_REFERENCE_OBJECT_CONTROL_IDS = (
    "tpl_chartVerticalReferenceObjectLabel1",
    "tpl_chartVerticalReferenceObjectSelector1",
    "tpl_chartVerticalReferenceLineValueLabel1",
    "tpl_chartVerticalReferenceLineValueInput1",
    "tpl_chartVerticalReferenceAreaLowLabel1",
    "tpl_chartVerticalReferenceAreaLowInput1",
    "tpl_chartVerticalReferenceAreaHighLabel1",
    "tpl_chartVerticalReferenceAreaHighInput1",
    "tpl_chartVerticalReferenceObjectColorInput1",
    "tpl_chartVerticalReferenceObjectLineStyleLabel1",
    "tpl_chartVerticalReferenceObjectLineStyleInput1",
    "tpl_chartHorizontalReferenceObjectLabel1",
    "tpl_chartHorizontalReferenceObjectSelector1",
    "tpl_chartHorizontalReferenceLineValueInput1",
    "tpl_chartHorizontalReferenceAreaLowInput1",
    "tpl_chartHorizontalReferenceAreaHighInput1",
    "tpl_chartHorizontalVariedReferenceLineDataInput1",
    "tpl_chartHorizontalVariedReferenceAreaDataInput1",
    "tpl_chartHorizontalReferenceObjectColorInput1",
    "tpl_chartHorizontalReferenceObjectLineStyleLabel1",
    "tpl_chartHorizontalReferenceObjectLineStyleInput1",
)
CHART_REFERENCE_OBJECT_SELECTOR_IDS = (
    "tpl_chartVerticalReferenceObjectLabel1",
    "tpl_chartVerticalReferenceObjectSelector1",
    "tpl_chartHorizontalReferenceObjectLabel1",
    "tpl_chartHorizontalReferenceObjectSelector1",
)

DATAGRID_MAX_ROW_OPTIONS = ("default", "3", "6", "9", "12")
COMPONENT_OPTIONS = [
    {"label": "Text", "value": "Text"},
    {"label": "Image", "value": "Image"},
    {"label": "Icon", "value": "Icon"},
    {"label": "Video", "value": "Video"},
    {"label": "AudioPlayer", "value": "AudioPlayer"},
    {"label": "Modal", "value": "Modal"},
    {"label": "Button", "value": "Button"},
    {"label": "CheckBox", "value": "CheckBox"},
    {"label": "Slider", "value": "Slider"},
    {"label": "Divider", "value": "Divider"},
    {"label": "DateTimeInput", "value": "DateTimeInput"},
    {"label": "Tabs", "value": "Tabs"},
    {"label": "ChoicePicker", "value": "ChoicePicker"},
    {"label": "TextField", "value": "TextField"},
    {"label": "Card", "value": "Card"},
    {"label": "List", "value": "List"},
    {"label": "Row", "value": "Row"},
    {"label": "Column", "value": "Column"},
    {"label": "OAChart", "value": "OAChart"},
    {"label": "OACombobox", "value": "OACombobox"},
    {"label": "OAListView", "value": "OAListView"},
    {"label": "OAActionCard", "value": "OAActionCard"},
    {"label": "OADataGrid", "value": "OADataGrid"},
    {"label": "OACollapsible", "value": "OACollapsible"},
    {"label": "OAProgressBar", "value": "OAProgressBar"},
    {"label": "OAProgressCircle", "value": "OAProgressCircle"},
    {"label": "OAPopup", "value": "OAPopup"},
    {"label": "OARadioSet", "value": "OARadioSet"},
    {"label": "OASwitch", "value": "OASwitch"},
    {"label": "OATruncatingText", "value": "OATruncatingText"},
]
CHART_TYPE_OPTIONS = [
    {"label": {"literalString": "Line"}, "value": "line"},
    {"label": {"literalString": "Bar"}, "value": "bar"},
    {"label": {"literalString": "Area"}, "value": "area"},
    {"label": {"literalString": "Bubble"}, "value": "bubble"},
    {"label": {"literalString": "Funnel"}, "value": "funnel"},
    {"label": {"literalString": "Pie"}, "value": "pie"},
    {"label": {"literalString": "Pyramid"}, "value": "pyramid"},
    {"label": {"literalString": "Scatter"}, "value": "scatter"},
]


def _load_chart_sample_data() -> dict[str, Any]:
    sample_path = Path(__file__).resolve().parent / "SampleData.json"
    try:
        with sample_path.open("r", encoding="utf-8") as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            return loaded
    except Exception:
        return {}
    return {}


CHART_SAMPLE_DATA = _load_chart_sample_data()
UNSUPPORTED_ACTION_MESSAGE = (
    "Only 'render_preview', 'update_chart_preview', 'update_chart_reference_controls', "
    "'update_grid_preview', 'preview_button_action', and 'preview_oaactioncard_action' are supported "
    "by this component gallery."
)
MISSING_SURFACE_ID_MESSAGE = "Missing surfaceId in action payload."
UNSUPPORTED_COMPONENTS_MESSAGE = (
    "This demo supports the standard components advertised by the negotiated A2UI catalog, plus OAChart, "
    "OAActionCard, OACombobox, OADataGrid, OAListView, OACollapsible, OAProgressBar, "
    "OAProgressCircle, OAPopup, OARadioSet, OASwitch, and OATruncatingText."
)


def _sanitize_chart_items_for_type(
    chart_type: str,
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Apply OAChart item-shape rules used by the low-code chart flow.

    - bubble/scatter items should use x/y(/z) coordinates and not value
    - non-bubble/non-scatter items should use value and not x/y/z
    """
    sanitized_items: list[dict[str, Any]] = []
    bubble_scatter = chart_type in ("bubble", "scatter")

    for item in items:
        if not isinstance(item, dict):
            continue
        normalized = deepcopy(item)

        if bubble_scatter:
            normalized.pop("value", None)
        else:
            normalized.pop("x", None)
            normalized.pop("y", None)
            normalized.pop("z", None)

        sanitized_items.append(normalized)

    return sanitized_items


def _apply_max_rows_to_datagrid_component(component: dict[str, Any], selected_data_grid_max_rows: str) -> None:
    """Apply the selected OADataGrid maxRows value to a copied preview component."""
    if component.get("component") != "OADataGrid":
        return

    if selected_data_grid_max_rows == "default":
        component.pop("maxRows", None)
        return

    try:
        component["maxRows"] = int(selected_data_grid_max_rows)
    except ValueError:
        component.pop("maxRows", None)


def _configured_oadatagrid_preview(
    *,
    preview_root: str,
    preview_components: list[dict[str, Any]],
    selected_data_grid_max_rows: str,
) -> tuple[str, list[dict[str, Any]]]:
    """Return OADataGrid preview components with the selected maxRows setting applied."""
    configured_components = deepcopy(preview_components)
    for component in configured_components:
        if isinstance(component, dict):
            _apply_max_rows_to_datagrid_component(component, selected_data_grid_max_rows)

    return preview_root, configured_components


def controls_components(
    *,
    selected_component: str,
    preview_variant_roots: list[str],
    preview_components: list[dict[str, Any]],
    component_options: list[dict[str, Any]],
    selected_chart_type: str = "line",
) -> list[dict[str, Any]]:
    root_children = [
        "title1",
        "componentChoice1",
        "renderPreviewButton1",
        "previewCard1",
    ]
    if selected_component == "Button":
        root_children.append("buttonContextCard1")
    if selected_component == "OAActionCard":
        root_children.append("oaActionCardContextCard1")

    components: list[dict[str, Any]] = [
        {
            "id": "root",
            "component": "Column",
            "children": root_children,
        },
        {
            "id": "title1",
            "component": "Text",
            "text": (
                "A2UI v0.9 Component Demo (all standard components plus "
                "OAChart, OAActionCard, OACombobox, OADataGrid, OAListView, OACollapsible, "
                "OAProgressBar, OAProgressCircle, OARadioSet, OASwitch, OATruncatingText)"
            ),
            "variant": "h4",
        },
        {
            "id": "componentChoice1",
            "component": "ChoicePicker",
            "variant": "mutuallyExclusive",
            "options": component_options,
            "value": {"path": "/demo/selections"},
        },
        {
            "id": "renderPreviewLabel1",
            "component": "Text",
            "text": "Render Preview",
            "variant": "body",
        },
        {
            "id": "renderPreviewButton1",
            "component": "Button",
            "child": "renderPreviewLabel1",
            "variant": "primary",
            "action": {
                "event": {
                    "name": "render_preview",
                    "context": {
                        "selectedComponent": {"path": "/demo/selections"},
                    },
                }
            },
        },
        {
            "id": "previewCard1",
            "component": "Card",
            "child": "previewContainer1",
        },
        {
            "id": "previewContainer1",
            "component": "Column",
            "children": preview_variant_roots,
        },
    ]

    if selected_component == "Button":
        components.extend(
            [
                {
                    "id": "buttonContextCard1",
                    "component": "Card",
                    "child": "buttonContextColumn1",
                },
                {
                    "id": "buttonContextColumn1",
                    "component": "Column",
                    "children": [
                        "buttonContextTitle1",
                        "buttonContextValue1",
                    ],
                },
                {
                    "id": "buttonContextTitle1",
                    "component": "Text",
                    "text": "Button action context",
                    "variant": "body",
                },
                {
                    "id": "buttonContextValue1",
                    "component": "Text",
                    "text": {"path": "/demo/buttonAction/explanation"},
                    "variant": "caption",
                },
            ]
        )

    if selected_component == "OAActionCard":
        components.extend(
            [
                {
                    "id": "oaActionCardContextCard1",
                    "component": "Card",
                    "child": "oaActionCardContextColumn1",
                },
                {
                    "id": "oaActionCardContextColumn1",
                    "component": "Column",
                    "children": [
                        "oaActionCardContextTitle1",
                        "oaActionCardContextValue1",
                    ],
                },
                {
                    "id": "oaActionCardContextTitle1",
                    "component": "Text",
                    "text": "OAActionCard action context",
                    "variant": "body",
                },
                {
                    "id": "oaActionCardContextValue1",
                    "component": "Text",
                    "text": {"path": "/demo/oaActionCardAction/explanation"},
                    "variant": "caption",
                },
            ]
        )

    components.extend(preview_components)
    return components


async def build_repaired_text_message_from_operations(
    *,
    operations: list[dict[str, Any]],
    user_query: str,
    schema_validate: Callable[[list[dict[str, Any]]], None],
    repair_async: Callable[[str], Awaitable[str]] | None = None,
    repair_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    llm_text = (
        f"{A2UI_OPEN_TAG}\n"
        f"{json.dumps(operations, ensure_ascii=True)}\n"
        f"{A2UI_CLOSE_TAG}"
    )
    return await build_text_response_from_llm_text_with_repair(
        llm_text,
        user_query=user_query,
        kwargs=repair_kwargs or {},
        validator=schema_validate,
        repair_async=repair_async,
        max_repair_attempts=1,
    )


class SurfaceBuilder:
    def __init__(self, template_selection: TemplateSelectionService, component_options: list[dict[str, Any]]) -> None:
        self.template_selection = template_selection
        self.component_options = component_options

    def build_surface_operations(
        self,
        *,
        surface_id: str,
        catalog_id: str,
        selected_component: str,
        include_begin: bool,
        button_action_explanation: str = "",
        oa_action_card_explanation: str = "",
        selected_chart_type: str = "line",
        selected_chart_orientation: str | None = None,
        selected_chart_stack: str | None = None,
        selected_chart_title: str | None = None,
        selected_chart_vertical_reference_object_option: str | None = None,
        selected_chart_vertical_reference_line_value: str | None = None,
        selected_chart_vertical_reference_area_low: str | None = None,
        selected_chart_vertical_reference_area_high: str | None = None,
        selected_chart_vertical_reference_object_color: str | None = None,
        selected_chart_vertical_reference_object_line_style: str | None = None,
        selected_chart_horizontal_reference_object_option: str | None = None,
        selected_chart_horizontal_reference_line_value: str | None = None,
        selected_chart_horizontal_reference_area_low: str | None = None,
        selected_chart_horizontal_reference_area_high: str | None = None,
        selected_chart_horizontal_varied_reference_line_data: str | None = None,
        selected_chart_horizontal_varied_reference_area_data: str | None = None,
        selected_chart_horizontal_reference_object_color: str | None = None,
        selected_chart_horizontal_reference_object_line_style: str | None = None,
        selected_data_grid_max_rows: str = "default",
        selected_oa_listview_dynamic_selections: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if selected_component not in self.template_selection.supported_components:
            selected_component = self.template_selection.default_component

        if selected_chart_type not in CHART_TYPES:
            selected_chart_type = "line"
        if selected_chart_orientation not in CHART_ORIENTATIONS:
            selected_chart_orientation = None
        if selected_chart_stack not in CHART_STACK_MODES:
            selected_chart_stack = None
        if selected_data_grid_max_rows not in DATAGRID_MAX_ROW_OPTIONS:
            selected_data_grid_max_rows = "default"

        preview_root, preview_components = self.template_selection.preview_components_for_component(selected_component)

        effective_oachart_title: str | None = None
        effective_oachart_orientation: str | None = None
        effective_oachart_stack: str | None = None
        effective_vertical_reference_object_option: str | None = None
        effective_vertical_reference_line_value: str | None = None
        effective_vertical_reference_area_low: str | None = None
        effective_vertical_reference_area_high: str | None = None
        effective_vertical_reference_object_color: str | None = None
        effective_vertical_reference_object_line_style: str | None = None
        effective_horizontal_reference_object_option: str | None = None
        effective_horizontal_reference_line_value: str | None = None
        effective_horizontal_reference_area_low: str | None = None
        effective_horizontal_reference_area_high: str | None = None
        effective_horizontal_varied_reference_line_data: str | None = None
        effective_horizontal_varied_reference_area_data: str | None = None
        effective_horizontal_reference_object_color: str | None = None
        effective_horizontal_reference_object_line_style: str | None = None

        if selected_component == "OAChart":
            selected_sample = CHART_SAMPLE_DATA.get(selected_chart_type)
            if not isinstance(selected_sample, dict):
                selected_sample = CHART_SAMPLE_DATA.get("line")
            if not isinstance(selected_sample, dict):
                selected_sample = {}

            sample_title = selected_sample.get("title")
            sample_orientation = selected_sample.get("orientation")
            sample_stack = selected_sample.get("stack")

            reference_sample = selected_sample if selected_chart_type in CHART_REFERENCE_OBJECT_TYPES else {}

            effective_vertical_reference_object_option = (
                selected_chart_vertical_reference_object_option
                if selected_chart_vertical_reference_object_option in CHART_VERTICAL_REFERENCE_OBJECT_OPTIONS
                else "line"
            )
            vertical_reference_line_default = _sample_chart_reference_object(reference_sample, axis_key="xAxis", option="line")
            vertical_reference_area_default = _sample_chart_reference_object(reference_sample, axis_key="xAxis", option="area")
            selected_vertical_reference_object_default = _sample_chart_reference_object(reference_sample, axis_key="xAxis", option=effective_vertical_reference_object_option)

            effective_vertical_reference_line_value = _chart_reference_object_value_to_text(vertical_reference_line_default.get("value"))
            effective_vertical_reference_area_low = _chart_reference_object_value_to_text(vertical_reference_area_default.get("low"))
            effective_vertical_reference_area_high = _chart_reference_object_value_to_text(vertical_reference_area_default.get("high"))
            effective_vertical_reference_object_color, effective_vertical_reference_object_line_style = _chart_reference_object_style_defaults(selected_vertical_reference_object_default)
            if effective_vertical_reference_object_color is None:
                effective_vertical_reference_object_color, effective_vertical_reference_object_line_style = _chart_reference_object_style_defaults(vertical_reference_line_default)

            effective_horizontal_reference_object_option = (
                selected_chart_horizontal_reference_object_option
                if selected_chart_horizontal_reference_object_option in CHART_HORIZONTAL_REFERENCE_OBJECT_OPTIONS
                else "line"
            )
            horizontal_reference_line_default = _sample_chart_reference_object(reference_sample, axis_key="yAxis", option="line")
            horizontal_reference_area_default = _sample_chart_reference_object(reference_sample, axis_key="yAxis", option="area")
            horizontal_varied_reference_line_default = _sample_chart_reference_object(reference_sample, axis_key="yAxis", option="variedLine")
            horizontal_varied_reference_area_default = _sample_chart_reference_object(reference_sample, axis_key="yAxis", option="variedArea")
            selected_horizontal_reference_object_default = _sample_chart_reference_object(reference_sample, axis_key="yAxis", option=effective_horizontal_reference_object_option)

            effective_horizontal_reference_line_value = _chart_reference_object_value_to_text(horizontal_reference_line_default.get("value"))
            effective_horizontal_reference_area_low = _chart_reference_object_value_to_text(horizontal_reference_area_default.get("low"))
            effective_horizontal_reference_area_high = _chart_reference_object_value_to_text(horizontal_reference_area_default.get("high"))
            effective_horizontal_varied_reference_line_data = _chart_reference_object_items_to_text(horizontal_varied_reference_line_default, value_keys=("value",))
            effective_horizontal_varied_reference_area_data = _chart_reference_object_items_to_text(horizontal_varied_reference_area_default, value_keys=("low", "high"))
            effective_horizontal_reference_object_color, effective_horizontal_reference_object_line_style = _chart_reference_object_style_defaults(selected_horizontal_reference_object_default)
            if effective_horizontal_reference_object_color is None:
                effective_horizontal_reference_object_color, effective_horizontal_reference_object_line_style = _chart_reference_object_style_defaults(horizontal_reference_line_default)

            if isinstance(sample_title, str):
                effective_oachart_title = sample_title
            if isinstance(sample_orientation, str) and sample_orientation in CHART_ORIENTATIONS:
                effective_oachart_orientation = sample_orientation
            if isinstance(sample_stack, str) and sample_stack in CHART_STACK_MODES:
                effective_oachart_stack = sample_stack

            if isinstance(selected_chart_title, str):
                effective_oachart_title = selected_chart_title
            if selected_chart_orientation in CHART_ORIENTATIONS:
                effective_oachart_orientation = selected_chart_orientation
            if selected_chart_stack in CHART_STACK_MODES:
                effective_oachart_stack = selected_chart_stack
            if isinstance(selected_chart_vertical_reference_line_value, str):
                effective_vertical_reference_line_value = selected_chart_vertical_reference_line_value
            if isinstance(selected_chart_vertical_reference_area_low, str):
                effective_vertical_reference_area_low = selected_chart_vertical_reference_area_low
            if isinstance(selected_chart_vertical_reference_area_high, str):
                effective_vertical_reference_area_high = selected_chart_vertical_reference_area_high
            if isinstance(selected_chart_vertical_reference_object_color, str) and selected_chart_vertical_reference_object_color.strip():
                effective_vertical_reference_object_color = selected_chart_vertical_reference_object_color.strip()
            if selected_chart_vertical_reference_object_line_style in CHART_REFERENCE_LINE_STYLES:
                effective_vertical_reference_object_line_style = selected_chart_vertical_reference_object_line_style
            if isinstance(selected_chart_horizontal_reference_line_value, str):
                effective_horizontal_reference_line_value = selected_chart_horizontal_reference_line_value
            if isinstance(selected_chart_horizontal_reference_area_low, str):
                effective_horizontal_reference_area_low = selected_chart_horizontal_reference_area_low
            if isinstance(selected_chart_horizontal_reference_area_high, str):
                effective_horizontal_reference_area_high = selected_chart_horizontal_reference_area_high
            if isinstance(selected_chart_horizontal_varied_reference_line_data, str):
                effective_horizontal_varied_reference_line_data = selected_chart_horizontal_varied_reference_line_data
            if isinstance(selected_chart_horizontal_varied_reference_area_data, str):
                effective_horizontal_varied_reference_area_data = selected_chart_horizontal_varied_reference_area_data
            if isinstance(selected_chart_horizontal_reference_object_color, str) and selected_chart_horizontal_reference_object_color.strip():
                effective_horizontal_reference_object_color = selected_chart_horizontal_reference_object_color.strip()
            if selected_chart_horizontal_reference_object_line_style in CHART_REFERENCE_LINE_STYLES:
                effective_horizontal_reference_object_line_style = selected_chart_horizontal_reference_object_line_style

            preview_root, preview_components = _configured_oachart_preview(
                preview_root=preview_root,
                preview_components=preview_components,
                selected_chart_type=selected_chart_type,
                selected_chart_orientation=selected_chart_orientation,
                selected_chart_stack=selected_chart_stack,
                selected_chart_title=selected_chart_title,
                selected_chart_vertical_reference_object_option=effective_vertical_reference_object_option,
                selected_chart_vertical_reference_line_value=effective_vertical_reference_line_value,
                selected_chart_vertical_reference_area_low=effective_vertical_reference_area_low,
                selected_chart_vertical_reference_area_high=effective_vertical_reference_area_high,
                selected_chart_vertical_reference_object_color=effective_vertical_reference_object_color,
                selected_chart_vertical_reference_object_line_style=effective_vertical_reference_object_line_style,
                selected_chart_horizontal_reference_object_option=effective_horizontal_reference_object_option,
                selected_chart_horizontal_reference_line_value=effective_horizontal_reference_line_value,
                selected_chart_horizontal_reference_area_low=effective_horizontal_reference_area_low,
                selected_chart_horizontal_reference_area_high=effective_horizontal_reference_area_high,
                selected_chart_horizontal_varied_reference_line_data=effective_horizontal_varied_reference_line_data,
                selected_chart_horizontal_varied_reference_area_data=effective_horizontal_varied_reference_area_data,
                selected_chart_horizontal_reference_object_color=effective_horizontal_reference_object_color,
                selected_chart_horizontal_reference_object_line_style=effective_horizontal_reference_object_line_style,
            )

        if selected_component == "OADataGrid":
            preview_root, preview_components = _configured_oadatagrid_preview(
                preview_root=preview_root,
                preview_components=preview_components,
                selected_data_grid_max_rows=selected_data_grid_max_rows,
            )

        preview_variant_roots = [preview_root]

        components = controls_components(
            selected_component=selected_component,
            preview_variant_roots=preview_variant_roots,
            preview_components=preview_components,
            component_options=self.component_options,
            selected_chart_type=selected_chart_type,
        )

        required_operations = self.template_selection.required_operations_for_component(selected_component)
        operations: list[dict[str, Any]] = []
        has_selections_update = False
        has_button_action_update = False
        has_oa_action_card_action_update = False
        for operation in required_operations:
            create_surface = operation.get("createSurface")
            if isinstance(create_surface, dict):
                if not include_begin:
                    continue
                create_surface["surfaceId"] = surface_id
                create_surface["catalogId"] = catalog_id
                operations.append({"version": "v0.9", "createSurface": create_surface})
                continue

            update_data_model = operation.get("updateDataModel")
            if isinstance(update_data_model, dict):
                update_data_model["surfaceId"] = surface_id
                path = update_data_model.get("path")

                if path == "/demo/selections":
                    has_selections_update = True
                    update_data_model["value"] = [selected_component]

                if path == "/demo/buttonAction":
                    has_button_action_update = True
                    value = update_data_model.get("value")
                    if isinstance(value, dict):
                        value["explanation"] = button_action_explanation
                if path == "/demo/oaActionCardAction":
                    has_oa_action_card_action_update = True
                    value = update_data_model.get("value")
                    if isinstance(value, dict):
                        value["explanation"] = oa_action_card_explanation
                if path == "/demo/oachart" and selected_component == "OAChart":
                    value = update_data_model.get("value")
                    if isinstance(value, dict):
                        if isinstance(effective_oachart_title, str):
                            value["title"] = effective_oachart_title
                        value["selectedTypes"] = [selected_chart_type]
                        value["currentType"] = selected_chart_type
                        value["useHorizontal"] = effective_oachart_orientation == "horizontal"
                        value["useStack"] = effective_oachart_stack == "on"
                        value["verticalReferenceObjectTypes"] = [effective_vertical_reference_object_option]
                        value["verticalReferenceObjectType"] = effective_vertical_reference_object_option
                        value["verticalReferenceObjectColor"] = effective_vertical_reference_object_color
                        if effective_vertical_reference_object_line_style:
                            value["verticalReferenceObjectLineStyle"] = [effective_vertical_reference_object_line_style]
                        if selected_chart_type in CHART_NUMERIC_X_AXIS_TYPES:
                            value["verticalReferenceLineValue"] = effective_vertical_reference_line_value
                            value["verticalReferenceAreaLow"] = effective_vertical_reference_area_low
                            value["verticalReferenceAreaHigh"] = effective_vertical_reference_area_high
                        else:
                            value["verticalReferenceLineValue"] = (
                                [effective_vertical_reference_line_value] if effective_vertical_reference_line_value else []
                            )
                            value["verticalReferenceAreaLow"] = (
                                [effective_vertical_reference_area_low] if effective_vertical_reference_area_low else []
                            )
                            value["verticalReferenceAreaHigh"] = (
                                [effective_vertical_reference_area_high] if effective_vertical_reference_area_high else []
                            )
                        value["horizontalReferenceObjectTypes"] = [effective_horizontal_reference_object_option]
                        value["horizontalReferenceObjectType"] = effective_horizontal_reference_object_option
                        value["horizontalReferenceLineValue"] = effective_horizontal_reference_line_value
                        value["horizontalReferenceAreaLow"] = effective_horizontal_reference_area_low
                        value["horizontalReferenceAreaHigh"] = effective_horizontal_reference_area_high
                        value["horizontalVariedReferenceLineData"] = effective_horizontal_varied_reference_line_data
                        value["horizontalVariedReferenceAreaData"] = effective_horizontal_varied_reference_area_data
                        value["horizontalReferenceObjectColor"] = effective_horizontal_reference_object_color
                        if effective_horizontal_reference_object_line_style:
                            value["horizontalReferenceObjectLineStyle"] = [effective_horizontal_reference_object_line_style]
                if path == "/demo/oadatagrid" and selected_component == "OADataGrid":
                    value = update_data_model.get("value")
                    if isinstance(value, dict):
                        value["selectedMaxRows"] = [selected_data_grid_max_rows]
                if (
                    path == "/demo/oaListView"
                    and selected_component == "OAListView"
                    and isinstance(selected_oa_listview_dynamic_selections, list)
                ):
                    value = update_data_model.get("value")
                    if isinstance(value, dict):
                        value["dynamicSelections"] = selected_oa_listview_dynamic_selections
                operations.append({"version": "v0.9", "updateDataModel": update_data_model})
                continue

            update_components = operation.get("updateComponents")
            if isinstance(update_components, dict):
                update_components["surfaceId"] = surface_id
                update_components["components"] = components
                operations.append({"version": "v0.9", "updateComponents": update_components})
                continue

            delete_surface = operation.get("deleteSurface")
            if isinstance(delete_surface, dict):
                delete_surface["surfaceId"] = surface_id
                operations.append({"version": "v0.9", "deleteSurface": delete_surface})
                continue

        if not has_selections_update:
            operations.append(
                {
                    "version": "v0.9",
                    "updateDataModel": {
                        "surfaceId": surface_id,
                        "path": "/demo/selections",
                        "value": [selected_component],
                    },
                }
            )

        if selected_component == "Button" and not has_button_action_update:
            operations.append(
                {
                    "version": "v0.9",
                    "updateDataModel": {
                        "surfaceId": surface_id,
                        "path": "/demo/buttonAction",
                        "value": {"explanation": button_action_explanation},
                    },
                }
            )

        if selected_component == "OAActionCard" and not has_oa_action_card_action_update:
            operations.append(
                {
                    "version": "v0.9",
                    "updateDataModel": {
                        "surfaceId": surface_id,
                        "path": "/demo/oaActionCardAction",
                        "value": {"explanation": oa_action_card_explanation},
                    },
                }
            )

        return operations

    def build_chart_reference_controls_operations(
        self,
        *,
        surface_id: str,
        selected_chart_type: str,
        selected_chart_vertical_reference_object_option: str,
        selected_chart_horizontal_reference_object_option: str,
        selected_chart_reference_object_axis: str | None = None,
    ) -> list[dict[str, Any]]:
        """Build an incremental update containing the active OAChart reference controls."""
        chart_type = selected_chart_type if selected_chart_type in CHART_TYPES else "line"
        vertical_reference_object_option = (
            selected_chart_vertical_reference_object_option
            if selected_chart_vertical_reference_object_option in CHART_VERTICAL_REFERENCE_OBJECT_OPTIONS
            else "line"
        )
        horizontal_reference_object_option = (
            selected_chart_horizontal_reference_object_option
            if selected_chart_horizontal_reference_object_option in CHART_HORIZONTAL_REFERENCE_OBJECT_OPTIONS
            else "line"
        )
        _, preview_components = self.template_selection.preview_components_for_component("OAChart")
        configured_components = _oachart_components_with_active_reference_controls(
            preview_components=preview_components,
            chart_type=chart_type,
            vertical_reference_object_option=vertical_reference_object_option,
            horizontal_reference_object_option=horizontal_reference_object_option,
        )
        reference_component_ids = {"tpl_variant_wrapper_chart", *CHART_REFERENCE_OBJECT_CONTROL_IDS}
        active_reference_control_components = [
            component
            for component in configured_components
            if component.get("id") in reference_component_ids
        ]

        operations = [
            {
                "version": "v0.9",
                "updateComponents": {
                    "surfaceId": surface_id,
                    "components": active_reference_control_components,
                },
            }
        ]
        reference_sample = CHART_SAMPLE_DATA.get(chart_type)
        if chart_type not in CHART_REFERENCE_OBJECT_TYPES or not isinstance(reference_sample, dict):
            reference_sample = {}

        selected_reference_object_option = None
        selected_color_path = None
        selected_axis_key = None
        if selected_chart_reference_object_axis == "vertical":
            selected_reference_object_option = vertical_reference_object_option
            selected_color_path = "/demo/oachart/verticalReferenceObjectColor"
            selected_axis_key = "xAxis"
        elif selected_chart_reference_object_axis == "horizontal":
            selected_reference_object_option = horizontal_reference_object_option
            selected_color_path = "/demo/oachart/horizontalReferenceObjectColor"
            selected_axis_key = "yAxis"

        selected_reference_object_default: dict[str, Any] = {}
        if selected_reference_object_option is not None and selected_axis_key is not None:
            selected_reference_object_default = _sample_chart_reference_object(reference_sample, axis_key=selected_axis_key, option=selected_reference_object_option)
        selected_color, _ = _chart_reference_object_style_defaults(selected_reference_object_default)
        if selected_color_path is not None and selected_color is not None:
            operations.append(
                {
                    "version": "v0.9",
                    "updateDataModel": {
                        "surfaceId": surface_id,
                        "path": selected_color_path,
                        "value": selected_color,
                    },
                }
            )

        return operations


def wants_ui(user_query: str) -> bool:
    lowered = user_query.lower()
    return any(
        re.search(rf"(?<!\w){re.escape(token)}(?!\w)", lowered)
        for token in [
            "demo",
            "gallery",
            "ui",
            "a2ui",
            "component",
            "preview",
            "button",
            "text",
            "image",
            "images",
            "icon",
            "video",
            "videos",
            "audio",
            "audioplayer",
            "audio player",
            "modal",
            "modals",
            "icons",
            "checkbox",
            "slider",
            "divider",
            "datetime",
            "date time",
            "tabs",
            "tab",
            "multiple choice",
            "multiplechoice",
            "choice picker",
            "choicepicker",
            "combo box",
            "combobox",
            "oa combobox",
            "oacombobox",
            "oa list view",
            "oa listview",
            "oalistview",
            "dropdown",
            "textfield",
            "text field",
            "card",
            "list",
            "row",
            "rows",
            "column",
            "columns",
            "chart",
            "charts",
            "oachart",
            "oa chart",
            "oaactioncard",
            "oa action card",
            "oacollapsible",
            "oa collapsible",
            "collapsible",
            "datagrid",
            "data grid",
            "progressbar",
            "progress bar",
            "progresscircle",
            "progress circle",
            "radio",
            "radioset",
            "radio set",
            "oa radio set",
            "oaradioset",
            "switch",
            "switches",
            "oaswitch",
            "oa switch",
            "truncating",
            "truncatingtext",
            "truncating text",
            "oatruncatingtext",
            "oa truncating text",
            "action card",
        ]
    )


def is_help_query(user_query: str) -> bool:
    """Recognize common capability questions without invoking the LLM."""
    normalized = " ".join(str(user_query or "").lower().strip(" ?.!").split())
    return normalized in {
        "help",
        "what can i ask",
        "what can i ask you",
        "what can you do",
        "what can you help me with",
        "how can you help",
        "how can you help me",
        "tell me what you can do",
        "show me what you can do",
        "what are your capabilities",
    }


def is_greeting_query(user_query: str) -> bool:
    """Recognize short greetings that should receive an immediate welcome."""
    normalized = " ".join(str(user_query or "").lower().strip(" ?.!").split())
    return normalized in {
        "hi",
        "hello",
        "hey",
        "hi there",
        "hello there",
        "good morning",
        "good afternoon",
        "good evening",
    }


def normalize_component_name(value: Any, supported_components: tuple[str, ...]) -> str | None:
    if isinstance(value, list) and value:
        return normalize_component_name(value[0], supported_components)

    if isinstance(value, dict):
        literal_array = value.get("literalArray")
        if isinstance(literal_array, list) and literal_array:
            return normalize_component_name(literal_array[0], supported_components)
        for candidate_key in ("literalString", "valueString"):
            candidate = value.get(candidate_key)
            if isinstance(candidate, str):
                return normalize_component_name(candidate, supported_components)
        return None

    if not isinstance(value, str):
        return None
    normalized = value.strip()
    aliases = {
        "multiple choice": "ChoicePicker",
        "multiplechoice": "ChoicePicker",
        "text field": "TextField",
        "textfield": "TextField",
        "rows": "Row",
        "columns": "Column",
        "images": "Image",
        "icons": "Icon",
        "combo box": "OACombobox",
        "combobox": "OACombobox",
        "oa combobox": "OACombobox",
        "oacombobox": "OACombobox",
        "oa list view": "OAListView",
        "oa listview": "OAListView",
        "oalistview": "OAListView",
        "listview": "OAListView",
        "dropdown": "OACombobox",
        "chart": "OAChart",
        "charts": "OAChart",
        "oa chart": "OAChart",
        "oachart": "OAChart",
        "oa action card": "OAActionCard",
        "oaactioncard": "OAActionCard",
        "oa collapsible": "OACollapsible",
        "oacollapsible": "OACollapsible",
        "collapsible": "OACollapsible",
        "data grid": "OADataGrid",
        "datagrid": "OADataGrid",
        "progress bar": "OAProgressBar",
        "progressbar": "OAProgressBar",
        "progress circle": "OAProgressCircle",
        "progresscircle": "OAProgressCircle",
        "popup": "OAPopup",
        "pop up": "OAPopup",
        "oa popup": "OAPopup",
        "oapopup": "OAPopup",
        "radio": "OARadioSet",
        "radioset": "OARadioSet",
        "radio set": "OARadioSet",
        "oa radio set": "OARadioSet",
        "oaradioset": "OARadioSet",
        "switch": "OASwitch",
        "switches": "OASwitch",
        "oa switch": "OASwitch",
        "oaswitch": "OASwitch",
        "truncating": "OATruncatingText",
        "truncatingtext": "OATruncatingText",
        "truncating text": "OATruncatingText",
        "oa truncating text": "OATruncatingText",
        "oatruncatingtext": "OATruncatingText",
        "videos": "Video",
        "audio": "AudioPlayer",
        "audio player": "AudioPlayer",
        "audioplayer": "AudioPlayer",
        "modals": "Modal",
        "action card": "OAActionCard",
        "choicepicker": "ChoicePicker",
        "choice picker": "ChoicePicker",
    }
    aliased = aliases.get(normalized.lower())
    if aliased in supported_components:
        return aliased

    for component in supported_components:
        if normalized.lower() == component.lower():
            return component
    return None


def extract_requested_component_from_action(
    action: dict[str, Any], supported_components: tuple[str, ...]
) -> str | None:
    # Some Agent Hub clients resolve path-backed action context into a
    # top-level field. MultipleChoice/ChoicePicker values arrive as a
    # one-element array in that shape.
    normalized = normalize_component_name(
        action.get("selectedComponent"), supported_components
    )
    if normalized:
        return normalized

    context = action.get("context")

    # Runtime event shape can be either:
    # 1) dict, e.g. {"selectedComponent": "Button"}
    # 2) list of key/value items, e.g. [{"key": "selectedComponent", "value": "Button"}]
    if isinstance(context, dict):
        direct_value = context.get("selectedComponent")
        normalized = normalize_component_name(direct_value, supported_components)
        if normalized:
            return normalized

    if not isinstance(context, list):
        return None

    for item in context:
        if not isinstance(item, dict) or item.get("key") != "selectedComponent":
            continue

        raw_value = item.get("value")
        normalized = normalize_component_name(raw_value, supported_components)
        if normalized:
            return normalized

        if isinstance(raw_value, dict):
            for candidate_key in ("literalString", "valueString"):
                normalized = normalize_component_name(raw_value.get(candidate_key), supported_components)
                if normalized:
                    return normalized

    return None


def _normalize_option_value(value: Any, allowed_values: tuple[str, ...]) -> str | None:
    if isinstance(value, str):
        normalized = value.strip().lower()
        for allowed in allowed_values:
            if normalized == allowed.lower():
                return allowed
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, str):
            normalized = first.strip().lower()
            for allowed in allowed_values:
                if normalized == allowed.lower():
                    return allowed
    return None


def _normalize_bool_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "1", "yes", "on", "enabled"):
            return True
        if normalized in ("false", "0", "no", "off", "disabled"):
            return False

    if isinstance(value, list) and value:
        return _normalize_bool_value(value[0])

    return None


def extract_requested_option_from_action(
    action: dict[str, Any],
    *,
    option_key: str,
    allowed_values: tuple[str, ...],
) -> str | None:
    context = action.get("context")

    if isinstance(context, dict):
        direct_value = context.get(option_key)
        normalized = _normalize_option_value(direct_value, allowed_values)
        if normalized:
            return normalized
        if isinstance(direct_value, dict):
            literal_array = direct_value.get("literalArray")
            normalized = _normalize_option_value(literal_array, allowed_values)
            if normalized:
                return normalized
            for candidate_key in ("literalString", "valueString"):
                normalized = _normalize_option_value(direct_value.get(candidate_key), allowed_values)
                if normalized:
                    return normalized

    if not isinstance(context, list):
        return None

    for item in context:
        if not isinstance(item, dict) or item.get("key") != option_key:
            continue

        raw_value = item.get("value")
        normalized = _normalize_option_value(raw_value, allowed_values)
        if normalized:
            return normalized

        if isinstance(raw_value, dict):
            literal_array = raw_value.get("literalArray")
            normalized = _normalize_option_value(literal_array, allowed_values)
            if normalized:
                return normalized
            for candidate_key in ("literalString", "valueString"):
                normalized = _normalize_option_value(raw_value.get(candidate_key), allowed_values)
                if normalized:
                    return normalized

    return None


def extract_requested_boolean_from_action(
    action: dict[str, Any],
    *,
    option_key: str,
) -> bool | None:
    context = action.get("context")

    if isinstance(context, dict):
        direct_value = context.get(option_key)
        normalized = _normalize_bool_value(direct_value)
        if normalized is not None:
            return normalized
        if isinstance(direct_value, dict):
            for candidate_key in ("literalBoolean", "valueBoolean", "literalString", "valueString"):
                normalized = _normalize_bool_value(direct_value.get(candidate_key))
                if normalized is not None:
                    return normalized

    if not isinstance(context, list):
        return None

    for item in context:
        if not isinstance(item, dict) or item.get("key") != option_key:
            continue

        raw_value = item.get("value")
        normalized = _normalize_bool_value(raw_value)
        if normalized is not None:
            return normalized

        if isinstance(raw_value, dict):
            for candidate_key in ("literalBoolean", "valueBoolean", "literalString", "valueString"):
                normalized = _normalize_bool_value(raw_value.get(candidate_key))
                if normalized is not None:
                    return normalized

    return None


def extract_requested_string_from_action(
    action: dict[str, Any],
    *,
    option_key: str,
) -> str | None:
    context = action.get("context")

    if isinstance(context, dict):
        direct_value = context.get(option_key)
        if isinstance(direct_value, str):
            return direct_value
        if isinstance(direct_value, dict):
            for candidate_key in ("literalString", "valueString"):
                candidate = direct_value.get(candidate_key)
                if isinstance(candidate, str):
                    return candidate

    if not isinstance(context, list):
        return None

    for item in context:
        if not isinstance(item, dict) or item.get("key") != option_key:
            continue

        raw_value = item.get("value")
        if isinstance(raw_value, str):
            return raw_value
        if isinstance(raw_value, dict):
            for candidate_key in ("literalString", "valueString"):
                candidate = raw_value.get(candidate_key)
                if isinstance(candidate, str):
                    return candidate

    return None


def _normalize_string_array_value(value: Any) -> list[str] | None:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]

    if isinstance(value, str):
        return [value]

    if isinstance(value, dict):
        literal_array = value.get("literalArray")
        if isinstance(literal_array, list):
            return [item for item in literal_array if isinstance(item, str)]

        value_array = value.get("valueArray")
        if isinstance(value_array, list):
            return [item for item in value_array if isinstance(item, str)]

    return None


def extract_requested_string_array_from_action(
    action: dict[str, Any],
    *,
    option_key: str,
) -> list[str] | None:
    context = action.get("context")

    if isinstance(context, dict):
        normalized = _normalize_string_array_value(context.get(option_key))
        if normalized is not None:
            return normalized

    if not isinstance(context, list):
        return None

    for item in context:
        if not isinstance(item, dict) or item.get("key") != option_key:
            continue

        normalized = _normalize_string_array_value(item.get("value"))
        if normalized is not None:
            return normalized

    return None

def _extract_requested_chart_x_axis_value(
    action: dict[str, Any],
    *,
    option_key: str,
    chart_type: str,
) -> str | None:
    categorical_values = CHART_CATEGORICAL_X_AXIS_VALUES.get(chart_type)
    if categorical_values:
        requested_value = extract_requested_option_from_action(
            action,
            option_key=option_key,
            allowed_values=categorical_values,
        )
    else:
        requested_value = extract_requested_string_from_action(action, option_key=option_key)
        if _parse_chart_reference_object_number(requested_value) is None:
            requested_value = None

    return requested_value

def _parse_chart_reference_object_number(value: str | None) -> int | float | None:
    parsed_value: int | float | None = None
    if isinstance(value, str) and value.strip():
        try:
            number_value = float(value)
            if math.isfinite(number_value):
                parsed_value = int(number_value) if number_value.is_integer() else number_value
        except ValueError:
            parsed_value = None

    return parsed_value


def _parse_chart_reference_object_x_value(
    value: str,
    *,
    chart_type: str,
) -> str | int | float:
    parsed_value: str | int | float | None = _parse_chart_reference_object_number(value)
    if parsed_value is None:
        categorical_values = CHART_CATEGORICAL_X_AXIS_VALUES.get(chart_type)
        if categorical_values and value in categorical_values:
            parsed_value = categorical_values.index(value)
        else:
            parsed_value = value

    return parsed_value


def _parse_chart_varied_reference_line_data(
    value: str | None,
    *,
    chart_type: str,
) -> list[dict[str, Any]] | None:
    """Parse comma-separated value or x:value entries for a varied reference line."""
    parsed_items: list[dict[str, Any]] | None = None
    if isinstance(value, str) and value.strip():
        candidate_items: list[dict[str, Any]] = []
        is_valid = True
        for entry in value.split(","):
            parts = [part.strip() for part in entry.split(":")]
            if len(parts) not in (1, 2) or (len(parts) == 2 and not parts[0]):
                is_valid = False
                continue
            parsed_value = _parse_chart_reference_object_number(parts[-1])
            if parsed_value is None:
                is_valid = False
                continue
            candidate_item: dict[str, Any] = {"value": parsed_value}
            if len(parts) == 2:
                candidate_item["x"] = _parse_chart_reference_object_x_value(parts[0], chart_type=chart_type)
            candidate_items.append(candidate_item)
        if is_valid and candidate_items:
            parsed_items = candidate_items

    return parsed_items


def _parse_chart_varied_reference_area_data(
    value: str | None,
    *,
    chart_type: str,
) -> list[dict[str, Any]] | None:
    """Parse comma-separated low:high or x:low:high entries for a varied reference area."""
    parsed_items: list[dict[str, Any]] | None = None
    if isinstance(value, str) and value.strip():
        candidate_items: list[dict[str, Any]] = []
        is_valid = True
        for entry in value.split(","):
            parts = [part.strip() for part in entry.split(":")]
            if len(parts) not in (2, 3) or (len(parts) == 3 and not parts[0]):
                is_valid = False
                continue
            parsed_low = _parse_chart_reference_object_number(parts[-2])
            parsed_high = _parse_chart_reference_object_number(parts[-1])
            if parsed_low is None or parsed_high is None:
                is_valid = False
                continue
            candidate_item: dict[str, Any] = {"low": parsed_low, "high": parsed_high}
            if len(parts) == 3:
                candidate_item["x"] = _parse_chart_reference_object_x_value(parts[0], chart_type=chart_type)
            candidate_items.append(candidate_item)
        if is_valid and candidate_items:
            parsed_items = candidate_items

    return parsed_items


def _sample_chart_reference_object(
    sample: dict[str, Any],
    *,
    axis_key: str,
    option: str,
) -> dict[str, Any]:
    """Return the sample reference object matching the axis and reference object option."""
    selected_reference_object: dict[str, Any] = {}
    reference_object_type = None
    if option in ("line", "variedLine"):
        reference_object_type = "line"
    elif option in ("area", "variedArea"):
        reference_object_type = "area"

    axis = sample.get(axis_key)
    if reference_object_type is not None and isinstance(axis, dict):
        reference_objects = axis.get("referenceObjects")
        if isinstance(reference_objects, list):
            expects_items = option in ("variedLine", "variedArea")
            for reference_object in reference_objects:
                if not isinstance(reference_object, dict):
                    continue
                has_items = isinstance(reference_object.get("items"), list)
                if reference_object.get("type") == reference_object_type and has_items == expects_items:
                    selected_reference_object = reference_object
                    break

    return selected_reference_object


def _chart_reference_object_value_to_text(value: Any) -> str:
    """Return a reference object scalar value in the demo control's text format."""
    string_value = ""
    if isinstance(value, str):
        string_value = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        string_value = str(value)
    return string_value


def _chart_reference_object_items_to_text(
    reference_object: dict[str, Any],
    *,
    value_keys: tuple[str, ...],
) -> str:
    """Return reference object items in the demo's comma-separated text format."""
    entries: list[str] = []
    items = reference_object.get("items")
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            parts: list[str] = []
            is_valid = True
            if "x" in item:
                x_value = _chart_reference_object_value_to_text(item.get("x"))
                if x_value:
                    parts.append(x_value)
                else:
                    is_valid = False
            for value_key in value_keys:
                item_value = _chart_reference_object_value_to_text(item.get(value_key))
                if item_value:
                    parts.append(item_value)
                else:
                    is_valid = False
            if is_valid:
                entries.append(":".join(parts))
    return ",".join(entries)


def _chart_reference_object_style_defaults(
    reference_object: dict[str, Any],
) -> tuple[str | None, str | None]:
    """Return the color and line style from a sample reference object."""
    color = reference_object.get("color")
    if not isinstance(color, str) or not color.strip():
        color = None

    line_style = reference_object.get("lineStyle")
    if line_style not in CHART_REFERENCE_LINE_STYLES:
        line_style = None

    return color, line_style


def _chart_reference_object_style(
    *,
    color: str | None,
    line_style: str | None,
    include_line_style: bool,
) -> dict[str, str]:
    """Return valid optional style properties for an OAChart reference object."""
    style: dict[str, str] = {}
    if isinstance(color, str) and color.strip():
        style["color"] = color.strip()
    if include_line_style and line_style in CHART_REFERENCE_LINE_STYLES:
        style["lineStyle"] = line_style
    return style


def _visible_chart_reference_object_control_ids(
    chart_type: str,
    vertical_reference_object_option: str,
    horizontal_reference_object_option: str,
) -> set[str]:
    """Return the selector and option-specific reference-object control ids to display."""
    visible_control_ids = set(CHART_REFERENCE_OBJECT_SELECTOR_IDS)
    vertical_reference_line_label_ids = (
        ()
        if chart_type in CHART_NUMERIC_X_AXIS_TYPES
        else ("tpl_chartVerticalReferenceLineValueLabel1",)
    )
    vertical_reference_area_label_ids = (
        ()
        if chart_type in CHART_NUMERIC_X_AXIS_TYPES
        else (
            "tpl_chartVerticalReferenceAreaLowLabel1",
            "tpl_chartVerticalReferenceAreaHighLabel1",
        )
    )
    vertical_input_ids = {
        "line": vertical_reference_line_label_ids
        + (
            "tpl_chartVerticalReferenceLineValueInput1",
        ),
        "area": vertical_reference_area_label_ids
        + (
            "tpl_chartVerticalReferenceAreaLowInput1",
            "tpl_chartVerticalReferenceAreaHighInput1",
        ),
        "none": (),
    }
    horizontal_input_ids = {
        "line": ("tpl_chartHorizontalReferenceLineValueInput1",),
        "area": (
            "tpl_chartHorizontalReferenceAreaLowInput1",
            "tpl_chartHorizontalReferenceAreaHighInput1",
        ),
        "variedLine": ("tpl_chartHorizontalVariedReferenceLineDataInput1",),
        "variedArea": ("tpl_chartHorizontalVariedReferenceAreaDataInput1",),
        "none": (),
    }
    visible_control_ids.update(vertical_input_ids.get(vertical_reference_object_option, ()))
    visible_control_ids.update(horizontal_input_ids.get(horizontal_reference_object_option, ()))
    if vertical_reference_object_option in ("line", "area"):
        visible_control_ids.add("tpl_chartVerticalReferenceObjectColorInput1")
    if vertical_reference_object_option == "line":
        visible_control_ids.add("tpl_chartVerticalReferenceObjectLineStyleLabel1")
        visible_control_ids.add("tpl_chartVerticalReferenceObjectLineStyleInput1")
    if horizontal_reference_object_option in ("line", "area", "variedLine", "variedArea"):
        visible_control_ids.add("tpl_chartHorizontalReferenceObjectColorInput1")
    if horizontal_reference_object_option in ("line", "variedLine"):
        visible_control_ids.add("tpl_chartHorizontalReferenceObjectLineStyleLabel1")
        visible_control_ids.add("tpl_chartHorizontalReferenceObjectLineStyleInput1")
    return visible_control_ids


def _configure_oachart_vertical_reference_value_controls(
    components: list[dict[str, Any]],
    chart_type: str,
) -> None:
    """Match vertical reference value controls to the selected chart's x-axis type."""
    categorical_values = CHART_CATEGORICAL_X_AXIS_VALUES.get(chart_type)
    control_fields = {
        "tpl_chartVerticalReferenceLineValueInput1": (
            "Value",
            "/demo/oachart/verticalReferenceLineValue",
        ),
        "tpl_chartVerticalReferenceAreaLowInput1": (
            "Low",
            "/demo/oachart/verticalReferenceAreaLow",
        ),
        "tpl_chartVerticalReferenceAreaHighInput1": (
            "High",
            "/demo/oachart/verticalReferenceAreaHigh",
        ),
    }
    for component in components:
        control_id = component.get("id")
        field = control_fields.get(control_id)
        if not field:
            continue
        if categorical_values and component.get("component") == "OACombobox":
            component["options"] = [
                {"label": value, "value": value}
                for value in categorical_values
            ]
        elif chart_type in CHART_NUMERIC_X_AXIS_TYPES:
            label, path = field
            component.clear()
            component.update(
                {
                    "id": control_id,
                    "component": "TextField",
                    "label": label,
                    "value": {"path": path},
                    "variant": "number",
                }
            )


def _oachart_components_with_active_reference_controls(
    *,
    preview_components: list[dict[str, Any]],
    chart_type: str,
    vertical_reference_object_option: str,
    horizontal_reference_object_option: str,
) -> list[dict[str, Any]]:
    """Filter OAChart form controls to the inputs used by the selected reference options."""
    visible_reference_control_ids = set()
    if chart_type in CHART_REFERENCE_OBJECT_TYPES:
        visible_reference_control_ids = _visible_chart_reference_object_control_ids(
            chart_type,
            vertical_reference_object_option,
            horizontal_reference_object_option,
        )
    hidden_reference_control_ids = set(CHART_REFERENCE_OBJECT_CONTROL_IDS) - visible_reference_control_ids
    configured_components = [
        deepcopy(component)
        for component in preview_components
        if isinstance(component, dict) and component.get("id") not in hidden_reference_control_ids
    ]
    _configure_oachart_vertical_reference_value_controls(configured_components, chart_type)
    for component in configured_components:
        if component.get("component") != "Column":
            continue
        children = component.get("children")
        if not isinstance(children, list):
            continue
        component["children"] = [
            child_id for child_id in children if child_id not in hidden_reference_control_ids
        ]

    return configured_components


def _configured_oachart_preview(
    *,
    preview_root: str,
    preview_components: list[dict[str, Any]],
    selected_chart_type: str,
    selected_chart_orientation: str | None = None,
    selected_chart_stack: str | None = None,
    selected_chart_title: str | None = None,
    selected_chart_vertical_reference_object_option: str = "line",
    selected_chart_vertical_reference_line_value: str | None = None,
    selected_chart_vertical_reference_area_low: str | None = None,
    selected_chart_vertical_reference_area_high: str | None = None,
    selected_chart_vertical_reference_object_color: str | None = None,
    selected_chart_vertical_reference_object_line_style: str | None = None,
    selected_chart_horizontal_reference_object_option: str = "line",
    selected_chart_horizontal_reference_line_value: str | None = None,
    selected_chart_horizontal_reference_area_low: str | None = None,
    selected_chart_horizontal_reference_area_high: str | None = None,
    selected_chart_horizontal_varied_reference_line_data: str | None = None,
    selected_chart_horizontal_varied_reference_area_data: str | None = None,
    selected_chart_horizontal_reference_object_color: str | None = None,
    selected_chart_horizontal_reference_object_line_style: str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    chart_type = selected_chart_type if selected_chart_type in CHART_TYPES else "line"
    sample = CHART_SAMPLE_DATA.get(chart_type)
    if not isinstance(sample, dict):
        sample = CHART_SAMPLE_DATA.get("line")
    if not isinstance(sample, dict):
        sample = {}

    chart_payload: dict[str, Any] = {
        "type": chart_type,
    }

    title = sample.get("title")
    if isinstance(selected_chart_title, str):
        chart_payload["title"] = selected_chart_title
    elif isinstance(title, str) and title:
        chart_payload["title"] = title

    colors = sample.get("colors")
    if isinstance(colors, list):
        chart_payload["colors"] = [color for color in colors if isinstance(color, str)]

    items = sample.get("items")
    if isinstance(items, list):
        source_items = [deepcopy(item) for item in items if isinstance(item, dict)]
        chart_payload["items"] = _sanitize_chart_items_for_type(chart_type, source_items)

    for axis_key in ("xAxis", "yAxis"):
        axis_value = sample.get(axis_key)
        if isinstance(axis_value, dict):
            chart_payload[axis_key] = deepcopy(axis_value)
            chart_payload[axis_key].pop("referenceObjects", None)

    orientation = selected_chart_orientation if selected_chart_orientation in CHART_ORIENTATIONS else sample.get("orientation")
    if isinstance(orientation, str) and orientation:
        chart_payload["orientation"] = orientation

    stack = selected_chart_stack if selected_chart_stack in CHART_STACK_MODES else sample.get("stack")
    if isinstance(stack, str) and stack:
        chart_payload["stack"] = stack

    vertical_reference_object_option = (
        selected_chart_vertical_reference_object_option
        if selected_chart_vertical_reference_object_option in CHART_VERTICAL_REFERENCE_OBJECT_OPTIONS
        else "line"
    )
    horizontal_reference_object_option = (
        selected_chart_horizontal_reference_object_option
        if selected_chart_horizontal_reference_object_option in CHART_HORIZONTAL_REFERENCE_OBJECT_OPTIONS
        else "line"
    )

    if chart_type in CHART_REFERENCE_OBJECT_TYPES:
        has_numeric_x_axis = chart_type in CHART_NUMERIC_X_AXIS_TYPES
        vertical_reference_line_value = (
            _parse_chart_reference_object_number(selected_chart_vertical_reference_line_value)
            if has_numeric_x_axis
            else (selected_chart_vertical_reference_line_value or "").strip()
        )
        vertical_reference_area_low = (
            _parse_chart_reference_object_number(selected_chart_vertical_reference_area_low)
            if has_numeric_x_axis
            else (selected_chart_vertical_reference_area_low or "").strip()
        )
        vertical_reference_area_high = (
            _parse_chart_reference_object_number(selected_chart_vertical_reference_area_high)
            if has_numeric_x_axis
            else (selected_chart_vertical_reference_area_high or "").strip()
        )
        horizontal_reference_line_value = _parse_chart_reference_object_number(selected_chart_horizontal_reference_line_value)
        horizontal_reference_area_low = _parse_chart_reference_object_number(selected_chart_horizontal_reference_area_low)
        horizontal_reference_area_high = _parse_chart_reference_object_number(selected_chart_horizontal_reference_area_high)
        horizontal_varied_reference_line_items = _parse_chart_varied_reference_line_data(selected_chart_horizontal_varied_reference_line_data, chart_type=chart_type,)
        horizontal_varied_reference_area_items = _parse_chart_varied_reference_area_data(selected_chart_horizontal_varied_reference_area_data, chart_type=chart_type,)
        vertical_reference_object_style = _chart_reference_object_style(
            color=selected_chart_vertical_reference_object_color,
            line_style=selected_chart_vertical_reference_object_line_style,
            include_line_style=vertical_reference_object_option == "line",
        )
        horizontal_reference_object_style = _chart_reference_object_style(
            color=selected_chart_horizontal_reference_object_color,
            line_style=selected_chart_horizontal_reference_object_line_style,
            include_line_style=horizontal_reference_object_option in ("line", "variedLine"),
        )
        if vertical_reference_object_option == "line" and vertical_reference_line_value not in (None, ""):
            chart_payload.setdefault("xAxis", {})["referenceObjects"] = [
                {
                    "type": "line",
                    "value": vertical_reference_line_value,
                    **vertical_reference_object_style,
                }
            ]
        elif vertical_reference_object_option == "area" and vertical_reference_area_low not in (None, "") and vertical_reference_area_high not in (None, ""):
            chart_payload.setdefault("xAxis", {})["referenceObjects"] = [
                {
                    "type": "area",
                    "low": vertical_reference_area_low,
                    "high": vertical_reference_area_high,
                    **vertical_reference_object_style,
                }
            ]

        if horizontal_reference_object_option == "line" and horizontal_reference_line_value is not None:
            chart_payload.setdefault("yAxis", {})["referenceObjects"] = [
                {
                    "type": "line",
                    "value": horizontal_reference_line_value,
                    **horizontal_reference_object_style,
                }
            ]
        elif horizontal_reference_object_option == "area" and horizontal_reference_area_low is not None and horizontal_reference_area_high is not None:
            chart_payload.setdefault("yAxis", {})["referenceObjects"] = [
                {
                    "type": "area",
                    "low": horizontal_reference_area_low,
                    "high": horizontal_reference_area_high,
                    **horizontal_reference_object_style,
                }
            ]
        elif horizontal_reference_object_option == "variedLine" and horizontal_varied_reference_line_items:
            chart_payload.setdefault("yAxis", {})["referenceObjects"] = [
                {
                    "type": "line",
                    "items": horizontal_varied_reference_line_items,
                    **horizontal_reference_object_style,
                }
            ]
        elif horizontal_reference_object_option == "variedArea" and horizontal_varied_reference_area_items:
            chart_payload.setdefault("yAxis", {})["referenceObjects"] = [
                {
                    "type": "area",
                    "items": horizontal_varied_reference_area_items,
                    **horizontal_reference_object_style,
                }
            ]

    configured_components = _oachart_components_with_active_reference_controls(
        preview_components=preview_components,
        chart_type=chart_type,
        vertical_reference_object_option=vertical_reference_object_option,
        horizontal_reference_object_option=horizontal_reference_object_option,
    )

    for component in configured_components:
        if component.get("component") == "OAChart":
            for key in list(component.keys()):
                if key not in ("id", "component"):
                    component.pop(key, None)
            component.update(deepcopy(chart_payload))
            break

        wrapper = component.get("component")
        if not isinstance(wrapper, dict):
            continue
        chart = wrapper.get("OAChart")
        if not isinstance(chart, dict):
            continue
        chart.clear()
        chart.update(deepcopy(chart_payload))
        break

    return preview_root, configured_components


def _stringify_context_value(value: Any) -> str:
    if isinstance(value, dict):
        for candidate_key in ("literalString", "valueString"):
            candidate = value.get(candidate_key)
            if isinstance(candidate, str):
                return f'"{candidate}" (string)'
        for candidate_key in ("literalNumber", "valueNumber"):
            candidate = value.get(candidate_key)
            if isinstance(candidate, (int, float)):
                return f"{candidate} (number)"
        for candidate_key in ("literalBoolean", "valueBoolean"):
            candidate = value.get(candidate_key)
            if isinstance(candidate, bool):
                return f"{candidate} (boolean)"
        path_value = value.get("path")
        if isinstance(path_value, str):
            return f'path="{path_value}"'

    if isinstance(value, str):
        return f'"{value}" (string)'
    if isinstance(value, bool):
        return f"{value} (boolean)"
    if isinstance(value, (int, float)):
        return f"{value} (number)"
    if value is None:
        return "null"

    return str(value)


def button_action_context_explanation(action: dict[str, Any]) -> str:
    context = action.get("context")
    if context is None:
        return "No action context was provided."

    lines: list[str] = []
    if isinstance(context, dict):
        for key, value in context.items():
            lines.append(f"{key}: {_stringify_context_value(value)}")
    elif isinstance(context, list):
        for item in context:
            if not isinstance(item, dict):
                continue
            key = item.get("key")
            if not isinstance(key, str) or not key:
                continue
            lines.append(f"{key}: {_stringify_context_value(item.get('value'))}")
    else:
        lines.append(_stringify_context_value(context))

    if not lines:
        return "Action context is present but empty."

    return "Dispatched action context:\n" + "\n".join(lines)


async def initial_ui_response(
    *,
    template_selection: TemplateSelectionService,
    select_component_with_llm: Callable[[str, dict[str, Any] | None, str | None], Any],
    user_query: str,
    surface_builder: SurfaceBuilder,
    catalog_id: str,
    schema_validate: Callable[[list[dict[str, Any]]], None],
    repair_async: Callable[[str], Awaitable[str]] | None = None,
    repair_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_component = await select_component_with_llm(
        user_query,
        None,
        None,
    )
    surface_id = f"component-demo-{uuid.uuid4().hex[:8]}"
    operations = surface_builder.build_surface_operations(
        surface_id=surface_id,
        catalog_id=catalog_id,
        selected_component=selected_component,
        include_begin=True,
    )
    return await build_repaired_text_message_from_operations(
        operations=operations,
        user_query=user_query,
        schema_validate=schema_validate,
        repair_async=repair_async,
        repair_kwargs=repair_kwargs,
    )


async def action_ui_response(
    *,
    template_selection: TemplateSelectionService,
    surface_builder: SurfaceBuilder,
    user_query: str,
    action: dict[str, Any],
    select_component_with_llm: Callable[[str, dict[str, Any] | None, str | None], Any],
    catalog_id: str,
    schema_validate: Callable[[list[dict[str, Any]]], None],
    repair_async: Callable[[str], Awaitable[str]] | None = None,
    repair_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    name = action.get("name")
    if name not in (
        "render_preview",
        "update_chart_preview",
        "update_chart_reference_controls",
        "update_grid_preview",
        "preview_button_action",
        "preview_oaactioncard_action",
    ):
        return build_text_message(UNSUPPORTED_ACTION_MESSAGE)

    surface_id = action.get("surfaceId") or action.get("surface_id")
    if not isinstance(surface_id, str) or not surface_id:
        return build_text_message(MISSING_SURFACE_ID_MESSAGE)

    if name == "preview_button_action":
        explanation = button_action_context_explanation(action)
        operations = surface_builder.build_surface_operations(
            surface_id=surface_id,
            catalog_id=catalog_id,
            selected_component="Button",
            include_begin=False,
            button_action_explanation=explanation,
        )
        return await build_repaired_text_message_from_operations(
            operations=operations,
            user_query=user_query,
            schema_validate=schema_validate,
            repair_async=repair_async,
            repair_kwargs=repair_kwargs,
        )

    if name == "preview_oaactioncard_action":
        explanation = button_action_context_explanation(action)
        operations = surface_builder.build_surface_operations(
            surface_id=surface_id,
            catalog_id=catalog_id,
            selected_component="OAActionCard",
            include_begin=False,
            oa_action_card_explanation=explanation,
        )
        return await build_repaired_text_message_from_operations(
            operations=operations,
            user_query=user_query,
            schema_validate=schema_validate,
            repair_async=repair_async,
            repair_kwargs=repair_kwargs,
        )

    requested_component = extract_requested_component_from_action(action, template_selection.supported_components)
    requested_chart_type = extract_requested_option_from_action(
        action,
        option_key="selectedChartType",
        allowed_values=CHART_TYPES,
    )
    requested_chart_type_fallback = extract_requested_option_from_action(
        action,
        option_key="selectedChartTypeFallback",
        allowed_values=CHART_TYPES,
    )
    requested_chart_orientation = extract_requested_boolean_from_action(
        action,
        option_key="selectedChartOrientation",
    )
    requested_chart_stack = extract_requested_boolean_from_action(
        action,
        option_key="selectedChartStack",
    )
    requested_chart_title = extract_requested_string_from_action(
        action,
        option_key="selectedChartTitle",
    )
    requested_data_grid_max_rows = extract_requested_option_from_action(
        action,
        option_key="selectedDataGridMaxRows",
        allowed_values=DATAGRID_MAX_ROW_OPTIONS,
    )
    requested_chart_vertical_reference_object_option = extract_requested_option_from_action(
        action,
        option_key="selectedChartVerticalReferenceObjectType",
        allowed_values=CHART_VERTICAL_REFERENCE_OBJECT_OPTIONS,
    )
    requested_chart_type_for_x_axis = requested_chart_type or requested_chart_type_fallback or "line"
    requested_chart_vertical_reference_line_value = _extract_requested_chart_x_axis_value(
        action,
        option_key="selectedChartVerticalReferenceLineValue",
        chart_type=requested_chart_type_for_x_axis,
    )
    requested_chart_vertical_reference_area_low = _extract_requested_chart_x_axis_value(
        action,
        option_key="selectedChartVerticalReferenceAreaLow",
        chart_type=requested_chart_type_for_x_axis,
    )
    requested_chart_vertical_reference_area_high = _extract_requested_chart_x_axis_value(
        action,
        option_key="selectedChartVerticalReferenceAreaHigh",
        chart_type=requested_chart_type_for_x_axis,
    )
    requested_chart_vertical_reference_object_color = extract_requested_string_from_action(
        action,
        option_key="selectedChartVerticalReferenceObjectColor",
    )
    requested_chart_vertical_reference_object_line_style = extract_requested_option_from_action(
        action,
        option_key="selectedChartVerticalReferenceObjectLineStyle",
        allowed_values=CHART_REFERENCE_LINE_STYLES,
    )
    requested_chart_horizontal_reference_object_option = extract_requested_option_from_action(
        action,
        option_key="selectedChartHorizontalReferenceObjectType",
        allowed_values=CHART_HORIZONTAL_REFERENCE_OBJECT_OPTIONS,
    )
    requested_chart_horizontal_reference_line_value = extract_requested_string_from_action(
        action,
        option_key="selectedChartHorizontalReferenceLineValue",
    )
    requested_chart_horizontal_reference_area_low = extract_requested_string_from_action(
        action,
        option_key="selectedChartHorizontalReferenceAreaLow",
    )
    requested_chart_horizontal_reference_area_high = extract_requested_string_from_action(
        action,
        option_key="selectedChartHorizontalReferenceAreaHigh",
    )
    requested_chart_horizontal_varied_reference_line_data = extract_requested_string_from_action(
        action,
        option_key="selectedChartHorizontalVariedReferenceLineData",
    )
    requested_chart_horizontal_varied_reference_area_data = extract_requested_string_from_action(
        action,
        option_key="selectedChartHorizontalVariedReferenceAreaData",
    )
    requested_chart_horizontal_reference_object_color = extract_requested_string_from_action(
        action,
        option_key="selectedChartHorizontalReferenceObjectColor",
    )
    requested_chart_horizontal_reference_object_line_style = extract_requested_option_from_action(
        action,
        option_key="selectedChartHorizontalReferenceObjectLineStyle",
        allowed_values=CHART_REFERENCE_LINE_STYLES,
    )
    requested_chart_reference_object_axis = extract_requested_option_from_action(
        action,
        option_key="selectedChartReferenceObjectAxis",
        allowed_values=("vertical", "horizontal"),
    )
    if (
        requested_chart_type is not None
        and requested_chart_type_fallback is not None
        and requested_chart_type != requested_chart_type_fallback
    ):
        requested_chart_vertical_reference_line_value = None
        requested_chart_vertical_reference_area_low = None
        requested_chart_vertical_reference_area_high = None
        requested_chart_horizontal_reference_line_value = None
        requested_chart_horizontal_reference_area_low = None
        requested_chart_horizontal_reference_area_high = None
        requested_chart_horizontal_varied_reference_line_data = None
        requested_chart_horizontal_varied_reference_area_data = None

    if name == "update_chart_reference_controls":
        operations = surface_builder.build_chart_reference_controls_operations(
            surface_id=surface_id,
            selected_chart_type=requested_chart_type or requested_chart_type_fallback or "line",
            selected_chart_vertical_reference_object_option=requested_chart_vertical_reference_object_option or "line",
            selected_chart_horizontal_reference_object_option=requested_chart_horizontal_reference_object_option or "line",
            selected_chart_reference_object_axis=requested_chart_reference_object_axis,
        )
        return await build_repaired_text_message_from_operations(
            operations=operations,
            user_query=user_query,
            schema_validate=schema_validate,
            repair_async=repair_async,
            repair_kwargs=repair_kwargs,
        )

    if requested_component in template_selection.supported_components:
        selected_component = requested_component
    else:
        selected_component = await select_component_with_llm(
            user_query,
            action,
            requested_component,
        )

    if selected_component not in template_selection.supported_components:
        return build_text_message(UNSUPPORTED_COMPONENTS_MESSAGE)

    operations = surface_builder.build_surface_operations(
        surface_id=surface_id,
        catalog_id=catalog_id,
        selected_component=selected_component,
        include_begin=False,
        selected_chart_type=requested_chart_type or requested_chart_type_fallback or "line",
        selected_chart_orientation=("horizontal" if requested_chart_orientation else "vertical")
        if requested_chart_orientation is not None
        else None,
        selected_chart_stack=("on" if requested_chart_stack else "off")
        if requested_chart_stack is not None
        else None,
        selected_chart_title=requested_chart_title,
        selected_chart_vertical_reference_object_option=requested_chart_vertical_reference_object_option or "line",
        selected_chart_vertical_reference_line_value=requested_chart_vertical_reference_line_value,
        selected_chart_vertical_reference_area_low=requested_chart_vertical_reference_area_low,
        selected_chart_vertical_reference_area_high=requested_chart_vertical_reference_area_high,
        selected_chart_vertical_reference_object_color=requested_chart_vertical_reference_object_color,
        selected_chart_vertical_reference_object_line_style=requested_chart_vertical_reference_object_line_style,
        selected_chart_horizontal_reference_object_option=requested_chart_horizontal_reference_object_option or "line",
        selected_chart_horizontal_reference_line_value=requested_chart_horizontal_reference_line_value,
        selected_chart_horizontal_reference_area_low=requested_chart_horizontal_reference_area_low,
        selected_chart_horizontal_reference_area_high=requested_chart_horizontal_reference_area_high,
        selected_chart_horizontal_varied_reference_line_data=requested_chart_horizontal_varied_reference_line_data,
        selected_chart_horizontal_varied_reference_area_data=requested_chart_horizontal_varied_reference_area_data,
        selected_chart_horizontal_reference_object_color=requested_chart_horizontal_reference_object_color,
        selected_chart_horizontal_reference_object_line_style=requested_chart_horizontal_reference_object_line_style,
        selected_data_grid_max_rows=requested_data_grid_max_rows or "default",
        selected_oa_listview_dynamic_selections=extract_requested_string_array_from_action(
            action,
            option_key="dynamicSelections",
        ),
    )
    return await build_repaired_text_message_from_operations(
        operations=operations,
        user_query=user_query,
        schema_validate=schema_validate,
        repair_async=repair_async,
        repair_kwargs=repair_kwargs,
    )
