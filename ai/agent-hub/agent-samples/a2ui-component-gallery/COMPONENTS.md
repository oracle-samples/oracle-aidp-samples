# A2UI v0.9 Component Reference

The gallery contains 31 preview experiences: one gallery/index surface and the
30 catalog components listed below. `SampleData.json` supplies deterministic
example values and is not itself an A2UI component.

## Standard components

`Text`, `Image`, `Icon`, `Video`, `AudioPlayer`, `Modal`, `Button`, `CheckBox`,
`Slider`, `Divider`, `DateTimeInput`, `Tabs`, `ChoicePicker`, `TextField`,
`Card`, `List`, `Row`, and `Column`.

## Oracle Agent Hub components

`OAChart`, `OACombobox`, `OAListView`, `OAActionCard`, `OADataGrid`,
`OACollapsible`, `OAProgressBar`, `OAProgressCircle`, `OAPopup`, `OARadioSet`,
`OASwitch`, and `OATruncatingText`.

## Supported actions

| Action | Purpose |
|---|---|
| `render_preview` | Replace the preview with the selected component. |
| `update_chart_preview` | Change chart type, orientation, stacking, or title. |
| `update_chart_reference_controls` | Update chart reference lines or areas. |
| `update_grid_preview` | Change the OADataGrid row limit. |
| `preview_button_action` | Display the action context emitted by a button. |
| `preview_oaactioncard_action` | Display the action context emitted by an action card. |

Unknown actions and missing surface IDs return a readable text response rather
than changing a surface.

## Capability metadata

The agent reads capabilities from:

```json
{
  "a2uiClientCapabilities": {
    "supportedCatalogIds": [
      "/a2ui_specification/2.0.0/agent_hub_a2ui_custom_component_catalog.json"
    ]
  }
}
```

Metadata-free Agent Hub playground requests use the bundled default catalog.
An explicitly empty capability object selects the text fallback.
