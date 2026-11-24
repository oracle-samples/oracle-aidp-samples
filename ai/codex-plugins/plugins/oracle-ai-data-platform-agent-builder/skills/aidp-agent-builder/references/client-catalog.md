# Shared client catalog

This is the sample's bounded `https://aidp.local/catalogs/a2app-v2` catalog,
not the complete A2UI Basic Catalog. See `runtime/web/a2app.mjs` and
`runtime/web/charts.mjs` for the implementation. Unknown widgets fail visibly.
Keep business values as text/data; none of these widgets accepts executable HTML.

Use an A2UI 0.8 component graph. Each node has `id` and a single `component`
entry. `Column` and `Row` use `children: {"explicitList": ["child-id"]}`.
`Text` uses `text: {"literalString": "Title"}` or a data-model path. Generate
Button/TextField/CheckBox/MultipleChoice controls with the bundled `action_nodes`
helper; it binds declared action arguments rather than inventing a form protocol.
MultipleChoice supports single selection in this renderer.

Display extensions take direct JSON properties:

| Widget | Properties |
| --- | --- |
| Metric | `label`, numeric `value`, optional `format: "usd"` |
| BarChart | `rows` (objects), `label` (row key), `value` (numeric row key); optional `title`, `format: "usd"`, `labelLabel`, `valueLabel` |
| Table | `columns: [{"key": "id", "label": "Case"}]`, `rows: [{"id": "sample-a"}]`; optional `actionChildren: {"explicitList": ["row-action-id"]}` with one node reference per row |
| Markdown | `text` (displayed as literal preformatted text, without HTML or Markdown interpretation) |
| RuleSection | `text: {"literalString": "Rule text"}`, optional `highlight: true` |

For example, an action result can present a recommendation table:

```json
[
  {
    "surfaceUpdate": {
      "surfaceId": "delivery-review",
      "components": [
        {"id": "root", "component": {"Column": {"children": {"explicitList": ["recommendations"]}}}},
        {"id": "recommendations", "component": {"Table": {
          "columns": [{"key": "id", "label": "Case"}, {"key": "action", "label": "Recommendation"}],
          "rows": [{"id": "sample-a", "action": "Contact carrier"}]
        }}}
      ]
    }
  },
  {"beginRendering": {"surfaceId": "delivery-review", "root": "root", "catalogId": "https://aidp.local/catalogs/a2app-v2"}}
]
```

Include the same underlying recommendations as the response's business `result`
so the CLI does not need to decode a Table. For row actions, prefer explicit
Button nodes from `action_nodes` and Table `actionChildren`, and declare their
typed actions in the component definition. `parentAction` describes the action
hierarchy; it does not authorize an otherwise undeclared action.

Bound response size and row count in the contract. Large tables need a declared
pagination or artifact action; the renderer does not fetch arbitrary data URLs.
