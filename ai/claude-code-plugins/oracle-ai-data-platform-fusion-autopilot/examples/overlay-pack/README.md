# Overlay Pack Example

This is a minimal additive overlay for the shipped `fusion-finance-starter`
content pack. It adds one new gold mart, `supplier_spend_by_currency`, without
modifying the starter pack.

For the full authoring and override workflow, see
[../../docs/mart_overlay_authoring.md](../../docs/mart_overlay_authoring.md).

Copy the overlay into a customer bundle, validate it, and wire it with:

```bash
aidp-fusion-autopilot content-pack validate examples/overlay-pack
aidp-fusion-autopilot use-pack examples/overlay-pack --profile finance-default --no-align
aidp-fusion-autopilot validate
aidp-fusion-autopilot run --mode seed --datasets supplier_spend_by_currency --layers gold --dry-run
```

`--no-align` preserves a narrow customer bundle instead of adding every silver
and gold node from the resolved pack. After using it, add only the new mart to
`bundle.yaml` if it is not already selected:

```yaml
gold:
  marts:
    - supplier_spend
    - supplier_spend_by_currency
```

In a real customer project, keep overlays under that project's `overlays/`
directory, for example `overlays/supplier-currency-summary/`.

After the new mart is seeded, run `oac-dataset-advisor` again so it can
recommend the OAC dataset over the live gold table.
