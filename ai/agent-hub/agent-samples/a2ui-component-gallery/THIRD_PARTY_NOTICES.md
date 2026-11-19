# Third-Party Notices

## A2UI

Portions of `a2ui_sdk/`, including the schema manager, parser helpers, and the
bundled v0.8/v0.9 protocol and catalog schemas, are adapted from the A2UI
open-source project.

- Upstream project: [a2ui-project/a2ui](https://github.com/a2ui-project/a2ui)
  (formerly published as `google/A2UI`)
- Upstream version: tag `v0.9` (the bundled specification schemas correspond
  to that tag; upstream `main` has since restructured its layout)
- Upstream license: Apache License 2.0
- Upstream copyright: Google LLC
- Local modifications: Oracle Agent Hub transport integration, catalog
  negotiation, compatibility conversion, validation, and custom catalog support

File provenance within `a2ui_sdk/`:

- Verbatim upstream copies (unmodified): `v0_9/basic_catalog.json`,
  `v0_9/common_types.json`, `v0_9/server_to_client.json`
- Adapted from upstream (modified by Oracle, per their headers): `manager.py`,
  `parser.py`, the `v0_8/` schemas, and `v0_9/complete_catalog.json`
- Oracle-original (no upstream derivation; covered by this repository's
  UPL-1.0 license): `compat.py`, `__init__.py`, `v0_9/__init__.py`

The original copyright and modification notices in adapted source files are
retained. A copy of the applicable license is included at
[`LICENSES/Apache-2.0.txt`](./LICENSES/Apache-2.0.txt).
