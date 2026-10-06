"""An emulated AIDP catalog API for dev mode. Zero network, zero OCI.

Faithful to the behaviours a live DataLake taught the deploy path — each one
broke a real run before it was code, and the demo exists to show them safely:

  * identifiers are LOWER-CASED by the server; keys must be resolved, not
    assumed;
  * a LIST response carries no fields; structure needs a GET;
  * creates are asynchronous and can fail SILENTLY — `LEGACY_AUDIT` is
    accepted (202-style empty return) and never appears, which drives the
    poisoned-name diagnosis;
  * a view's column types are re-derived by the target — `ORDER_SUMMARY_VW`
    comes back with TOTAL_AMOUNT narrowed to decimal(28,2);
  * an EXTERNAL catalog is a read-only pointer; the INTERNAL catalog
    `snowdemo` (what the UI calls Standard) pre-exists, exactly as a real
    customer would have created it -- the documented catalogType enum is
    EXTERNAL | INTERNAL.
"""
from __future__ import annotations

__all__ = ["EmulatedAidp"]


class EmulatedAidp:
    """A `call(operation, **kwargs) -> dict` double with catalog-API semantics."""

    def __init__(self, *, standard_catalog: str = "snowdemo",
                 never_visible: tuple[str, ...] = ("legacy_audit",),
                 drift: dict[tuple[str, str], str] | None = None):
        self.catalogs: dict[str, str] = {standard_catalog: "INTERNAL"}
        self.schemas: dict[str, str] = {}          # key -> lifecycleState
        self.objects: dict[str, dict] = {}         # key -> {fields, is_view}
        self.ops: list[tuple[str, dict]] = []
        self.never_visible = {n.lower() for n in never_visible}
        # (view name, field name) -> the type the engine "derives".
        self.drift = drift if drift is not None else {
            ("order_summary_vw", "total_amount"): "decimal(28,2)"}

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _fold(*parts: str) -> str:
        return ".".join(p.lower() for p in parts if p)

    def _store(self, catalog: str, schema: str, name: str, body: dict,
               *, is_view: bool) -> dict:
        key = self._fold(catalog, schema, name)
        if name.lower() in self.never_visible:
            # Accepted — and the asynchronous create then fails reporting
            # nothing, exactly like the live 202-and-vanish.
            return {}
        fields = body.get("viewFields") if is_view else body.get("tableFields")
        self.objects[key] = {"key": key, "is_view": is_view,
                             "fields": [dict(f) for f in fields or []]}
        return {"key": key}

    def _derived(self, key: str, fields: list[dict]) -> list[dict]:
        out = []
        view = key.rsplit(".", 1)[-1]
        for f in fields:
            name = str(f.get("fieldName", "")).lower()
            derived = self.drift.get((view, name))
            if derived:
                f = {"fieldName": f.get("fieldName"), "fieldType": derived}
            out.append(dict(f))
        return out

    # -- the transport -----------------------------------------------------

    def __call__(self, operation: str, **kw) -> dict:
        self.ops.append((operation, kw))
        catalog = kw.get("catalog", "")

        if operation == "list_catalogs":
            return {"items": [{"displayName": name, "catalogType": kind}
                              for name, kind in self.catalogs.items()]}
        if operation == "create_catalog":
            body = kw.get("body") or {}
            name = str(body.get("displayName") or "unnamed")
            self.catalogs[name] = str(body.get("catalogType") or "EXTERNAL")
            return {"key": name.lower()}

        if operation == "create_schema":
            key = self._fold(catalog, kw["schema"])
            self.schemas[key] = "ACTIVE"
            return {"key": key}
        if operation == "list_schemas":
            return {"items": [{"key": k, "lifecycleState": state}
                              for k, state in self.schemas.items()
                              if k.startswith(catalog.lower() + ".")]}
        if operation == "delete_schema":
            self.schemas.pop(self._fold(catalog, kw["schema"]), None)
            return {}

        if operation in ("create_table", "create_view"):
            is_view = operation == "create_view"
            name = kw.get("view") if is_view else kw.get("table")
            return self._store(catalog, kw["schema"], name, kw.get("body") or {},
                               is_view=is_view)
        if operation in ("list_tables_in", "list_views_in"):
            want_views = operation == "list_views_in"
            schema = kw["schema"]
            prefix = (schema if schema.lower().startswith(catalog.lower() + ".")
                      else self._fold(catalog, schema)) + "."
            # A real list entry carries NO fields — existence and key case only.
            return {"items": [{"key": o["key"]}
                              for o in self.objects.values()
                              if o["is_view"] == want_views
                              and o["key"].startswith(prefix.lower())]}
        if operation in ("get_table", "get_view"):
            is_view = operation == "get_view"
            name = kw.get("view") if is_view else kw.get("table")
            key = self._fold(catalog, kw["schema"], name)
            obj = self.objects.get(key)
            if obj is None:
                raise RuntimeError(f"404 NotFound: {key}")
            fields = (self._derived(key, obj["fields"]) if is_view
                      else [dict(f) for f in obj["fields"]])
            return {"key": key,
                    ("viewFields" if is_view else "tableFields"): fields}
        if operation in ("delete_table", "delete_view"):
            name = kw.get("view") if operation == "delete_view" else kw["table"]
            self.objects.pop(self._fold(catalog, kw["schema"], name), None)
            return {}

        raise ValueError(f"the emulated AIDP has no operation {operation!r}")
