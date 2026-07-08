"""Flask backend for the AIDP Marketplace UI.

Endpoints:
  GET  /                      -> the UI
  GET  /api/config            -> current instance settings + field metadata
  POST /api/config            -> save instance settings (editable later)
  POST /api/test-connection   -> read-only connectivity check
  POST /api/create-table      -> CREATE EXTERNAL TABLE + preview 5 rows
  POST /api/publish-view      -> CREATE VIEW in the marketplace catalog
  POST /api/share             -> GRANT SELECT on the view to user(s)

Every write operation runs Spark SQL on the configured AIDP instance via the
job-submission engine (aidp_engine.AIDPEngine) using the OCI ~/.oci/config
profile named in config.yaml (DEFAULT by default).
"""

import os
import re
import datetime

from flask import Flask, jsonify, request, send_from_directory

from mkt_config import load_config, save_config, EDITABLE_FIELDS
from aidp_engine import AIDPEngine, EngineError
from db_access import DBAccess

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _expiry(value):
    """Validate an access-expiry date (YYYY-MM-DD, today or later)."""
    value = (value or "").strip()
    if not _DATE.match(value):
        raise ValueError("Access expiry date is required (YYYY-MM-DD).")
    try:
        d = datetime.date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"Invalid date {value!r}.")
    if d < datetime.date.today():
        raise ValueError("Access expiry date must be today or in the future.")
    return value

HERE = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=os.path.join(HERE, "static"))

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _ident(value, what):
    """Validate a SQL identifier (table/view/schema name)."""
    value = (value or "").strip()
    if not _IDENT.match(value):
        raise ValueError(f"{what} must start with a letter/underscore and contain "
                         f"only letters, digits, or underscores (got {value!r}).")
    return value


def _engine():
    return AIDPEngine(load_config())


def _public_config(cfg):
    """Config is non-secret (no keys/passwords), safe to return as-is."""
    return cfg


def _err(message, code=400):
    return jsonify({"ok": False, "error": message}), code


# --------------------------------------------------------------------------- #
# Static UI
# --------------------------------------------------------------------------- #
@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify({
        "ok": True,
        "config": _public_config(load_config()),
        "fields": [{"key": k, "label": lbl, "help": h} for k, lbl, h in EDITABLE_FIELDS],
    })


@app.route("/api/config", methods=["POST"])
def post_config():
    updates = request.get_json(silent=True) or {}
    cfg = save_config(updates)
    return jsonify({"ok": True, "config": _public_config(cfg)})


@app.route("/api/test-connection", methods=["POST"])
def test_connection():
    try:
        return jsonify({"ok": True, **_engine().test_connection()})
    except Exception as exc:  # noqa: BLE001 - surface any auth/setup failure
        return _err(f"Connection failed: {exc}", 502)


# --------------------------------------------------------------------------- #
# 1) Publish to Marketplace: create external table in marketplace.default + preview
# --------------------------------------------------------------------------- #
@app.route("/api/create-table", methods=["POST"])
def create_table():
    body = request.get_json(silent=True) or {}
    cfg = load_config()
    try:
        table = _ident(body.get("table_name"), "Table name")
        location = (body.get("location") or "").strip()
        if not location:
            raise ValueError("Directory location is required.")
        if "'" in location:
            raise ValueError("Directory location must not contain quote characters.")
        fmt = (body.get("format") or cfg["default_table_format"]).strip().upper()
        if not _IDENT.match(fmt):
            raise ValueError(f"Invalid format {fmt!r}.")
    except ValueError as exc:
        return _err(str(exc))

    fqn = f"{cfg['marketplace_catalog']}.{cfg['marketplace_schema']}.{table}"

    try:
        result = _engine().create_external_table(fqn, location, fmt,
                                                 preview_limit=5, label="publish")
    except EngineError as exc:
        return _err(f"Execution transport error: {exc}", 502)

    if result["status"] != "OK":
        return jsonify({"ok": False, "table": fqn,
                        "error": result.get("error"), "trace": result.get("trace"),
                        "run_key": result.get("run_key")}), 200

    return jsonify({
        "ok": True,
        "table": fqn,
        "sql": result.get("create_sql"),
        "message": f"Table published successfully to {fqn}.",
        "columns": result["columns"],
        "rows": result["rows"],
        "elapsed_seconds": result.get("elapsed_seconds"),
    })


# --------------------------------------------------------------------------- #
# 2) Grant access to users: GRANT SELECT on the published object to user(s)
# --------------------------------------------------------------------------- #
@app.route("/api/share", methods=["POST"])
def share():
    body = request.get_json(silent=True) or {}
    cfg = load_config()
    # The shared object is the table published into the marketplace catalog.
    try:
        obj = _ident(body.get("view_name") or body.get("table_name"), "Object name")
    except ValueError as exc:
        return _err(str(exc))

    raw = body.get("users") or []
    if isinstance(raw, str):
        raw = re.split(r"[,\s;]+", raw)
    users = [u.strip() for u in raw if u and u.strip()]
    if not users:
        return _err("Enter at least one email id or AIDP user id.")
    try:
        expiry_date = _expiry(body.get("expiry_date"))
    except ValueError as exc:
        return _err(str(exc))

    obj_fqn = f"{cfg['marketplace_catalog']}.{cfg['marketplace_schema']}.{obj}"
    eng = _engine()

    # Prerequisite: each recipient must be part of the AIDP platform (an OCI /
    # identity-domain user). Reject non-members up front with a clear message.
    targets, not_members = [], []
    for u in users:
        member, matched = eng.check_membership(u)
        if member is None:
            return _err("Could not verify AIDP membership (OCI identity lookup "
                        "unavailable for this profile).", 502)
        if not member:
            not_members.append(u)
        else:
            # Grant by user OCID so AIDP resolves the proper IDCS principal and
            # stores a display name (e.g. "OracleIdentityCloudService/Full Name")
            # rather than the raw email.
            targets.append(matched.get("id") or u)
    if not_members:
        listed = ", ".join(not_members)
        verb = "is" if len(not_members) == 1 else "are"
        return jsonify({
            "ok": False, "view": obj_fqn, "users": users, "not_members": not_members,
            "error": f"{listed} {verb} not part of the AIDP platform. "
                     f"Only existing AIDP users can be granted access.",
        }), 200

    # Grant SELECT via the AIDP REST API (POST .../tables/{key}/actions/managePermission).
    res = eng.grant_table_permission(cfg["marketplace_catalog"],
                                     cfg["marketplace_schema"], obj, targets,
                                     grantee_type="USER", permissions=["SELECT"])
    acl = eng.read_object_permissions(cfg["marketplace_catalog"],
                                      cfg["marketplace_schema"], obj, "TABLE")
    if not res.get("ok"):
        return jsonify({"ok": False, "view": obj_fqn, "users": users,
                        "error": f"Grant failed: {res.get('error')}",
                        "acl": acl.get("items", [])}), 200

    # Record the access validity in coder.aidp_marketplace_access (ADW), then
    # return the full tracked-access list for the object.
    granted_date = DBAccess.today()
    rec = DBAccess(eng).record_and_list(obj_fqn, users, granted_date, expiry_date)
    accesses = rec.get("rows", []) if rec.get("status") == "OK" else []
    db_error = None if rec.get("status") == "OK" else (rec.get("error") or "Access record failed.")

    messages = [f"{u} is given SELECT privilege to {obj_fqn} object "
                f"(valid until {expiry_date})." for u in users]
    return jsonify({"ok": True, "view": obj_fqn, "users": users,
                    "messages": messages, "message": " ".join(messages),
                    "acl": acl.get("items", []), "accesses": accesses,
                    "expiry_date": expiry_date, "granted_date": granted_date,
                    "db_error": db_error})


@app.route("/api/permissions", methods=["POST"])
def permissions():
    """Read the live access list (grantees) for the published object."""
    body = request.get_json(silent=True) or {}
    cfg = load_config()
    try:
        obj = _ident(body.get("view_name") or body.get("table_name"), "Object name")
    except ValueError as exc:
        return _err(str(exc))
    acl = _engine().read_object_permissions(cfg["marketplace_catalog"],
                                            cfg["marketplace_schema"], obj, "TABLE")
    return jsonify({"ok": acl.get("ok", False),
                    "view": f"{cfg['marketplace_catalog']}.{cfg['marketplace_schema']}.{obj}",
                    "items": acl.get("items", []), "error": acl.get("error")})


@app.route("/api/access", methods=["POST"])
def access():
    """List the tracked access records (with validity dates) for an object."""
    body = request.get_json(silent=True) or {}
    cfg = load_config()
    try:
        obj = _ident(body.get("view_name") or body.get("table_name"), "Object name")
    except ValueError as exc:
        return _err(str(exc))
    obj_fqn = f"{cfg['marketplace_catalog']}.{cfg['marketplace_schema']}.{obj}"
    rec = DBAccess(_engine()).list_access(obj_fqn)
    return jsonify({"ok": rec.get("status") == "OK", "object": obj_fqn,
                    "accesses": rec.get("rows", []),
                    "error": None if rec.get("status") == "OK" else rec.get("error")})


@app.route("/api/review", methods=["POST"])
def review():
    """Review tracked accesses and revoke any whose access has expired (expiry
    date today or earlier and not already revoked): remove the SELECT grant on
    the object via the AIDP REST API and stamp permission_revoked_date.

    Scope: ``scope='all'`` reviews every object in the tracking table;
    ``scope='object'`` (default) reviews a single object (``view_name``).
    """
    body = request.get_json(silent=True) or {}
    cfg = load_config()
    scope = (body.get("scope") or "object").strip().lower()
    eng = _engine()
    dba = DBAccess(eng)

    obj_fqn = None
    if scope == "all":
        rec = dba.list_all_access()
        scope_label = "all objects"
    else:
        try:
            obj = _ident(body.get("view_name") or body.get("table_name"), "Object name")
        except ValueError as exc:
            return _err(str(exc))
        obj_fqn = f"{cfg['marketplace_catalog']}.{cfg['marketplace_schema']}.{obj}"
        rec = dba.list_access(obj_fqn)
        scope_label = obj_fqn

    if rec.get("status") != "OK":
        return jsonify({"ok": False, "scope": scope, "object": obj_fqn, "accesses": [],
                        "error": rec.get("error") or "Could not read access records."}), 200
    rows = rec.get("rows", [])

    today = datetime.date.today().isoformat()
    due = [r for r in rows
           if r.get("expiry_date") and r["expiry_date"] <= today and not r.get("revoked_date")]

    # Revoke the SELECT grant on each due object (rows may span many objects).
    revoked_pairs, revoked_users, failed = [], [], []
    for r in due:
        on = r["object_name"]
        parts = on.split(".")
        if len(parts) < 3:
            failed.append(f"{r['grantee']} on {on}")
            continue
        catalog, schema, table = parts[0], parts[1], ".".join(parts[2:])
        _, matched = eng.check_membership(r["grantee"])
        target = matched["id"] if matched else r["grantee"]
        res = eng.revoke_table_permission(catalog, schema, table, [target],
                                          permissions=["SELECT"])
        if res.get("ok"):
            revoked_pairs.append([on, r["grantee"]])
            revoked_users.append(f"{r['grantee']} ({on})")
        else:
            failed.append(f"{r['grantee']} on {on}")

    accesses = rows
    if revoked_pairs:
        upd = dba.revoke_and_list(revoked_pairs, today, object_name=obj_fqn)
        if upd.get("status") == "OK":
            accesses = upd.get("rows", rows)

    if revoked_pairs:
        message = (f"Revoked {len(revoked_pairs)} expired access grant(s) (as of {today}): "
                   + "; ".join(revoked_users) + ".")
    else:
        message = (f"Reviewed {len(rows)} access record(s) for {scope_label}. "
                   f"No access is due for revocation today.")
    return jsonify({"ok": True, "scope": scope, "object": obj_fqn, "accesses": accesses,
                    "revoked": revoked_users, "failed": failed,
                    "reviewed": len(rows), "as_of": today, "message": message,
                    "db_error": None if not failed else f"Could not revoke: {', '.join(failed)}"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8090"))
    app.run(host="127.0.0.1", port=port, debug=False)
