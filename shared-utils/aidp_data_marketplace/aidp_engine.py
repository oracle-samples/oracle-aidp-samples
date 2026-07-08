"""AIDP SQL execution engine for the Marketplace UI.

AR_AIDP_V0 exposes no synchronous "run SQL" REST endpoint, and the Jupyter
kernel websocket is not reachable with an OCI API key. The only fully
REST/api-key path that executes Spark SQL is to submit a notebook as a *job*
and poll for completion. This module wraps that into a single synchronous call:

    engine.run_sql(statements, preview_sql=..., preview_limit=5)
        -> {"status": "OK"|"ERROR", "columns": [...], "rows": [...],
            "statements": [...], "error": ..., "run_key": ..., "job_key": ...}

How it works (all OCI-signed REST, verified live against AR_AIDP_V0):
  1. Generate a notebook whose Spark code runs the statements and writes a JSON
     result file into the workspace (``/Workspace/<work_dir>/_out/<token>.json``).
  2. Upload the notebook via the Workbench objects API (POST /objects).
  3. Create a job (POST /jobs) with one NOTEBOOK_TASK on the configured cluster.
  4. Trigger a run (POST /jobRuns) and poll GET /jobRuns/{key} until terminal.
  5. Download + parse the JSON result file via the objects API.
  6. Best-effort cleanup of the generated job, notebook and result file.
"""

import json
import time
import uuid
import urllib.parse

import requests
import oci.config
import oci.identity
import oci.pagination

try:
    from oci.signer import Signer
except ImportError:  # older SDKs
    from oci.auth.signers import RequestSigner as Signer

API_VERSION = "20260430"
WORK_DIR = "marketplace_ui"          # workspace folder for generated artifacts
TERMINAL_OK = {"SUCCESS", "SUCCEEDED"}
TERMINAL_BAD = {"FAILED", "ERROR", "CANCELED", "CANCELLED", "STOPPED", "TIMED_OUT"}


class EngineError(Exception):
    """Raised when the engine cannot complete a run (transport/setup error)."""


class AIDPEngine:
    def __init__(self, config):
        """``config`` is the dict returned by mkt_config.load_config()."""
        self.cfg = config
        self.endpoint = config["endpoint"].rstrip("/")
        self.platform_id = config["platform_id"]
        self.workspace = config["workspace"]
        self.cluster_key = config["cluster_key"]
        self.cluster_name = config.get("cluster_name") or "cluster"
        self.region = config.get("region") or "us-ashburn-1"
        self.timeout = int(config.get("job_timeout_seconds") or 420)
        self.profile = config.get("oci_profile") or "DEFAULT"

        oci_cfg = oci.config.from_file(profile_name=self.profile)
        self._oci_cfg = oci_cfg
        self._tenancy = oci_cfg.get("tenancy")
        self._identity = None
        self._users_cache = None
        try:
            self.signer = Signer(
                tenancy=oci_cfg["tenancy"], user=oci_cfg["user"],
                fingerprint=oci_cfg["fingerprint"],
                private_key_file_location=oci_cfg.get("key_file"),
                pass_phrase=oci_cfg.get("pass_phrase"),
            )
        except TypeError:
            self.signer = Signer(
                oci_cfg["tenancy"], oci_cfg["user"], oci_cfg["fingerprint"],
                oci_cfg.get("key_file"), pass_phrase=oci_cfg.get("pass_phrase"),
            )
        self.session = requests.Session()

    # ---- URL helpers --------------------------------------------------- #
    @property
    def _platform_base(self):
        return f"{self.endpoint}/{API_VERSION}/aiDataPlatforms/{self.platform_id}"

    @property
    def _ws_base(self):
        return f"{self._platform_base}/workspaces/{self.workspace}"

    def _cluster_ref(self):
        return {"clusterKey": self.cluster_key, "clusterName": self.cluster_name,
                "newCluster": None}

    # ---- low-level REST ------------------------------------------------ #
    def _req(self, method, url, **kw):
        kw.setdefault("timeout", 90)
        r = self.session.request(method, url, auth=self.signer, **kw)
        if r.status_code >= 300:
            raise EngineError(f"{method} {url} -> {r.status_code}: {r.text[:500]}")
        return r

    # ---- objects API (upload / download / delete) ---------------------- #
    def _upload_object(self, rel_path, data: bytes, content_type="application/json"):
        self._req("POST", self._ws_base + "/objects", data=data,
                  headers={"path": rel_path, "type": "NOTEBOOK"
                           if rel_path.lower().endswith(".ipynb") else "FILE",
                           "Content-Type": content_type})

    def _download_object(self, rel_path) -> bytes:
        enc = urllib.parse.quote(rel_path, safe="")
        return self._req("GET", self._ws_base + "/objects/" + enc, timeout=120).content

    def _list_dir(self, rel_path):
        r = self._req("GET", self._ws_base + "/objects", params={"path": rel_path})
        return r.json().get("items", [])

    def _delete_object(self, rel_path, otype):
        enc = urllib.parse.quote(rel_path, safe="")
        try:
            self._req("DELETE", self._ws_base + "/objects/" + enc,
                      headers={"type": otype})
        except EngineError:
            pass  # best-effort cleanup

    # ---- notebook generation ------------------------------------------- #
    @staticmethod
    def _build_notebook(code: str) -> bytes:
        cell = {"cell_type": "code", "execution_count": None, "id": uuid.uuid4().hex,
                "metadata": {"type": "python"}, "outputs": [], "source": code}
        nb = {"cells": [cell],
              "metadata": {"kernelspec": {"name": "notebook"},
                           "language_info": {"file_extension": ".py",
                                             "mimetype": "text/x-python",
                                             "name": "python"}},
              "nbformat": 4, "nbformat_minor": 5}
        return json.dumps(nb).encode()

    @staticmethod
    def _wrap(out_path, body):
        """Wrap a body that fills the ``result`` dict with prelude + file tail.

        ``body`` is Spark/Python source that runs inside a try/except and
        assigns into the pre-initialized ``result`` dict.
        """
        prelude = ("import json, os, traceback\n"
                   "result = {'status': 'OK', 'statements': [], 'columns': [], 'rows': []}\n"
                   "try:\n")
        # indent the body one level into the try block
        indented = "".join("    " + ln + "\n" for ln in body.splitlines())
        handler = ("except Exception as e:\n"
                   "    result['status'] = 'ERROR'\n"
                   "    result['error'] = str(e)\n"
                   "    result['trace'] = traceback.format_exc()\n")
        tail = (f"_out = {out_path!r}\n"
                "_d = os.path.dirname(_out)\n"
                "if _d:\n"
                "    os.makedirs(_d, exist_ok=True)\n"
                "with open(_out, 'w') as _f:\n"
                "    _f.write(json.dumps(result, default=str))\n"
                "print('AIDP_MARKETPLACE_DONE', result['status'])\n")
        return prelude + indented + handler + tail

    @staticmethod
    def _sql_body(statements, preview_sql, preview_limit):
        """Body that runs each statement, then an optional preview SELECT."""
        return (
            f"STMTS = {statements!r}\n"
            f"PREVIEW = {preview_sql!r}\n"
            f"LIMIT = {int(preview_limit)}\n"
            "for s in STMTS:\n"
            "    spark.sql(s)\n"
            "    result['statements'].append({'sql': s, 'ok': True})\n"
            "if PREVIEW:\n"
            "    df = spark.sql(PREVIEW)\n"
            "    result['columns'] = list(df.columns)\n"
            "    result['rows'] = [r.asDict(recursive=True) for r in df.limit(LIMIT).collect()]\n"
        )

    @staticmethod
    def _create_external_body(fqn, location, fmt, preview_limit):
        """Body that infers the schema from the data files, creates the external
        table with an explicit column list (the catalog does not auto-infer the
        schema for ``USING <fmt> LOCATION`` tables), then previews rows."""
        csv = fmt.upper() == "CSV"
        # For CSV the stored table must also carry header='true' so the header
        # line is skipped on read (otherwise it shows up as a data row).
        opts = " OPTIONS ('header'='true')" if csv else ""
        return (
            f"FQN = {fqn!r}\n"
            f"LOC = {location!r}\n"
            f"FMT = {fmt!r}\n"
            f"OPTS = {opts!r}\n"
            f"LIMIT = {int(preview_limit)}\n"
            "rdr = spark.read.format(FMT.lower())\n"
            + ("rdr = rdr.option('header', 'true').option('inferSchema', 'true')\n" if csv else "")
            + "src = rdr.load(LOC)\n"
            "cols = ', '.join('`%s` %s' % (f.name, f.dataType.simpleString()) for f in src.schema.fields)\n"
            "ddl = 'CREATE EXTERNAL TABLE IF NOT EXISTS %s (%s) USING %s%s LOCATION %r' % (FQN, cols, FMT, OPTS, LOC)\n"
            "spark.sql(ddl)\n"
            "result['statements'].append({'sql': ddl, 'ok': True})\n"
            "result['create_sql'] = ddl\n"
            "df = spark.sql('SELECT * FROM %s' % FQN)\n"
            "result['columns'] = list(df.columns)\n"
            "result['rows'] = [r.asDict(recursive=True) for r in df.limit(LIMIT).collect()]\n"
        )

    # ---- job lifecycle ------------------------------------------------- #
    def _create_job(self, name, notebook_path):
        body = {
            "name": name, "path": "/Workspace/jobs", "maxConcurrentRuns": 1,
            "jobClusters": [self._cluster_ref()],
            "tasks": [{
                "taskKey": "run", "type": "NOTEBOOK_TASK",
                "notebookPath": notebook_path, "cluster": self._cluster_ref(),
                "dependsOn": [], "parameters": [], "runIf": "ALL_SUCCESS",
            }],
        }
        return self._req("POST", self._ws_base + "/jobs", json=body).json()["key"]

    def _trigger_run(self, job_key):
        return self._req("POST", self._ws_base + "/jobRuns",
                         json={"jobKey": job_key}).json()["key"]

    def _poll_run(self, run_key):
        deadline = time.time() + self.timeout
        state = {}
        while time.time() < deadline:
            data = self._req("GET", self._ws_base + f"/jobRuns/{run_key}").json()
            data = data.get("data", data)
            state = data.get("state", {}) or {}
            status = state.get("status")
            if status in TERMINAL_OK or status in TERMINAL_BAD:
                return status, state
            time.sleep(5)
        return "TIMEOUT", state

    def _delete_job(self, job_key):
        try:
            self._req("DELETE", self._ws_base + f"/jobs/{job_key}")
        except EngineError:
            pass

    # ---- public API ---------------------------------------------------- #
    def run_sql(self, statements, preview_sql=None, preview_limit=5, label="op"):
        """Execute Spark SQL statements as a job and return a structured result."""
        if isinstance(statements, str):
            statements = [statements]
        body = self._sql_body(statements, preview_sql, preview_limit)
        return self._run_body(body, label=label)

    def create_external_table(self, fqn, location, fmt, preview_limit=5, label="create_table"):
        """Create an external table over a directory, inferring its schema, and
        return a preview of the first rows."""
        body = self._create_external_body(fqn, location, fmt, preview_limit)
        return self._run_body(body, label=label)

    def _run_body(self, body, label="op"):
        """Run a Spark/Python body (that fills ``result``) as a job; return the
        parsed result dict with run/job keys and timing.

        Result keys: status (OK|ERROR|TIMEOUT|FAILED...), columns, rows,
        statements, create_sql, error, trace, run_key, job_key, elapsed_seconds.
        """
        token = uuid.uuid4().hex[:12]
        ts = int(time.time())
        nb_rel = f"{WORK_DIR}/_mkt/{label}_{ts}_{token}.ipynb"
        out_rel = f"{WORK_DIR}/_out/{token}.json"
        out_abs = f"/Workspace/{out_rel}"
        nb_abs = f"/Workspace/{nb_rel}"

        code = self._wrap(out_abs, body)
        self._upload_object(nb_rel, self._build_notebook(code))

        job_key = run_key = None
        started = time.time()
        try:
            job_key = self._create_job(f"mkt_{label}_{ts}.job", nb_abs)
            run_key = self._trigger_run(job_key)
            status, state = self._poll_run(run_key)
            elapsed = round(time.time() - started, 1)

            result = {"status": status, "columns": [], "rows": [], "statements": [],
                      "error": None, "run_key": run_key, "job_key": job_key,
                      "elapsed_seconds": elapsed}

            if status in TERMINAL_OK:
                try:
                    parsed = json.loads(self._download_object(out_rel).decode())
                    result.update({
                        "status": parsed.get("status", "OK"),
                        "columns": parsed.get("columns", []),
                        "rows": parsed.get("rows", []),
                        "statements": parsed.get("statements", []),
                        "create_sql": parsed.get("create_sql"),
                        "error": parsed.get("error"),
                        "trace": parsed.get("trace"),
                    })
                except EngineError:
                    # Job succeeded but the result file was unreadable.
                    result["status"] = "OK"
                    result["error"] = "Job completed but no result file was produced."
            else:
                result["error"] = (state.get("stateMessage")
                                   or state.get("errorTrace")
                                   or f"Job run ended with status {status}.")
            return result
        finally:
            if job_key:
                self._delete_job(job_key)
            self._delete_object(nb_rel, "NOTEBOOK")
            self._delete_object(out_rel, "FILE")

    # ---- catalog data-object permissions (AIDP REST API) -------------- #
    @staticmethod
    def _table_key(catalog, schema, table):
        return f"{catalog}.{schema}.{table}"

    def read_object_permissions(self, catalog, schema, data_object, object_type="TABLE"):
        """Read the access list for a table/view via the AIDP REST API:
        GET {platform}/tables/{tableKey}/permissions  (tableKey = cat.schema.obj).
        Returns {"ok", "items"} (one row per principal) or {"ok": False, ...}.
        """
        tk = self._table_key(catalog, schema, data_object)
        url = f"{self._platform_base}/tables/{urllib.parse.quote(tk)}/permissions"
        try:
            r = self.session.get(url, auth=self.signer, timeout=60)
            if r.status_code >= 300:
                return {"ok": False, "status": r.status_code, "error": r.text[:300]}
            return {"ok": True, "items": self._dedupe_acl(r.json().get("items", []))}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    @staticmethod
    def _dedupe_acl(items):
        """Collapse the raw ACL to one row per principal. The API returns a row
        per scope (direct on the object + inherited from schema/catalog), so the
        same grantee appears multiple times; merge them and union permissions."""
        by_key, order = {}, []
        for it in items:
            key = it.get("grantee") or it.get("granteeName")
            if key not in by_key:
                by_key[key] = {
                    "grantee": it.get("grantee"),
                    "granteeName": it.get("granteeName"),
                    "granteeType": it.get("granteeType"),
                    "granteePermissions": list(it.get("granteePermissions") or []),
                }
                order.append(key)
            else:
                row = by_key[key]
                for p in (it.get("granteePermissions") or []):
                    if p not in row["granteePermissions"]:
                        row["granteePermissions"].append(p)
                # prefer a resolved (direct, non-inherited) display name
                if not it.get("isInherited") and it.get("granteeName"):
                    row["granteeName"] = it.get("granteeName")
        for key in order:
            by_key[key]["granteePermissions"] = sorted(set(by_key[key]["granteePermissions"]))
        return [by_key[k] for k in order]

    # ---- AIDP membership (OCI/IDCS identities) ------------------------ #
    def _load_users(self):
        """List the tenancy/identity-domain users once, with id/email/name."""
        if self._users_cache is not None:
            return self._users_cache
        if self._identity is None:
            self._identity = oci.identity.IdentityClient(self._oci_cfg)
        users = oci.pagination.list_call_get_all_results(
            self._identity.list_users, self._tenancy).data
        self._users_cache = [{
            "id": u.id,
            "name": (u.name or "").strip(),
            "email": (getattr(u, "email", None) or "").strip(),
        } for u in users]
        return self._users_cache

    def check_membership(self, identifier):
        """Is ``identifier`` (email, username or user OCID) an AIDP/OCI user?

        Returns (is_member, matched_user_dict_or_None). On any identity-lookup
        failure returns (None, None) so callers can degrade gracefully.
        """
        ident = (identifier or "").strip()
        try:
            users = self._load_users()
        except Exception:  # noqa: BLE001 - no identity access / API error
            return None, None
        low = ident.lower()
        local = low.split("@", 1)[0]          # local-part if an email was entered
        for u in users:
            if ident.startswith("ocid1.user") and u["id"] == ident:
                return True, u
            # IAM/IDCS names are domain-prefixed, e.g.
            # "oracleidentitycloudservice/arun.rengasamy@oracle.com" with no
            # email field — compare against the unqualified username.
            name = (u["name"] or "").lower()
            unq = name.split("/", 1)[1] if "/" in name else name
            candidates = {name, unq, unq.split("@", 1)[0]}
            if u["email"]:
                candidates.add(u["email"].lower())
            if low in candidates or local in candidates:
                return True, u
        return False, None

    def _manage_table_permission(self, catalog, schema, table, targets, details_key,
                                 grantee_type="USER", permissions=("SELECT",),
                                 include_columns=("*",), exclude_columns=()):
        """POST {platform}/tables/{tableKey}/actions/managePermission with either
        assignTablePermissionDetails (grant) or revokeTablePermissionDetails."""
        tk = self._table_key(catalog, schema, table)
        url = f"{self._platform_base}/tables/{urllib.parse.quote(tk)}/actions/managePermission"
        body = {details_key: {
            "assignees": {"type": grantee_type, "targets": list(targets)},
            "permissions": list(permissions),
            "includeColumns": list(include_columns),
            "excludeColumns": list(exclude_columns),
        }}
        try:
            r = self.session.post(url, auth=self.signer, json=body, timeout=90,
                                  headers={"Content-Type": "application/json"})
            if r.status_code >= 300:
                return {"ok": False, "status": r.status_code, "error": r.text[:400]}
            return {"ok": True, "status": r.status_code}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def grant_table_permission(self, catalog, schema, table, targets, **kw):
        """Grant permissions on a table/view to one or more principals (targets =
        user emails/usernames/OCIDs). Returns {"ok": True} on HTTP 204."""
        return self._manage_table_permission(
            catalog, schema, table, targets, "assignTablePermissionDetails", **kw)

    def revoke_table_permission(self, catalog, schema, table, targets, **kw):
        """Revoke permissions on a table/view from one or more principals."""
        return self._manage_table_permission(
            catalog, schema, table, targets, "revokeTablePermissionDetails", **kw)

    # ---- connectivity check ------------------------------------------- #
    def test_connection(self):
        """Lightweight read-only check that the instance + profile work."""
        r = self.session.get(self._ws_base + "/clusters", auth=self.signer, timeout=60)
        r.raise_for_status()
        items = r.json().get("items", [])
        match = next((c for c in items
                      if c.get("clusterKey") == self.cluster_key
                      or c.get("key") == self.cluster_key), None)
        return {
            "ok": True,
            "cluster_count": len(items),
            "configured_cluster_found": match is not None,
            "cluster_state": (match or {}).get("lifecycleState"),
        }
