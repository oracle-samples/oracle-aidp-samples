"""Access-validity tracking in Oracle ADW (coder.aidp_marketplace_access).

ADW4IDL is a PRIVATE-ENDPOINT Autonomous Database — unreachable from this
machine, but reachable from the AIDP compute cluster (it is the same DB exposed
as the ext_private_catalog_adw4idl external catalog). So every DB operation runs
as a job on the cluster (via aidp_engine.AIDPEngine), connecting with
python-oracledb (pre-installed on the cluster, thin mode) using the wallet
uploaded into the workspace.

Table columns (coder.aidp_marketplace_access):
  OBJECT_NAME, GRANTEE, PERMISSION_GRANTED_DATE, PERMISSION_EXPIRY_DATE
"""

import os
import json
import datetime

import yaml

DB_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db_config.yaml")
_WALLET_FILES = ("tnsnames.ora", "sqlnet.ora", "ewallet.pem")


def load_db_config():
    with open(DB_CONFIG_PATH) as fh:
        return yaml.safe_load(fh) or {}


class DBAccess:
    """Maintains the access-tracking table on ADW via cluster jobs."""

    def __init__(self, engine, db_cfg=None):
        self.engine = engine
        self.cfg = db_cfg or load_db_config()
        self.schema = self.cfg.get("schema", "coder")
        self.table = self.cfg.get("table", "aidp_marketplace_access")
        self.fqtn = f"{self.schema}.{self.table}"
        self.ws_dir = self.cfg.get("wallet_workspace_dir", "marketplace_ui/adw_wallet")

    # -- wallet bootstrap ------------------------------------------------ #
    def ensure_wallet(self):
        """Upload the wallet files into the workspace if any are missing."""
        try:
            present = {it.get("path", "").split("/")[-1]
                       for it in self.engine._list_dir(self.ws_dir)}
        except Exception:  # noqa: BLE001
            present = set()
        if all(f in present for f in _WALLET_FILES):
            return
        local = self.cfg.get("wallet_local_dir")
        if not local or not os.path.isdir(local):
            raise RuntimeError(
                f"Wallet not in workspace and local wallet dir {local!r} not found. "
                f"Extract the ADW wallet zip there.")
        for fn in _WALLET_FILES:
            with open(os.path.join(local, fn), "rb") as fh:
                self.engine._upload_object(f"{self.ws_dir}/{fn}", fh.read(),
                                           content_type="text/plain")

    # -- code generation ------------------------------------------------- #
    def _connect_code(self):
        wd = f"/Workspace/{self.ws_dir}"
        return (
            "import oracledb, json, datetime\n"
            "try:\n"
            "    import oracledb\n"
            "except ImportError:\n"
            "    import subprocess, sys\n"
            "    subprocess.check_call([sys.executable,'-m','pip','install','-q','oracledb'])\n"
            "    import oracledb\n"
            f"WD = {wd!r}\n"
            f"con = oracledb.connect(user={self.cfg['user']!r}, password={self.cfg['password']!r}, "
            f"dsn={self.cfg['dsn']!r}, config_dir=WD, wallet_location=WD, "
            f"wallet_password={self.cfg['wallet_password']!r})\n"
            "cur = con.cursor()\n"
            f"TBL = {self.fqtn!r}\n"
            "try:\n"
            "    cur.execute('CREATE TABLE ' + TBL + ' (object_name VARCHAR2(256), "
            "grantee VARCHAR2(256), permission_granted_date DATE, permission_expiry_date DATE, "
            "permission_revoked_date DATE)')\n"
            "except oracledb.DatabaseError as _e:\n"
            "    if 'ORA-00955' not in str(_e):\n"  # name already used -> table exists
            "        raise\n"
            "try:\n"  # ensure the revoked-date column exists on pre-existing tables
            "    cur.execute('ALTER TABLE ' + TBL + ' ADD (permission_revoked_date DATE)')\n"
            "except oracledb.DatabaseError as _e:\n"
            "    if 'ORA-01430' not in str(_e):\n"  # column already exists
            "        raise\n"
        )

    _COLS = ("object_name, grantee, TO_CHAR(permission_granted_date,'YYYY-MM-DD'), "
             "TO_CHAR(permission_expiry_date,'YYYY-MM-DD'), "
             "TO_CHAR(permission_revoked_date,'YYYY-MM-DD')")
    _ROWMAP = ("result['rows'] = [{'object_name': r[0], 'grantee': r[1], "
               "'granted_date': r[2], 'expiry_date': r[3], 'revoked_date': r[4]} for r in cur]\n"
               "con.close()\n")

    def _select_code(self, object_name):
        return (
            'cur.execute("SELECT ' + self._COLS + ' FROM " + TBL + '
            '" WHERE object_name = :o ORDER BY permission_expiry_date DESC NULLS LAST, grantee", '
            'o=' + repr(object_name) + ")\n"
            + self._ROWMAP
        )

    def _select_all_code(self):
        return (
            'cur.execute("SELECT ' + self._COLS + ' FROM " + TBL + '
            '" ORDER BY permission_expiry_date DESC NULLS LAST, object_name, grantee")\n'
            + self._ROWMAP
        )

    # -- operations ------------------------------------------------------ #
    def record_and_list(self, object_name, grantees, granted_date, expiry_date):
        """Upsert one access row per grantee (MERGE on object+grantee), then
        return all access rows for the object. Dates are 'YYYY-MM-DD' strings."""
        self.ensure_wallet()
        entries = [[object_name, g, granted_date, expiry_date] for g in grantees]
        merge = (
            "ENTRIES = " + json.dumps(entries) + "\n"
            "for _obj, _g, _gd, _ed in ENTRIES:\n"
            "    cur.execute(\n"
            "        'MERGE INTO ' + TBL + ' t USING (SELECT :o object_name, :g grantee FROM dual) s '\n"
            "        'ON (t.object_name = s.object_name AND t.grantee = s.grantee) '\n"
            "        'WHEN MATCHED THEN UPDATE SET permission_granted_date = TO_DATE(:gd,\\'YYYY-MM-DD\\'), "
            "permission_expiry_date = TO_DATE(:ed,\\'YYYY-MM-DD\\'), permission_revoked_date = NULL '\n"
            "        'WHEN NOT MATCHED THEN INSERT (object_name, grantee, permission_granted_date, permission_expiry_date) '\n"
            "        'VALUES (:o, :g, TO_DATE(:gd,\\'YYYY-MM-DD\\'), TO_DATE(:ed,\\'YYYY-MM-DD\\'))',\n"
            "        o=_obj, g=_g, gd=_gd, ed=_ed)\n"
            "con.commit()\n"
        )
        body = self._connect_code() + merge + self._select_code(object_name)
        return self.engine._run_body(body, label="db_record")

    def list_access(self, object_name):
        """Return all access rows for the object (no writes)."""
        self.ensure_wallet()
        body = self._connect_code() + self._select_code(object_name)
        return self.engine._run_body(body, label="db_list")

    def list_all_access(self):
        """Return every access row across all objects (no writes)."""
        self.ensure_wallet()
        body = self._connect_code() + self._select_all_code()
        return self.engine._run_body(body, label="db_list_all")

    def revoke_and_list(self, pairs, revoked_date, object_name=None):
        """Stamp permission_revoked_date for each (object_name, grantee) pair
        (only rows not already revoked), then return the access list — for one
        object (object_name set) or across all objects (object_name None)."""
        self.ensure_wallet()
        update = (
            "PAIRS = " + json.dumps(pairs) + "\n"
            "for _o, _g in PAIRS:\n"
            "    cur.execute('UPDATE ' + TBL + ' SET permission_revoked_date = "
            "TO_DATE(:rd,\\'YYYY-MM-DD\\') WHERE object_name = :o AND grantee = :g "
            "AND permission_revoked_date IS NULL', rd=" + repr(revoked_date) + ", o=_o, g=_g)\n"
            "con.commit()\n"
        )
        tail = self._select_code(object_name) if object_name else self._select_all_code()
        body = self._connect_code() + update + tail
        return self.engine._run_body(body, label="db_revoke")

    def mark_revoked(self, object_name, grantees, revoked_date):
        """Stamp permission_revoked_date for the given grantees on the object
        (only rows not already revoked), then return the full access list."""
        self.ensure_wallet()
        update = (
            "GRX = " + json.dumps(grantees) + "\n"
            "for _g in GRX:\n"
            "    cur.execute('UPDATE ' + TBL + ' SET permission_revoked_date = "
            "TO_DATE(:rd,\\'YYYY-MM-DD\\') WHERE object_name = :o AND grantee = :g "
            "AND permission_revoked_date IS NULL', rd=" + repr(revoked_date) +
            ", o=" + repr(object_name) + ", g=_g)\n"
            "con.commit()\n"
        )
        body = self._connect_code() + update + self._select_code(object_name)
        return self.engine._run_body(body, label="db_revoke")

    @staticmethod
    def today():
        return datetime.date.today().isoformat()
