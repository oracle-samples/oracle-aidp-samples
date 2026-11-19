"""AIDP Job Utils - Replacement for dbutils.jobs"""
import os, json, tempfile

def _default_task_values_file():
    # Per-user path so a file left behind by another user does not block writes
    # (a shared /tmp/aidp_task_values.json owned by someone else → PermissionError).
    try:
        uid = os.getuid()
    except AttributeError:  # non-POSIX
        uid = os.environ.get("USER", "user")
    return os.path.join(tempfile.gettempdir(), f"aidp_task_values_{uid}.json")

class TaskValues:
    def __init__(self):
        self._values = {}
        self._file = os.environ.get("AIDP_TASK_VALUES_FILE") or _default_task_values_file()
        try:
            if os.path.exists(self._file):
                with open(self._file) as f:
                    self._values = json.load(f)
        except (OSError, ValueError):
            # Unreadable/corrupt store — start empty rather than crash the cell.
            self._values = {}

    def _save(self):
        try:
            with open(self._file, 'w') as f:
                json.dump(self._values, f)
        except OSError as e:
            # Not writable (e.g. /tmp owned by another user) — keep values
            # in-memory for this session instead of crashing the cell. WARN
            # (don't swallow silently): a later get() in a fresh kernel would
            # otherwise raise "Task value not found" detached from the cause.
            print(f"[aidp_compat.jobs] WARNING: could not persist task values to "
                  f"{self._file}: {e}. Values kept in-memory only (may not survive "
                  f"a kernel recycle or cross-task read).")

    def set(self, key: str, value) -> bool:
        self._values[key] = value
        self._save()
        return True

    def get(self, taskKey: str = "", key: str = "", default=None, debugValue=None):
        full_key = f"{taskKey}.{key}" if taskKey else key
        if full_key in self._values:
            return self._values[full_key]
        if key in self._values:
            return self._values[key]
        if default is not None:
            return default
        if debugValue is not None:
            return debugValue
        raise ValueError(f"Task value not found: {full_key}")

class AIDPJobUtils:
    def __init__(self):
        self.taskValues = TaskValues()
    def help(self, method=None):
        print("dbutils.jobs - AIDP Job Utils")
        print("  taskValues.set(key, value) - Set task value")
        print("  taskValues.get(taskKey, key, default, debugValue) - Get task value")
