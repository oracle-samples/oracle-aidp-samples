"""
AIDP Notebook Utils - Replacement for dbutils.notebook
Uses the same exec()-based approach proven to work on AIDP
(as used by the customer's aidp_dbutils).
"""

import os
import sys
import json
import nbformat
from typing import Dict, Any, Optional


class NotebookExit(Exception):
    """Custom exception for notebook exit with value."""
    def __init__(self, value: str):
        self.value = value
        super().__init__(value)


# Module-level current notebook directory â€” set by the migration bootstrap so that
# dbutils.notebook.run("../relative/path") resolves relative to the calling notebook's
# directory rather than the workspace root.
_current_notebook_dir: Optional[str] = None


def set_notebook_dir(path: str) -> None:
    """Set the directory of the currently-executing notebook.

    Called by the migration bootstrap before executing each notebook's cells:
        from aidp_compat.notebook import set_notebook_dir
        set_notebook_dir("/Users/user@example.com/path/to/notebook_dir")
    """
    global _current_notebook_dir
    _current_notebook_dir = path


class _NotebookContext:
    """Mock for dbutils.notebook.entry_point...getContext()."""
    def toJson(self):
        import json
        return json.dumps({
            'workspaceid': '<your-workspace-id>',
            'clusterId': '<your-cluster-id>',
            'orgId': '0',
            'notebookPath': '',
            'notebookId': '',
        })


class _NotebookHandle:
    """Mock for dbutils.notebook.entry_point...notebook()."""
    def getContext(self):
        return _NotebookContext()


class _EntryPoint:
    """Mock for dbutils.notebook.entry_point."""
    def getDbutils(self):
        return self
    def notebook(self):
        return _NotebookHandle()


class AIDPNotebookUtils:
    """Drop-in replacement for dbutils.notebook."""

    def __init__(self, spark=None, workspace_root: str = "/Workspace"):
        self._spark = spark
        self._workspace_root = workspace_root

    @property
    def entry_point(self):
        """Mock for dbutils.notebook.entry_point chain."""
        return _EntryPoint()

    def _resolve_path(self, path: str) -> str:
        """Resolve notebook path to actual filesystem path."""
        # Add .ipynb if needed
        if not path.endswith(".ipynb"):
            path += ".ipynb"

        # Translate /Workspace to actual workspace root
        if path.startswith("/Workspace"):
            path = path.replace("/Workspace", self._workspace_root, 1)

        # Handle relative paths â€” resolve against the current notebook's directory if known,
        # otherwise fall back to the workspace root.
        if not path.startswith("/"):
            base = _current_notebook_dir if _current_notebook_dir else self._workspace_root
            path = os.path.normpath(os.path.join(base, path))

        return path

    def exit(self, value: str):
        """Exit the notebook with a return value."""
        raise NotebookExit(str(value))

    def run(self, path: str, timeout_seconds: int = 0,
            timeout: int = 0,
            arguments: Optional[Dict[str, Any]] = None) -> str:
        """Run another notebook and return its exit value.

        Uses exec() on notebook cells - proven to work on AIDP.
        This matches the customer's aidp_dbutils approach.
        """
        notebook_path = self._resolve_path(path)

        if not os.path.exists(notebook_path):
            raise FileNotFoundError(
                f"Notebook not found: {notebook_path} (original: {path})"
            )

        # Read the notebook
        nb = nbformat.read(notebook_path, as_version=4)

        # Use the IPython kernel's namespace so variables propagate back
        # This mimics Databricks %run behavior (shared namespace)
        try:
            from IPython import get_ipython
            ip = get_ipython()
            if ip is not None:
                caller_globals = ip.user_ns
            else:
                import inspect
                caller_globals = inspect.stack()[1][0].f_globals
        except Exception:
            import inspect
            try:
                caller_globals = inspect.stack()[1][0].f_globals
            except Exception:
                caller_globals = globals()

        # Set widget parameters
        if arguments:
            try:
                from aidp_compat import dbutils as _db
                for key, value in arguments.items():
                    _db.widgets.text(key, str(value))
            except Exception as e:
                print(f"[notebook.run] warning: could not set widget parameters: {e}")

        # Execute each code cell in the caller's namespace
        try:
            for cell in nb.cells:
                if cell.cell_type == "code":
                    source = cell.source.strip()

                    if not source:
                        continue

                    # Skip magic commands that exec() can't handle
                    if source.startswith("%") or source.startswith("!"):
                        continue

                    exec(source, caller_globals)

        except NotebookExit as e:
            return str(e.value)

        return "ok"

    def help(self, method: str = None):
        print("dbutils.notebook - AIDP Notebook Utils")
        print("  exit(value) - Exit notebook with return value")
        print("  run(path, timeout, arguments) - Run another notebook")
