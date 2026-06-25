"""
AIDP Widget Utils - Replacement for dbutils.widgets
Uses environment variables and a parameter store for notebook parameterization.
"""

import os
import json


class AIDPWidgetUtils:
    """Drop-in replacement for dbutils.widgets."""

    def __init__(self):
        self._widgets = {}
        self._values = {}
        self._load_params()

    def _load_params(self):
        """Load parameters from environment and config."""
        # Load from AIDP_PARAMS environment variable (JSON)
        params_json = os.environ.get("AIDP_PARAMS")
        if params_json:
            try:
                self._values.update(json.loads(params_json))
            except json.JSONDecodeError:
                pass

        # Load from params file
        params_file = os.environ.get("AIDP_PARAMS_FILE", "/opt/aidp/config/params.json")
        if os.path.exists(params_file):
            with open(params_file) as f:
                self._values.update(json.load(f))

        # Individual env vars: AIDP_PARAM_<NAME>=value
        for key, value in os.environ.items():
            if key.startswith("AIDP_PARAM_"):
                param_name = key[11:]
                self._values[param_name] = value

    def text(self, name: str, defaultValue: str = "", label: str = "", hint: str = ""):
        """Create a text input widget."""
        self._widgets[name] = {
            "type": "text",
            "default": defaultValue,
            "label": label
        }
        if name not in self._values:
            self._values[name] = defaultValue

    def dropdown(self, name: str, defaultValue: str = "", choices: list = None, label: str = ""):
        """Create a dropdown widget."""
        self._widgets[name] = {
            "type": "dropdown",
            "default": defaultValue,
            "choices": choices or [],
            "label": label
        }
        if name not in self._values:
            self._values[name] = defaultValue

    def combobox(self, name: str, defaultValue: str = "", choices: list = None, label: str = ""):
        """Create a combobox widget."""
        self._widgets[name] = {
            "type": "combobox",
            "default": defaultValue,
            "choices": choices or [],
            "label": label
        }
        if name not in self._values:
            self._values[name] = defaultValue

    def multiselect(self, name: str, defaultValue: str = "", choices: list = None, label: str = ""):
        """Create a multiselect widget."""
        self._widgets[name] = {
            "type": "multiselect",
            "default": defaultValue,
            "choices": choices or [],
            "label": label
        }
        if name not in self._values:
            self._values[name] = defaultValue

    def get(self, name: str) -> str:
        """Get the current value of a widget/parameter."""
        if name in self._values:
            return str(self._values[name])

        # Check environment variable
        env_val = os.environ.get(f"AIDP_PARAM_{name}") or os.environ.get(name)
        if env_val is not None:
            return env_val

        # Check widget defaults
        if name in self._widgets:
            return str(self._widgets[name].get("default", ""))

        raise ValueError(f"Widget/parameter not found: {name}")

    def getAll(self) -> dict:
        """Get all widget values."""
        return dict(self._values)

    def getArgument(self, name: str, optional: str = None) -> str:
        """Get widget value with optional fallback (deprecated in Databricks)."""
        try:
            return self.get(name)
        except ValueError:
            if optional is not None:
                return optional
            raise

    def remove(self, name: str):
        """Remove a widget."""
        self._widgets.pop(name, None)
        self._values.pop(name, None)

    def removeAll(self):
        """Remove all widgets."""
        self._widgets.clear()
        self._values.clear()

    def help(self, method: str = None):
        print("dbutils.widgets - AIDP Widget/Parameter Utils")
        print("  text(name, default, label) - Define text parameter")
        print("  dropdown(name, default, choices, label) - Define dropdown")
        print("  combobox(name, default, choices, label) - Define combobox")
        print("  multiselect(name, default, choices, label) - Define multiselect")
        print("  get(name) - Get parameter value")
        print("  getAll() - Get all parameter values")
        print("  remove(name) / removeAll() - Remove parameters")
