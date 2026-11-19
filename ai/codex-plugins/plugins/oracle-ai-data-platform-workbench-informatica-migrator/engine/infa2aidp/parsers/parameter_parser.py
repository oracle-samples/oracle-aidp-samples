"""Parse Informatica PowerCenter parameter files (.par).

Informatica parameter files are simple key=value text files used to
externalize runtime variables like connection strings, file paths,
date ranges, and environment-specific values.

Format:
  [FolderName.SessionName]
  $$LAST_EXTRACT_DATE=01/01/2024 00:00:00
  $$DB_CONNECTION=ORACLE_PROD
  $PMRootDir=/informatica/server
  $PMSessionLogDir=$PMRootDir/SessLogs

The parser:
  1. Reads .par files and extracts all parameters
  2. Resolves $PMxxx variable references
  3. Groups parameters by folder/session scope
  4. Generates equivalent spark.conf or notebook widget code

Usage:
  parser = ParameterFileParser()
  params = parser.parse("my_workflow.par")
  spark_code = parser.to_spark_conf(params)
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field


@dataclass
class ParameterScope:
    """A scoped block of parameters (folder.session or global)."""
    scope: str = ""  # e.g., "FolderName.SessionName" or "[Global]"
    folder: str = ""
    session: str = ""
    parameters: dict = field(default_factory=dict)  # {key: value}


@dataclass
class ParameterFileResult:
    """Complete result of parsing one or more parameter files."""
    file_path: str = ""
    scopes: list = field(default_factory=list)  # List[ParameterScope]
    global_params: dict = field(default_factory=dict)  # Params outside any scope
    all_params: dict = field(default_factory=dict)  # Flattened: all params merged


class ParameterFileParser:
    """Parse Informatica .par parameter files."""

    # Informatica built-in server variables
    BUILTIN_VARS = {
        "$PMRootDir", "$PMSessionLogDir", "$PMBadFileDir",
        "$PMCacheDir", "$PMTempDir", "$PMSourceFileDir",
        "$PMTargetFileDir", "$PMExtProcDir", "$PMLookupFileDir",
        "$PMWorkflowLogDir", "$PMSessionLogFile",
    }

    def parse(self, file_path: str) -> ParameterFileResult:
        """Parse a single .par file.

        Args:
            file_path: Path to the .par file.

        Returns:
            ParameterFileResult with all scopes and parameters.
        """
        result = ParameterFileResult(file_path=file_path)

        if not os.path.isfile(file_path):
            return result

        with open(file_path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        current_scope = None

        for line in lines:
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("#") or line.startswith("//"):
                continue

            # Scope header: [FolderName.SessionName] or [FolderName.WF:WorkflowName.ST:SessionName]
            if line.startswith("[") and line.endswith("]"):
                scope_name = line[1:-1].strip()
                current_scope = ParameterScope(scope=scope_name)

                # Parse folder.session from scope. The parts carry kind
                # prefixes -- WF:workflow, WT:worklet, ST:session -- which are
                # not part of the object name (the session "ST:s_m" matched
                # no session called s_m).
                parts = scope_name.split(".")
                if len(parts) >= 2:
                    current_scope.folder = parts[0]
                    current_scope.session = parts[-1].split(":", 1)[-1]
                elif len(parts) == 1:
                    current_scope.folder = parts[0]

                result.scopes.append(current_scope)
                continue

            # Parameter: key=value
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()

                if current_scope:
                    current_scope.parameters[key] = value
                else:
                    result.global_params[key] = value

                # Always add to flattened dict
                result.all_params[key] = value

        # Resolve variable references (e.g., $PMRootDir in values)
        self._resolve_references(result)

        return result

    def parse_directory(self, dir_path: str) -> list[ParameterFileResult]:
        """Parse all .par files in a directory.

        Args:
            dir_path: Directory containing .par files.

        Returns:
            List of ParameterFileResult, one per file.
        """
        results = []
        if not os.path.isdir(dir_path):
            return results

        for fname in sorted(os.listdir(dir_path)):
            if fname.lower().endswith(".par"):
                results.append(self.parse(os.path.join(dir_path, fname)))

        return results

    def _resolve_references(self, result: ParameterFileResult):
        """Resolve $variable references in parameter values.

        E.g., $PMSessionLogDir=$PMRootDir/SessLogs
        """
        # Each section resolves against ITSELF first, then the values outside
        # any section: resolving through the flattened all_params (last
        # section wins) and writing the result back into every section made
        # one session's $$FILE=$$DIR/a.csv take ANOTHER session's $$DIR.
        def resolve(params: dict, fallback: dict) -> None:
            for _ in range(5):  # bounded: a reference cycle must not loop forever
                changed = False
                for key, value in list(params.items()):
                    # ``\$\$?\w+`` takes the ``$$`` form whole (``$DESC`` inside
                    # ``$$DESC`` is not a key).
                    for ref in sorted(set(re.findall(r'\$\$?\w+', value)), key=len, reverse=True):
                        if ref == key:
                            continue
                        repl = params.get(ref, fallback.get(ref))
                        if repl is not None and ref in value:
                            value = value.replace(ref, repl)
                            params[key] = value
                            changed = True
                if not changed:
                    break

        resolve(result.global_params, {})
        for scope in result.scopes:
            resolve(scope.parameters, result.global_params)
        resolve(result.all_params, result.global_params)

    # ── Code Generation ─────────────────────────────────────────────────

    def to_spark_conf(self, result: ParameterFileResult) -> str:
        """Generate PySpark spark.conf.set() code from parameters.

        Args:
            result: Parsed parameter file result.

        Returns:
            String of PySpark code setting all parameters via spark.conf.
        """
        lines = [
            "# Parameters loaded from Informatica parameter file",
            f"# Source: {os.path.basename(result.file_path)}",
            "",
        ]

        # Values are emitted through json.dumps so a quote, backslash
        # (Windows paths) or accented character in a .prm value cannot
        # produce invalid Python.
        import json as _json

        # Global params
        if result.global_params:
            lines.append("# Global parameters")
            for key, value in sorted(result.global_params.items()):
                conf_key = self._to_conf_key(key)
                lines.append(f'spark.conf.set({_json.dumps(conf_key)}, {_json.dumps(str(value))})')
            lines.append("")

        # Scoped params
        for scope in result.scopes:
            if scope.parameters:
                lines.append(f"# Scope: {scope.scope}")
                for key, value in sorted(scope.parameters.items()):
                    conf_key = self._to_conf_key(key)
                    lines.append(f'spark.conf.set({_json.dumps(conf_key)}, {_json.dumps(str(value))})')
                lines.append("")

        return "\n".join(lines)

    def to_notebook_widgets(self, result: ParameterFileResult) -> str:
        """Generate AIDP notebook widget code from parameters.

        Args:
            result: Parsed parameter file result.

        Returns:
            String of PySpark code creating dbutils.widgets for each parameter.
        """
        lines = [
            "# Parameters loaded from Informatica parameter file",
            f"# Source: {os.path.basename(result.file_path)}",
            "# Use dbutils.widgets for interactive parameter override in AIDP",
            "",
        ]

        for key, value in sorted(result.all_params.items()):
            # Clean the key for widget naming
            widget_name = key.lstrip("$").lower()
            # json.dumps: a Windows path's backslashes or a double quote in the
            # value must survive as data, not become escapes or a SyntaxError.
            lines.append(f"dbutils.widgets.text({json.dumps(widget_name)}, {json.dumps(value)}, {json.dumps(key)})")

        lines.append("")
        lines.append("# Read parameters")
        for key in sorted(result.all_params.keys()):
            var_name = key.lstrip("$").upper()
            widget_name = key.lstrip("$").lower()
            lines.append(f'{var_name} = dbutils.widgets.get("{widget_name}")')

        return "\n".join(lines)

    def to_env_mapping(self, result: ParameterFileResult) -> dict:
        """Map Informatica parameters to their AIDP equivalents.

        Returns:
            Dict of {infa_param: {aidp_equivalent, description}}.
        """
        mapping = {}
        for key, value in result.all_params.items():
            aidp_equiv = self._to_conf_key(key)
            desc = self._describe_param(key)
            mapping[key] = {
                "original_value": value,
                "aidp_equivalent": aidp_equiv,
                "aidp_method": "spark.conf.get()" if key.startswith("$$") else "environment/config",
                "description": desc,
            }
        return mapping

    def generate_report(self, results: list[ParameterFileResult], output_path: str):
        """Generate a markdown report of all parsed parameter files.

        Args:
            results: List of parsed results (from parse_directory).
            output_path: Path to write the markdown report.
        """
        lines = ["# Informatica Parameter File Analysis", ""]

        total_params = sum(len(r.all_params) for r in results)
        lines.append(f"**Files parsed:** {len(results)}")
        lines.append(f"**Total parameters:** {total_params}")
        lines.append("")

        for result in results:
            lines.append(f"## {os.path.basename(result.file_path)}")
            lines.append("")

            if result.global_params:
                lines.append("### Global Parameters")
                lines.append("")
                lines.append("| Parameter | Value | AIDP Equivalent |")
                lines.append("|-----------|-------|-----------------|")
                for key, value in sorted(result.global_params.items()):
                    aidp = self._to_conf_key(key)
                    lines.append(f"| `{key}` | `{value}` | `spark.conf.get(\"{aidp}\")` |")
                lines.append("")

            for scope in result.scopes:
                if scope.parameters:
                    lines.append(f"### Scope: {scope.scope}")
                    lines.append("")
                    lines.append("| Parameter | Value | AIDP Equivalent |")
                    lines.append("|-----------|-------|-----------------|")
                    for key, value in sorted(scope.parameters.items()):
                        aidp = self._to_conf_key(key)
                        lines.append(f"| `{key}` | `{value}` | `spark.conf.get(\"{aidp}\")` |")
                    lines.append("")

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _to_conf_key(infa_key: str) -> str:
        """Convert Informatica parameter name to spark.conf key.

        $$LAST_EXTRACT_DATE -> migration.last_extract_date
        $PMRootDir -> migration.pmroot_dir   (only a lower->upper boundary splits)
        """
        key = infa_key.lstrip("$")
        # Convert CamelCase to snake_case
        key = re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', key)
        key = key.lower()
        return f"migration.{key}"

    @staticmethod
    def _describe_param(key: str) -> str:
        """Generate a human-readable description for known parameter types."""
        key_upper = key.upper()
        if "EXTRACT_DATE" in key_upper or "LAST_" in key_upper:
            return "Incremental extraction date — used for change data capture"
        if "CONNECTION" in key_upper or "DB_" in key_upper:
            return "Database connection identifier — map to AIDP secret scope"
        if "BATCH" in key_upper:
            return "ETL batch identifier"
        if key.startswith("$PM"):
            return f"Informatica server directory variable — map to AIDP workspace path"
        if "FILE" in key_upper or "PATH" in key_upper or "DIR" in key_upper:
            return "File/directory path — map to AIDP DBFS or Object Storage path"
        return "Application-specific parameter"
