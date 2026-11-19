"""Compatibility checker for Informatica-to-AIDP migration.

Analyzes parsed Informatica components and identifies what can be
migrated automatically, what needs manual work, and what is unsupported.
"""

import os
import re
from datetime import datetime

from infa2aidp.models import (
    CompatibilityIssue,
    MigrationResult,
    Mapping,
    Transformation,
    TransformationType,
)

# Transformation types that are fully unsupported in AIDP migration
_UNSUPPORTED_TYPES = {
    TransformationType.JAVA,
    TransformationType.HTTP,
    TransformationType.XML_PARSER,
    TransformationType.XML_GENERATOR,
    TransformationType.CUSTOM,
}

# Transformation types that need manual review
_MANUAL_REVIEW_TYPES = {
    TransformationType.STORED_PROCEDURE,
    TransformationType.SQL,
    TransformationType.TRANSACTION_CONTROL,
}

# Oracle-specific SQL patterns
_ORACLE_SQL_PATTERNS = [
    (r"\bCONNECT\s+BY\b", "CONNECT BY (hierarchical query)"),
    (r"\bSTART\s+WITH\b", "START WITH (hierarchical query)"),
    (r"\bMODEL\s+", "MODEL clause"),
    (r"\bXMLTABLE\b", "XMLTABLE"),
    (r"\bXMLELEMENT\b", "XMLELEMENT"),
    (r"\bXMLFOREST\b", "XMLFOREST"),
    (r"\bXMLAGG\b", "XMLAGG"),
    (r"\bDBMS_", "DBMS_ package call"),
    (r"\bUTL_", "UTL_ package call"),
    (r"\bSYS_CONTEXT\b", "SYS_CONTEXT"),
    (r"\bROWNUM\b", "ROWNUM (use ROW_NUMBER() instead)"),
    (r"\bDECODE\s*\(", "DECODE (use CASE WHEN instead)"),
    (r"\bNVL2\s*\(", "NVL2 (use CASE WHEN instead)"),
    (r"\(\+\)", "Oracle outer join syntax (+)"),
]

# Data type mapping concerns
_DATATYPE_WARNINGS = {
    "CLOB": "CLOB maps to StringType — large values may cause memory issues",
    "BLOB": "BLOB maps to BinaryType — large values may cause memory issues",
    "LONG": "LONG maps to StringType — deprecated Oracle type",
    "RAW": "RAW maps to BinaryType",
    "XMLTYPE": "XMLTYPE has no direct Spark equivalent",
    "SDO_GEOMETRY": "SDO_GEOMETRY (spatial) is not supported",
}

# Informatica session features that don't apply in Spark
_SESSION_FEATURES = [
    "pushdown optimization",
    "session partitioning",
    "incremental aggregation",
    "constraint based load ordering",
]


class CompatibilityChecker:
    """Analyzes migration results and flags compatibility issues."""

    def check(self, migration_result: MigrationResult) -> list[CompatibilityIssue]:
        """Analyze all components and return compatibility issues."""
        issues: list[CompatibilityIssue] = []

        for mapping in migration_result.mappings:
            issues.extend(self._check_mapping(mapping))

        for session in migration_result.sessions:
            issues.extend(self._check_session(session))

        for workflow in migration_result.workflows:
            issues.extend(self._check_workflow(workflow))

        return issues

    # ------------------------------------------------------------------
    # Mapping checks
    # ------------------------------------------------------------------

    def _check_mapping(self, mapping: Mapping) -> list[CompatibilityIssue]:
        issues: list[CompatibilityIssue] = []

        for tx in mapping.transformations:
            issues.extend(self._check_transformation(tx, mapping.name))

        for src in mapping.sources:
            if src.connection and src.connection.db_type:
                issues.extend(
                    self._check_connection(src.connection, mapping.name)
                )
            for fld in src.fields:
                dt = getattr(fld, "datatype", "")
                if dt.upper() in _DATATYPE_WARNINGS:
                    issues.append(
                        CompatibilityIssue(
                            # Source fields are FieldMapping (source_field), not .name
                            component=f"{mapping.name} / {src.name}."
                                      f"{getattr(fld, 'source_field', '') or getattr(fld, 'name', '')}",
                            issue_type="WARNING",
                            description=_DATATYPE_WARNINGS[dt.upper()],
                            suggestion="Verify data sizes before migration.",
                            severity="WARNING",
                        )
                    )

        return issues

    def _check_transformation(
        self, tx: Transformation, mapping_name: str
    ) -> list[CompatibilityIssue]:
        issues: list[CompatibilityIssue] = []
        component = f"{mapping_name} / {tx.name}"

        if tx.type in _UNSUPPORTED_TYPES:
            issues.append(
                CompatibilityIssue(
                    component=component,
                    issue_type="UNSUPPORTED",
                    description=f"{tx.type.value} is not supported for automatic migration.",
                    suggestion="Rewrite manually in PySpark or use Code Llama (--use-llm).",
                    severity="ERROR",
                )
            )

        if tx.type in _MANUAL_REVIEW_TYPES:
            issues.append(
                CompatibilityIssue(
                    component=component,
                    issue_type="PARTIAL",
                    description=f"{tx.type.value} requires manual review.",
                    suggestion="Review generated code and validate logic.",
                    severity="WARNING",
                )
            )

        # Check SQL overrides for Oracle-specific syntax
        for sql_field in (tx.sql_override, tx.lookup_sql):
            if sql_field:
                issues.extend(
                    self._check_sql(sql_field, component)
                )

        return issues

    def _check_sql(self, sql: str, component: str) -> list[CompatibilityIssue]:
        issues: list[CompatibilityIssue] = []
        sql_upper = sql.upper()
        for pattern, label in _ORACLE_SQL_PATTERNS:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                issues.append(
                    CompatibilityIssue(
                        component=component,
                        issue_type="PARTIAL",
                        description=f"SQL contains Oracle-specific syntax: {label}",
                        suggestion="Convert to Spark SQL equivalent or use --use-llm.",
                        severity="WARNING",
                    )
                )
        return issues

    def _check_connection(
        self, conn, mapping_name: str
    ) -> list[CompatibilityIssue]:
        issues: list[CompatibilityIssue] = []
        db = conn.db_type.upper()

        if db == "FLAT_FILE":
            issues.append(
                CompatibilityIssue(
                    component=f"{mapping_name} / {conn.name}",
                    issue_type="PARTIAL",
                    description="Flat file connection requires path remapping to cloud storage.",
                    suggestion="Update file paths to OCI Object Storage or DBFS.",
                    severity="WARNING",
                )
            )
        elif db in ("SAP", "MAINFRAME", "IMS", "VSAM"):
            issues.append(
                CompatibilityIssue(
                    component=f"{mapping_name} / {conn.name}",
                    issue_type="UNSUPPORTED",
                    description=f"{db} connection type is not supported in AIDP.",
                    suggestion="Stage data to Oracle DB or Object Storage first.",
                    severity="ERROR",
                )
            )

        return issues

    # ------------------------------------------------------------------
    # Session checks
    # ------------------------------------------------------------------

    def _check_session(self, session) -> list[CompatibilityIssue]:
        issues: list[CompatibilityIssue] = []
        # Session ATTRIBUTEs (Pushdown Optimization, ...) are properties; only
        # $$-named overrides are parameters. Reading parameters alone meant
        # this check never fired on a parsed export.
        props = {**(getattr(session, "properties", None) or {}),
                 **(getattr(session, "parameters", None) or {})}

        for feature in _SESSION_FEATURES:
            for key, val in props.items():
                if feature.lower() in key.lower() and str(val).lower() not in (
                    "",
                    "0",
                    "no",
                    "false",
                    "none",
                ):
                    issues.append(
                        CompatibilityIssue(
                            component=f"Session: {session.name}",
                            issue_type="WARNING",
                            description=f"Session feature '{feature}' is not applicable in Spark.",
                            suggestion="Spark handles partitioning/optimization natively.",
                            severity="INFO",
                        )
                    )

        return issues

    # ------------------------------------------------------------------
    # Workflow checks
    # ------------------------------------------------------------------

    def _check_workflow(self, workflow) -> list[CompatibilityIssue]:
        issues: list[CompatibilityIssue] = []
        if workflow.scheduler:
            issues.append(
                CompatibilityIssue(
                    component=f"Workflow: {workflow.name}",
                    issue_type="PARTIAL",
                    description="Workflow scheduler settings need conversion to AIDP job scheduling.",
                    suggestion="Configure equivalent schedule in AIDP Jobs UI.",
                    severity="INFO",
                )
            )
        return issues

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------

    def generate_report(
        self, issues: list[CompatibilityIssue], output_path: str
    ) -> str:
        """Generate a markdown compatibility report. Returns the file path."""
        total = len(issues)
        by_severity = {"ERROR": 0, "WARNING": 0, "INFO": 0}
        for i in issues:
            by_severity[i.severity] = by_severity.get(i.severity, 0) + 1

        errors = by_severity["ERROR"]
        warnings = by_severity["WARNING"]
        infos = by_severity["INFO"]

        unsupported_pct = (errors / total * 100) if total else 0
        partial_pct = (warnings / total * 100) if total else 0
        supported_pct = (infos / total * 100) if total else 0

        lines = [
            "# Compatibility Report",
            "",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Summary",
            "",
            f"| Metric | Count | % |",
            f"|--------|------:|--:|",
            f"| Total issues | {total} | |",
            f"| Unsupported (ERROR) | {errors} | {unsupported_pct:.1f}% |",
            f"| Partial support (WARNING) | {warnings} | {partial_pct:.1f}% |",
            f"| Info | {infos} | {supported_pct:.1f}% |",
            "",
            "## Issues",
            "",
            "| Severity | Component | Type | Description | Suggestion |",
            "|----------|-----------|------|-------------|------------|",
        ]

        sorted_issues = sorted(
            issues,
            key=lambda i: {"ERROR": 0, "WARNING": 1, "INFO": 2}.get(
                i.severity, 3
            ),
        )
        for i in sorted_issues:
            lines.append(
                f"| {i.severity} | {i.component} | {i.issue_type} "
                f"| {i.description} | {i.suggestion} |"
            )

        lines.extend(
            [
                "",
                "## Migration Effort Estimate",
                "",
                "| Complexity | Description |",
                "|------------|-------------|",
                "| Simple | Standard Source Qualifier + Expression + Target patterns |",
                "| Medium | Lookups, Joiners, Aggregators, Routers with moderate SQL |",
                "| Complex | Stored Procedures, Java/Custom transformations, Oracle-specific SQL |",
                "",
                "## Recommendations",
                "",
            ]
        )

        if errors:
            lines.append(
                "- **Unsupported items**: Rewrite manually in PySpark or use "
                "`--use-llm` flag to get Code Llama suggestions."
            )
        if warnings:
            lines.append(
                "- **Partial support items**: Review generated code carefully. "
                "Validate Oracle-specific SQL conversions."
            )
        lines.append(
            "- Run `infa2aidp analyze` first to understand the full scope before migrating."
        )

        report_text = "\n".join(lines) + "\n"
        os.makedirs(output_path, exist_ok=True)
        file_path = os.path.join(output_path, "compatibility_report.md")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(report_text)

        return file_path
