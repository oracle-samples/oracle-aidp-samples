"""Checked, ordered bootstrap execution shared by migration entry points.

Kept below the orchestrator/session layers so cluster switching does not need
to import job_migrate (and its migration configuration) to enforce this policy.
"""

from aidp_executor import format_outputs


async def execute_bootstrap_snippets(session, snippets, timeout: int = 60):
    """Execute bootstrap statements in order and fail before registering errors.

    Each statement retains its own timeout budget. Combining all statements
    into one submission made their cumulative runtime compete for one timeout
    and allowed an unchecked early exception to skip the remaining setup.
    """
    statements = [str(item).strip() for item in snippets if str(item).strip()]
    for index, _bootstrap_statement in enumerate(statements, start=1):
        # Only tool-authored setup belongs here; customer code must go through
        # the migration execution boundary and its external-side-effect checks.
        result = await session.execute(_bootstrap_statement, timeout=timeout)
        if not isinstance(result, dict) or result.get("status", "error") != "ok":
            rendered = ""
            if isinstance(result, dict):
                try:
                    rendered = format_outputs(result.get("outputs", [])) or ""
                except (KeyError, TypeError, ValueError):
                    rendered = repr(result.get("outputs", []))
            detail = rendered.strip()[:500] or repr(result)[:500]
            raise RuntimeError(
                f"bootstrap statement {index}/{len(statements)} failed: {detail}"
            )
        session.register_bootstrap(_bootstrap_statement)
