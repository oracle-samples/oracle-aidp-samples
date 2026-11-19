"""The OCI object-storage namespace: its placeholder, and its one validator.

`plan --namespace` supplies the namespace half of every `oci://<bucket>@<ns>/`
path this tool emits. Left out, the planner substitutes a visible placeholder
so the whole estate can still be translated and read without an OCI tenancy --
docs/RUNBOOK.md offers that on purpose, as "fine for review and wrong to run".

What was missing is the second half of that sentence reaching the report. An
artifact carrying the placeholder used to grade `ok` / PASS with zero flags:
on the bundled estate, `plan` with no `--namespace` put it into three
artifacts and `migrate` still said `error=0`. So the placeholder is *named
here*, once, and the runner and the verifier both check for it.

Flagged rather than refused, deliberately:

  * refusing at plan time would remove the reviewable offline pass the
    RUNBOOK documents, and would not protect a user who reuses or hand-edits
    an older plan -- `plan` runs once, `migrate` runs many times;
  * the grading vocabulary already has the right word for "emitted, and a
    human must finish it": `needs_manual_review` / REVIEW. A placeholder
    target is exactly that, and saying so puts the object in the same list
    as every other thing the reader has to act on.

The placeholder string and the namespace rule used to exist in four and two
copies respectively (cli.py, plan/planner.py, migrate/runner.py,
translate/onelake_to_oci.py). This module imports nothing from the package so
that all of them can read it.
"""
from __future__ import annotations

import re

# What `plan` writes when no namespace was given. Angle brackets are not legal
# in an OCI namespace, so it can never be mistaken for a real one, and any
# artifact still containing this substring names a bucket that cannot resolve.
PLACEHOLDER = "<your-oci-namespace>"
# OCI object-storage namespaces are lowercase alphanumerics; `_` and `-` are
# allowed here because tenancy namespaces in the wild carry them.
_NAMESPACE_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,254}$")


def validated_namespace(namespace):
    """`namespace` unchanged, or a ValueError naming what is wrong with it.

    The placeholder passes: it is the documented "no tenancy yet" value, and
    the artifacts it produces are flagged downstream rather than refused here.
    """
    if namespace == PLACEHOLDER:
        return namespace
    if not isinstance(namespace, str) or _NAMESPACE_RE.fullmatch(namespace) is None:
        raise ValueError(
            "OCI namespace must be lowercase letters, digits, underscores or hyphens"
        )
    return namespace


def carries_placeholder(text) -> bool:
    """Whether emitted text still contains the placeholder namespace.

    Checked on the text rather than on the run's namespace setting, so a
    hand-edited plan that claims a real namespace and an artifact that
    nonetheless carries the placeholder is still caught.
    """
    return isinstance(text, str) and PLACEHOLDER in text


# The one sentence every caller says about it, so the CLI, the migration
# report and `verify` do not each word it differently.
ADVICE = (
    f"the OCI namespace is still the placeholder {PLACEHOLDER!r}, so every "
    f"oci:// path here names a bucket that cannot resolve; re-run `plan "
    f"--namespace <your tenancy's object-storage namespace>` (or set "
    f"OCI_NAMESPACE) and migrate again before using this artifact"
)
