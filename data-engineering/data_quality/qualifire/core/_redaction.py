"""Column-name redaction policy for SHAP / drift / schema-change emission.

Source columns listed on the policy never enter ``details_json``;
their names are replaced with the literal placeholder
``"<redacted>"`` at compute time inside the validators. Because
the redaction lives upstream of the system-table write and every
notification path, no per-channel policy plumbing is needed —
all egress automatically inherits the protection.

Effective set per dataset is computed by ``RedactionPolicy.build()``
from the union of an instance-level (``Qualifire(...)``) policy
and a dataset-level (``DatasetConfig``) policy. Denylist takes
precedence over allowlist where they overlap.

Allowlist semantics — one-sided configuration:
    None at both scopes:    no allowlist policy (every column passes)
    None at one scope:      the OTHER scope's allowlist governs
    Set at both scopes:     intersection (strictest scope wins)
"""
from __future__ import annotations

from dataclasses import dataclass, field


REDACTED_PLACEHOLDER = "<redacted>"


@dataclass(frozen=True)
class RedactionPolicy:
    """Effective redaction policy for one dataset.

    Built by ``build()`` from the instance-level and dataset-level
    inputs. ``redacted(source_column)`` is the single decision
    point all validators consult.
    """

    denylist: frozenset[str] = field(default_factory=frozenset)
    allowlist: frozenset[str] | None = None  # None means "no allowlist policy"

    @classmethod
    def build(
        cls,
        *,
        instance_denylist: list[str] | None,
        instance_allowlist: list[str] | None,
        dataset_denylist: list[str] | None,
        dataset_allowlist: list[str] | None,
    ) -> RedactionPolicy:
        # Denylist: additive (union).
        denylist = frozenset(
            (instance_denylist or []) + (dataset_denylist or []),
        )

        # Allowlist: one-sided semantics codified in the docstring.
        if instance_allowlist is None and dataset_allowlist is None:
            allowlist: frozenset[str] | None = None
        elif instance_allowlist is None:
            allowlist = frozenset(dataset_allowlist or [])
        elif dataset_allowlist is None:
            allowlist = frozenset(instance_allowlist or [])
        else:
            allowlist = frozenset(instance_allowlist) & frozenset(
                dataset_allowlist,
            )

        return cls(denylist=denylist, allowlist=allowlist)

    def redacted(self, source_column: str) -> bool:
        """Returns True if ``source_column`` must be redacted.

        Denylist always wins: a column on the denylist is
        redacted regardless of allowlist membership. When the
        allowlist is set, columns not in it are also redacted.
        """
        if source_column in self.denylist:
            return True
        if self.allowlist is not None and source_column not in self.allowlist:
            return True
        return False
