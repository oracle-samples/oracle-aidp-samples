"""Shared AIDP cluster lifecycle-state taxonomy.

Consumers retain context-specific policy (for example, STOPPED is startable
for lifecycle management but terminal for an executing notebook). State names
live here so Phase 1 and lifecycle handling cannot drift.

``RESTARTING``, ``PROVISIONING``, ``INSTALLING_LIBRARIES``, and
``RESTARTING_LIBRARIES`` are unobserved compatibility names consolidated from
the shipped ``aidp_executor.py`` consumer taxonomy. Runtime-path state claims
and their live provenance belong in the PR evidence rather than in this shared
definition.
"""

ACTIVE_CLUSTER_STATES = frozenset({"ACTIVE"})
STARTABLE_CLUSTER_STATES = frozenset({"INACTIVE", "STOPPED"})
SHUTDOWN_CLUSTER_STATES = frozenset({"STOPPING", "TERMINATING"})
START_PROGRESS_CLUSTER_STATES = frozenset({
    "CREATING",
    "STARTING",
    "RESTARTING",
    "UPDATING",
    "PROVISIONING",
    "INSTALLING_LIBRARIES",
    "RESTARTING_LIBRARIES",
})
TRANSIENT_CLUSTER_STATES = frozenset(
    START_PROGRESS_CLUSTER_STATES | SHUTDOWN_CLUSTER_STATES
)
TERMINAL_CLUSTER_STATES = frozenset({
    "FAILED",
    "DELETED",
    "DELETING",
})
STABLE_LIBRARY_MUTATION_CLUSTER_STATES = frozenset(
    ACTIVE_CLUSTER_STATES | STARTABLE_CLUSTER_STATES
)
