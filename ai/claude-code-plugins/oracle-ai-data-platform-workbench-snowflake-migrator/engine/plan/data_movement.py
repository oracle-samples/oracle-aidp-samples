"""Data-movement options for the later phase. PLACEHOLDER -- presents, never acts.

This MVP moves no bytes. What it does is lay out the realistic ways bytes could
move later, with the trade-offs and the unknowns attached, so the choice is made
deliberately rather than defaulting to whichever path someone happened to build
first. The choice drives cost, wall-clock, and whether the migration can run
unattended, so it belongs to the customer.

`execute_transfer` exists only to refuse, loudly, and to name what would have to
be settled before any of this becomes real. Every option is marked
`proposal_only`.
"""
from __future__ import annotations

import datetime

__all__ = [
    "MAINTENANCE_TRAPS","OPTIONS", "CUSTOMER_DEFINED_ID", "NotImplementedInMvp",
           "architecture_decision", "capability_matrix", "execute_transfer",
           "options_for", "record_choice"]

# The open slot. A customer may want an architecture that is not in this list,
# or may not have decided yet -- both are legitimate answers, not errors, and
# neither is forced into one of A1-A5.
CUSTOMER_DEFINED_ID = "A6_CUSTOMER_DEFINED"

# What each option is FOR. A future MVP picks an option by capability, so these
# are the axes: which of them a path actually covers.
# Delta maintenance behaviours a Snowflake customer has never had to think
# about, because Snowflake did it for them. Which of these bite depends on the
# architecture chosen below, so they travel WITH the options rather than living
# in a reference file nobody opens.
MAINTENANCE_TRAPS: tuple[dict, ...] = (
    {"id": "T_VACUUM_BOUNDS_TIME_TRAVEL",
     "trap": "On Delta, VACUUM is what bounds time travel.",
     "consequence": "On Snowflake, retention and storage reclamation are "
                    "independent and automatic. On Delta they are the same "
                    "setting, so reclaiming storage aggressively also "
                    "shortens the recovery window. Snowflake's 7-day "
                    "Fail-safe has no Delta equivalent, so set retention "
                    "deliberately."},
    {"id": "T_OPTIMIZE_GROWS_STORAGE",
     "trap": "OPTIMIZE increases storage until VACUUM runs.",
     "consequence": "It writes compacted files and leaves the originals as "
                    "tombstones until retention expires, so schedule "
                    "reclamation (VACUUM) alongside compaction."},
    {"id": "T_NOTHING_RUNS_ITSELF",
     "trap": "Every OPTIMIZE and VACUUM is a scheduled job.",
     "consequence": "On AIDP, table maintenance runs as scheduled jobs the "
                    "customer owns, monitors and pays for, where Snowflake "
                    "ran it in the background. Plan these jobs as part of "
                    "the migration."},
)

_ALL_TRAPS = [t["id"] for t in MAINTENANCE_TRAPS]

CAPABILITIES = ("historic_bulk", "ongoing_incremental", "read_without_copy",
                "cutover")


class NotImplementedInMvp(NotImplementedError):
    """Data movement is out of scope. This exists to refuse, not to defer quietly."""


OPTIONS: tuple[dict, ...] = (
    {
        "id": "A1_UNLOAD_OBJECT_STORAGE",
        "maintenance_ownership": {
            "owner": 'customer', "traps_apply": _ALL_TRAPS,
            "note": 'Every table lands as managed Delta, so the whole maintenance obligation transfers on day one. Budget for a compaction and reclamation job alongside the migration, not after it.'},
        "name": "Bulk unload to object storage, land as managed Delta",
        "catalog_type": "INTERNAL",
        "phase": ("historic",),
        "moves_bytes": True,
        "etl": ("Snowflake COPY INTO @stage as Parquet -> cloud object storage -> "
                "OCI Object Storage (Interconnect or replication) -> Spark read -> "
                "managed Delta in an INTERNAL catalog"),
        "pros": [
            "Parquet carries DECIMAL natively, so NUMBER(p,s) survives if the "
            "round trip is verified",
            "unloading to storage in the SAME cloud region avoids Snowflake "
            "egress charges",
            "lands as managed Delta, which is the only writable AIDP catalog type",
            "resumable and re-runnable per table, so it suits waves",
        ],
        "cons": [
            "the largest moving part: staging, parallelism, throttling and "
            "reconciliation all have to be built",
            "Snowflake has no native OCI external-stage target, so it implies an "
            "S3/Azure/GCS hop plus a transfer into OCI",
            "wall-clock at PB scale is measured in days, not hours",
        ],
        "unknowns": [
            "which region the source account is actually in -- this decides "
            "whether same-region unload and Interconnect apply at all",
            "whether NUMBER(p,s) survives unload -> Parquet -> Delta with exact "
            "precision (to be measured)",
            "transfer throughput actually achievable into OCI Object Storage",
        ],
        "handles": ["historic_bulk", "cutover"],
        "implementation_notes": [
            "An unload driver: per-table COPY INTO @stage as Parquet, chunked by partition or by a key range, resumable per chunk.",
            "A transfer step into OCI Object Storage -- rclone, OCI CLI bulk-upload, or storage replication -- with throughput measured, not assumed.",
            "A landing step: Spark reads the Parquet and writes managed Delta, then registers the table explicitly in the catalog.",
            "A reconciliation harness: exact row counts and exact-decimal column sums compared against the source. Float tolerance is wrong for money.",
            "Cross-process throttling and 429 handling on the OCI side.",
        ],
    },
    {
        "id": "A2_FEDERATE_EXTERNAL_CATALOG",
        "maintenance_ownership": {
            "owner": 'snowflake', "traps_apply": [],
            "note": 'The data never becomes a Delta table, so none of the Delta maintenance items apply. Snowflake continues to maintain the layout, and that maintenance stays on the Snowflake bill. The trade is that Delta features (time travel on the target, ZORDER, CDF) are equally unavailable.'},
        "name": "Federate: read Snowflake in place through an EXTERNAL catalog",
        "catalog_type": "EXTERNAL",
        "phase": ("historic", "ongoing"),
        "moves_bytes": False,
        "etl": ("AIDP registers Snowflake as an EXTERNAL, read-only catalog over "
                "JDBC; queries read through. Copy only the subset that needs to be "
                "resident"),
        "pros": [
            "no bulk transfer, so no staging, no egress and no wall-clock risk",
            "available immediately once credentials exist",
            "good for a coexistence period, and for validating results against "
            "the source before anything is copied",
            "reduces the migration to only the objects that genuinely must move",
        ],
        "cons": [
            "EXTERNAL catalogs are READ-ONLY in AIDP: nothing can be written back, "
            "so this is not a destination",
            "Snowflake keeps running and keeps costing credits -- it defers spend "
            "rather than removing it",
            "predicates and aggregations that do not push down to Snowflake "
            "pull their rows across the network on every scan, so large scans "
            "need measuring first",
            "the native connector is read-only in AIDP 4.0, which rules out "
            "dual-write and write-back reconciliation",
        ],
        "unknowns": [
            "how much aggregation actually pushes down to Snowflake",
            "whether the target AIDP version's Snowflake connector supports the "
            "auth the customer can grant",
        ],
        "handles": ["read_without_copy"],
        "implementation_notes": [
            "Register Snowflake as an EXTERNAL catalog and confirm the auth mode the customer can grant is supported by the target AIDP version.",
            "Measure pushdown: which predicates and aggregations reach Snowflake, and which pull rows across the wire.",
            "No transfer code at all -- this is the option that needs the least building and the most measurement.",
        ],
    },
    {
        "id": "A3_REDIRECT_INGESTION",
        "maintenance_ownership": {
            "owner": 'customer', "traps_apply": _ALL_TRAPS,
            "note": 'The highest-churn case, and the one where compaction matters most: streaming and CDC writers produce many small files, which is exactly what OPTIMIZE exists for. Prefer preventing them at write time (optimizeWrite / AQE coalesce) over compacting after -- on OCI Object Storage a small-file write burst can also draw HTTP 429.'},
        "name": "Redirect ingestion at the source (Fivetran and pipelines)",
        "catalog_type": "BOTH",
        "phase": ("ongoing",),
        "moves_bytes": True,
        "etl": ("Stop feeding Snowflake for the migrated domains. Point Fivetran "
                "at an Oracle destination (ADW/ALH, Beta) or via Kafka -> OCI "
                "Streaming -> Spark consumer -> Delta. Historic data is handled "
                "separately by A1"),
        "pros": [
            "forward-only: new data arrives in AIDP from day one, so the copied "
            "history stops growing",
            "removes the double-write period that a long bulk migration implies",
            "reuses the existing ingestion tooling rather than replacing it",
        ],
        "cons": [
            "Fivetran has no AIDP destination and no generic protocol destination; "
            "its object-storage destination is AWS-only",
            "the Oracle destination is Beta, so volume and object-type coverage "
            "need confirming",
            "the Kafka route means owning the sink -- more moving parts we operate",
            "solves nothing for historic data on its own",
        ],
        "unknowns": [
            "Fivetran Oracle-destination Beta limits at the customer's ingest rate",
            "whether OCI Streaming's SASL_SSL is compatible with Fivetran's Kafka "
            "destination",
        ],
        "handles": ["ongoing_incremental", "cutover"],
        "implementation_notes": [
            "Decide the Fivetran path: Oracle destination (Beta) into ADW/ALH, or the Kafka destination into OCI Streaming.",
            "If Kafka: a Spark consumer that lands to Delta, plus offset management and exactly-once semantics -- we would own the sink.",
            "If Oracle destination: confirm Beta volume and object-type coverage at the customer's real ingest rate.",
            "A per-domain switchover runbook, since ingestion is redirected domain by domain, not all at once.",
        ],
    },
    {
        "id": "A4_ICEBERG_INTEROP",
        "maintenance_ownership": {
            "owner": 'shared', "traps_apply": _ALL_TRAPS,
            "note": 'Whoever WRITES the Iceberg tables owns their maintenance. If Snowflake writes them it compacts them; if AIDP writes them the obligation is yours. Iceberg has its own vocabulary for this -- compaction and expire-snapshots rather than OPTIMIZE and VACUUM -- so the same maintenance items apply with different command names, and snapshot expiry is what bounds time travel.'},
        "name": "Iceberg interop: share storage instead of copying",
        "catalog_type": "EXTERNAL",
        "phase": ("historic", "ongoing"),
        "moves_bytes": False,
        "etl": ("Snowflake writes Iceberg tables to external object storage; AIDP "
                "reads the same files through an Iceberg catalog. No duplication "
                "for tables already in, or convertible to, Iceberg format"),
        "pros": [
            "no copy at all for Iceberg-format tables -- one set of files, two "
            "engines",
            "avoids the precision and fidelity risk of an unload round trip "
            "entirely",
            "a genuine coexistence story rather than a cutover",
        ],
        "cons": [
            "only applies to Iceberg tables; native Snowflake FDN tables must be "
            "converted first, which is itself a rewrite of the data",
            "storage stays wherever Snowflake writes it, so cross-cloud latency "
            "and egress may persist",
            "feature maturity on both sides needs verifying against the actual "
            "versions in play",
        ],
        "unknowns": [
            "what share of the estate is or could be Iceberg rather than native",
            "whether the target AIDP version can read Iceberg from the "
            "customer's storage account",
        ],
        "handles": ["read_without_copy", "ongoing_incremental"],
        "implementation_notes": [
            "Survey what share of the estate is already Iceberg versus native FDN.",
            "For native tables, a conversion step in Snowflake -- which is itself a full rewrite of the data, so it is not free.",
            "Register the Iceberg catalog in AIDP and confirm the target version can read from the customer's storage account.",
            "Decide who owns compaction and snapshot expiry once two engines read the same files.",
        ],
    },
    {
        "id": "A5_HYBRID_WAVES",
        "maintenance_ownership": {
            "owner": 'both', "traps_apply": _ALL_TRAPS,
            "note": "Two maintenance regimes running at once for the length of the transition: Snowflake maintaining what has not moved, and a scheduled AIDP job maintaining what has. Plan for the operational cost of both, and for the fact that a table's regime changes the day it migrates."},
        "name": "Hybrid: federate first, copy selectively, redirect forward",
        "catalog_type": "BOTH",
        "phase": ("historic", "ongoing"),
        "moves_bytes": True,
        "etl": ("A2 for immediate read access and result validation, A1 for the "
                "objects usage data shows actually matter, A3 to stop the history "
                "growing. Iceberg (A4) wherever it already applies"),
        "pros": [
            "avoids an all-or-nothing cutover and lets value land before the bulk "
            "transfer finishes",
            "usage data decides what is worth copying, so the expensive path runs "
            "on the smallest possible set",
            "each wave is independently reversible",
        ],
        "cons": [
            "the most governance: two live systems, a moving boundary, and lineage "
            "spanning both",
            "needs the usage profile to be available, which needs ACCOUNT_USAGE",
            "hardest to explain to stakeholders",
        ],
        "unknowns": [
            "everything A1, A2 and A3 do not yet know, plus how long a coexistence "
            "window the business will accept",
        ],
        "handles": ["historic_bulk", "ongoing_incremental", "read_without_copy", "cutover"],
        "implementation_notes": [
            "Everything A1, A2 and A3 require, plus the wave planner that decides which objects take which path.",
            "A usage profile to drive that decision, which needs ACCOUNT_USAGE.",
            "Lineage that spans both systems for the duration of the coexistence window.",
            "A governance model for the moving boundary: what is authoritative where, and when that changes.",
        ],
    },
    {
        "id": CUSTOMER_DEFINED_ID,
        # Unknown until they describe it. An architecture we have not assessed
        # has no maintenance model we can state.
        "maintenance_ownership": {
            "owner": None, "traps_apply": None,
            "note": "Unknown until the design is described. Whether the Delta "
                    "maintenance items apply depends on whether the design "
                    "lands Delta tables at all, and this plugin has not "
                    "assessed it."},
        "name": "Customer-defined — something not listed here, or not decided yet",
        "catalog_type": "TBD",
        "phase": ("historic", "ongoing"),
        "moves_bytes": None,          # unknown until described. Not assumed.
        "handles": [],                # unknown until described. Not assumed.
        "etl": ("Whatever the customer's platform team specifies. This option "
                "exists so that 'none of the above' and 'not yet decided' are "
                "first-class answers rather than gaps in a form"),
        "pros": [
            "matches what the customer actually needs rather than what we "
            "happened to enumerate",
            "lets the decision wait until the people who own it are in the room, "
            "without blocking the read-only assessment",
            "their platform team may already have a pattern, tooling and "
            "operational experience that beats anything proposed here",
        ],
        "cons": [
            "nothing can be planned, sequenced or costed until it is described",
            "this plugin cannot assess it, so none of the trade-offs or unknowns "
            "listed against A1-A5 transfer to it",
        ],
        "unknowns": [
            "everything -- by definition. Once described, the unknowns become "
            "whatever that design implies, and this plugin has not evaluated them",
        ],
        "implementation_notes": [
            "Capture the customer's design verbatim first: name, description, and "
            "who owns it. Do not paraphrase it into one of A1-A5.",
            "Then assess it on the same axes used here -- does it move bytes, "
            "which capabilities does it cover, what does it need to exist -- and "
            "add it to this catalogue if it is reusable.",
            "Until it is described there is nothing to build, and saying so is "
            "more useful than proposing a substitute.",
        ],
    },
)

_PHASES = ("historic", "ongoing")
_BY_ID = {o["id"]: o for o in OPTIONS}

for _o in OPTIONS:                      # every option is a proposal, nothing more
    _o["status"] = "proposal_only"


def options_for(phase: str) -> list[dict]:
    if phase not in _PHASES:
        raise ValueError(f"unknown phase {phase!r}; expected one of {_PHASES}")
    return [o for o in OPTIONS if phase in o["phase"]]


def record_choice(option_id: str, *, chosen_by: str, rationale: str,
                  custom_architecture: dict | None = None) -> dict:
    """Record which option the customer picked. Records only -- acts on nothing.

    `custom_architecture` belongs to A6_CUSTOMER_DEFINED only, and is recorded
    VERBATIM. It is never paraphrased into one of A1-A5, because a customer
    design filed under someone else's option looks like an assessed variant of it
    and is not.

    A6 with no `custom_architecture` is a DEFERRAL: a deliberate "not yet", which
    is different from never having been asked.
    """
    if option_id not in _BY_ID:
        raise ValueError(f"unknown option {option_id!r}; expected one of "
                         f"{sorted(_BY_ID)}")
    if not (rationale or "").strip():
        raise ValueError(
            "a rationale is required: this choice drives cost, wall-clock and "
            "whether the migration can run unattended, so an unexplained pick is "
            "not a decision")

    if custom_architecture is not None:
        if option_id != CUSTOMER_DEFINED_ID:
            raise ValueError(
                f"a custom architecture belongs to {CUSTOMER_DEFINED_ID}, not to "
                f"{option_id}. Filing a customer design under a listed option "
                "would present it as an assessed variant of that option, which it "
                "is not.")
        name = (custom_architecture.get("name") or "").strip()
        description = (custom_architecture.get("description") or "").strip()
        if not name or not description:
            raise ValueError(
                "a custom architecture needs a name and a description; there is "
                "nothing to record otherwise")
        custom_architecture = {"name": name, "description": description}

    option = _BY_ID[option_id]
    deferred = option_id == CUSTOMER_DEFINED_ID and custom_architecture is None
    return {
        "option_id": option_id,
        "option_name": option["name"],
        "chosen_by": chosen_by,
        "rationale": rationale,
        "custom_architecture": custom_architecture,
        "deferred": deferred,
        "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "executed": False,
        "unknowns_outstanding": list(option["unknowns"]),
        "next_step": (
            "Describe the intended architecture when the customer is ready. "
            "Nothing here blocks the read-only assessment or the shallow clone."
            if deferred else
            "Retire the unknowns above with a hand-run spike on one "
            "representative table before any tooling is built. This plugin does "
            "not implement data movement."),
    }


def capability_matrix() -> dict:
    """Which option covers which capability. The axis a future MVP selects on."""
    return {
        "capabilities": list(CAPABILITIES),
        "options": {o["id"]: list(o["handles"]) for o in OPTIONS},
        "note": ("No single option covers every capability except A5, which is a "
                 "composition of the others. Expect to combine rather than pick."),
    }


def architecture_decision(recorded_choice: dict | None) -> dict:
    """The current architecture decision state, with the options ALWAYS attached.

    The options are returned whether or not a choice exists: before one, because
    the user has to choose; after one, because the alternatives are what make the
    choice reviewable.
    """
    options = [
        {"id": o["id"], "name": o["name"], "catalog_type": o["catalog_type"],
         "moves_bytes": o["moves_bytes"], "handles": list(o["handles"]),
         # Carried through the projection deliberately: which architecture is
         # chosen decides who inherits OPTIMIZE/VACUUM, and that is a cost the
         # customer must see while choosing, not afterwards.
         "maintenance_ownership": dict(o["maintenance_ownership"]),
         "etl": o["etl"], "unknowns": list(o["unknowns"]),
         "implementation_notes": list(o["implementation_notes"])}
        for o in OPTIONS]

    if not recorded_choice:
        return {
            "decided": False, "deferred": False, "chosen": None,
            "options": options, "unknowns_outstanding": [],
            "statement": ("No architecture has been chosen for moving data. The "
                          "control-plane CLI moves no bytes, so nothing is "
                          "blocked today -- but the choice drives cost, "
                          "wall-clock and whether a later migration can run "
                          "unattended, so it belongs to the customer rather "
                          "than to whoever builds first."),
        }

    option_id = recorded_choice.get("option_id")
    option = _BY_ID.get(option_id)
    if option is None:
        return {
            "decided": False, "deferred": False, "chosen": None,
            "options": options, "unknowns_outstanding": [],
            "statement": (f"A choice was recorded for {option_id!r}, which is not "
                          "a known option. Treating the architecture as undecided "
                          "rather than guessing what was meant."),
        }

    custom = recorded_choice.get("custom_architecture")
    if option_id == CUSTOMER_DEFINED_ID and not custom:
        return {
            "decided": False, "deferred": True, "chosen": None,
            "options": options, "unknowns_outstanding": [],
            "statement": (
                "The architecture is **deliberately deferred**: the customer will "
                f'specify it later (recorded by {recorded_choice.get("chosen_by")}: '
                f'{recorded_choice.get("rationale")}). That is a valid answer and '
                "blocks nothing here -- the assessment, the plan and the shallow "
                "clone all proceed. The options below stay on the table, and the "
                "eventual design need not be one of them."),
        }

    chosen = {"id": option["id"], "name": option["name"],
              "chosen_by": recorded_choice.get("chosen_by"),
              "rationale": recorded_choice.get("rationale"),
              "executed": bool(recorded_choice.get("executed")),
              "custom_architecture": custom,
              "mapped_to": None}

    if option_id == CUSTOMER_DEFINED_ID:
        return {
            "decided": True, "deferred": False, "chosen": chosen,
            "options": options,
            "unknowns_outstanding": [
                "everything: this plugin does not assess a customer-supplied "
                "architecture, so none of the trade-offs or unknowns listed "
                "against A1-A5 apply to it"],
            "statement": (
                f'Architecture chosen: **{custom["name"]}** -- the customer\'s own '
                "design, recorded verbatim and NOT mapped to any listed option. "
                "This plugin does not assess it: no trade-off, cost or unknown "
                "listed against A1-A5 transfers to it. Nothing is executed."),
        }

    return {
        "decided": True, "deferred": False, "chosen": chosen,
        "options": options,
        "unknowns_outstanding": list(option["unknowns"]),
        "statement": (f'Architecture chosen: {option["name"]} '
                      f'({option["id"]}). Recorded only -- this plugin executes '
                      "no transfer. The unknowns below must be retired by a "
                      "hand-run spike before any of it is built."),
    }


def execute_transfer(option_id: str, **_kwargs):
    """Always refuses. Data movement is out of scope for this plugin."""
    if option_id == CUSTOMER_DEFINED_ID:
        raise NotImplementedInMvp(
            f"{option_id}: there is nothing to execute. Describe the intended "
            "architecture first -- this plugin has not assessed it and will not "
            "substitute one of its own options for it. It moves no bytes either "
            "way.")
    option = _BY_ID.get(option_id)
    unknowns = option["unknowns"] if option else [
        "the source region, and whether NUMBER(p,s) survives a round trip"]
    raise NotImplementedInMvp(
        f"{option_id}: data movement is not implemented and this plugin moves no "
        "bytes. Before it could, these must be settled: "
        + "; ".join(unknowns)
        + ". Record the chosen option with record_choice() and run a manual spike "
          "first.")
