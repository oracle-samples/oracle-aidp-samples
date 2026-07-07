"""
SizeWise — input field registry (single source of truth for the UI form).

Mirrors the SizingInput contract in sizing_engine.py. The Flask app serves these
to the browser so the form is generated dynamically; adding an input is a
one-line change here (plus the matching field on SizingInput).
"""

FIELD_GROUPS = [
    {
        "title": "Core — Data & Users",
        "help": "The primary sizing drivers. The tool runs from the customer name alone; everything else has a sensible default.",
        "fields": [
            {"name": "customer_name", "label": "Customer / Opportunity Name", "type": "text",
             "default": "Customer", "help": "Shown on the report header."},
            {"name": "total_source_data_tb", "label": "Total Source Data", "type": "number",
             "unit": "TB", "default": 82, "help": "Total data to land/query on AIDP (raw). Primary driver."},
            {"name": "num_data_sources", "label": "Number of Data Sources", "type": "number",
             "unit": "count", "default": 6, "help": "Distinct source systems. Drives ingest parallelism & catalog scale."},
            {"name": "per_source_volume_tb", "label": "Per-Source Volumes (optional)", "type": "text",
             "unit": "TB, comma-list", "default": "", "help": "e.g. 40,20,12,10. Blank = split evenly."},
            {"name": "num_users", "label": "Total Users", "type": "number",
             "unit": "users", "default": 255, "help": "Total named/licensed users. Drives cluster count & cost/user."},
            {"name": "num_concurrent_users", "label": "Peak Concurrent Users (optional)", "type": "number",
             "unit": "users", "default": "", "help": "Peak simultaneously-active users. Blank = 65% of total."},
            {"name": "sla_tier", "label": "SLA Tier", "type": "select", "default": "standard",
             "options": ["relaxed", "standard", "strict", "mission_critical"],
             "help": "Query-latency expectation. Sets slots/query, executor size, utilization headroom."},
        ],
    },
    {
        "title": "Growth & Workload",
        "help": "How the data grows and what kind of work runs on it.",
        "fields": [
            {"name": "monthly_growth_tb", "label": "Monthly Data Growth (optional)", "type": "number",
             "unit": "TB/mo", "default": "", "help": "Net new data/month. Blank = derived from annual growth %."},
            {"name": "annual_growth_pct", "label": "Annual Growth (fallback)", "type": "number",
             "unit": "%/yr", "default": 110, "help": "Used only when monthly growth is blank."},
            {"name": "daily_ingest_gb", "label": "Daily Ingest Volume (optional)", "type": "number",
             "unit": "GB/day", "default": "", "help": "New data/day. Above the trigger, a dedicated ETL cluster is added."},
            {"name": "workload_mix", "label": "Workload Mix", "type": "text", "unit": "int/etl/ml %",
             "default": "90/10/0", "help": "Interactive/ETL-batch/ML-GPU percentages. ML>0 adds a GPU cluster."},
            {"name": "avg_query_runtime_sec", "label": "Avg Interactive Query Runtime", "type": "number",
             "unit": "sec", "default": 45, "help": "Mean query wall time (Little's Law for concurrency sizing)."},
            {"name": "queries_per_active_user_per_hr", "label": "Queries / Active User / Hour", "type": "number",
             "unit": "q/hr", "default": 20, "help": "Peak submission rate per active user."},
            {"name": "business_hours_per_day", "label": "Business Hours / Day", "type": "number",
             "unit": "h/day", "default": 12, "help": "Hours prod clusters run (24 = always-on). DMC is always 24/7."},
        ],
    },
    {
        "title": "Topology",
        "help": "How workloads are isolated across clusters.",
        "fields": [
            {"name": "num_teams", "label": "Teams / Isolation Groups (optional)", "type": "number",
             "unit": "count", "default": "", "help": "Groups needing cluster isolation. Blank = users / users-per-team."},
            {"name": "users_per_team", "label": "Users per Team", "type": "number",
             "unit": "users", "default": 15, "help": "Used to auto-derive team count when teams is blank."},
            {"name": "isolation_model", "label": "Isolation Model", "type": "select", "default": "per_team",
             "options": ["per_team", "shared_pool", "hybrid"],
             "help": "per_team = 1 cluster/team; shared_pool = few large clusters; hybrid = grouped teams."},
            {"name": "processor_preference", "label": "Processor Preference", "type": "select", "default": "auto",
             "options": ["auto", "arm", "amd", "intel"],
             "help": "auto = ARM executors + AMD catalog (cheapest). GPU auto-added only when ML mix > 0."},
        ],
    },
    {
        "title": "Options & Services",
        "help": "Availability, non-prod, storage and commercial toggles.",
        "fields": [
            {"name": "need_dev_env", "label": "Include Dev/Test Environment", "type": "checkbox", "default": True,
             "help": "Add one small shared non-prod cluster (ARM, short hours)."},
            {"name": "need_ha", "label": "High Availability", "type": "checkbox", "default": False,
             "help": "Redundant catalog (2x) and no-cold-start executors. Forced on in Conservative."},
            {"name": "include_storage_cost", "label": "Include Object Storage Cost", "type": "checkbox", "default": True,
             "help": "Add Object Storage line (avg-over-retention GB x $0.0255/GB-mo hot)."},
            {"name": "object_storage_hot_pct", "label": "Hot-Tier Storage %", "type": "number", "unit": "%",
             "default": 100, "help": "Share on standard/hot tier; remainder priced at archive rate."},
            {"name": "data_retention_months", "label": "Data Retention Window", "type": "number", "unit": "months",
             "default": 12, "help": "Retention window; scales growth for storage sizing (avg-over-retention)."},
            {"name": "region", "label": "OCI Region", "type": "text", "default": "us-ashburn-1",
             "help": "Report metadata; may adjust data-transfer default. No AIDPU rate change."},
            {"name": "billing_model", "label": "Billing Model", "type": "select", "default": "universal_credits",
             "options": ["universal_credits", "byol"],
             "help": "Report note only. BYOL adds a footnote; does not change AIDPU math."},
        ],
    },
]
