#!/usr/bin/env python3
"""
Clear (or set) the execution timeout on an AIDP Job's task(s) via the Jobs REST API.

The Workbench UI won't let you blank a task's execution timeout once it's set. This calls
the API directly:  GET /jobs/{key}  ->  null the task timeoutSeconds  ->  PUT /jobs/{key}

Standalone — run locally with your OCI API credentials (~/.oci/config):

    pip install oci requests

    python tools/clear_job_task_timeout.py                            # uses the CONFIG block
    python tools/clear_job_task_timeout.py test_job.job               # job by name (all tasks)
    python tools/clear_job_task_timeout.py test_job.job Task_67681    # job + one task
    python tools/clear_job_task_timeout.py test_job.job Task_67681 --set-seconds 300
    python tools/clear_job_task_timeout.py --list                     # list jobs + task timeouts
    python tools/clear_job_task_timeout.py test_job.job --dry-run     # preview, no change

JOB NAME and TASK NAME are positional parameters (fall back to the CONFIG block if omitted);
everything else is in the CONFIG block and can also be overridden with CLI flags.
"""

import argparse
import json
import os
import sys

import oci
import requests

try:
    from oci.signer import Signer
except ImportError:  # older SDKs
    from oci.auth.signers import RequestSigner as Signer

# ================================ CONFIG ================================
# OCI API credentials (local machine)
OCI_CONFIG_FILE = "~/.oci/config"
OCI_PROFILE     = "DEFAULT"

# AIDP instance / workspace
REGION       = "us-ashburn-1"
# Provide your own values for PLATFORM_ID and WORKSPACE
PLATFORM_ID  = "ocid1.aidataplatform.oc1.iad.amaaaaaaai22xp***********************"
WORKSPACE    = "85e8fbd1-bb40-4eb9-***************"
API_VERSION  = "20260430"

# Which job
JOB_NAME = "test_job.job"     # used when JOB_KEY is empty
JOB_KEY  = ""                 # set to target a job by key directly

# What to change
TASK_KEYS         = []        # [] = ALL tasks;  or e.g. ["Task_67681"]
SET_SECONDS       = None      # None = clear (null);  or an int to SET the timeout
INCLUDE_JOB_LEVEL = True      # also change the job-level timeout (job.timeoutSeconds)
# ========================================================================

# Server-managed fields that must not be sent back on update.
READONLY = {
    "key", "createdBy", "createdByName", "updatedBy", "updatedByName",
    "timeCreated", "timeUpdated",
}


def make_signer(config_file, profile):
    cfg = oci.config.from_file(os.path.expanduser(config_file), profile)
    return Signer(
        cfg["tenancy"], cfg["user"], cfg["fingerprint"],
        cfg.get("key_file"), pass_phrase=cfg.get("pass_phrase"),
    )


def task_summary(job):
    return [(t.get("taskKey"), t.get("timeoutSeconds")) for t in job.get("tasks", [])]


def main():
    p = argparse.ArgumentParser(description="Clear/set an AIDP Job task execution timeout.")
    p.add_argument("job_name", nargs="?", default=None,
                   help=f"Job name (positional). Falls back to CONFIG JOB_NAME ({JOB_NAME!r}).")
    p.add_argument("task_name", nargs="?", default=None,
                   help="Task key (positional). Omit = ALL tasks (or CONFIG TASK_KEYS).")
    p.add_argument("--job-key", default=JOB_KEY or None,
                   help="Target a job by key instead of by name.")
    p.add_argument("--set-seconds", type=int, default=SET_SECONDS,
                   help="Set the timeout to this value instead of clearing it.")
    p.add_argument("--keep-job-level", action="store_true", default=not INCLUDE_JOB_LEVEL,
                   help="Do NOT touch the job-level timeout.")
    p.add_argument("--region", default=REGION)
    p.add_argument("--platform-id", default=PLATFORM_ID)
    p.add_argument("--workspace", default=WORKSPACE)
    p.add_argument("--api-version", default=API_VERSION)
    p.add_argument("--config", default=OCI_CONFIG_FILE)
    p.add_argument("--profile", default=OCI_PROFILE)
    p.add_argument("--list", action="store_true", help="List jobs + task timeouts and exit.")
    p.add_argument("--dry-run", action="store_true", help="Show the intended change without PUTting.")
    args = p.parse_args()

    signer = make_signer(args.config, args.profile)
    session = requests.Session()
    base = (f"https://aidp.{args.region}.oci.oraclecloud.com/{args.api_version}"
            f"/aiDataPlatforms/{args.platform_id}/workspaces/{args.workspace}")

    def get_job(key):
        r = session.get(f"{base}/jobs/{key}", auth=signer, timeout=60)
        r.raise_for_status()
        return r.json()

    if args.list:
        r = session.get(f"{base}/jobs", auth=signer, params={"limit": 100}, timeout=60)
        r.raise_for_status()
        for j in r.json().get("items", []):
            full = get_job(j["key"])
            print(f"{j['key']}  {j.get('name')}  job_timeout={full.get('timeoutSeconds')}  "
                  f"tasks={task_summary(full)}")
        return

    # Resolve job name + task filter (positional args win, else CONFIG defaults)
    job_name = args.job_name or JOB_NAME
    if args.task_name:
        task_filter = [args.task_name]
    else:
        task_filter = TASK_KEYS or None        # None = all tasks

    job_key = args.job_key
    if not job_key:
        r = session.get(f"{base}/jobs", auth=signer, params={"limit": 100}, timeout=60)
        r.raise_for_status()
        match = [j for j in r.json().get("items", []) if (j.get("name") or j.get("displayName")) == job_name]
        if not match:
            sys.exit(f"ERROR: job not found by name: {job_name!r}")
        job_key = match[0]["key"]

    new_value = args.set_seconds  # None => clear
    verb = f"set to {new_value}s" if new_value is not None else "clear (null)"

    job = get_job(job_key)
    print(f"Job: {job.get('name')}  key={job_key}")
    print(f"BEFORE  job.timeoutSeconds={job.get('timeoutSeconds')}  tasks={task_summary(job)}")

    body = {k: v for k, v in job.items() if k not in READONLY}
    changed = []
    for t in body.get("tasks", []):
        if task_filter is None or t.get("taskKey") in task_filter:
            if t.get("timeoutSeconds") != new_value:
                changed.append(t.get("taskKey"))
            t["timeoutSeconds"] = new_value
    if not args.keep_job_level:
        body["timeoutSeconds"] = new_value

    print(f"PLAN    {verb} on task(s): {changed or '(no change needed)'}"
          f"{'' if args.keep_job_level else '  + job-level'}")

    if args.dry_run:
        print("DRY RUN -- no changes made.")
        return

    resp = session.put(f"{base}/jobs/{job_key}", auth=signer,
                       data=json.dumps(body), headers={"Content-Type": "application/json"}, timeout=60)
    print(f"PUT /jobs/{{key}} -> {resp.status_code}")
    if not resp.ok:
        print(resp.text[:800])
        resp.raise_for_status()

    after = get_job(job_key)
    print(f"AFTER   job.timeoutSeconds={after.get('timeoutSeconds')}  tasks={task_summary(after)}")
    print("Done.")


if __name__ == "__main__":
    main()
