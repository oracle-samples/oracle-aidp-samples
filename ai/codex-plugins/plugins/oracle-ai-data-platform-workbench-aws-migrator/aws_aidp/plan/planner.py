"""Read a manifest, emit a migration plan: one row per source asset with target + transform_chain."""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from pathlib import Path

OCI_NAMESPACE_DEFAULT = "<your-oci-namespace>"     # override via OCI_NAMESPACE env or --namespace
_SOURCE_NAMES = ("s3", "glue", "athena", "emr", "sagemaker")
_COLLECTION_FIELDS = {
    "glue": {
        "databases": ("name",),
        "tables": ("database", "name"),
        "jobs": ("name",),
    },
    "athena": {"named_queries": ("id", "name", "query")},
    # Notebook execution names are optional in the EMR API; the immutable
    # execution id is the only safe identity requirement.
    "emr": {"clusters": ("id", "name"), "notebooks": ("id",)},
    "sagemaker": {
        "notebooks": ("name",),
        "training_jobs": ("name",),
        "models": ("name",),
        "pipelines": ("name",),
    },
}


def _validated_namespace(namespace: str) -> str:
    if not isinstance(namespace, str) or not namespace:
        raise ValueError("OCI namespace must be a non-empty string")
    if namespace != OCI_NAMESPACE_DEFAULT and re.fullmatch(r"[a-z0-9][a-z0-9_-]*", namespace) is None:
        raise ValueError(
            "OCI namespace must contain only lowercase letters, digits, underscores, or hyphens"
        )
    if len(namespace.encode("utf-8")) > 255:
        raise ValueError("OCI namespace is too long")
    return namespace


def _validated_manifest_items(manifest: dict) -> dict[str, object]:
    """Validate just the manifest contract consumed by the planner.

    Inventory summaries and future metadata remain forward compatible, while
    unknown source names and malformed asset collections fail closed instead of
    silently producing an incomplete plan.
    """
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be a JSON object")
    sources = manifest.get("sources", {})
    if not isinstance(sources, dict):
        raise ValueError("manifest field 'sources' must be a JSON object")
    unknown = sorted(set(sources) - set(_SOURCE_NAMES))
    if unknown:
        raise ValueError("unsupported manifest source(s): " + ", ".join(unknown))

    normalized: dict[str, object] = {}
    for source_name, source_data in sources.items():
        if not isinstance(source_data, dict):
            raise ValueError(f"manifest source {source_name!r} must be a JSON object")
        items = source_data.get("items", [] if source_name == "s3" else {})
        if source_name == "s3":
            # A failed scanner historically emits an empty object for `items`.
            if items == {}:
                items = []
            if not isinstance(items, list):
                raise ValueError("manifest source 's3'.items must be a JSON array")
            collections = {"items": (items, ("name",))}
        else:
            if not isinstance(items, dict):
                raise ValueError(
                    f"manifest source {source_name!r}.items must be a JSON object"
                )
            collections = {
                collection: (items.get(collection, []), required_fields)
                for collection, required_fields in _COLLECTION_FIELDS[source_name].items()
            }

        checked_items: dict[str, list[dict]] = {}
        for collection, (rows, required_fields) in collections.items():
            context = f"{source_name}.items"
            if collection != "items":
                context += f".{collection}"
            if not isinstance(rows, list):
                raise ValueError(f"manifest {context} must be a JSON array")
            checked_rows: list[dict] = []
            for index, row in enumerate(rows):
                if not isinstance(row, dict):
                    raise ValueError(f"manifest {context}[{index}] must be a JSON object")
                for field in required_fields:
                    value = row.get(field)
                    if not isinstance(value, str) or not value.strip():
                        raise ValueError(
                            f"manifest {context}[{index}].{field} must be a non-empty string"
                        )
                checked_rows.append(row)
            checked_items[collection] = checked_rows
        normalized[source_name] = (
            checked_items["items"] if source_name == "s3" else checked_items
        )
    return normalized


def _object_storage_uri(uri: str, namespace: str) -> str:
    if not isinstance(uri, str) or re.search(r"[\x00-\x1f\x7f]", uri):
        return ""
    match = re.fullmatch(
        r"s3a?://([^/:@?#\[\]\s'\"\\<>]+)(/.*)?", uri,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    bucket, key = match.group(1), match.group(2) or ""
    return f"oci://{bucket}@{namespace}{key}"


def _s3_plan(items: list[dict], ns: str) -> list[dict]:
    out = []
    for it in items:
        bucket = it["name"]
        out.append({
            "id": f"s3.bucket.{bucket}",
            "source": {"type": "s3_bucket", "name": bucket, "region": it.get("region")},
            "target": {"type": "oci_bucket", "name": bucket, "namespace": ns},
            "transform_chain": ["copy_s3_to_oci"],
        })
    return out


def _glue_plan(databases: list[dict], tables: list[dict], jobs: list[dict], ns: str) -> list[dict]:
    out = []
    for db in databases:
        out.append({
            "id": f"glue.db.{db['name']}",
            "source": {"type": "glue_database", "name": db["name"], "location_uri": db.get("location_uri", "")},
            "target": {"type": "aidp_schema", "name": db["name"]},
            "transform_chain": ["create_aidp_schema"],
        })
    for t in tables:
        s3_loc = t.get("location", "")
        oci_loc = _object_storage_uri(s3_loc, ns)
        asset = {
            "id": f"glue.table.{t['database']}.{t['name']}",
            "source": {"type": "glue_table", "database": t["database"], "name": t["name"],
                       "location": s3_loc, "partition_keys": t.get("partition_keys", []),
                       "table_type": t.get("table_type", "")},
            "target": {"type": "aidp_dcat_external_table", "schema": t["database"], "name": t["name"],
                       "location": oci_loc},
            "transform_chain": ["s3_to_oci_location", "create_aidp_external_table"],
        }
        if s3_loc and not oci_loc:
            asset["note"] = (
                f"Source location {s3_loc!r} is not an S3 URI; review target location manually"
            )
        out.append(asset)
    for j in jobs:
        out.append({
            "id": f"glue.job.{j['name']}",
            "source": {"type": "glue_job", "name": j["name"],
                       "script_location": j.get("script_location"),
                       "script": j.get("script", ""),
                       "glue_version": j.get("glue_version"), "worker_type": j.get("worker_type"),
                       "num_workers": j.get("num_workers")},
            "target": {"type": "aidp_job", "name": j["name"]},
            "transform_chain": ["glue_to_spark", "create_aidp_job"],
        })
    return out


def _athena_plan(queries: list[dict]) -> list[dict]:
    out = []
    for q in queries:
        out.append({
            "id": f"athena.query.{q['id']}",
            "source": {"type": "athena_named_query", "id": q["id"], "name": q["name"],
                       "workgroup": q.get("workgroup"), "database": q.get("database"),
                       "query": q["query"]},
            "target": {"type": "aidp_saved_query", "name": q["name"]},
            "transform_chain": ["athena_to_spark_sql", "register_aidp_saved_query"],
        })
    return out


def _emr_plan(clusters: list[dict], notebooks: list[dict]) -> list[dict]:
    out = []
    for c in clusters:
        out.append({
            "id": f"emr.cluster.{c['id']}",
            "source": {"type": "emr_cluster", "id": c["id"], "name": c["name"],
                       "applications": c.get("applications", []), "state": c.get("state")},
            "target": {"type": "aidp_spark_cluster", "name": c["name"]},
            "transform_chain": ["map_instance_shape", "provision_aidp_cluster"],
            "note": "EMR clusters are not 1:1 migrated; choose an AIDP cluster to run the workloads on",
        })
    for n in notebooks:
        target_name = n.get("name") or n["id"]
        out.append({
            "id": f"emr.notebook.{n['id']}",
            "source": {"type": "emr_notebook", "id": n["id"], "name": n.get("name"),
                       "cluster_id": n.get("cluster_id")},
            "target": {"type": "aidp_notebook", "name": target_name},
            "transform_chain": ["emr_to_aidp_notebook"],
        })
    return out


def _sagemaker_plan(notebooks: list[dict], training: list[dict], models: list[dict], pipelines: list[dict]) -> list[dict]:
    out = []
    for n in notebooks:
        out.append({
            "id": f"sagemaker.notebook.{n['name']}",
            "source": {"type": "sm_notebook_instance", "name": n["name"],
                       "status": n.get("status"), "instance_type": n.get("instance_type")},
            "target": {"type": "aidp_notebook", "name": n["name"]},
            "transform_chain": ["sm_notebook_to_aidp_notebook"],
        })
    for t in training:
        out.append({
            "id": f"sagemaker.training.{t['name']}",
            "source": {"type": "sm_training_job", "name": t["name"], "status": t.get("status")},
            "target": {"type": "aidp_mlops_experiment_run", "name": t["name"]},
            "transform_chain": ["sm_training_to_mlops_run"],
        })
    for m in models:
        out.append({
            "id": f"sagemaker.model.{m['name']}",
            "source": {"type": "sm_model", "name": m["name"]},
            "target": {"type": "aidp_mlops_registered_model", "name": m["name"]},
            "transform_chain": ["sm_model_to_mlops_model"],
        })
    for p in pipelines:
        out.append({
            "id": f"sagemaker.pipeline.{p['name']}",
            "source": {"type": "sm_pipeline", "name": p["name"], "display_name": p.get("display_name")},
            "target": {"type": "aidp_job", "name": p["name"]},
            "transform_chain": ["sm_pipeline_to_aidp_job"],
        })
    return out


def build_plan(manifest: dict, *, oci_namespace: str = OCI_NAMESPACE_DEFAULT) -> dict:
    """Walk manifest, produce a list of plan assets in dependency order."""
    oci_namespace = _validated_namespace(oci_namespace)
    sources = _validated_manifest_items(manifest)
    assets: list[dict] = []

    if "s3" in sources:
        assets += _s3_plan(sources["s3"], oci_namespace)
    if "glue" in sources:
        it = sources["glue"]
        assets += _glue_plan(it.get("databases", []), it.get("tables", []), it.get("jobs", []), oci_namespace)
    if "athena" in sources:
        it = sources["athena"]
        assets += _athena_plan(it.get("named_queries", []))
    if "emr" in sources:
        it = sources["emr"]
        assets += _emr_plan(it.get("clusters", []), it.get("notebooks", []))
    if "sagemaker" in sources:
        it = sources["sagemaker"]
        assets += _sagemaker_plan(it.get("notebooks", []), it.get("training_jobs", []),
                                   it.get("models", []), it.get("pipelines", []))

    seen_ids: set[str] = set()
    duplicate_ids: set[str] = set()
    for asset in assets:
        asset_id = asset["id"]
        if asset_id in seen_ids:
            duplicate_ids.add(asset_id)
        seen_ids.add(asset_id)
    if duplicate_ids:
        raise ValueError(
            "duplicate asset id(s) in manifest: " + ", ".join(sorted(duplicate_ids))
        )

    by_target: dict[str, int] = {}
    for a in assets:
        by_target[a["target"]["type"]] = by_target.get(a["target"]["type"], 0) + 1

    return {
        "plan_id": time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + f"-{uuid.uuid4().hex[:12]}",
        "source_manifest_account": manifest.get("account_id"),
        "source_manifest_region": manifest.get("region"),
        "source_manifest_scanned_at": manifest.get("scanned_at"),
        "target_aidp": {"namespace": oci_namespace},
        "summary": {"asset_count": len(assets), "by_target_type": by_target},
        "assets": assets,
    }


def write_plan(plan: dict, out: str | Path) -> Path:
    p = Path(out)
    p.parent.mkdir(parents=True, exist_ok=True)
    temp = p.with_name(f".{p.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("w", encoding="utf-8", newline="") as stream:
            stream.write(json.dumps(plan, indent=2))
            stream.flush()
            os.fsync(stream.fileno())
        temp.replace(p)
        try:
            directory_fd = os.open(p.parent, os.O_RDONLY)
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                try:
                    os.fsync(directory_fd)
                except OSError:
                    pass
            finally:
                os.close(directory_fd)
    finally:
        temp.unlink(missing_ok=True)
    return p


def summarize_plan(plan: dict) -> str:
    s = plan["summary"]
    lines = [
        f"plan {plan['plan_id']}",
        f"  source account: {plan['source_manifest_account']}  region: {plan['source_manifest_region']}",
        f"  total assets:   {s['asset_count']}",
        "",
        "  by target type:",
    ]
    for k, v in sorted(s["by_target_type"].items(), key=lambda kv: -kv[1]):
        lines.append(f"    {v:4d}  {k}")
    return "\n".join(lines)
