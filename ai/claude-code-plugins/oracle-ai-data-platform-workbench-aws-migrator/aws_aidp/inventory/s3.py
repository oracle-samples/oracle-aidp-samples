"""S3 inventory — buckets + (best-effort) region + sample object key.

We deliberately skip per-bucket object count / total size for the default scan.
Computing those accurately requires CloudWatch BucketSizeBytes metrics or full
list_objects_v2 sweeps, both of which can be slow on large estates. Pass
`with_metrics=True` to opt in.
"""
from __future__ import annotations

from typing import Any

from aws_aidp.aws_client import AwsClient, AwsConfig


def _bucket_region(s3, name: str) -> str | None:
    loc = s3.get_bucket_location(Bucket=name)["LocationConstraint"]
    if not loc:
        return "us-east-1"  # AWS returns None for us-east-1 historically.
    if loc == "EU":
        return "eu-west-1"  # Legacy LocationConstraint returned for old EU buckets.
    return loc


def _bucket_metrics(cw, name: str, region: str) -> tuple[dict, str | None]:
    """CloudWatch daily metrics — cheap and approximate. Skipped by default."""
    import datetime as dt
    out = {"size_bytes": None, "object_count": None}
    end = dt.datetime.utcnow()
    start = end - dt.timedelta(days=2)
    try:
        for metric, dim, key in (
            ("BucketSizeBytes", "StandardStorage", "size_bytes"),
            ("NumberOfObjects", "AllStorageTypes", "object_count"),
        ):
            r = cw.get_metric_statistics(
                Namespace="AWS/S3",
                MetricName=metric,
                Dimensions=[
                    {"Name": "BucketName", "Value": name},
                    {"Name": "StorageType", "Value": dim},
                ],
                StartTime=start, EndTime=end, Period=86400, Statistics=["Average"],
            )
            dp = r.get("Datapoints", [])
            if dp:
                out[key] = int(sorted(dp, key=lambda d: d["Timestamp"])[-1]["Average"])
    except Exception as e:
        return out, str(e)
    return out, None


def scan(client: AwsClient, *, with_metrics: bool = False) -> dict[str, Any]:
    s3 = client.client("s3")
    try:
        resp = s3.list_buckets()
    except Exception as e:
        return {"summary": {"error": str(e)}, "items": []}
    items: list[dict] = []
    warnings: list[dict[str, str]] = []
    for b in resp.get("Buckets", []):
        name = b.get("Name")
        if not name:
            warnings.append({
                "scope": "bucket",
                "resource": "<unknown>",
                "error": "list_buckets returned an entry without Name",
            })
            continue
        try:
            region = _bucket_region(s3, name)
        except Exception as e:
            region = None
            warnings.append({"scope": "bucket_region", "resource": name, "error": str(e)})
        item = {"name": name, "region": region, "created_at": str(b.get("CreationDate", ""))}
        if with_metrics and region:
            # CloudWatch S3 metrics must be queried in the bucket's region.
            cw_cfg = AwsConfig(profile=client.cfg.profile, region=region)
            try:
                cw = AwsClient(cw_cfg).client("cloudwatch")
                metrics, error = _bucket_metrics(cw, name, region)
                item.update(metrics)
                if error:
                    warnings.append({"scope": "bucket_metrics", "resource": name, "error": error})
            except Exception as e:
                warnings.append({"scope": "bucket_metrics", "resource": name, "error": str(e)})
        items.append(item)
    by_region: dict[str, int] = {}
    for it in items:
        by_region[it["region"] or "unknown"] = by_region.get(it["region"] or "unknown", 0) + 1
    return {
        "summary": {"bucket_count": len(items), "by_region": by_region},
        "items": items,
        "warnings": warnings,
    }
