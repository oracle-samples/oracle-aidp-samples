"""Fusion saas-batch REST batch-extraction extractor (Tier 3, v1.5+).

Mirrors the pdf2 §"Batch Extraction using Fusion Business Objects" pattern
(Eloi Lopes / Jerome Francoisse, ateam blog 2026-02-27). Use case: targeted
JSON extracts with fine-grained filters; complements BICC for higher-frequency
or non-bulk pulls.

The flow has four steps:

1. **Token Relay** — `GET /saas-batch/security/tokenrelay` with HTTP Basic.
   Returns a token relay value to use in subsequent calls.
2. **Submit job** — `POST /api/saas-batch/jobScheduler/v1/jobRequests` with
   ``jobDefinitionName=AsyncDataExtraction``, ``serviceName=boss``, and
   ``boss.*`` request parameters describing the resource to extract.
3. **Poll** — `GET {jobLocation}` (returned in the Location header) until
   ``jobStatus=COMPLETED`` (or fails / times out).
4. **Fetch outputs** — `GET /api/saas-batch/jobFileManager/v1/jobRequests/{jobId}/outputFiles`
   and download the JSON files.

Output is JSON (not zipped CSV like BICC), suitable for cases where you want
fine-grained filtering (`boss.maxAdvanceQuery`) or hourly/event-driven pulls.
"""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlparse


def get_token_relay(
    session: Any,
    host: str,
    pod_name: str,
    *,
    audience_template: str = "urn:opc:resource:fusion:pod:name={pod}",
    scope: str = "*",
    timeout: int = 60,
) -> str:
    """Acquire a Token Relay required for saas-batch calls.

    Per pdf2 p3:

        GET https://{host}/saas-batch/security/tokenrelay
            ?username={username}
            &audience=urn:opc:resource:fusion:pod:name={pod}
            &scope=*
            (Authorization: Basic <base64(user:pwd)>)

    Args:
        session: A ``requests.Session`` already configured with HTTP Basic auth
            (e.g. via ``requests.Session().auth = (user, pwd)``).
        host: Fusion pod hostname (e.g. ``my-pod.fa.us-phoenix-1.oraclecloud.com``).
            Will be used as-is; do NOT include the scheme.
        pod_name: Pod identifier passed in the audience query param. Often the
            same as the host's first label (e.g. ``my-pod`` from
            ``my-pod.fa.us-phoenix-1.oraclecloud.com``).
        audience_template: Format string with ``{pod}`` placeholder. Override if
            your Fusion environment uses a different audience grammar.
        scope: OAuth-style scope. Default ``*``.
        timeout: Per-request timeout (seconds).

    Returns:
        The token relay value (string) to pass to ``submit_extract_job``.

    Raises:
        requests.HTTPError: If the token endpoint returns non-2xx.
        ValueError: If the response body doesn't contain a recognizable token.
    """
    audience = audience_template.format(pod=pod_name)
    # Pull username from the session's auth tuple
    auth = getattr(session, "auth", None)
    username = auth[0] if isinstance(auth, tuple) else None
    if username is None:
        raise ValueError("session.auth must be set to (user, pwd) before calling get_token_relay()")

    url = f"https://{host}/saas-batch/security/tokenrelay"
    params = {
        "username": username,
        "audience": audience,
        "scope": scope,
    }
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()

    # Per pdf2, the response text contains the token; parse defensively across
    # plain-text or JSON-wrapped variants.
    body = response.text.strip()
    if body.startswith("{"):
        import json
        data = json.loads(body)
        for key in ("token", "access_token", "tokenRelay"):
            if key in data:
                return str(data[key])
        raise ValueError(f"token-relay JSON had no recognizable token key: {sorted(data.keys())}")
    if not body:
        raise ValueError("empty response from /saas-batch/security/tokenrelay")
    return body


def submit_extract_job(
    session: Any,
    host: str,
    *,
    resource_path: str,
    resource_version: str = "11",
    output_format: str = "json",
    extract_module: str = "crackXMLCircleBegineen",
    advance_query: dict[str, Any] | None = None,
    job_definition_name: str = "AsyncDataExtraction",
    service_name: str = "boss",
    timeout: int = 60,
) -> str:
    """Submit a saas-batch extraction job.

    Per pdf2 p4:

        POST https://{host}/api/saas-batch/jobScheduler/v1/jobRequests
        {
          "jobDefinitionName": "AsyncDataExtraction",
          "serviceName": "boss",
          "requestParameters": {
            "boss.module": "crackXMLCircleBegineen",
            "boss.resource.path": "{resource_path}",
            "boss.resource.version": "{resource_version}",
            "boss.outputFormat": "json",
            "boss.maxAdvanceQuery": "<filter JSON>"
          }
        }

    The response 202 includes a ``Location`` header pointing at the job — use
    that with :func:`poll_job_until_complete`.

    Args:
        session: ``requests.Session`` with auth + Token Relay header attached
            (caller's responsibility — see module docstring for full sequence).
        host: Fusion pod hostname.
        resource_path: e.g. ``"workerAssignmentExtracts"`` (per pdf2 p4 example).
        resource_version: Fusion REST version segment. Default ``"11"``.
        output_format: ``"json"`` (default) or ``"csv"`` per pod support.
        extract_module: ``boss.module`` value. Default matches pdf2's example;
            override per Fusion release.
        advance_query: Optional dict serialized to JSON for ``boss.maxAdvanceQuery``.
            Use to express filter conditions (e.g. ``{"collection":[{"filter":"primaryFlag = true"}]}``).
        job_definition_name: Default ``AsyncDataExtraction``.
        service_name: Default ``boss``.
        timeout: Per-request timeout.

    Returns:
        The job's ``Location`` URL — pass to :func:`poll_job_until_complete`.

    Raises:
        requests.HTTPError: On non-2xx.
        KeyError: If the response is missing ``Location`` header.
    """
    import json

    request_params: dict[str, str] = {
        "boss.module": extract_module,
        "boss.resource.path": resource_path,
        "boss.resource.version": resource_version,
        "boss.outputFormat": output_format,
    }
    if advance_query is not None:
        request_params["boss.maxAdvanceQuery"] = json.dumps(advance_query)

    payload = {
        "jobDefinitionName": job_definition_name,
        "serviceName": service_name,
        "requestParameters": request_params,
    }
    url = f"https://{host}/api/saas-batch/jobScheduler/v1/jobRequests"
    response = session.post(url, json=payload, timeout=timeout)
    response.raise_for_status()

    location = response.headers.get("Location")
    if not location:
        raise KeyError("submit_extract_job: response missing 'Location' header")
    return location


def poll_job_until_complete(
    session: Any,
    job_location: str,
    *,
    poll_interval_seconds: int = 30,
    timeout_seconds: int = 3600,
) -> dict[str, Any]:
    """Poll a saas-batch job until ``jobStatus = COMPLETED`` (or fails / times out).

    Returns the final ``jobDetails[0]`` dict (which contains ``jobRequestId``
    needed by :func:`fetch_output_files`).

    Args:
        session: Same authenticated ``requests.Session`` from ``submit_extract_job``.
        job_location: The ``Location`` URL returned from job submission.
        poll_interval_seconds: How often to poll. Default 30.
        timeout_seconds: Give up after this many seconds. Default 1h.

    Raises:
        TimeoutError: If timeout reached before COMPLETED.
        RuntimeError: If status becomes FAILED / CANCELLED.
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = session.get(job_location, timeout=60)
        response.raise_for_status()
        body = response.json()
        details_list = body.get("jobDetails") or []
        if not details_list:
            time.sleep(poll_interval_seconds)
            continue
        details = details_list[0]
        status = (details.get("jobStatus") or "").upper()
        if status == "COMPLETED":
            return details
        if status in ("FAILED", "CANCELLED", "ERROR"):
            raise RuntimeError(f"saas-batch job ended in status {status}: {details}")
        time.sleep(poll_interval_seconds)
    raise TimeoutError(
        f"saas-batch job at {job_location} did not reach COMPLETED in {timeout_seconds}s"
    )


def fetch_output_files(
    session: Any,
    host: str,
    job_request_id: str,
    *,
    timeout: int = 60,
) -> list[dict[str, Any]]:
    """List the output files produced by a completed saas-batch job.

    Per pdf2 p4:

        GET https://{host}/api/saas-batch/jobFileManager/v1/jobRequests/{jobId}/outputFiles

    Args:
        session: Same authenticated ``requests.Session``.
        host: Fusion pod hostname.
        job_request_id: From :func:`poll_job_until_complete`'s returned details.
        timeout: Per-request timeout.

    Returns:
        List of file-descriptor dicts. Each dict's shape is Fusion-version-
        dependent; typical fields include a download URL and content type.
        Caller iterates and downloads each via a separate ``session.get(url)``.
    """
    url = f"https://{host}/api/saas-batch/jobFileManager/v1/jobRequests/{job_request_id}/outputFiles"
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    body = response.json()
    # Fusion's response shape varies; normalize to a list
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("items", "outputFiles", "files"):
            if isinstance(body.get(key), list):
                return body[key]
    return []


def extract_to_dataframe(
    spark: Any,
    session: Any,
    host: str,
    *,
    resource_path: str,
    resource_version: str = "11",
    output_format: str = "json",
    advance_query: dict[str, Any] | None = None,
    poll_interval_seconds: int = 30,
    timeout_seconds: int = 3600,
):
    """Convenience wrapper: submit + poll + fetch + materialize as a Spark DataFrame.

    For one-shot use cases. For long-running pipelines, call the four steps
    directly so you can persist intermediate state.

    Args:
        spark: Active SparkSession.
        session: ``requests.Session`` already authenticated (Basic auth + Token Relay).
        host: Fusion pod hostname.
        resource_path: e.g. ``"workerAssignmentExtracts"``.
        Other args: see :func:`submit_extract_job`.

    Returns:
        A Spark DataFrame holding the JSON output rows.

    Notes:
        - The output URLs from :func:`fetch_output_files` are downloaded with
          the same authenticated session, written to a tempfile, and read by
          ``spark.read.json``. For very large outputs prefer keeping the URLs
          in OCI Object Storage and reading directly with ``oci://`` paths.
    """
    import json
    import os
    import tempfile

    job_location = submit_extract_job(
        session,
        host,
        resource_path=resource_path,
        resource_version=resource_version,
        output_format=output_format,
        advance_query=advance_query,
    )
    details = poll_job_until_complete(
        session,
        job_location,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )
    job_request_id = details.get("jobRequestId")
    if not job_request_id:
        raise ValueError(f"poll details missing jobRequestId: {details}")
    files = fetch_output_files(session, host, str(job_request_id))

    # Download all output files locally, concatenate to one JSONL file, read with Spark.
    with tempfile.TemporaryDirectory(prefix="aidp_fusion_saasbatch_") as tmpdir:
        local_paths = []
        for i, fdesc in enumerate(files):
            url = (
                fdesc.get("url")
                or fdesc.get("downloadUrl")
                or fdesc.get("href")
            )
            if not url:
                continue
            r = session.get(url, timeout=300)
            r.raise_for_status()
            body = r.text
            local = os.path.join(tmpdir, f"part-{i:04d}.json")
            with open(local, "w", encoding="utf-8") as f:
                f.write(body)
            local_paths.append(local)
        if not local_paths:
            raise ValueError("saas-batch job completed but produced 0 downloadable files")
        return spark.read.json(local_paths)
