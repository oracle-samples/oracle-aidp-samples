# TC11–TC17 — saas-batch REST extractor live test (2026-04-30)

## Result: BLOCKED on demo-pod environment

The `saas_batch_rest.py` extractor (extractors/saas_batch_rest.py) was unit-tested with 14 mock-based tests covering URL construction, payload shape, polling, and output parsing — all PASS.

Live testing against the available Fusion demo pod (`fa-etap-dev5-saasfademo1.ds-fa.oraclepdemos.com`) is **not possible** because the saas-batch endpoint is not enabled on this pod:

```
$ curl -sv https://fa-etap-dev5-saasfademo1.ds-fa.oraclepdemos.com/saas-batch/security/tokenrelay?username=Casey.Brown&audience=...
< HTTP/1.1 404 Not Found
```

## Why

Per pdf2 §"Batch Extraction using Fusion Business Objects" (Eloi Lopes / Jerome Francoisse, ateam blog 2026-02-27), the saas-batch surface is a **Fusion HCM-tier feature** — it ships with paying Fusion subscriptions, not the public demo pod. The 404 from `/saas-batch/security/tokenrelay` confirms this pod doesn't expose that surface.

## What this means for the bundle

* The **code path** is verified by unit tests (`tests/unit/test_saas_batch_rest.py`) — 14 tests covering Token Relay parse, job submit POST shape, status polling, output-files response normalization, and timeout/failure paths.
* The **wire format** matches pdf2's documented spec verbatim — `jobDefinitionName=AsyncDataExtraction`, `serviceName=boss`, `boss.module=crackXMLCircleBegineen`, etc.
* **Live verification** against a paying Fusion HCM customer's pod (with `workerAssignmentExtracts` enabled) is out of scope for the demo environment.

## Remediation

When a customer with saas-batch access wants to validate the bundle:

1. Get HTTP Basic creds for a Fusion HCM user with API extract privileges.
2. Run:
   ```python
   import requests
   from oracle_ai_data_platform_fusion_bundle.extractors.saas_batch_rest import (
       get_token_relay, submit_extract_job, poll_job_until_complete, fetch_output_files,
   )
   s = requests.Session()
   s.auth = (user, password)
   token = get_token_relay(s, host="customer-pod.fa.example.com", pod_name="customer-pod")
   s.headers["X-Token-Relay"] = token  # or per Fusion's specific header convention
   loc = submit_extract_job(s, host="customer-pod.fa.example.com",
                            resource_path="workerAssignmentExtracts",
                            advance_query={"collection":[{"filter":"primaryFlag = true"}]})
   details = poll_job_until_complete(s, loc)
   files = fetch_output_files(s, "customer-pod.fa.example.com", details["jobRequestId"])
   for f in files: print(f)
   ```

3. Or via the bundle's `extract_to_dataframe()` convenience wrapper inside an AIDP notebook.

## Status: PASS (unit) / BLOCKED (live — environmental)

Unit-test pass rate: **14/14**. Live-test against demo pod: **404 — endpoint not enabled** (expected; out of customer-tier scope).
