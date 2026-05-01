"""Fusion data extractors.

Three extraction paths, each in its own module:

* :mod:`bicc` — bulk BICC via the AIDP ``aidataplatform`` Spark format
  (``type=FUSION_BICC``). Mirrors the official Oracle AIDP sample. Recommended
  default for batch-style loads.
* :mod:`rest` — paged Fusion REST API for tiny dimensions (≤ 5k rows). Hard-capped
  at 499 rows/page per MOS Doc ID 2429019.1.
* :mod:`saas_batch_rest` — Fusion saas-batch REST batch-extraction API for v1.5+
  (modern integration, JSON output, fine-grained filtering).

The orchestrator's router (``orchestrator.routing``) chooses among them per
the rule from pdf2's conclusion: BICC for ≥10k rows / day; saas-batch for
hourly or fine-grained pulls; REST for small reference dimensions only.
"""
