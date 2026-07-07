# SizeWise — Oracle AI Data Platform (AIDP) Capacity & Cost Planner

SizeWise turns a customer's business inputs — data volume, number of sources,
users, concurrency, SLA, growth, workload mix — into a **defensible AIDP cluster
topology and monthly cost**, across three scenarios (**Conservative / Balanced /
Cost‑Optimized**), and renders it as Oracle‑branded HTML reports ready to share
with a customer.

It generalizes the one‑off `generate_aidp_capacity_reports_v4.py` script (which
was hard‑wired to a single 17‑team / 255‑user Entel configuration) into an
**input‑driven sizing engine** with a simple web UI.

---

## Highlights

- **Hybrid sizing model** — per‑cluster size is derived from data volume, daily
  ingest, concurrency (Little's Law) and the SLA tier; cluster *count* is derived
  from users / teams / isolation model. Every input influences the result.
- **Three scenarios from one set of inputs** — Balanced (recommended),
  Conservative (HA + headroom + redundant services), Cost‑Optimized (tighter
  utilization, shorter hours, leaner footprint).
- **Calibrated** — fed the Entel reference inputs, the Balanced *core* (catalog +
  production + fixed services) reproduces the accepted `…-v20.html` figure of
  ~$62k/mo to within **−0.07 %**. A self‑test enforces ≤ 15 %.
- **Authoritative pricing** — AIDP Unit (AIDPU) rates straight from Oracle
  pricing (ARM 27 / AMD 67 / Intel 87 per OCPU‑hr, GPU 4110/GPU‑hr, memory
  3/GB‑hr, $0.001/AIDPU, Object Storage $0.0255/GB‑mo, 230‑AIDPU catalog floor).
- **Simple Flask UI** — a data‑driven form (25 inputs, sensible defaults) →
  three scenario cards → live report preview → one‑click download of the
  comprehensive and executive HTML reports.
- **Everything tunable** — all knobs live in `config.yaml`; no magic numbers
  buried in code. Also usable as a CLI and as an importable library.

---

## Quick start

```bash
cd tools/sizewise
./run.sh                      # http://127.0.0.1:8095/   (installs flask + pyyaml if needed)
# PORT=9000 ./run.sh          # custom port
```

Open the URL, fill in the customer inputs (only the name is required — defaults
fill the rest), click **Generate Resource Plan**, review the three scenarios and
download the reports.

### CLI

```bash
python3 sizing_engine.py                    # runs the Entel calibration case + self-test
python3 sizing_engine.py sample_inputs.json # size from a JSON inputs file
```

### As a library

```python
from sizing_engine import size, SizingInput, SizingConfig
import report_generator as rg

result = size(SizingInput(customer_name="Acme", total_source_data_tb=200,
                          num_users=400, num_concurrent_users=120, sla_tier="strict"))
open("acme.html", "w").write(rg.generate_comprehensive(result))
print(result["scenarios"]["balanced"]["total_usd_month"])
```

---

## Inputs

| Group | Inputs |
|---|---|
| **Core — Data & Users** | customer name, total source data (TB), # data sources, per‑source volumes (optional), total users, peak concurrent users (optional), SLA tier |
| **Growth & Workload** | monthly growth (or annual %), daily ingest (GB/day), workload mix (interactive/ETL/ML %), avg query runtime, queries/user/hr, business hours/day |
| **Topology** | # teams / isolation groups, users per team, isolation model (per_team / shared_pool / hybrid), processor preference (auto / ARM / AMD / Intel) |
| **Options & Services** | dev/test env, high availability, include storage cost, hot‑tier %, retention window, region, billing model (Universal Credits / BYOL) |

Optional numeric fields left blank are **derived** (e.g. concurrent users → 65 %
of total; monthly growth → from annual %; daily ingest → from growth).

---

## The sizing model (summary)

For each scenario, the engine runs, in order:

1. **Normalize** inputs → derived intermediates (concurrent users, teams,
   growth, ingest, per‑source split, workload mix, TB/team, catalog scale).
2. **Cluster count** — from the isolation model (`per_team` → one per team;
   `shared_pool` → few large clusters; `hybrid` → grouped teams), floored by a
   data‑scale guardrail so the fleet can hold the data.
3. **Per‑cluster size** = `max(` **concurrency demand** (calibrated
   executors‑per‑concurrent‑user, floored by Little's Law throughput),
   data‑scan parallelism (a realistic ~2 TB/executor of *active* scan — not
   total stored volume), memory working‑set floor `)`, then scenario headroom,
   clamped. The binding dimension is reported per cluster ("concurrency‑bound",
   "data‑bound", "memory‑floored"). Concurrency is the primary driver;
   stored‑data volume mainly drives **storage cost**, not compute.
4. **Default Master Catalog** (fixed, AMD, 24/7; scenarios pin its shape; honors
   the 230‑AIDPU platform floor; doubles under HA).
5. **Optional clusters** — a dedicated **ingest/ETL** cluster above an ingest
   threshold; a **GPU/ML** cluster when the ML mix > 0; a **dev/test** cluster
   when requested.
6. **Storage + services** — Object Storage (avg‑over‑retention), Autonomous DB,
   networking, data transfer.
7. **Roll‑up + envelope guardrails** — totals, $/user, $/TB, and OVER/UNDER‑sized
   flags.

### Scenarios

| Knob | Conservative | **Balanced** | Cost‑Optimized |
|---|---|---|---|
| Executor processor | AMD | **ARM** | ARM |
| Utilization | lower (more warm headroom) | **calibrated** | higher (tighter) |
| Autoscale min | = max (no cold start) | **~0.83 × max** | ~0.5 × max |
| Executor headroom | +25 % | **baseline** | −10 % |
| Hours/day | as input | **as input** | ×0.83 (floor 8h) |
| Catalog size | larger | **32 OCPU/512 GB** | smaller |
| High availability | forced on (2× catalog) | as input | off |
| Dev cluster | on | as input | off |
| Redundant services | 2× | **1×** | 1× |

See `config.yaml` for the exact numbers and `MODEL.md`‑style detail inline in
`sizing_engine.py` docstrings.

---

## Calibration

The Balanced scenario is calibrated to the accepted Entel v20 reference:

```
Balanced core = Default Master Catalog + production clusters + fixed services
              = $2,649.60 + $58,605.12 + $700.00 = $61,954.72 / month
reference (AIDP-Capacity-Comprehensive-v20.html) = $61,999.60 / month
delta = -0.07 %   (self-test enforces |delta| <= 15 %)
```

`python3 sizing_engine.py` prints and PASS/FAIL‑checks this on every run.
Storage, ingest and dev clusters are itemized **on top** of the core when their
inputs are enabled (the v20 reference excluded them).

---

## Project structure

```
tools/sizewise/
  app.py               Flask backend (GET / , /api/fields , /api/size , /api/report)
  sizing_engine.py     SizingConfig + SizingInput + size()  — the model
  pricing.py           AIDPPricing + AIDPCluster             — cost math (Oracle AIDPU rates)
  report_generator.py  comprehensive + executive HTML (Oracle-branded, v20 style)
  fields.py            UI field registry (single source of truth for the form)
  config.yaml          all tunable constants, SLA tiers, scenario overlays
  static/index.html    the input UI (self-contained)
  sample_inputs.json   the Entel calibration case
  requirements.txt     flask, pyyaml
  run.sh               launcher
  README.md            this file
```

---

## Tuning & extending

- **Re‑tune the model** without touching code: edit `config.yaml` (rates, SLA
  tiers, scenario knobs, envelope bands).
- **Add an input**: add it to `FIELD_GROUPS` in `fields.py` and to `SizingInput`
  in `sizing_engine.py` — the form is generated from the registry.
- **Change branding/layout**: edit the `_CSS` and section builders in
  `report_generator.py`.

---

## Caveats

- This is a **planning estimate, not a quote.** Validate against a pilot and your
  Oracle pricing/agreement.
- Costs use **30‑day months**; annual = monthly × 12.
- **BYOL** changes no AIDPU compute math (report footnote only); it may change
  your effective credit rate commercially.
- Additional‑service costs (Autonomous DB, networking, egress) are defensible
  **placeholders** — refine per region and workload.
- Concurrency sizing uses Little's Law with default query rate/runtime; override
  those for concurrency‑bound workloads.

---

*Not an official Oracle product. Internal field‑engineering aid. Uses Oracle AIDP
Unit pricing; verify current rates before quoting.*
