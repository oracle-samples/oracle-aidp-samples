# Life Sciences industry demo pack

Exercises all six Qualifire validation types against a canonical
clinical-trial warehouse (`lab_results_fact`, `adverse_events_fact`,
`patients_dim`, `sites_dim`).

## Files

| Path | Purpose |
| --- | --- |
| `config.yaml` | 13 declarative datasets — one per scenario |
| [`../../../tests/test_e2e_industries/life_sciences/test_life_sciences_e2e.py`](../../../tests/test_e2e_industries/life_sciences/test_life_sciences_e2e.py) | Executable pytest mirror with seeded fixtures |
| [`../../../tests/_e2e_support/life_sciences/generate.py`](../../../tests/_e2e_support/life_sciences/generate.py) | Seeded pandas generator (`SEED=2027`, 31 daily partitions: 30 history + 1 current) |

## Coverage matrix

| Scenario key | Validation type | Expected outcome | Backends |
| --- | --- | --- | --- |
| `slo_pass` / `slo_fail` | SLO freshness on `updated_at` | PASS / ERROR | pandas, spark |
| `threshold_pass` / `threshold_fail` | Row-count bound / `flag='HIGH'` ratio | PASS / ERROR | pandas, spark |
| `drift_pass` / `drift_fail` | `AVG(value)` vs 3 past 7-day windows | PASS / ERROR | pandas, spark |
| `trend_pass` / `trend_fail` | Prophet on daily `COUNT(event_id)` | PASS / ERROR | pandas, spark |
| `shape_pass` / `shape_fail` | IsolationForest on current-day sample vs 3 past daily samples — fail path simulates a `unit`→`units` rename via NaN pattern + biases `value` 3× upward (distributional drift, not literal schema drift; see plan "Shape schema drift scope caveat") | PASS / WARN-ERR | pandas, spark |
| `query_join` | `lab_results_fact` LEFT JOIN `patients_dim` on `patient_id`; gate `orphan_count = 0` (plan §LS #11) | PASS | pandas, spark |
| `pattern_pass` / `pattern_fail` | RF two-sample on `value` / `flag` shift | PASS / WARN-ERR | pandas, spark |

## Running

```bash
pytest tests/test_e2e_industries/life_sciences/ -v
qualifire run --config examples/industries/life_sciences/config.yaml  # point at your warehouse first
```

Produces an HTML health report at
`$QUALIFIRE_ARTIFACTS_DIR/industry-reports/life_sciences/report.html`
(falls back to a pytest tmp path when the env var is unset, per plan
§147–150).
