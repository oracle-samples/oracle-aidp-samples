# Retail industry demo pack

Exercises all six Qualifire validation types against a canonical
retail warehouse (`sales_fact`, `inventory_fact`, `products_dim`,
`stores_dim`).

## Files

| Path | Purpose |
| --- | --- |
| `config.yaml` | 13 declarative datasets — one per scenario |
| [`../../../tests/test_e2e_industries/retail/test_retail_e2e.py`](../../../tests/test_e2e_industries/retail/test_retail_e2e.py) | Executable pytest mirror with seeded fixtures |
| [`../../../tests/_e2e_support/retail/generate.py`](../../../tests/_e2e_support/retail/generate.py) | Seeded pandas generator (`SEED=2026`, 31 daily partitions: 30 history + 1 current, ≤10k rows/dataset) |

## Coverage matrix

| Scenario key | Validation type | Expected outcome | Backends |
| --- | --- | --- | --- |
| `slo_pass` | SLO | PASS — `updated_at` within 4h | pandas, spark |
| `slo_fail` | SLO | ERROR — `updated_at` shifted 30h back | pandas, spark |
| `threshold_pass` | Threshold | PASS — `row_count >= 100` | pandas, spark |
| `threshold_fail` | Threshold | ERROR — `null_pid_pct > 0.5` | pandas, spark |
| `drift_pass` | Drift (historical) | PASS — avg amount within ±2% of history | pandas, spark |
| `drift_fail` | Drift (historical) | ERROR — history is ×3 current | pandas, spark |
| `trend_pass` | Trend (Prophet) | PASS — flat-synthetic 30-day history (seed 42) → current-day `SUM(amount)` inside forecast band | pandas, spark |
| `trend_fail` | Trend (Prophet) | ERROR — actual ×10 above forecast band | pandas, spark |
| `shape_pass` | Shape (IsolationForest) | PASS — current day matches historical shape | pandas, spark |
| `shape_fail` | Shape (IsolationForest) | WARN/ERR — current-day `currency` nulled + `amount` ×10 → anomaly_ratio above warn threshold (literal schema drift not exercised by design — see plan "Shape schema drift scope caveat") | pandas, spark |
| `query_join` | Threshold on SQL `JOIN` | PASS — category revenue roll-up | pandas, spark |
| `pattern_pass` | Pattern (RF two-sample) | PASS — `auc < 0.65` | pandas, spark |
| `pattern_fail` | Pattern (RF two-sample) | WARN/ERR — inflated amount/quantity, SHAP drivers populated | pandas, spark |

## Running the pytest pack

```bash
pytest tests/test_e2e_industries/retail/ -v
```

Produces an HTML health report at
`$QUALIFIRE_ARTIFACTS_DIR/industry-reports/retail/report.html` (falls
back to a pytest tmp path when the env var is unset).

## Running the YAML standalone

The `config.yaml` is authored for Spark catalogs. Point `owner`,
`bu`, `system_table`, and the table names at your warehouse, then:

```bash
qualifire run --config examples/industries/retail/config.yaml
```

Thresholds and hyperparameters in the YAML are illustrative; the
pytest fixtures inject seeded deterministic values (generator seed
`2026`) to guarantee the pass/fail outcomes listed above.
