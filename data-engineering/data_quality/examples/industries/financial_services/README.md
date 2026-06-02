# Financial Services industry demo pack

Exercises all six Qualifire validation types against a canonical
retail-banking warehouse (`transactions_fact`, `market_prices_fact`,
`accounts_dim`, `merchants_dim`).

## Files

| Path | Purpose |
| --- | --- |
| `config.yaml` | 13 declarative datasets — one per scenario |
| [`../../../tests/test_e2e_industries/financial_services/test_financial_services_e2e.py`](../../../tests/test_e2e_industries/financial_services/test_financial_services_e2e.py) | Executable pytest mirror with seeded fixtures |
| [`../../../tests/_e2e_support/financial_services/generate.py`](../../../tests/_e2e_support/financial_services/generate.py) | Seeded pandas generator (`SEED=2028`, 31 daily partitions: 30 history + 1 current) |

## Coverage matrix

| Scenario key | Validation type | Expected outcome | Backends |
| --- | --- | --- | --- |
| `slo_pass` / `slo_fail` | SLO freshness on `updated_at` (transactions / market prices) | PASS / ERROR | pandas, spark |
| `threshold_pass` / `threshold_fail` | Row-count bound / negative-amount count | PASS / ERROR | pandas, spark |
| `drift_pass` / `drift_fail` | `AVG(amount)` vs 3 past 7-day windows | PASS / ERROR | pandas, spark |
| `trend_pass` / `trend_fail` | Prophet on daily `SUM(amount)` | PASS / ERROR | pandas, spark |
| `shape_pass` / `shape_fail` | IsolationForest + channel/MCC distributional shift | PASS / WARN-ERR | pandas, spark |
| `query_join` | `transactions × merchants` MCC-category aggregate | PASS | pandas, spark |
| `pattern_pass` / `pattern_fail` | RF two-sample on `amount` / `channel` shift | PASS / WARN-ERR | pandas, spark |

## Running

```bash
pytest tests/test_e2e_industries/financial_services/ -v
qualifire run --config examples/industries/financial_services/config.yaml  # point at your warehouse first
```

Produces an HTML health report at
`$QUALIFIRE_ARTIFACTS_DIR/industry-reports/financial_services/report.html`
(falls back to a pytest tmp path when the env var is unset, per plan
§147–150).
