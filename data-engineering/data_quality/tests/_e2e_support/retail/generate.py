"""Seeded retail pack data generator.

Invoked exactly once per pack run (pack-scoped fixture contract — see
plan §Step 5, §B7). Tests that need to mutate data (e.g. Shape's
corrupted-sample scenario) take a deep copy from the fixture output;
they must not re-invoke this function, because doing so re-enters the
seeded ``RandomState`` and breaks reproducibility across runs that
exercise different test orderings.

30 daily partitions, ≤10k rows per dataset (budget from idea.md §4).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

SEED = 2026
# 31 partitions = 30 history + 1 current. Trend scenarios need
# exactly 30 seeded history points to match plan §Config shape
# conventions: `history_count: 30`. idea.md §4 asks for "≥30 daily
# partitions"; 31 is the minimum that leaves one partition free as
# the current test input while the remaining 30 land in
# `_seed_history`.
NUM_DAYS = 31


_PRODUCT_CATEGORIES = ("Electronics", "Clothing", "Grocery", "Home", "Sports")
_PRODUCT_NAMES = tuple(f"Product_{i:02d}" for i in range(50))
_CURRENCIES = ("USD", "EUR", "GBP", "JPY")
_STORE_CITIES = (
    "London", "Berlin", "Madrid", "Paris", "Rome", "Amsterdam",
    "New York", "Chicago", "Los Angeles", "Seattle", "Toronto", "Vancouver",
)
_STORE_COUNTRIES = ("UK", "DE", "ES", "FR", "IT", "NL", "US", "US", "US", "US", "CA", "CA")


@dataclass(frozen=True)
class RetailData:
    """In-memory snapshot of the retail pack's four tables.

    ``base_date`` is the first partition's sale_date; ``current_date``
    is the last partition (used by SLO pass and Shape/Pattern's
    ``slice_value``). Both are ISO-formatted ``YYYY-MM-DD`` strings.
    """

    sales_fact: pd.DataFrame
    inventory_fact: pd.DataFrame
    products_dim: pd.DataFrame
    stores_dim: pd.DataFrame
    base_date: str
    current_date: str
    history_dates: tuple[str, ...]  # three past-day filters for shape/pattern


def _daily_sales_rows(rng: np.random.RandomState, base_dt: datetime,
                      products_dim: pd.DataFrame, stores_dim: pd.DataFrame,
                      fresh_updated_at: str) -> list[dict]:
    """``fresh_updated_at`` overrides the final partition's
    ``updated_at`` so the plan-locked SLO PASS budget
    (``warn=4H err=8H`` per plan §Retail #1) holds regardless of when the test
    run starts.
    """
    rows: list[dict] = []
    sale_id = 1
    n_products = len(products_dim)
    n_stores = len(stores_dim)
    final_day = NUM_DAYS - 1
    for day in range(NUM_DAYS):
        dt = base_dt + timedelta(days=day)
        is_weekend = dt.weekday() >= 5
        trend = 1.0 + 0.003 * day  # mild positive trend
        base_n = 150.0 if is_weekend else 210.0
        n = max(30, int(rng.normal(base_n * trend, base_n * 0.08)))
        upd = (fresh_updated_at if day == final_day
               else dt.strftime("%Y-%m-%d %H:%M:%S"))
        for _ in range(n):
            # 1% null product_id (plan §Retail coverage "realistic noise").
            null_pid = rng.rand() < 0.01
            product_id = None if null_pid else int(rng.randint(1, n_products + 1))
            # 0.5% outlier amount ×10.
            outlier = rng.rand() < 0.005
            mean = 45.0 * trend
            raw = float(rng.normal(mean, mean * 0.35))
            amount = round(max(1.0, raw * (10.0 if outlier else 1.0)), 2)
            rows.append({
                "sale_id": sale_id,
                "sale_date": dt.strftime("%Y-%m-%d"),
                "store_id": int(rng.randint(1, n_stores + 1)),
                "product_id": product_id,
                "quantity": int(max(1, rng.poisson(2))),
                "amount": amount,
                "currency": _CURRENCIES[sale_id % len(_CURRENCIES)],
                "updated_at": upd,
            })
            sale_id += 1
    return rows


def _inventory_rows(rng: np.random.RandomState, base_dt: datetime,
                    products_dim: pd.DataFrame, stores_dim: pd.DataFrame) -> list[dict]:
    """Sparse inventory snapshots — ~100 (store, product) pairs per day."""
    rows: list[dict] = []
    n_products = len(products_dim)
    n_stores = len(stores_dim)
    pairs_per_day = 100
    for day in range(NUM_DAYS):
        dt = base_dt + timedelta(days=day)
        picked_stores = rng.randint(1, n_stores + 1, size=pairs_per_day)
        picked_products = rng.randint(1, n_products + 1, size=pairs_per_day)
        for store_id, product_id in zip(picked_stores, picked_products):
            null_oo = rng.rand() < 0.02
            if day == 22 and rng.rand() < 0.10:
                on_hand = int(rng.randint(-50, -5))
            else:
                on_hand = int(max(0, rng.poisson(30)))
            rows.append({
                "snapshot_date": dt.strftime("%Y-%m-%d"),
                "store_id": int(store_id),
                "product_id": int(product_id),
                "on_hand": on_hand,
                "on_order": (None if null_oo else int(max(0, rng.poisson(5)))),
                "updated_at": dt.strftime("%Y-%m-%d %H:%M:%S"),
            })
    return rows


def _generate_products(rng: np.random.RandomState) -> pd.DataFrame:
    n = 50
    return pd.DataFrame({
        "product_id": list(range(1, n + 1)),
        "name": [_PRODUCT_NAMES[i] for i in range(n)],
        "category": [_PRODUCT_CATEGORIES[i % len(_PRODUCT_CATEGORIES)] for i in range(n)],
        "unit_price": [round(float(rng.uniform(5, 200)), 2) for _ in range(n)],
        "is_active": [bool(rng.rand() > 0.1) for _ in range(n)],
    })


def _generate_stores() -> pd.DataFrame:
    n = 12
    opened_anchor = datetime(2018, 1, 1)
    return pd.DataFrame({
        "store_id": list(range(1, n + 1)),
        "city": list(_STORE_CITIES[:n]),
        "country": list(_STORE_COUNTRIES[:n]),
        "opened_at": [
            (opened_anchor + timedelta(days=i * 137)).strftime("%Y-%m-%d %H:%M:%S")
            for i in range(n)
        ],
    })


def generate(base_date: datetime | None = None) -> RetailData:
    """Generate the retail pack's four tables with deterministic noise.

    Called once per pack run from the pack-scoped ``retail_data``
    fixture. ``base_date`` defaults to ``today - (NUM_DAYS - 1)`` (day
    resolution), so the final partition's ``sale_date`` is today —
    required for SLO ``max_column`` PASS against ``datetime.now()``.
    """
    if base_date is None:
        today = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        base_date = today - timedelta(days=NUM_DAYS - 1)

    # Tight SLO PASS: ``warn=4H err=8H`` per plan §Retail #1 — leave a
    # 10-minute buffer so pack runtime variance (≤50 min per CI
    # gate) stays far below warn=4H.
    fresh_ts = (datetime.now() - timedelta(minutes=10)).strftime(
        "%Y-%m-%d %H:%M:%S")

    rng = np.random.RandomState(SEED)
    products_dim = _generate_products(rng)
    stores_dim = _generate_stores()

    sales_rows = _daily_sales_rows(
        rng, base_date, products_dim, stores_dim, fresh_ts)
    inventory_rows = _inventory_rows(rng, base_date, products_dim, stores_dim)

    sales_fact = pd.DataFrame(sales_rows)
    inventory_fact = pd.DataFrame(inventory_rows)

    sorted_dates = sorted(sales_fact["sale_date"].unique())
    current_date = sorted_dates[-1]
    # 3 past dates spaced 7 days apart, ending 7 days before current.
    history_dates = tuple(sorted_dates[i] for i in (-22, -15, -8))

    return RetailData(
        sales_fact=sales_fact,
        inventory_fact=inventory_fact,
        products_dim=products_dim,
        stores_dim=stores_dim,
        base_date=sorted_dates[0],
        current_date=current_date,
        history_dates=history_dates,
    )
