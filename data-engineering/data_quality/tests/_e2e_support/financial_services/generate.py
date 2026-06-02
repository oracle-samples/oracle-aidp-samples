"""Seeded Financial Services pack data generator (SEED=2028).

Domain: retail banking transactions + daily market prices, indexed
by account / merchant dimensions, per plan §Financial Services
coverage (locked).

Tables (plan spec, locked):
- ``transactions_fact``   — ~7,000 rows; ``txn_id``, ``txn_ts``,
  ``account_id``, ``merchant_id``, ``amount``, ``currency``,
  ``channel``, ``status``, ``updated_at``. 0.3% null ``merchant_id``,
  heavy-tail amount (log-normal), day-24 fraud spike.
- ``market_prices_fact``  — ~900 rows; ``price_date``, ``symbol``,
  ``open``, ``high``, ``low``, ``close``, ``volume``, ``updated_at``.
  1% null ``volume``, day-18 black-swan.
- ``accounts_dim``        — 150 rows; ``account_id``, ``opened_at``,
  ``customer_id``, ``segment``, ``risk_band``, ``active``.
- ``merchants_dim``       — 60 rows; ``merchant_id``, ``name``,
  ``mcc``, ``country``. >20 distinct MCCs so Shape label-encoding
  lands on a meaningful column.

Invoked exactly once per pack run (pack-scoped fixture contract).
Tests that mutate data must deep-copy; re-invoking ``generate()``
re-enters the seeded ``RandomState``.

30 daily partitions, ≤10k rows per dataset (budget from idea.md §4).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

SEED = 2028
# 31 partitions = 30 history + 1 current. Trend scenarios need
# exactly 30 seeded history points to match plan §Config shape
# conventions: `history_count: 30`. Plan §Financial Services
# coverage asks for "≥30 daily partitions"; 31 is the minimum that
# leaves one partition free as the current test input while the
# remaining 30 land in `_seed_history`.
NUM_DAYS = 31

_CHANNELS = ("web", "mobile", "pos", "atm", "wire")
_STATUSES = ("settled", "settled", "settled", "pending", "reversed")
_CURRENCIES = ("USD", "EUR", "GBP")
_SEGMENTS = ("retail", "smb", "private", "corporate")
_RISK_BANDS = ("low", "medium", "high")
# 24 MCC codes so label-encoding yields a meaningful categorical fan-out.
_MCCS = tuple(f"{5000 + 37 * i}" for i in range(24))
_MERCHANT_NAMES = tuple(f"Merchant_{i:02d}" for i in range(60))
_MERCHANT_COUNTRIES = ("US", "UK", "DE", "FR", "IT", "ES", "NL", "JP",
                       "SG", "AU", "CA", "BR")
_SYMBOLS = tuple(f"SYM{i:02d}" for i in range(30))
# Per-symbol base price, deterministic across Python processes.
# Python's builtin ``hash`` is salted (``PYTHONHASHSEED=random`` by
# default), so the previous ``hash(sym) % 100`` drifted between CI
# runs and broke the pack's reproducibility contract. The index-based
# formula below lands in the same [50, 150) range but is stable.
_SYMBOL_BASE_PRICE = {sym: 50.0 + (i * 37) % 100 for i, sym in enumerate(_SYMBOLS)}


@dataclass(frozen=True)
class FinServicesData:
    """In-memory snapshot of the Financial Services pack's four tables."""

    transactions_fact: pd.DataFrame
    market_prices_fact: pd.DataFrame
    accounts_dim: pd.DataFrame
    merchants_dim: pd.DataFrame
    base_date: str
    current_date: str
    history_dates: tuple[str, ...]


def _generate_merchants() -> pd.DataFrame:
    n = 60
    return pd.DataFrame({
        "merchant_id": list(range(1, n + 1)),
        "name": list(_MERCHANT_NAMES),
        "mcc": [_MCCS[i % len(_MCCS)] for i in range(n)],
        "country": [_MERCHANT_COUNTRIES[i % len(_MERCHANT_COUNTRIES)]
                    for i in range(n)],
    })


def _generate_accounts(rng: np.random.RandomState) -> pd.DataFrame:
    n = 150
    opened_anchor = datetime(2020, 1, 1)
    return pd.DataFrame({
        "account_id": list(range(1, n + 1)),
        "opened_at": [
            (opened_anchor + timedelta(days=i * 9)).strftime("%Y-%m-%d %H:%M:%S")
            for i in range(n)
        ],
        "customer_id": [int(rng.randint(1, 61)) for _ in range(n)],
        "segment": [_SEGMENTS[i % len(_SEGMENTS)] for i in range(n)],
        "risk_band": [_RISK_BANDS[i % len(_RISK_BANDS)] for i in range(n)],
        "active": [bool((i % 13) != 0) for i in range(n)],
    })


def _daily_txn_rows(rng: np.random.RandomState, base_dt: datetime,
                    accounts: pd.DataFrame, merchants: pd.DataFrame,
                    fresh_updated_at: str) -> list[dict]:
    """~233 rows/day × 31 days ≈ 7,230 (plan target ≈ 7,000).

    ``fresh_updated_at`` is a near-``datetime.now()`` timestamp used
    for the final partition. Plan §Financial Services coverage locks
    the SLO PASS budget at ``warn=1H err=4H``; leaving the final
    partition at mid-day would make ``max(updated_at)`` age by a
    full day and flip the test to FAIL on any run started after
    ``01:00`` local.
    """
    rows: list[dict] = []
    txn_id = 1
    n_accounts = len(accounts)
    n_merchants = len(merchants)
    final_day = NUM_DAYS - 1
    for day in range(NUM_DAYS):
        dt = base_dt + timedelta(days=day)
        is_weekend = dt.weekday() >= 5
        base_n = 180 if is_weekend else 250
        n = max(60, int(rng.normal(base_n, base_n * 0.07)))
        for _ in range(n):
            # Heavy-tail amount via log-normal.
            amount = float(rng.lognormal(mean=3.8, sigma=0.7))
            # Day 24 fraud spike — a fraction flip to negative (chargeback) +
            # amplified magnitude.
            if day == 24 and rng.rand() < 0.15:
                amount = -round(amount * 3.0, 2)
            else:
                amount = round(amount, 2)
            # 0.3% null merchant_id (per plan noise spec).
            null_merchant = rng.rand() < 0.003
            merchant_id = None if null_merchant else int(
                rng.randint(1, n_merchants + 1))
            # txn_ts sits mid-day so range-based aggregation filters
            # (``txn_ts >= '{ds}' AND txn_ts < '{next_ds}'``) stay
            # simple. ``txn_date`` denormalizes the date portion so
            # Shape / Pattern can use the structured ``ts_column`` +
            # ``current_value`` predicate without writing a range.
            # ``updated_at`` on the final partition uses the fresh
            # timestamp so the tight SLO PASS budget holds.
            ts = dt.replace(hour=12, minute=0, second=0).strftime(
                "%Y-%m-%d %H:%M:%S")
            upd = fresh_updated_at if day == final_day else ts
            rows.append({
                "txn_id": txn_id,
                "txn_ts": ts,
                "txn_date": dt.strftime("%Y-%m-%d"),
                "account_id": int(rng.randint(1, n_accounts + 1)),
                "merchant_id": merchant_id,
                "amount": amount,
                "currency": _CURRENCIES[txn_id % len(_CURRENCIES)],
                "channel": _CHANNELS[txn_id % len(_CHANNELS)],
                "status": _STATUSES[txn_id % len(_STATUSES)],
                "updated_at": upd,
            })
            txn_id += 1
    return rows


def _daily_price_rows(rng: np.random.RandomState, base_dt: datetime) -> list[dict]:
    """30 symbols × 31 days = 930 rows (plan target ≈ 900)."""
    rows: list[dict] = []
    for day in range(NUM_DAYS):
        dt = base_dt + timedelta(days=day)
        for sym in _SYMBOLS:
            base = _SYMBOL_BASE_PRICE[sym]
            day_drift = 1.0 + 0.001 * day
            open_px = round(float(rng.normal(base * day_drift, base * 0.02)), 2)
            close_px = round(float(rng.normal(open_px, base * 0.02)), 2)
            high_px = round(max(open_px, close_px) *
                            (1 + abs(rng.normal(0.005, 0.003))), 2)
            low_px = round(min(open_px, close_px) *
                           (1 - abs(rng.normal(0.005, 0.003))), 2)
            # Day 18 black-swan on all symbols (close dropped 30%).
            if day == 18:
                close_px = round(close_px * 0.7, 2)
                low_px = round(low_px * 0.7, 2)
            null_vol = rng.rand() < 0.01
            volume = (float("nan") if null_vol
                      else int(max(1_000, rng.lognormal(12.0, 0.5))))
            rows.append({
                "price_date": dt.strftime("%Y-%m-%d"),
                "symbol": sym,
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "volume": volume,
                "updated_at": dt.strftime("%Y-%m-%d %H:%M:%S"),
            })
    return rows


def generate(base_date: datetime | None = None) -> FinServicesData:
    """Generate the Financial Services pack's four tables.

    Called once per pack run from the pack-scoped ``fs_data`` fixture.
    ``base_date`` defaults to ``today - (NUM_DAYS - 1)`` so the final
    partition is today — required for SLO freshness PASS.
    """
    if base_date is None:
        today = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        base_date = today - timedelta(days=NUM_DAYS - 1)

    # Tight SLO PASS: ``warn=1H err=4H`` — the final partition's
    # ``updated_at`` must sit inside the warn window. Leave a 10-minute
    # buffer so even slow pack runs (≤50 min) stay below warn=1H.
    fresh_ts = (datetime.now() - timedelta(minutes=10)).strftime(
        "%Y-%m-%d %H:%M:%S")

    rng = np.random.RandomState(SEED)
    merchants_dim = _generate_merchants()
    accounts_dim = _generate_accounts(rng)

    txn_rows = _daily_txn_rows(
        rng, base_date, accounts_dim, merchants_dim, fresh_ts)
    price_rows = _daily_price_rows(rng, base_date)

    transactions_fact = pd.DataFrame(txn_rows)
    market_prices_fact = pd.DataFrame(price_rows)

    sorted_dates = sorted(transactions_fact["txn_ts"].str[:10].unique())
    current_date = sorted_dates[-1]
    history_dates = tuple(sorted_dates[i] for i in (-22, -15, -8))

    return FinServicesData(
        transactions_fact=transactions_fact,
        market_prices_fact=market_prices_fact,
        accounts_dim=accounts_dim,
        merchants_dim=merchants_dim,
        base_date=sorted_dates[0],
        current_date=current_date,
        history_dates=history_dates,
    )
