"""Seeded Life Sciences pack data generator (SEED=2027).

Domain: clinical-trial lab results + adverse events, indexed by
patient and site dimensions, per plan §Life Sciences coverage.

Tables (plan spec, locked):
- ``lab_results_fact``      — ~5,000 rows, 3% null ``value``, outlier
  injection on day 18, realistic ``unit``.
- ``adverse_events_fact``   — ~800 rows, severity skewed to ``mild``,
  day-24 event-count spike drives Trend FAIL.
- ``patients_dim``          — 300 rows, 2% null ``age``, mixed categorical
  (``arm``, ``sex``).
- ``sites_dim``             — 20 rows, bool ``active``.

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

SEED = 2027
# 31 partitions = 30 history + 1 current. Trend scenarios need
# exactly 30 seeded history points to match plan §Config shape
# conventions: `history_count: 30`. Plan §Life Sciences coverage
# asks for "≥30 daily partitions"; 31 is the minimum that leaves one
# partition free as the current test input while the remaining 30
# land in `_seed_history`.
NUM_DAYS = 31

_ASSAYS = ("glucose", "hemoglobin", "creatinine", "alt", "cholesterol")
_UNITS = ("mg/dL", "g/dL", "mg/dL", "U/L", "mg/dL")
_FLAGS = ("NORMAL", "HIGH", "LOW")  # plan §Threshold fail: flag='HIGH'
_ARMS = ("Placebo", "Low-dose", "High-dose")
_SEXES = ("F", "M")
_EVENT_TERMS = ("headache", "nausea", "fatigue", "rash", "dizziness",
                "insomnia", "injection_site_reaction")
_EVENT_SEVERITY = ("mild", "mild", "mild", "moderate", "severe")
_PI_NAMES = (
    "Dr. Nguyen", "Dr. Patel", "Dr. Smith", "Dr. Rossi",
    "Dr. Kowalski", "Dr. Garcia", "Dr. Chen", "Dr. Müller",
    "Dr. Okafor", "Dr. Tanaka", "Dr. Jensen", "Dr. Fernandez",
    "Dr. Kim", "Dr. Brown", "Dr. Andersson", "Dr. Dubois",
    "Dr. Singh", "Dr. Svensson", "Dr. Yılmaz", "Dr. de Souza",
)
_SITE_COUNTRIES = (
    "US", "US", "US", "UK", "UK", "DE", "DE", "FR", "FR", "IT",
    "ES", "NL", "SE", "PL", "JP", "CA", "AU", "BR", "IN", "SG",
)


@dataclass(frozen=True)
class LifeSciencesData:
    """In-memory snapshot of the Life Sciences pack's four tables."""

    lab_results_fact: pd.DataFrame
    adverse_events_fact: pd.DataFrame
    patients_dim: pd.DataFrame
    sites_dim: pd.DataFrame
    base_date: str
    current_date: str
    history_dates: tuple[str, ...]


def _generate_sites() -> pd.DataFrame:
    n = 20
    return pd.DataFrame({
        "site_id": list(range(1, n + 1)),
        "country": [_SITE_COUNTRIES[i] for i in range(n)],
        "principal_investigator": [_PI_NAMES[i] for i in range(n)],
        "active": [bool((i % 7) != 0) for i in range(n)],
    })


def _generate_patients(rng: np.random.RandomState, n_sites: int) -> pd.DataFrame:
    n = 300
    enroll_anchor = datetime(2023, 1, 1)
    ages = [int(rng.randint(18, 85)) for _ in range(n)]
    null_age_mask = rng.rand(n) < 0.02
    return pd.DataFrame({
        "patient_id": list(range(1, n + 1)),
        "enrolled_at": [
            (enroll_anchor + timedelta(days=int(rng.randint(0, 400))))
            .strftime("%Y-%m-%d")
            for _ in range(n)
        ],
        "site_id": [int(rng.randint(1, n_sites + 1)) for _ in range(n)],
        "age": [None if null_age_mask[i] else ages[i] for i in range(n)],
        "sex": [_SEXES[i % len(_SEXES)] for i in range(n)],
        "arm": [_ARMS[i % len(_ARMS)] for i in range(n)],
    })


def _daily_lab_rows(rng: np.random.RandomState, base_dt: datetime,
                    patients: pd.DataFrame, sites: pd.DataFrame,
                    fresh_updated_at: str) -> list[dict]:
    """~170 rows/day × 31 days ≈ 5,270 (plan target ≈ 5,000).

    ``fresh_updated_at`` is a near-``datetime.now()`` timestamp used
    for the final partition so the plan-locked SLO PASS budget
    (``warn=6H err=12H`` per plan §LS #1) holds regardless of when the test
    run starts.
    """
    rows: list[dict] = []
    result_id = 1
    n_patients = len(patients)
    n_sites = len(sites)
    final_day = NUM_DAYS - 1
    for day in range(NUM_DAYS):
        dt = base_dt + timedelta(days=day)
        is_weekend = dt.weekday() >= 5
        base_n = 140 if is_weekend else 175
        n = max(40, int(rng.normal(base_n, base_n * 0.06)))
        for _ in range(n):
            idx = int(rng.randint(0, len(_ASSAYS)))
            assay = _ASSAYS[idx]
            unit = _UNITS[idx]
            # Day 18 outlier spike (plan noise spec).
            outlier = (day == 18) and (rng.rand() < 0.10)
            mean = {
                "glucose": 95.0, "hemoglobin": 14.0, "creatinine": 1.0,
                "alt": 30.0, "cholesterol": 190.0,
            }[assay]
            raw = float(rng.normal(mean, mean * 0.12))
            value = round(raw * (2.5 if outlier else 1.0), 2)
            null_val = rng.rand() < 0.03
            # flag distribution: ~85% NORMAL, ~10% HIGH, ~5% LOW.
            p = rng.rand()
            flag = "HIGH" if p < 0.10 else ("LOW" if p < 0.15 else "NORMAL")
            upd = (fresh_updated_at if day == final_day
                   else dt.strftime("%Y-%m-%d %H:%M:%S"))
            rows.append({
                "result_id": result_id,
                "result_date": dt.strftime("%Y-%m-%d"),
                "patient_id": int(rng.randint(1, n_patients + 1)),
                "site_id": int(rng.randint(1, n_sites + 1)),
                "assay": assay,
                "value": float("nan") if null_val else value,
                "unit": unit,
                "flag": flag,
                "updated_at": upd,
            })
            result_id += 1
    return rows


def _daily_event_rows(rng: np.random.RandomState, base_dt: datetime,
                      patients: pd.DataFrame, sites: pd.DataFrame) -> list[dict]:
    """~27 rows/day × 30 days ≈ 810 (plan target ≈ 800).

    Day 24 has a seeded count spike to give the fixture realistic
    noise; the Trend FAIL path does NOT rely on it — that test
    amplifies the current-day slice 10× via ``_trend_fixture_slice``
    + ``pd.concat`` (see ``test_trend_fail``). Leaving the spike in
    keeps the history non-degenerate without entangling the fail path.
    """
    rows: list[dict] = []
    event_id = 1
    n_patients = len(patients)
    n_sites = len(sites)
    for day in range(NUM_DAYS):
        dt = base_dt + timedelta(days=day)
        base_n = 27
        if day == 24:
            base_n = 95
        n = max(5, int(rng.normal(base_n, base_n * 0.15)))
        for _ in range(n):
            rows.append({
                "event_id": event_id,
                "event_date": dt.strftime("%Y-%m-%d"),
                "patient_id": int(rng.randint(1, n_patients + 1)),
                "site_id": int(rng.randint(1, n_sites + 1)),
                "severity": _EVENT_SEVERITY[event_id % len(_EVENT_SEVERITY)],
                "term": _EVENT_TERMS[event_id % len(_EVENT_TERMS)],
                "updated_at": dt.strftime("%Y-%m-%d %H:%M:%S"),
            })
            event_id += 1
    return rows


def generate(base_date: datetime | None = None) -> LifeSciencesData:
    """Generate the Life Sciences pack's four tables.

    Called once per pack run from the pack-scoped ``ls_data`` fixture.
    ``base_date`` defaults to ``today - (NUM_DAYS - 1)`` so the final
    partition is today — required for SLO freshness PASS.
    """
    if base_date is None:
        today = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        base_date = today - timedelta(days=NUM_DAYS - 1)

    # Tight SLO PASS: ``warn=6H err=12H`` per plan §LS #1 — leave a
    # 10-minute buffer so pack runtime variance (≤50 min per CI
    # gate) stays far below warn=6H.
    fresh_ts = (datetime.now() - timedelta(minutes=10)).strftime(
        "%Y-%m-%d %H:%M:%S")

    rng = np.random.RandomState(SEED)
    sites_dim = _generate_sites()
    patients_dim = _generate_patients(rng, len(sites_dim))

    lab_rows = _daily_lab_rows(
        rng, base_date, patients_dim, sites_dim, fresh_ts)
    event_rows = _daily_event_rows(rng, base_date, patients_dim, sites_dim)

    lab_results_fact = pd.DataFrame(lab_rows)
    adverse_events_fact = pd.DataFrame(event_rows)

    sorted_dates = sorted(lab_results_fact["result_date"].unique())
    current_date = sorted_dates[-1]
    history_dates = tuple(sorted_dates[i] for i in (-22, -15, -8))

    return LifeSciencesData(
        lab_results_fact=lab_results_fact,
        adverse_events_fact=adverse_events_fact,
        patients_dim=patients_dim,
        sites_dim=sites_dim,
        base_date=sorted_dates[0],
        current_date=current_date,
        history_dates=history_dates,
    )
