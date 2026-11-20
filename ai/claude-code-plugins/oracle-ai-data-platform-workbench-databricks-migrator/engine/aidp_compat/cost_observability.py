"""
Reusable AIDP cost-observability collection + seaborn plotting.

Complements ``aidp_compat.budget`` (which owns the cost model + budget enforcement)
by providing a *rich* per-run collector and the finalized plot set distilled from
the cost-observability notebooks. Plots are seaborn/matplotlib PNGs — no plotly, no
CDN (plotly's CDN gets rate-limited on the cluster). See
``reports/aidp_cost_observability_PLAN.md`` for the full plan.

Cost model, unit rates, and price all come from ``aidp_compat.budget`` — this module
never re-implements the AIDP-units math.

Typical use (in a notebook, on-cluster):

    from aidp_compat import budget as B
    from aidp_compat import cost_observability as CO

    signer = B.make_signer()
    base   = B.base_url()
    model  = B.CostModel(signer, base)
    df     = CO.collect_runs(signer, base, model, since_days=90)   # rich DataFrame

    CO.set_plot_dir("/Workspace/migration-output/plots")
    CO.plot_top_n_trend(df, "job_name", n=5, metric="units", freq="W")   # weekly, top-5 workflows
    CO.plot_top_n_trend(df, "compute",  n=5, metric="cost",  freq="D",   # day-wise, top-5 clusters
                        window_days=35)
    CO.plot_total_trend(df, metric="units", freq="D", window_days=35)    # all clusters/workflows total
"""

import os
import base64
import datetime

# Plotting deps are imported lazily inside functions so importing this module
# never fails on a node without matplotlib/seaborn (collection still works).

SUCCESS_STATES = {"SUCCEEDED", "SUCCESS", "COMPLETED", "OK"}
_PLOT_DIR = "."
_SINCE_TAG = ""   # appended to saved filenames, e.g. "_90d"

def _sns():
    """Return seaborn if available, else None (callers fall back to matplotlib)."""
    try:
        import seaborn as sns
        sns.set_theme(style="whitegrid")
        return sns
    except Exception:
        return None

def _lineplot(ax, data, x, y, hue=None, hue_order=None):
    sns = _sns()
    if sns is not None:
        sns.lineplot(data=data, x=x, y=y, hue=hue, hue_order=hue_order, marker="o", ax=ax)
        return
    if hue:
        for h in (hue_order or sorted(data[hue].unique())):
            d = data[data[hue] == h]
            ax.plot(range(len(d)), d[y].values, marker="o", label=str(h)[:32])
        ax.legend(fontsize=7)
    else:
        ax.plot(range(len(data)), data[y].values, marker="o")

def _barh(ax, data, x, y, hue=None, hue_order=None):
    sns = _sns()
    if sns is not None:
        sns.barplot(data=data, x=x, y=y, hue=hue, hue_order=hue_order, orient="h", ax=ax)
        return
    ax.barh(range(len(data)), data[x].values)
    ax.set_yticks(range(len(data))); ax.set_yticklabels(data[y].astype(str))


# ─── Collection ─────────────────────────────────────────────────────
def _is_success(status):
    return str(status or "").upper() in SUCCESS_STATES


def collect_runs(signer, base, model, since_days=None, max_runs=100000):
    """Rich per-run DataFrame for cost observability.

    Pages ``jobRuns`` newest-first; if ``since_days`` is set, stops once runs fall
    before ``now - since_days`` (UTC). Each row carries owner, workflow, cluster,
    day, week (Mon-start), status/success, hours, units_hr, units, cost.

    Units + cost use ``model`` (aidp_compat.budget.CostModel) — the single source
    of truth for the AIDP-units math.
    """
    import pandas as pd
    from aidp_compat.budget import paginate  # reuse the throttled pager

    jobs = {}
    for j in paginate(signer, base, "jobs"):
        jobs[j["key"]] = {"name": j.get("name"),
                          "clusters": j.get("clusters", []) or [],
                          "owner": j.get("createdByName")}

    _name_cache = {}

    def cluster_name(ck):
        if ck not in _name_cache:
            try:
                from aidp_compat.budget import _get
                _name_cache[ck] = _get(signer, f"{base}/clusters/{ck}").json().get("displayName") or (ck or "")[:8]
            except Exception:
                _name_cache[ck] = (ck or "")[:8]
        return _name_cache[ck]

    def job_units_compute(jk):
        for ck in jobs.get(jk, {}).get("clusters", []):
            u = model.cluster_units_hr(ck)
            if u:
                return u, cluster_name(ck)
        return None, None

    now_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
    floor_ms = (now_ms - since_days * 86400_000) if since_days else None

    rows, seen = [], 0
    for run in paginate(signer, base, "jobRuns"):
        end = run.get("endTime") or run.get("startTime") or 0
        start_ms = run.get("startTime") or end
        if not start_ms or start_ms > now_ms:
            continue
        if floor_ms and start_ms < floor_ms:
            break
        seen += 1
        if max_runs and seen > max_runs:
            break
        jk = run.get("jobKey")
        units_hr, compute = job_units_compute(jk)
        dur = run.get("executionDuration")
        if dur is None and run.get("startTime") and run.get("endTime"):
            dur = run["endTime"] - run["startTime"]
        hours = (dur or 0) / 3600_000.0
        status = (run.get("state") or {}).get("status")
        rows.append({
            "job_id": jk,
            "job_name": run.get("jobName") or (jobs.get(jk) or {}).get("name") or "(unknown)",
            "compute": compute or "(unknown compute)",
            "owner": run.get("createdByName") or "(unknown)",
            "user": run.get("createdByName") or "(unknown)",
            "status": status,
            "success": _is_success(status),
            "failed": not _is_success(status),
            "start_ms": start_ms,
            "hours": hours,
            "units_hr": units_hr,
            "units": (units_hr or 0) * hours,
            "cost": (units_hr or 0) * hours * model.price,
            "cost_known": units_hr is not None,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["day"] = pd.to_datetime(df["start_ms"], unit="ms")
        df["date"] = df["day"].dt.normalize()
        df["week"] = df["day"].dt.to_period("W-SUN").dt.start_time.dt.strftime("%Y-%m-%d")
        df["month"] = df["day"].dt.strftime("%Y-%m")
    return df


# ─── Plot helpers ───────────────────────────────────────────────────
def set_plot_dir(path, since_tag=""):
    """Set the output directory (and optional filename suffix, e.g. '_90d')."""
    global _PLOT_DIR, _SINCE_TAG
    _PLOT_DIR = path
    _SINCE_TAG = since_tag
    os.makedirs(path, exist_ok=True)


def _save(fig, name, show=True):
    import matplotlib.pyplot as plt
    path = os.path.join(_PLOT_DIR, f"{name}{_SINCE_TAG}.png")
    fig.savefig(path, dpi=130, bbox_inches="tight")
    print("saved:", path)
    if show:
        try:
            from IPython.display import HTML, display as ipy_display
            b = base64.b64encode(open(path, "rb").read()).decode()
            ipy_display(HTML(f'<img src="data:image/png;base64,{b}"/>'))
        except Exception:
            pass
    plt.close(fig)
    return path


def _window(df, window_days):
    """Slice df to the last ``window_days`` ending at the latest run (else all)."""
    import pandas as pd
    if not window_days or df.empty:
        return df
    end = df["date"].max()
    start = end - pd.Timedelta(days=window_days - 1)
    return df[df["date"] >= start]


def _axis(freq):
    return "week" if freq.upper().startswith("W") else "date"


# ─── Core plots ─────────────────────────────────────────────────────
def top_n(df, key, metric="units", n=5):
    """Distinct top-N ``key`` values by summed ``metric``."""
    return (df.groupby(key)[metric].sum()
            .sort_values(ascending=False).head(n).index.tolist())


def plot_top_n_trend(df, key, n=5, metric="units", freq="W", window_days=None,
                     title=None, name=None, show=True):
    """Line trend (seaborn) of the distinct top-N ``key`` by ``metric``.

    key:    'job_name' or 'compute'
    metric: 'units' | 'cost' | 'hours'
    freq:   'W' (weekly, Mon-start) or 'D' (day-wise)
    """
    import pandas as pd, matplotlib.pyplot as plt
    sns = _sns()
    d = _window(df, window_days)
    ax_col = _axis(freq)
    order = sorted(d[ax_col].unique())
    tops = top_n(d, key, metric, n)
    piv = (d[d[key].isin(tops)].groupby([ax_col, key])[metric].sum()
           .unstack(key).reindex(index=order, columns=tops).fillna(0)
           .reset_index().melt(id_vars=ax_col, var_name=key, value_name=metric))
    piv[ax_col] = pd.Categorical(piv[ax_col], categories=order, ordered=True)
    fig, ax = plt.subplots(figsize=(max(10, 1.1 * len(order)), 6))
    _lineplot(ax, piv, ax_col, metric, key, tops)
    ax.set_xlabel(""); ax.set_ylabel({"units": "AIDP units", "cost": "Cost (USD)",
                                      "hours": "Runtime hours"}.get(metric, metric))
    lbl = "workflows" if key == "job_name" else "clusters"
    ax.set_title(title or f"{freq}-wise {metric} — top {n} {lbl}")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(fontsize=8, title=key, bbox_to_anchor=(1.01, 1), loc="upper left")
    fig.tight_layout()
    return _save(fig, name or f"obs_{freq.lower()}_top{n}_{key}_{metric}", show)


def plot_total_trend(df, metric="units", freq="D", window_days=None,
                     title=None, name=None, show=True):
    """Single-line total across ALL workflows/clusters, day- or week-wise."""
    import pandas as pd, matplotlib.pyplot as plt
    sns = _sns()
    d = _window(df, window_days)
    ax_col = _axis(freq)
    order = sorted(d[ax_col].unique())
    t = d.groupby(ax_col)[metric].sum().reindex(order).fillna(0).reset_index()
    t[ax_col] = pd.Categorical(t[ax_col], categories=order, ordered=True)
    fig, ax = plt.subplots(figsize=(max(10, 1.1 * len(order)), 5))
    _lineplot(ax, t, ax_col, metric)
    ax.set_xlabel(""); ax.set_ylabel({"units": "AIDP units", "cost": "Cost (USD)",
                                      "hours": "Runtime hours"}.get(metric, metric))
    ax.set_title(title or f"{freq}-wise {metric} — all (total)")
    ax.tick_params(axis="x", rotation=45); fig.tight_layout()
    return _save(fig, name or f"obs_{freq.lower()}_total_{metric}", show)


def plot_cost_by_period(df, key, n=5, freq="W", window_days=35,
                        title=None, name=None, show=True):
    """Grouped horizontal bar: $ cost of top-N ``key`` per period (last window)."""
    import pandas as pd, matplotlib.pyplot as plt
    sns = _sns()
    d = _window(df, window_days)
    ax_col = _axis(freq)
    order = sorted(d[ax_col].unique())
    tops = top_n(d, key, "cost", n)
    g = (d[d[key].isin(tops)].groupby([ax_col, key])["cost"].sum().reset_index())
    g[ax_col] = pd.Categorical(g[ax_col], categories=order, ordered=True)
    fig, ax = plt.subplots(figsize=(11, max(5, 1.4 * len(order))))
    _barh(ax, g, "cost", ax_col, key, tops)
    ax.set_ylabel(""); ax.set_xlabel("Cost (USD)")
    lbl = "workflows" if key == "job_name" else "clusters"
    ax.set_title(title or f"Cost by top-{n} {lbl} — per {ax_col}")
    ax.legend(fontsize=8, title=key, bbox_to_anchor=(1.01, 1), loc="upper left")
    for c in ax.containers:
        ax.bar_label(c, fmt="$%.0f", fontsize=6, padding=2)
    fig.tight_layout()
    return _save(fig, name or f"obs_cost_by_{ax_col}_top{n}_{key}", show)


def summarize(df, key, metric="units"):
    """Distinct rollup table by ``key`` with runs/hours/units/cost/success-rate."""
    g = (df.groupby(key)
         .agg(runs=("cost", "size"), ok=("success", "sum"), failed=("failed", "sum"),
              hours=("hours", "sum"), units=("units", "sum"), cost=("cost", "sum"))
         .reset_index())
    g["success_rate_%"] = (100 * g["ok"] / g["runs"]).round(1)
    g["units"] = g["units"].round(0)
    g["hours"] = g["hours"].round(2)
    g["cost"] = g["cost"].round(2)
    return g.sort_values(metric, ascending=False).reset_index(drop=True)


# ─── Dated report output ────────────────────────────────────────────
def report_dir(root=None, run_date=None):
    """Create/return a dated report folder: <root>/YYYY-MM-DD/.

    Called on every run so each execution's plots + tables land in their own
    date folder (idempotent — reuses the folder if the same day is re-run).
    Defaults root to the current PLOT_DIR's parent, or './reports'.
    """
    if run_date is None:
        run_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    if root is None:
        root = os.path.dirname(_PLOT_DIR.rstrip("/")) or "reports"
        root = os.path.join(root, "cost_reports")
    d = os.path.join(root, run_date)
    os.makedirs(d, exist_ok=True)
    return d


def save_report(df, root=None, run_date=None, since_tag=None):
    """Write a full dated cost report (JSON summaries + CSVs) for this run.

    Folder: <root>/YYYY-MM-DD/. Also repoints the plot dir there so all
    subsequent plot_* calls save into the same dated folder.
    """
    d = report_dir(root, run_date)
    set_plot_dir(d, since_tag if since_tag is not None else _SINCE_TAG)
    if df.empty:
        print("no runs to report"); return d
    for key, fname in [("job_name", "by_workflow"), ("compute", "by_cluster"),
                       ("user", "by_user")]:
        summarize(df, key).to_csv(os.path.join(d, f"{fname}.csv"), index=False)
    # per-run detail + headline totals
    df.to_csv(os.path.join(d, "runs_detail.csv"), index=False)
    import json as _json
    totals = {
        "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "runs": int(len(df)),
        "workflows": int(df["job_name"].nunique()),
        "clusters": int(df["compute"].nunique()),
        "users": int(df["user"].nunique()),
        "total_units": round(float(df["units"].sum()), 0),
        "total_cost_usd": round(float(df["cost"].sum()), 2),
        "ok_runs": int(df["success"].sum()),
        "failed_runs": int(df["failed"].sum()),
        "date_range": [str(df["date"].min().date()), str(df["date"].max().date())],
    }
    with open(os.path.join(d, "summary.json"), "w") as f:
        _json.dump(totals, f, indent=2)
    print("report saved to:", d)
    return d


# ─── Drill-down helpers (on-demand deep dives) ──────────────────────
def _dive(df, mask, label, split_by, show):
    """Shared body: tables + trend for a filtered slice."""
    import matplotlib.pyplot as plt
    sns = _sns()
    sub = df[mask].copy()
    if sub.empty:
        print(f"no runs for {label}")
        return sub
    tot = (f"{label}: {len(sub)} runs | {sub['units'].sum():,.0f} units | "
           f"${sub['cost'].sum():,.2f} | {sub['success'].sum()} ok / {sub['failed'].sum()} failed")
    print(tot)
    breakdown = summarize(sub, split_by)
    try:
        from IPython.display import HTML, display as ipy_display
        ipy_display(HTML(breakdown.to_html(index=False)))
    except Exception:
        print(breakdown.to_string(index=False))
    # weekly units trend for this slice, colored by split_by (top 8)
    tops = top_n(sub, split_by, "units", 8)
    order = sorted(sub["week"].unique())
    import pandas as pd
    piv = (sub[sub[split_by].isin(tops)].groupby(["week", split_by])["units"].sum()
           .unstack(split_by).reindex(index=order, columns=tops).fillna(0)
           .reset_index().melt(id_vars="week", var_name=split_by, value_name="units"))
    piv["week"] = pd.Categorical(piv["week"], categories=order, ordered=True)
    fig, ax = plt.subplots(figsize=(max(9, 1.1 * len(order)), 5))
    _lineplot(ax, piv, "week", "units", split_by, tops)
    ax.set_xlabel(""); ax.set_ylabel("AIDP units")
    ax.set_title(f"{label} — weekly units by {split_by}")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(fontsize=7, bbox_to_anchor=(1.01, 1), loc="upper left")
    fig.tight_layout()
    safe = "".join(ch if ch.isalnum() else "_" for ch in label)[:40]
    _save(fig, f"drill_{safe}", show)
    return breakdown


def inspect_user(df, user, show=True):
    """Deep dive on one user: their workflows/clusters, cost, weekly trend."""
    return _dive(df, df["user"] == user, f"user={user}", "job_name", show)


def inspect_cluster(df, compute, show=True):
    """Deep dive on one cluster: workflows + users on it, cost, weekly trend."""
    return _dive(df, df["compute"] == compute, f"cluster={compute}", "user", show)


def inspect_workflow(df, job_name, show=True):
    """Deep dive on one workflow: clusters/users, success rate, weekly trend."""
    return _dive(df, df["job_name"] == job_name, f"workflow={job_name}", "compute", show)


# ════════════════════════════════════════════════════════════════════
#  Executive layer: summary, composition, trends, waste, efficiency,
#  forecast. All seaborn/matplotlib (falls back to matplotlib if no
#  seaborn). Every function is safe to call from any cell.
# ════════════════════════════════════════════════════════════════════

def _family_of(shape_or_name):
    """Best-effort hardware family from a shape/cluster name."""
    s = str(shape_or_name or "").lower()
    if "gpu" in s:
        return "GPU"
    if s.startswith("arm") or "_arm" in s or ".arm" in s or s.startswith("a1") or s.startswith("a2"):
        return "ARM"
    if "intel" in s or ".e" in s or "standard.e" in s:
        return "Intel"
    if "amd" in s:
        return "AMD"
    return "AMD"   # AIDP default family


def executive_summary(df, top=5, show=True):
    """Headline tiles: total spend, runs, workflow/user counts, top spenders,
    top workflows, and $ burned on FAILED runs. Returns a dict."""
    if df.empty:
        print("no data"); return {}
    failed_cost = float(df.loc[df["failed"], "cost"].sum())
    total = float(df["cost"].sum())
    summ = {
        "total_cost_usd": round(total, 2),
        "total_units": round(float(df["units"].sum()), 0),
        "runs": int(len(df)),
        "workflows": int(df["job_name"].nunique()),
        "users": int(df["user"].nunique()),
        "clusters": int(df["compute"].nunique()),
        "failed_runs": int(df["failed"].sum()),
        "failed_cost_usd": round(failed_cost, 2),
        "failed_cost_pct": round(100 * failed_cost / total, 1) if total else 0,
        "top_users": df.groupby("user")["cost"].sum().sort_values(ascending=False).head(top).round(2).to_dict(),
        "top_workflows": df.groupby("job_name")["cost"].sum().sort_values(ascending=False).head(top).round(2).to_dict(),
        "date_range": [str(df["date"].min().date()), str(df["date"].max().date())],
    }
    if show:
        lines = [
            "═" * 60, "  AIDP COST — EXECUTIVE SUMMARY", "═" * 60,
            f"  Window        : {summ['date_range'][0]} → {summ['date_range'][1]}",
            f"  Total spend   : ${summ['total_cost_usd']:,.2f}  ({summ['total_units']:,.0f} units)",
            f"  Runs          : {summ['runs']:,}   Workflows: {summ['workflows']}   "
            f"Users: {summ['users']}   Clusters: {summ['clusters']}",
            f"  FAILED runs   : {summ['failed_runs']:,}  costing "
            f"${summ['failed_cost_usd']:,.2f}  ({summ['failed_cost_pct']}% of spend)  ⚠",
            "  Top spenders  : " + ", ".join(f"{u.split('@')[0]} ${v:,.0f}"
                                             for u, v in summ["top_users"].items()),
            "  Top workflows : " + ", ".join(f"{w[:24]} ${v:,.0f}"
                                             for w, v in summ["top_workflows"].items()),
            "═" * 60,
        ]
        print("\n".join(lines))
    return summ


def plot_composition(df, show=True):
    """Two donut pies: cost by hardware family (AMD/Intel/ARM/GPU) and by user."""
    import matplotlib.pyplot as plt
    if df.empty:
        print("no data"); return
    fam = df.copy(); fam["family"] = fam["compute"].map(_family_of)
    by_fam = fam.groupby("family")["cost"].sum().sort_values(ascending=False)
    by_user = df.groupby("user")["cost"].sum().sort_values(ascending=False)
    top_u = by_user.head(8)
    if len(by_user) > 8:
        top_u = top_u.copy(); top_u["other"] = by_user.iloc[8:].sum()

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 6))
    for ax, ser, title in [(a1, by_fam, "Cost by hardware family"),
                           (a2, top_u, "Cost by user (top 8)")]:
        w, *_ = ax.pie(ser.values, labels=[str(i)[:18] for i in ser.index],
                       autopct=lambda p: f"${p/100*ser.sum():,.0f}", pctdistance=0.78,
                       wedgeprops=dict(width=0.42), textprops=dict(fontsize=8))
        ax.set_title(title)
    fig.suptitle("Cost composition", fontsize=13); fig.tight_layout()
    return _save(fig, "obs_composition_donuts", show)


def plot_daily_bar(df, metric="cost", window_days=35, show=True):
    """Daily bar of total spend/units — quick 'which days were expensive' view."""
    import pandas as pd, matplotlib.pyplot as plt
    d = _window(df, window_days)
    order = sorted(d["date"].unique())
    t = d.groupby("date")[metric].sum().reindex(order).fillna(0)
    fig, ax = plt.subplots(figsize=(max(10, 0.4 * len(order)), 5))
    ax.bar(range(len(order)), t.values, color="#4c72b0")
    ax.set_xticks(range(len(order)))
    ax.set_xticklabels([pd.Timestamp(x).strftime("%m-%d") for x in order], rotation=60, fontsize=7)
    ax.set_ylabel({"cost": "Cost (USD)", "units": "AIDP units"}.get(metric, metric))
    ax.set_title(f"Daily {metric} (last {window_days}d)")
    fig.tight_layout()
    return _save(fig, f"obs_daily_bar_{metric}", show)


def plot_dow_hour_heatmap(df, metric="cost", show=True):
    """Day-of-week × hour heatmap — reveals scheduling clumps / off-hours idle."""
    import numpy as np, matplotlib.pyplot as plt
    if df.empty:
        print("no data"); return
    d = df.copy()
    d["dow"] = d["day"].dt.dayofweek
    d["hour"] = d["day"].dt.hour
    grid = (d.groupby(["dow", "hour"])[metric].sum()
            .unstack("hour").reindex(index=range(7), columns=range(24)).fillna(0))
    fig, ax = plt.subplots(figsize=(14, 4.5))
    sns = _sns()
    if sns is not None:
        sns.heatmap(grid, cmap="YlOrRd", ax=ax,
                    cbar_kws={"label": {"cost": "Cost (USD)"}.get(metric, metric)})
    else:
        im = ax.imshow(grid.values, aspect="auto", cmap="YlOrRd"); fig.colorbar(im, ax=ax)
        ax.set_xticks(range(24))
    ax.set_yticks(range(7)); ax.set_yticklabels(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], rotation=0)
    ax.set_xlabel("Hour (UTC)"); ax.set_ylabel("")
    ax.set_title(f"{metric} by day-of-week × hour — scheduling map")
    fig.tight_layout()
    return _save(fig, f"obs_dow_hour_heatmap_{metric}", show)


def idle_active_clusters(signer, base, df, show=True):
    """WASTE DETECTOR: clusters currently ACTIVE but with zero runs in the window.
    These are usually forgotten dev/experiment clusters silently burning money."""
    import pandas as pd
    from aidp_compat.budget import paginate
    ran = set(df["compute"].unique()) if not df.empty else set()
    rows = []
    for c in paginate(signer, base, "clusters"):
        state = (c.get("lifecycleState") or c.get("state") or "").upper()
        name = c.get("displayName") or c.get("key", "")[:8]
        if state in {"ACTIVE", "RUNNING", "INSERVICE", "UP"} and name not in ran:
            rows.append({"cluster": name, "key": c.get("key"), "state": state})
    idle = pd.DataFrame(rows)
    if show:
        if idle.empty:
            print("✓ No idle-but-active clusters found.")
        else:
            print(f"⚠ {len(idle)} ACTIVE clusters with NO runs in window (likely wasted spend):")
            try:
                from IPython.display import HTML, display as ipy_display
                ipy_display(HTML(idle.to_html(index=False)))
            except Exception:
                print(idle.to_string(index=False))
    return idle


def efficiency_table(df, top=15, show=True):
    """Top workflows by $/run, with fail-rate — surfaces expensive AND unreliable jobs."""
    if df.empty:
        print("no data"); return df
    g = (df.groupby("job_name")
         .agg(runs=("cost", "size"), cost=("cost", "sum"),
              failed=("failed", "sum"), units=("units", "sum"))
         .reset_index())
    g["cost_per_run"] = (g["cost"] / g["runs"]).round(2)
    g["fail_rate_%"] = (100 * g["failed"] / g["runs"]).round(1)
    g["cost"] = g["cost"].round(2); g["units"] = g["units"].round(0)
    g = g.sort_values("cost_per_run", ascending=False).head(top).reset_index(drop=True)
    if show:
        try:
            from IPython.display import HTML, display as ipy_display

            def _color(v):
                return ("background-color:#f8d7da" if v >= 25 else
                        "background-color:#fff3cd" if v >= 5 else
                        "background-color:#d4edda")
            sty = (g.style.applymap(_color, subset=["fail_rate_%"])
                   .format({"cost": "${:,.2f}", "cost_per_run": "${:,.2f}"}))
            ipy_display(sty)
        except Exception:
            print(g.to_string(index=False))
    return g


def forecast(df, history_dir=None, show=True):
    """Forecast tiles: projected monthly + annual burn from the window's daily rate,
    plus a week-over-week trend that self-populates as dated reports accumulate."""
    import pandas as pd, matplotlib.pyplot as plt, glob, json as _json
    if df.empty:
        print("no data"); return {}
    ndays = max((df["date"].max() - df["date"].min()).days + 1, 1)
    daily = float(df["cost"].sum()) / ndays
    tiles = {"daily_avg_usd": round(daily, 2),
             "projected_month_usd": round(daily * 30, 2),
             "projected_year_usd": round(daily * 365, 2),
             "based_on_days": ndays}
    if show:
        print("┌── FORECAST " + "─" * 40)
        print(f"│  Daily avg     : ${tiles['daily_avg_usd']:,.2f}   (over {ndays} days)")
        print(f"│  → Monthly     : ${tiles['projected_month_usd']:,.2f}")
        print(f"│  → Annual      : ${tiles['projected_year_usd']:,.2f}")
        print("└" + "─" * 52)

    # Self-populating week-over-week from accumulated dated summary.json files.
    if history_dir and os.path.isdir(history_dir):
        pts = []
        for sf in sorted(glob.glob(os.path.join(history_dir, "*", "summary.json"))):
            try:
                with open(sf) as _f:
                    j = _json.load(_f)
                pts.append({"run_date": os.path.basename(os.path.dirname(sf)),
                            "total_cost_usd": j.get("total_cost_usd", 0)})
            except Exception:
                pass
        if len(pts) >= 2:
            h = pd.DataFrame(pts)
            fig, ax = plt.subplots(figsize=(max(8, 0.6 * len(h)), 4.5))
            ax.plot(range(len(h)), h["total_cost_usd"], marker="o", color="#1f77b4")
            ax.set_xticks(range(len(h))); ax.set_xticklabels(h["run_date"], rotation=45, fontsize=7)
            ax.set_ylabel("Total cost (USD)"); ax.set_title("Report-over-report total cost trend")
            fig.tight_layout(); _save(fig, "obs_forecast_history", show)
    return tiles


def run_full_report(signer, base, df, root=None, run_date=None, since_tag="",
                    include_waste=True):
    """One-call executive report: saves everything into a dated folder and renders
    the full exec plot set. Returns the report directory."""
    d = save_report(df, root=root, run_date=run_date, since_tag=since_tag)
    executive_summary(df)
    plot_composition(df)
    plot_daily_bar(df, "cost")
    plot_dow_hour_heatmap(df, "cost")
    plot_top_n_trend(df, "job_name", 5, "cost", "W")
    plot_top_n_trend(df, "user", 5, "cost", "W")
    efficiency_table(df)
    forecast(df, history_dir=os.path.dirname(d))
    if include_waste:
        idle_active_clusters(signer, base, df)
    print("full report at:", d)
    return d
