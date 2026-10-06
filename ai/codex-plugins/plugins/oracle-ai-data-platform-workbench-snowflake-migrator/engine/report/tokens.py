"""LLM token usage per migration stage and phase.

THE ENGINE MAKES NO LLM CALL. Every token in a migration is spent by the agent
driving it -- reading reports, deciding the next stage, writing the command --
so the numbers cannot come from this process. They come from the agent's own
transcript, and this module attributes each one to the stage it was spent on.

Two halves:

  * THE RUN LOG. `snowmig.main` appends one line per stage to `run_log.jsonl`
    in the artifact directory: stage, phase, start, end, exit code, and the
    Claude Code session id when the stage ran under one. No argument values
    are recorded -- they carry paths and coordinates, and a log is the last
    place a secret should be able to land.

  * ATTRIBUTION. Assistant messages are read from the session transcript and
    its subagent transcripts, de-duplicated by message id (the transcript
    repeats a message on several lines). A stage owns every token spent after
    the previous stage ended, up to its own end: the reasoning that prepared
    it, the call, and the stage itself. Tokens before the first stage or after
    the last are EXCLUDED and counted as such -- the same session may have
    spent them on something that is not this migration, and folding them in
    would overstate its cost.

A stage run by hand, outside any agent session, or under a session whose
transcript was not found, is reported as not measured, never as zero -- per
run: one transcript found does not make every other run a measured zero. A
report with any such run is flagged `partial`.
"""
from __future__ import annotations

import collections
import datetime
import json
import pathlib

__all__ = ["PHASES", "STAGE_LOG", "attribute", "build_token_report",
           "save_exclusions",
           "find_transcripts", "load_usage", "phase_of", "record_stage_run",
           "render_tokens", "tokens_section"]

STAGE_LOG = "run_log.jsonl"

# Stage -> phase, in pipeline order. A stage not listed is "other", so a new
# stage is still counted before anyone remembers to classify it.
PHASES: dict[str, tuple[str, ...]] = {
    "setup": ("init-config", "preflight", "demo", "build-notebooks", "clean"),
    "discovery": ("assess", "fetch", "ingest", "deps", "maintenance",
                  "security", "compute", "databases", "catalogs"),
    "planning": ("data-options", "plan", "ddl"),
    "target": ("smoke", "provision", "catalog", "deploy", "run", "notebook"),
    "reporting": ("summary", "stages", "tokens"),
    "teardown": ("teardown",),
}
_PHASE_OF = {s: p for p, stages in PHASES.items() for s in stages}

_KINDS = ("input", "output", "cache_creation", "cache_read")


def phase_of(stage: str) -> str:
    """A phase stage's group comes from STAGES, the one list of phases;
    utility commands fall back to the table above."""
    from .stages import STAGES
    for spec in STAGES:
        if spec["stage"] == stage:
            return spec["phase"]
    return _PHASE_OF.get(stage, "other")


def _parse_ts(value) -> datetime.datetime:
    text = str(value).replace("Z", "+00:00")
    ts = datetime.datetime.fromisoformat(text)
    return ts if ts.tzinfo else ts.replace(tzinfo=datetime.timezone.utc)


def record_stage_run(out_dir, stage: str, started_at: str, ended_at: str,
                     exit_code: int | None, session_id: str | None, *,
                     job: str | None = None, retries: int = 0) -> None:
    """Append one stage run to the run log. Never raises into the caller's
    exit code: a log that cannot be written must not fail a migration."""
    rec = {"stage": stage, "phase": phase_of(stage),
           "started_at": started_at, "ended_at": ended_at,
           "exit_code": exit_code, "claude_session_id": session_id}
    if job:
        rec["job"] = job
    if retries:
        rec["retries"] = retries
    try:
        out = pathlib.Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        with (out / STAGE_LOG).open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec) + "\n")
    except OSError:
        pass


def read_stage_log(out_dir) -> list[dict]:
    path = pathlib.Path(out_dir) / STAGE_LOG
    if not path.is_file():
        return []
    runs = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            runs.append(json.loads(line))
        except ValueError:
            continue
    return runs


def find_transcripts(session_id: str | None, projects_dir) -> list[pathlib.Path]:
    """The session's transcript and its subagents' transcripts."""
    if not session_id:
        return []
    root = pathlib.Path(projects_dir)
    if not root.is_dir():
        return []
    found: list[pathlib.Path] = []
    for project in root.iterdir():
        main = project / f"{session_id}.jsonl"
        if main.is_file():
            found.append(main)
            found += sorted((project / session_id / "subagents").glob("*.jsonl"))
    return found


def load_usage(paths) -> list[dict]:
    """One row per assistant message, de-duplicated by message id.

    A message can appear on several lines as it streams; the fullest usage
    seen for it is kept, so a partial line never undercounts it and a repeat
    never double-counts it.
    """
    by_id: dict[str, dict] = {}
    for path in paths:
        try:
            lines = pathlib.Path(path).read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("type") != "assistant":
                continue
            msg = event.get("message") or {}
            usage = msg.get("usage")
            if not usage or not event.get("timestamp"):
                continue
            mid = msg.get("id") or f'{path}:{event.get("uuid")}'
            row = {"id": mid, "ts": _parse_ts(event["timestamp"]),
                   "model": msg.get("model"),
                   "input": int(usage.get("input_tokens") or 0),
                   "output": int(usage.get("output_tokens") or 0),
                   "cache_creation": int(
                       usage.get("cache_creation_input_tokens") or 0),
                   "cache_read": int(usage.get("cache_read_input_tokens") or 0),
                   "sidechain": bool(event.get("isSidechain"))}
            prev = by_id.get(mid)
            if prev is None:
                by_id[mid] = row
            else:
                for k in _KINDS:
                    prev[k] = max(prev[k], row[k])
                prev["ts"] = max(prev["ts"], row["ts"])
    return sorted(by_id.values(), key=lambda r: r["ts"])


def _empty() -> dict:
    return {**{k: 0 for k in _KINDS}, "total": 0, "messages": 0,
            "subagent_messages": 0}


def _add(bucket: dict, row: dict) -> None:
    for k in _KINDS:
        bucket[k] += row[k]
    bucket["total"] += sum(row[k] for k in _KINDS)
    bucket["messages"] += 1
    bucket["subagent_messages"] += 1 if row["sidechain"] else 0


def attribute(runs: list[dict], usage: list[dict], *, since: str | None = None,
              in_flight: dict | None = None,
              exclude: list[tuple[str, str]] | None = None) -> dict:
    """Assign every usage row to the stage whose window holds it.

    A run carrying `measured: False` had no transcript read for its
    session: its window still bounds its neighbours, but it gets no numbers
    (`tokens: None`), stays out of every total, and whatever the read
    transcripts spent inside its window is counted apart
    (`excluded.during_unmeasured_runs`) rather than credited to it."""
    ordered = sorted((dict(r) for r in runs), key=lambda r: r["started_at"])
    if in_flight:
        ordered.append({**in_flight, "phase": phase_of(in_flight["stage"]),
                        "in_flight": True})
    if not ordered:
        return {"runs": [], "by_stage": {}, "by_phase": {}, "totals": _empty(),
                "excluded": {"before_first_stage": _empty(),
                             "after_last_stage": _empty(),
                             "excluded_windows": _empty(),
                             "during_unmeasured_runs": _empty()},
                "by_model": {}, "unmeasured_runs": 0, "partial": False}

    windows = []
    start = _parse_ts(since) if since else _parse_ts(ordered[0]["started_at"])
    for run in ordered:
        end = _parse_ts(run["ended_at"])
        windows.append((start, end, run))
        run["tokens"] = _empty()
        run["measured"] = run.get("measured", True) is not False
        start = end

    before, after, carved = _empty(), _empty(), _empty()
    unmeasured = _empty()
    cuts = [(_parse_ts(a), _parse_ts(b)) for a, b in (exclude or [])]
    by_model: dict[str, dict] = collections.defaultdict(_empty)
    first_start, last_end = windows[0][0], windows[-1][1]
    for row in usage:
        if any(a <= row["ts"] <= b for a, b in cuts):
            _add(carved, row)
            continue
        if row["ts"] < first_start:
            _add(before, row)
            continue
        if row["ts"] > last_end:
            _add(after, row)
            continue
        for i, (lo, hi, run) in enumerate(windows):
            # The first window includes its start; later ones start exclusive
            # at the previous end, so a token on a boundary is counted once.
            inside = (lo <= row["ts"] <= hi) if i == 0 else (lo < row["ts"] <= hi)
            if inside:
                if not run["measured"]:
                    _add(unmeasured, row)
                    break
                _add(run["tokens"], row)
                _add(by_model[row["model"] or "unknown"], row)
                break

    by_stage: dict[str, dict] = {}
    by_phase: dict[str, dict] = {}
    totals = _empty()
    for _, _, run in windows:
        for key, table in ((run["stage"], by_stage), (run["phase"], by_phase)):
            bucket = table.setdefault(key, {**_empty(), "runs": 0,
                                            "unmeasured_runs": 0})
            bucket["runs"] += 1
            if not run["measured"]:
                bucket["unmeasured_runs"] += 1
                continue
            for k, v in run["tokens"].items():
                bucket[k] += v
        if run["measured"]:
            for k, v in run["tokens"].items():
                totals[k] += v
        else:
            run["tokens"] = None
    # A stage or phase none of whose runs was measured has no numbers: not
    # measured is not zero.
    for table in (by_stage, by_phase):
        for bucket in table.values():
            bucket["measured"] = bucket["unmeasured_runs"] < bucket["runs"]
            if not bucket["measured"]:
                bucket.update({k: None for k in _empty()})
    missed = sum(1 for _, _, run in windows if not run["measured"])

    return {"runs": [w[2] for w in windows], "by_stage": by_stage,
            "by_phase": by_phase, "totals": totals, "by_model": dict(by_model),
            "excluded": {"before_first_stage": before,
                         "after_last_stage": after,
                         "excluded_windows": carved,
                         "during_unmeasured_runs": unmeasured},
            "exclude_windows": [list(w) for w in (exclude or [])],
            "unmeasured_runs": missed, "partial": bool(missed)}


def _named(runs: list[dict], out_dir) -> list[dict]:
    """Give every logged run its phase name and group. A `run` carries its
    job, or -- logged before jobs were recorded -- is matched to the workflow
    whose artifact it wrote; either way its tokens land in that phase."""
    from .stages import _legacy_run_stage, stage_for
    out = []
    for run in runs:
        run = dict(run)
        if run.get("stage") == "run":
            run["stage"] = (stage_for("run", run.get("job")) if run.get("job")
                            else _legacy_run_stage(run, pathlib.Path(out_dir)))
        run["phase"] = phase_of(run["stage"])
        out.append(run)
    return out


EXCLUSIONS = "token_exclusions.json"


def save_exclusions(out_dir, windows: list[tuple[str, str]]) -> list:
    """Persist exclusion windows with the run, merged with any saved before,
    so every later token build -- including the per-stage publish -- applies
    them. A window given once holds for the rest of the run."""
    path = pathlib.Path(out_dir) / EXCLUSIONS
    saved = _saved_exclusions(out_dir)
    for w in windows or []:
        if list(w) not in saved:
            saved.append(list(w))
    if saved:
        path.write_text(json.dumps({"windows": saved}, indent=2))
    return saved


def _saved_exclusions(out_dir) -> list:
    path = pathlib.Path(out_dir) / EXCLUSIONS
    if not path.is_file():
        return []
    try:
        return [list(w) for w in json.loads(path.read_text()).get("windows") or []]
    except ValueError:
        return []


def build_token_report(out_dir, *, transcripts=None, projects_dir=None,
                       since: str | None = None,
                       in_flight: dict | None = None,
                       exclude: list[tuple[str, str]] | None = None) -> dict:
    """The whole token stage: run log + transcripts -> the report."""
    exclude = [tuple(w) for w in save_exclusions(out_dir, exclude or [])]
    runs = read_stage_log(out_dir)
    if not runs and not in_flight:
        return {"measured": False,
                "reason": f"no {STAGE_LOG} in the artifact directory: no stage "
                          f"has run since stage logging was added"}
    if transcripts is None:
        projects = pathlib.Path(projects_dir) if projects_dir else (
            pathlib.Path.home() / ".claude" / "projects")
        sessions = sorted({r.get("claude_session_id") for r in runs
                           if r.get("claude_session_id")}
                          | ({in_flight.get("claude_session_id")}
                             if in_flight and in_flight.get("claude_session_id")
                             else set()))
        if not sessions:
            return {"measured": False,
                    "reason": "no stage ran under a Claude Code session "
                              "(CLAUDE_CODE_SESSION_ID was unset), so there is "
                              "no transcript to read. Pass --transcript to "
                              "point at one."}
        found = {s: find_transcripts(s, projects) for s in sessions}
        transcripts = [p for s in sessions for p in found[s]]
        if not transcripts:
            return {"measured": False, "sessions": sessions,
                    "reason": f"no transcript found for session(s) "
                              f"{', '.join(sessions)} under {projects}"}
        # Measured per run: only a run whose OWN session's transcript was
        # read has numbers. The others are named, with the reason.
        read = {s for s, paths in found.items() if paths}

        def _mark(run: dict) -> dict:
            sid = run.get("claude_session_id")
            if sid in read:
                return {**run, "measured": True}
            return {**run, "measured": False, "unmeasured_reason": (
                f"no transcript found for session {sid} under {projects}"
                if sid else "ran outside a Claude Code session "
                            "(CLAUDE_CODE_SESSION_ID was unset)")}

        runs = [_mark(r) for r in runs]
        in_flight = _mark(in_flight) if in_flight else in_flight
    usage = load_usage(transcripts)
    rep = attribute(_named(runs, out_dir), usage, since=since,
                    in_flight=in_flight, exclude=exclude)
    rep.update({"measured": True, "since": since,
                "transcripts": [str(p) for p in transcripts],
                "generated_at": datetime.datetime.now(
                    datetime.timezone.utc).isoformat()})
    return rep


# ------------------------------------------------------------------ render

def _n(v: int) -> str:
    return f"{v:,}"


def _row(name: str, b: dict, runs: bool = True) -> str:
    cells = [name]
    if runs:
        cells.append(str(b.get("runs", "")))
    if b.get("measured") is False:
        # No transcript was read for any of these runs: not zero.
        return "| " + " | ".join(cells + ["not measured"] * 6) + " |"
    if b.get("unmeasured_runs"):
        cells[0] += f' *({b["unmeasured_runs"]} run(s) not measured)*'
    cells += [_n(b["input"]), _n(b["output"]), _n(b["cache_creation"]),
              _n(b["cache_read"]), f'**{_n(b["total"])}**',
              str(b["messages"])]
    return "| " + " | ".join(cells) + " |"


def _partial_line(rep: dict) -> list[str]:
    if not rep.get("partial"):
        return []
    return [f'**Partial: {rep["unmeasured_runs"]} stage run(s) not '
            'measured** -- no transcript was read for their session, so '
            'their tokens are unknown. They are marked "not measured" '
            'below and left out of every total; they are not zero.', ""]


_HEAD = ("| Input | Output | Cache write | Cache read | Total | Msgs |")
_RULE = "|---:|---:|---:|---:|---:|---:|"


def _phase_order(by_phase: dict) -> list[str]:
    known = [p for p in (*PHASES, "other") if p in by_phase]
    return known + sorted(set(by_phase) - set(known))


def render_tokens(rep: dict) -> str:
    out = ["# LLM token usage — per stage and phase", ""]
    if not rep.get("measured"):
        return "\n".join(out + [f'**Not measured:** {rep.get("reason")}', ""])
    t = rep["totals"]
    out += [f'**{_n(t["total"])} tokens** over {t["messages"]} model call(s) '
            f'({t["subagent_messages"]} from subagents): '
            f'{_n(t["input"])} input · {_n(t["output"])} output · '
            f'{_n(t["cache_creation"])} cache write · '
            f'{_n(t["cache_read"])} cache read.', "",
            "The engine makes no LLM call; these are the tokens the agent "
            "driving it spent. A stage is credited with everything spent "
            "after the previous stage ended, up to its own end.", ""]
    out += _partial_line(rep)
    out += ["## By phase", "", "| Phase | Stage runs " + _HEAD,
            "|---|---:" + _RULE]
    out += [_row(p, rep["by_phase"][p]) for p in _phase_order(rep["by_phase"])]
    out += [_row("**Total**", {**t, "runs": len(rep["runs"])}), ""]

    out += ["## By stage", "", "| Stage | Phase | Runs " + _HEAD,
            "|---|---|---:" + _RULE]
    phase = {r["stage"]: r["phase"] for r in rep["runs"]}
    for stage, b in rep["by_stage"].items():
        out.append(_row(f"`{stage}` | {phase.get(stage, 'other')}", b))
    out.append("")

    out += ["## Stage runs, in order", "",
            "| # | Stage | Started | Ended | Exit | Total | Msgs |",
            "|---:|---|---|---|---:|---:|---:|"]
    for i, r in enumerate(rep["runs"], 1):
        flight = " *(in flight)*" if r.get("in_flight") else ""
        tokens = r.get("tokens")
        cells = ((_n(tokens["total"]), str(tokens["messages"])) if tokens
                 else (f'not measured ({r.get("unmeasured_reason") or "no transcript read"})',
                       "not measured"))
        out.append(f'| {i} | `{r["stage"]}`{flight} | {r["started_at"]} | '
                   f'{r["ended_at"]} | {r.get("exit_code", "")} | '
                   f'{cells[0]} | {cells[1]} |')
    out.append("")

    if rep.get("by_model"):
        out += ["## By model", "", "| Model " + _HEAD, "|---" + _RULE]
        out += [_row(f"`{m}`", b, runs=False)
                for m, b in sorted(rep["by_model"].items())]
        out.append("")

    ex = rep["excluded"]
    out += ["## Excluded", "",
            f'- Before the first stage: **{_n(ex["before_first_stage"]["total"])}** '
            f'tokens ({ex["before_first_stage"]["messages"]} call(s)). Pass '
            f'`--since` to include setup work.',
            f'- After the last stage: **{_n(ex["after_last_stage"]["total"])}** '
            f'tokens ({ex["after_last_stage"]["messages"]} call(s)).',
            f'- Excluded windows (`--exclude-window`, work in the same session '
            f'that is not this migration): '
            f'**{_n(ex.get("excluded_windows", _empty())["total"])}** tokens'
            + (" — " + ", ".join(f"{a} → {b}" for a, b in
                                 rep.get("exclude_windows") or [])
               if rep.get("exclude_windows") else "") + "."]
    if rep.get("partial"):
        during = ex.get("during_unmeasured_runs") or _empty()
        out.append(f'- During the stage runs that were not measured: '
                   f'**{_n(during["total"])}** tokens from the transcripts '
                   f'that were read ({during["messages"]} call(s)) -- not '
                   f'credited to a stage whose own session is unknown.')
    out += ["",
            "Transcripts read: " + ", ".join(
                f"`{pathlib.Path(p).name}`" for p in rep.get("transcripts") or []),
            ""]
    return "\n".join(out)


def tokens_section(rep: dict | None) -> list[str]:
    """The per-stage / per-phase totals, for the end of the summary."""
    out = ["## LLM token usage", ""]
    if not rep or not rep.get("measured"):
        reason = (rep or {}).get("reason", "no token report was built")
        return out + [f"Not measured: {reason}", ""]
    t = rep["totals"]
    out += _partial_line(rep)
    out += ["| Phase | Stage runs " + _HEAD, "|---|---:" + _RULE]
    out += [_row(p, rep["by_phase"][p]) for p in _phase_order(rep["by_phase"])]
    out += [_row("**Total**", {**t, "runs": len(rep["runs"])}), "",
            "| Stage | Runs " + _HEAD, "|---|---:" + _RULE]
    out += [_row(f"`{s}`", b) for s, b in rep["by_stage"].items()]
    out += ["", "Tokens the agent spent driving each stage (the engine itself "
            "calls no model). Detail: `TOKENS.md` / `tokens.json`.", ""]
    return out
