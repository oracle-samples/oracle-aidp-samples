"""``run --auto-remediate-coa`` loop (design §10.4, FR-12/13, D-12, R9).

The loop is pure orchestration over three seams — the run backends (patched),
the reconciled verdict (real ``RunOutcome`` objects via the sink contract),
and the bootstrap entry point (patched) — so every stop/resume row is
unit-testable without a cluster:

* resumes ONLY when the resolution phase exits 0 (FR-14a S1);
* the internal resume passes ``mode=None`` — adopts the recorded mode
  (AIDPF-1046 protection), identically for seed AND incremental aborts;
* at most ONE pass (the resumed run never re-enters the loop, R9);
* non-COA aborts, dry-runs, and user-issued resumes never remediate.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from rich.console import Console

import oracle_ai_data_platform_fusion_autopilot.commands.run as run_mod
from oracle_ai_data_platform_fusion_autopilot.commands.run import run
from oracle_ai_data_platform_fusion_autopilot.commands.run_reconcile import RunOutcome


def _console() -> Console:
    return Console(record=True, width=120)


def _outcome(codes: tuple[str, ...], exit_code: int = 1) -> RunOutcome:
    return RunOutcome(
        verdict="aborted" if exit_code else "completed",
        exit_code=exit_code,
        codes=codes,
        lines=(f"RUN VERDICT: {'ABORTED' if exit_code else 'COMPLETE'}",),
    )


class _FakeBackend:
    """Stands in for BOTH `_run_inline` and `_run_via_aidp_dispatch`.

    First (fresh) call reports the scripted abort through the sink; a call
    carrying ``resume_run_id`` reports the scripted resume result. Records
    every call's interesting kwargs for assertions.
    """

    def __init__(self, *, abort_outcome, run_id="cp-run-1", mode="seed",
                 resume_exit=0):
        self.abort_outcome = abort_outcome
        self.run_id = run_id
        self.mode = mode
        self.resume_exit = resume_exit
        self.calls: list[dict] = []

    def inline(self, bundle_path, mode, datasets, layers, resume_run_id,
               dry_run, console, *, coa_abort_sink=None, **kw):
        return self._record(mode, resume_run_id, coa_abort_sink)

    def dispatch(self, bundle_path, config_path, env_name, datasets, layers,
                 mode, dry_run, poll_timeout_s, console, *,
                 resume_run_id=None, coa_abort_sink=None, **kw):
        return self._record(mode, resume_run_id, coa_abort_sink)

    def _record(self, mode, resume_run_id, sink) -> int:
        self.calls.append({"mode": mode, "resume_run_id": resume_run_id})
        if resume_run_id is not None:
            if sink is not None:
                sink.append((_outcome((), self.resume_exit), resume_run_id, self.mode))
            return self.resume_exit
        if sink is not None:
            sink.append((self.abort_outcome, self.run_id, self.mode))
        return self.abort_outcome.exit_code


@pytest.fixture()
def bundle(tmp_path: Path) -> Path:
    p = tmp_path / "bundle.yaml"
    p.write_text("project: x\n")
    return p


def _wire(monkeypatch, backend: _FakeBackend, bootstrap_rc: int):
    monkeypatch.setattr(run_mod, "_run_inline", backend.inline)
    monkeypatch.setattr(run_mod, "_run_via_aidp_dispatch", backend.dispatch)
    calls: list[dict] = []

    def _fake_bootstrap(bundle_path, config_path, env_name, **kw):
        calls.append(kw)
        return bootstrap_rc

    import oracle_ai_data_platform_fusion_autopilot.commands.bootstrap as bs
    monkeypatch.setattr(bs, "bootstrap", _fake_bootstrap)
    return calls


@pytest.mark.parametrize("aborted_mode", ["seed", "incremental"])
@pytest.mark.parametrize("inline", [False, True])
def test_resolution_0_resumes_without_mode(monkeypatch, bundle, aborted_mode, inline):
    """FR-14a S1 → resume; D-12: `mode=None` for BOTH recorded modes and
    BOTH backends — the resume adopts the run's recorded mode."""
    backend = _FakeBackend(
        abort_outcome=_outcome(("AIDPF-2018",)), mode=aborted_mode,
        resume_exit=0,
    )
    boot_calls = _wire(monkeypatch, backend, bootstrap_rc=0)
    rc = run(
        bundle, bundle.parent / "aidp.config.yaml", "dev",
        mode=aborted_mode, inline=inline, auto_remediate_coa=True,
        console=_console(),
    )
    assert rc == 0
    assert len(boot_calls) == 1
    assert boot_calls[0]["refresh"] is True
    assert boot_calls[0]["resolve_coa_from_metadata"] is True
    assert boot_calls[0]["non_interactive"] is True
    resume_calls = [c for c in backend.calls if c["resume_run_id"] is not None]
    assert resume_calls == [{"mode": None, "resume_run_id": "cp-run-1"}]


def test_resolution_nonzero_stops_without_resume(monkeypatch, bundle):
    """FR-14a S2/S3/S4 (phase exit non-zero) → STOP with the phase's
    diagnostic; the aborted run's exit code is preserved; no resume."""
    backend = _FakeBackend(abort_outcome=_outcome(("AIDPF-2018",)))
    boot_calls = _wire(monkeypatch, backend, bootstrap_rc=1)
    console = _console()
    rc = run(
        bundle, bundle.parent / "aidp.config.yaml", "dev",
        auto_remediate_coa=True, console=console,
    )
    assert rc == 1
    assert len(boot_calls) == 1
    assert all(c["resume_run_id"] is None for c in backend.calls)
    text = console.export_text()
    assert "STOP" in text and "AIDPF-2021" in text and "AIDPF-2023" in text


def test_2017_abort_also_triggers(monkeypatch, bundle):
    backend = _FakeBackend(abort_outcome=_outcome(("AIDPF-2017",)), resume_exit=0)
    boot_calls = _wire(monkeypatch, backend, bootstrap_rc=0)
    rc = run(
        bundle, bundle.parent / "aidp.config.yaml", "dev",
        auto_remediate_coa=True, console=_console(),
    )
    assert rc == 0 and len(boot_calls) == 1


def test_non_coa_abort_never_remediates(monkeypatch, bundle):
    """A non-COA failure (e.g. a render error) exits as-is — no bootstrap,
    no resume."""
    backend = _FakeBackend(abort_outcome=_outcome(("AIDPF-5010",)))
    boot_calls = _wire(monkeypatch, backend, bootstrap_rc=0)
    rc = run(
        bundle, bundle.parent / "aidp.config.yaml", "dev",
        auto_remediate_coa=True, console=_console(),
    )
    assert rc == 1
    assert boot_calls == []
    assert len(backend.calls) == 1


def test_flag_off_is_inert(monkeypatch, bundle):
    backend = _FakeBackend(abort_outcome=_outcome(("AIDPF-2018",)))
    boot_calls = _wire(monkeypatch, backend, bootstrap_rc=0)
    rc = run(
        bundle, bundle.parent / "aidp.config.yaml", "dev",
        auto_remediate_coa=False, console=_console(),
    )
    assert rc == 1 and boot_calls == [] and len(backend.calls) == 1


def test_user_resume_never_chains_a_second_pass(monkeypatch, bundle):
    """A user-issued --resume that aborts again with 2018 must STOP for a
    human (R9) — remediation only ever follows a FRESH aborted run."""
    backend = _FakeBackend(abort_outcome=_outcome(("AIDPF-2018",)), resume_exit=1)
    boot_calls = _wire(monkeypatch, backend, bootstrap_rc=0)
    rc = run(
        bundle, bundle.parent / "aidp.config.yaml", "dev",
        resume_run_id="cp-run-1", auto_remediate_coa=True, console=_console(),
    )
    assert rc == 1 and boot_calls == []


def test_resumed_run_aborting_again_stops(monkeypatch, bundle):
    """One pass only: resolution 0 → resume → the resumed run aborts with
    2018 AGAIN → exit non-zero, bootstrap ran exactly once (R9)."""
    backend = _FakeBackend(abort_outcome=_outcome(("AIDPF-2018",)), resume_exit=1)
    boot_calls = _wire(monkeypatch, backend, bootstrap_rc=0)
    rc = run(
        bundle, bundle.parent / "aidp.config.yaml", "dev",
        auto_remediate_coa=True, console=_console(),
    )
    assert rc == 1
    assert len(boot_calls) == 1
    resume_calls = [c for c in backend.calls if c["resume_run_id"] is not None]
    assert len(resume_calls) == 1


def test_dry_run_never_remediates(monkeypatch, bundle):
    backend = _FakeBackend(abort_outcome=_outcome(("AIDPF-2018",)))
    boot_calls = _wire(monkeypatch, backend, bootstrap_rc=0)
    rc = run(
        bundle, bundle.parent / "aidp.config.yaml", "dev",
        dry_run=True, auto_remediate_coa=True, console=_console(),
    )
    assert rc == 1 and boot_calls == []


def test_injected_2022_detail_does_not_block_when_phase_exits_0(monkeypatch, bundle):
    """P3 ship gate's loop-stop nuance (FR-14a): an AIDPF-2022 per-arm detail
    for an inactive chart coexists with phase exit 0 — the loop keys on the
    EXIT CODE, so the resume proceeds."""
    backend = _FakeBackend(abort_outcome=_outcome(("AIDPF-2018",)), resume_exit=0)
    boot_calls = _wire(monkeypatch, backend, bootstrap_rc=0)  # 2022 recorded, exit 0
    rc = run(
        bundle, bundle.parent / "aidp.config.yaml", "dev",
        auto_remediate_coa=True, console=_console(),
    )
    assert rc == 0 and len(boot_calls) == 1
