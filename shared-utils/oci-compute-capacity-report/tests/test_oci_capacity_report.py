# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v1.0 as shown at
# https://oss.oracle.com/licenses/upl/
"""Offline tests for oci_capacity_report.py — the OCI CLI is fully mocked.

Run:  python3 -m pytest tests/
"""
import importlib.util
import json
import pathlib

import pytest

_SRC = pathlib.Path(__file__).resolve().parents[1] / "oci_capacity_report.py"
_spec = importlib.util.spec_from_file_location("ocr", _SRC)
ocr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ocr)

AD1, AD2 = "xxxx:EXAMPLE-AD-1", "xxxx:EXAMPLE-AD-2"

# Processor descriptions as ListShapes returns them.
ALTRA = "3.0 GHz Ampere® Altra™"
AMPEREONE = "3.0 GHz Ampere® AmpereOne™ (Siryn™)"
AMPEREONE_M = "3.6 GHz Ampere® AmpereOne-M™ (Banshee 12™)"
GENOA = "2.3 GHz AMD EPYC™ 9J14 (Genoa)"


def flex(shape, arch, proc=None, min_oc=1, max_oc=64, min_mem=1, max_mem=1024,
         min_per=1, max_per=64):
    proc = proc or (ALTRA if arch == "ARM" else GENOA)
    return {"shape": shape, "arch": arch, "proc": proc,
            "vpo": ocr.vcpu_per_ocpu(arch, proc), "flexible": True, "ocpus": None,
            "mem": None, "min_oc": min_oc, "max_oc": max_oc, "min_mem": min_mem,
            "max_mem": max_mem, "min_per_oc": min_per, "max_per_oc": max_per}


def report(*rows):
    """A capacity-report response body. rows: (shape, status, count-or-None)."""
    return {"data": {"shape-availabilities": [
        {"instance-shape": s, "availability-status": st,
         **({"available-count": c} if c is not None else {})} for s, st, c in rows]}}


class FakeOCI:
    """Stands in for ocr._oci. `answers[(ad, shape)]` = (status, count) or a CLI
    error string for that shape."""

    def __init__(self, answers, fail_batches_containing=()):
        self.answers = answers
        self.fail_batches_containing = set(fail_batches_containing)
        self.calls = []

    def __call__(self, args, profile, region):
        assert args[:3] == ["compute", "compute-capacity-report", "create"]
        ad = args[args.index("--availability-domain") + 1]
        batch = json.loads(args[args.index("--shape-availabilities") + 1])
        shapes = [b["instanceShape"] for b in batch]
        self.calls.append((ad, shapes))
        assert len(shapes) == len(set(shapes)), "same shape twice in one batch"
        # Like the real API, one failing shape fails the WHOLE request.
        for s in shapes:
            ans = self.answers[(ad, s)]
            if isinstance(ans, str):
                if len(shapes) > 1 and s in self.fail_batches_containing:
                    return None, "InvalidParameter: one config in the batch is invalid"
                return None, ans
        return report(*[(s, *self.answers[(ad, s)]) for s in shapes]), None


# ---------------------------------------------------------------- vCPU per OCPU

@pytest.mark.parametrize("arch,proc,expected", [
    ("ARM", ALTRA, 1),                  # A1
    ("ARM", AMPEREONE, 2),              # A2
    ("ARM", AMPEREONE_M, 2),            # A4
    ("ARM", "AmpereOne-M", 2),          # A4.Ax (short description)
    ("ARM", "Ampere FutureChip", None), # unknown family: never guess
    ("AMD", GENOA, 2),
])
def test_vcpu_per_ocpu_follows_processor(arch, proc, expected):
    assert ocr.vcpu_per_ocpu(arch, proc) == expected


# ---------------------------------------------------------------- fits()

def test_fits_flexible_limits():
    sh = flex("VM.Standard.A1.Flex", "ARM", max_oc=80, max_mem=512, max_per=64)
    assert ocr.fits(sh, 32, 256) == (True, "")
    assert ocr.fits(sh, 96, 512)[0] is False          # > max OCPU
    assert ocr.fits(sh, 8, 1024)[0] is False          # > max memory
    assert "GB/OCPU" in ocr.fits(sh, 1, 128)[1]       # > max memory per OCPU


def test_fits_fixed_shape_matches_exactly_only():
    sh = {**flex("VM.Standard.E4.Fixed", "AMD"), "flexible": False, "ocpus": 8, "mem": 128}
    assert ocr.fits(sh, 8, 128) == (True, "")
    assert ocr.fits(sh, 8, 64)[0] is False


def test_fits_missing_metadata_never_guesses():
    sh = flex("VM.X.Flex", "AMD", min_oc=None, max_oc=None)
    assert ocr.fits(sh, 4, 64) == (False, "no OCPU metadata")


# ---------------------------------------------------------------- _parse()

def test_parse_statuses():
    batch = [("A", 8, 64, True), ("B", 8, 64, True), ("C", 8, 64, True),
             ("D", 8, 64, True), ("E", 8, 64, True)]
    data = report(("A", "AVAILABLE", 12), ("B", "AVAILABLE", None),
                  ("C", "OUT_OF_HOST_CAPACITY", None), ("D", "HARDWARE_NOT_SUPPORTED", None))
    out = ocr._parse(batch, data)
    assert out[("A", 8, 64)] == {"status": "AVAILABLE", "count": 12, "error": None}
    assert out[("B", 8, 64)]["status"] == ocr.NO_COUNT and out[("B", 8, 64)]["count"] is None
    assert out[("C", 8, 64)]["count"] == 0
    assert out[("D", 8, 64)]["status"] == "HARDWARE_NOT_SUPPORTED"
    assert out[("E", 8, 64)]["status"] == "MISSING_FROM_RESPONSE"


def test_parse_duplicate_rows_are_ambiguous():
    data = report(("A", "AVAILABLE", 1), ("A", "AVAILABLE", 2))
    assert ocr._parse([("A", 1, 8, True)], data)[("A", 1, 8)]["status"] == "AMBIGUOUS"


# ---------------------------------------------------------------- real CLI output
# Fixtures keep the OCI CLI's exact field spellings (kebab-case such as
# min-in-g-bs next to min-per-ocpu-in-gbs). shape_list.json holds real
# `oci compute shape list` rows (public shape limits only). capacity_report.json
# has the real response structure with illustrative counts.

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"


def _fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_discover_shapes_from_real_shape_list(monkeypatch):
    monkeypatch.setattr(ocr, "_oci", lambda args, p, r: (_fixture("shape_list.json"), None))
    found = {s["shape"]: s for s in ocr.discover_shapes("p", "r", "t", AD1)}
    # Kept: non-GPU ARM and AMD VM shapes. Skipped: Intel, GPU, bare metal, .Generic.
    assert set(found) == {"VM.Standard.A1.Flex", "VM.Standard.A2.Flex", "VM.Standard.A4.Flex",
                          "VM.Standard.A4.Ax.Flex", "VM.Standard.E5.Flex", "VM.Standard.E2.1"}
    a1 = found["VM.Standard.A1.Flex"]
    assert (a1["arch"], a1["vpo"], a1["flexible"]) == ("ARM", 1, True)
    assert (a1["min_oc"], a1["max_oc"], a1["min_mem"], a1["max_mem"]) == (1, 80, 1, 512)
    assert (a1["min_per_oc"], a1["max_per_oc"]) == (1, 64)
    assert {found[s]["vpo"] for s in ("VM.Standard.A2.Flex", "VM.Standard.A4.Flex",
                                      "VM.Standard.A4.Ax.Flex")} == {2}
    assert (found["VM.Standard.E5.Flex"]["arch"], found["VM.Standard.E5.Flex"]["vpo"]) == ("AMD", 2)
    e21 = found["VM.Standard.E2.1"]                      # fixed shape: null options
    assert e21["flexible"] is False
    assert (e21["min_oc"], e21["max_oc"], e21["min_mem"], e21["max_mem"]) == (1, 1, 8, 8)
    assert ocr.fits(e21, 1, 8) == (True, "") and ocr.fits(e21, 2, 16)[0] is False


def test_parse_real_capacity_report_structure():
    batch = [("VM.Standard.A1.Flex", 8.0, 64.0, True), ("VM.Standard.A2.Flex", 8.0, 64.0, True),
             ("VM.Standard.E5.Flex", 8.0, 128.0, True)]
    out = ocr._parse(batch, _fixture("capacity_report.json"))
    assert {k[0]: (v["status"], v["count"]) for k, v in out.items()} == {
        "VM.Standard.A1.Flex": ("AVAILABLE", 120),
        "VM.Standard.A2.Flex": ("AVAILABLE", 7),
        "VM.Standard.E5.Flex": ("AVAILABLE", 45)}


# ---------------------------------------------------------------- probe()

def test_probe_isolates_invalid_config_and_keeps_others(monkeypatch):
    fake = FakeOCI({(AD1, "A"): ("AVAILABLE", 5), (AD1, "B"): "InvalidParameter: bad config",
                    (AD1, "C"): ("AVAILABLE", 7)}, fail_batches_containing={"B"})
    monkeypatch.setattr(ocr, "_oci", fake)
    res = ocr.probe("p", "r", "t", AD1, [("A", 8, 64, True), ("B", 8, 64, True), ("C", 8, 64, True)])
    assert res[("A", 8, 64)]["count"] == 5
    assert res[("C", 8, 64)]["count"] == 7
    assert res[("B", 8, 64)]["status"] == "INVALID_CONFIG"


def test_probe_other_failure_is_request_failed(monkeypatch):
    fake = FakeOCI({(AD1, "A"): "ServiceError: 500 internal"})
    monkeypatch.setattr(ocr, "_oci", fake)
    res = ocr.probe("p", "r", "t", AD1, [("A", 8, 64, True)])
    assert res[("A", 8, 64)]["status"] == "REQUEST_FAILED"


def test_probe_respects_batch_size(monkeypatch):
    answers = {(AD1, s): ("AVAILABLE", 1) for s in "ABCDE"}
    fake = FakeOCI(answers)
    monkeypatch.setattr(ocr, "_oci", fake)
    ocr.probe("p", "r", "t", AD1, [(s, 1, 8, True) for s in "ABCDE"], batch_size=2)
    assert [len(shapes) for _ad, shapes in fake.calls] == [2, 2, 1]


class _Run:
    """Fake subprocess.run: returns queued (returncode, stdout, stderr) results."""

    def __init__(self, *results):
        self.results = list(results)
        self.cmds = []

    def __call__(self, cmd, **kw):
        self.cmds.append(cmd)
        rc, out, err = self.results.pop(0)
        return ocr.subprocess.CompletedProcess(cmd, rc, out, err)


THROTTLED = 'ServiceError: {"code": "TooManyRequests", "status": 429}'


def test_auth_is_passed_to_the_cli(monkeypatch):
    run = _Run((0, "{}", ""), (0, "{}", ""))
    monkeypatch.setattr(ocr.subprocess, "run", run)
    monkeypatch.setattr(ocr, "AUTH", None)
    ocr._oci(["compute", "x"], "p", "r")
    assert "--auth" not in run.cmds[0]
    monkeypatch.setattr(ocr, "AUTH", "security_token")
    ocr._oci(["compute", "x"], "p", "r")
    assert run.cmds[1][-2:] == ["--auth", "security_token"]


def test_throttled_call_is_retried_then_succeeds(monkeypatch):
    run = _Run((1, "", THROTTLED), (1, "", THROTTLED), (0, '{"ok": 1}', ""))
    sleeps = []
    monkeypatch.setattr(ocr.subprocess, "run", run)
    monkeypatch.setattr(ocr.time, "sleep", sleeps.append)
    assert ocr._oci(["compute", "x"], "p", "r") == ({"ok": 1}, None)
    assert sleeps == [2, 4]


def test_throttling_gives_up_after_retries(monkeypatch):
    run = _Run(*[(1, "", THROTTLED)] * (ocr.THROTTLE_RETRIES + 1))
    monkeypatch.setattr(ocr.subprocess, "run", run)
    monkeypatch.setattr(ocr.time, "sleep", lambda s: None)
    data, err = ocr._oci(["compute", "x"], "p", "r")
    assert data is None and "TooManyRequests" in err
    assert len(run.cmds) == ocr.THROTTLE_RETRIES + 1


def test_other_errors_are_not_retried(monkeypatch):
    run = _Run((1, "", "ServiceError: NotAuthorizedOrNotFound"))
    monkeypatch.setattr(ocr.subprocess, "run", run)
    data, err = ocr._oci(["compute", "x"], "p", "r")
    assert data is None and len(run.cmds) == 1


def test_cli_timeout_becomes_request_failed(monkeypatch):
    def boom(*a, **k):
        raise ocr.subprocess.TimeoutExpired(cmd="oci", timeout=1)
    monkeypatch.setattr(ocr.subprocess, "run", boom)
    data, err = ocr._oci(["compute", "x"], "p", "r")
    assert data is None and "timed out" in err


# ---------------------------------------------------------------- sweep / fit check

A1, A2, A4, E5 = "VM.Standard.A1.Flex", "VM.Standard.A2.Flex", "VM.Standard.A4.Flex", "VM.Standard.E5.Flex"
SHAPES = {AD1: [flex(A1, "ARM"), flex(E5, "AMD")],
          AD2: [flex(A1, "ARM"), flex(E5, "AMD")]}


def _run_sweep(monkeypatch, capsys, answers, shapes=SHAPES, sizes=(8,)):
    monkeypatch.setattr(ocr, "_oci", FakeOCI(answers))
    ads = list(shapes)
    code = ocr.sweep("p", "r", "t", ads, shapes, {"ARM": 8, "AMD": 16}, list(sizes), False)
    return code, capsys.readouterr().out


def test_sweep_all_counts_exit_0(monkeypatch, capsys):
    ans = {(ad, s): ("AVAILABLE", 10) for ad in (AD1, AD2) for s in (A1, E5)}
    code, out = _run_sweep(monkeypatch, capsys, ans)
    assert code == 0
    assert "20   (AD-1=10, AD-2=10)" in out
    assert "Limitations:" in out and "NO COUNT" not in out
    assert "simultaneously launchable" in out and "same machine" not in out


def test_sweep_hidden_counts_are_flagged_not_guessed(monkeypatch, capsys):
    ans = {(ad, s): ("AVAILABLE", None) for ad in (AD1, AD2) for s in (A1, E5)}
    ans[(AD1, E5)] = ("AVAILABLE", 4)
    code, out = _run_sweep(monkeypatch, capsys, ans)
    assert code == 3
    assert f"avail{ocr.DAGGER}" in out                 # ARM: no counts at all
    assert f"4+{ocr.DAGGER}" in out                    # AMD: partial count = lower bound
    assert "OCI may return availability" in out
    assert "INCOMPLETE" not in out


def test_sweep_failure_is_incomplete_exit_2(monkeypatch, capsys):
    ans = {(ad, s): ("AVAILABLE", 1) for ad in (AD1, AD2) for s in (A1, E5)}
    ans[(AD2, A1)] = "ServiceError: 500"
    code, out = _run_sweep(monkeypatch, capsys, ans)
    assert code == 2 and "INCOMPLETE" in out


def test_sweep_separates_altra_and_ampereone(monkeypatch, capsys):
    """A1 (1 vCPU/OCPU) and A2/A4 (2 vCPU/OCPU) must never share a row."""
    shapes = {AD1: [flex(A1, "ARM", ALTRA), flex(A2, "ARM", AMPEREONE),
                    flex(A4, "ARM", AMPEREONE_M, max_oc=45), flex(E5, "AMD")]}
    ans = {(AD1, A1): ("AVAILABLE", 100), (AD1, A2): ("AVAILABLE", 7),
           (AD1, A4): ("AVAILABLE", 5), (AD1, E5): ("AVAILABLE", 50)}
    code, out = _run_sweep(monkeypatch, capsys, ans, shapes, sizes=(32,))
    assert code == 0
    a1_sec = out.split("=== ARM (1 vCPU/OCPU)")[1].split("===")[1]
    a24_sec = out.split("=== ARM (2 vCPU/OCPU)")[1].split("===")[1]
    assert "families: A1" in out and "families: A2, A4" in out
    # 32 OCPU: A1 row = 32 vCPU; A2/A4 row = 64 vCPU, and their counts stay apart.
    assert "      32       32         256           100   (A1=100)" in out
    assert "      32       64         256            12   (A2=7, A4=5)" in out
    assert "A2" not in a1_sec and "A1=" not in a24_sec


def test_fit_check_vcpu_per_shape_and_comparables(monkeypatch, capsys):
    shapes = [flex(A1, "ARM", ALTRA), flex(A2, "ARM", AMPEREONE), flex(E5, "AMD")]
    ans = {(AD1, A1): ("AVAILABLE", 9), (AD1, A2): ("AVAILABLE", None),
           (AD1, E5): ("OUT_OF_HOST_CAPACITY", None)}
    monkeypatch.setattr(ocr, "_oci", FakeOCI(ans))
    code = ocr.fit_check("p", "r", "t", [AD1], {AD1: shapes}, 8, 64)
    out = capsys.readouterr().out
    assert code == 3
    assert "VM.Standard.A1.Flex        ARM       8 AVAILABLE" in out
    assert "VM.Standard.A2.Flex        ARM      16 AVAILABLE_NO_COUNT" in out
    assert f"not returned{ocr.DAGGER}" in out
    assert "ARM (1 vCPU/OCPU)" in out and "(8 vCPU per node)" in out
    assert "ARM (2 vCPU/OCPU)" in out and "(16 vCPU per node)" in out
    assert "2-vCPU/OCPU shapes at 8 vCPU: --ocpus 4 --memory-gb 64" in out
    assert "1-vCPU/OCPU shapes at 16 vCPU: --ocpus 16 --memory-gb 64" in out


def test_unknown_processor_shows_question_mark(monkeypatch, capsys):
    shapes = {AD1: [flex("VM.Standard.A9.Flex", "ARM", "Ampere FutureChip")]}
    code, out = _run_sweep(monkeypatch, capsys, {(AD1, "VM.Standard.A9.Flex"): ("AVAILABLE", 3)},
                           shapes)
    assert code == 0
    assert "ARM (? vCPU/OCPU)" in out
    assert "       8        ?          64             3" in out


# ---------------------------------------------------------------- AD and config

def test_resolve_ad_accepts_short_or_full_name():
    ads = ["xxxx:US-EXAMPLE-AD-1", "xxxx:US-EXAMPLE-AD-2"]
    assert ocr.resolve_ad("AD-2", ads) == ads[1]
    assert ocr.resolve_ad("ad-1", ads) == ads[0]
    assert ocr.resolve_ad(ads[1], ads) == ads[1]
    with pytest.raises(SystemExit):
        ocr.resolve_ad("AD-3", ads)


def test_region_defaults_to_profile(monkeypatch, tmp_path):
    cfg = tmp_path / "config"
    cfg.write_text("[DEFAULT]\ntenancy=TENANCY_PLACEHOLDER\nregion=us-ashburn-1\n")
    monkeypatch.setenv("OCI_CLI_CONFIG_FILE", str(cfg))
    assert ocr._region_from_profile("DEFAULT") == "us-ashburn-1"
    assert ocr._tenancy_from_profile("DEFAULT") == "TENANCY_PLACEHOLDER"


def test_missing_region_is_a_setup_error(monkeypatch, tmp_path):
    cfg = tmp_path / "config"
    cfg.write_text("[DEFAULT]\ntenancy=TENANCY_PLACEHOLDER\n")
    monkeypatch.setenv("OCI_CLI_CONFIG_FILE", str(cfg))
    with pytest.raises(SystemExit):
        ocr._region_from_profile("DEFAULT")
