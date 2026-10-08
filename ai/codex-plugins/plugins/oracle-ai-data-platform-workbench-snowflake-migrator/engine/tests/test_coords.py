"""AIDP target coordinates: runtime-supplied only, never discovered."""
import pathlib

import pytest

from target.coords import MissingTarget, Target, region_from_ocid, resolve_target

OK = dict(datalake_ocid="ocid1.aidataplatform.oc1.iad.aaaa",
          workspace="ws-1", cluster_id="cl-1", catalog="bronze")


def test_all_four_arguments_produce_a_target():
    t = resolve_target(**OK)
    assert isinstance(t, Target)
    assert t.catalog == "bronze"


@pytest.mark.parametrize("missing", list(OK))
def test_each_argument_is_mandatory(missing):
    args = {k: v for k, v in OK.items() if k != missing}
    with pytest.raises(MissingTarget, match=missing):
        resolve_target(**args)


def test_nothing_supplied_names_all_four():
    with pytest.raises(MissingTarget) as exc:
        resolve_target()
    for k in OK:
        assert k in str(exc.value)


def test_environment_variables_are_ignored(monkeypatch):
    # The safety requirement: coordinates cannot be DISCOVERED, only passed.
    for k, v in OK.items():
        monkeypatch.setenv(k.upper(), v)
        monkeypatch.setenv(f"AIDP_{k.upper()}", v)
    with pytest.raises(MissingTarget):
        resolve_target()


def test_module_contains_no_environment_or_file_reads():
    # Enforced by inspection so a future edit cannot quietly add a lookup.
    text = (pathlib.Path(__file__).resolve().parents[1] / "target/coords.py").read_text(encoding="utf-8")
    for forbidden in ("os.environ", "getenv", "open(", "read_text", "Path("):
        assert forbidden not in text, f"coords.py must not use {forbidden}"


def test_target_is_immutable():
    with pytest.raises(Exception):
        resolve_target(**OK).catalog = "gold"


def test_blank_strings_rejected_like_missing():
    with pytest.raises(MissingTarget):
        resolve_target(**{**OK, "workspace": "   "})


@pytest.mark.parametrize("short,region", [
    ("iad", "us-ashburn-1"), ("phx", "us-phoenix-1"), ("fra", "eu-frankfurt-1"),
])
def test_region_derived_from_ocid(short, region):
    assert region_from_ocid(f"ocid1.aidataplatform.oc1.{short}.aaaa") == region


def test_unmapped_region_code_errors_rather_than_defaulting():
    with pytest.raises(ValueError, match="zzz"):
        region_from_ocid("ocid1.aidataplatform.oc1.zzz.aaaa")


def test_malformed_ocid_errors():
    with pytest.raises(ValueError):
        region_from_ocid("not-an-ocid")
