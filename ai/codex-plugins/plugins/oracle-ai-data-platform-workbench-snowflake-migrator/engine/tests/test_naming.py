"""AIDP display-name translation: conservative, deterministic, attributable."""
import pytest

from target.naming import UnusableName, translate_name


def test_a_clean_name_passes_through_unchanged():
    t = translate_name("sales_workspace")
    assert t.name == "sales_workspace"
    assert t.changed is False
    assert t.notes == []


def test_spaces_dots_and_hyphens_become_single_underscores():
    t = translate_name("Acme - PROD.Data Platform")
    assert t.name == "acme_prod_data_platform"
    assert t.changed


def test_accents_are_folded_not_dropped_silently():
    t = translate_name("Café Résumé Zoë")
    assert t.name == "cafe_resume_zoe"
    assert any("ASCII" in n for n in t.notes)


def test_a_leading_digit_gets_a_kind_prefix():
    t = translate_name("360-analytics", kind="workspace")
    assert t.name == "w_360_analytics"
    assert t.name[0].isalpha()


def test_every_change_is_recorded_in_notes():
    t = translate_name("WH ÉTL")
    assert t.changed
    assert t.notes, "a silent rename is not attributable"


def test_truncation_is_collision_safe():
    a = translate_name("x" * 100 + "a", max_length=30)
    b = translate_name("x" * 100 + "b", max_length=30)
    assert len(a.name) <= 30 and len(b.name) <= 30
    assert a.name != b.name, \
        "two long names sharing a prefix must not fold into one"


def test_truncation_is_deterministic():
    assert (translate_name("y" * 90, max_length=30).name
            == translate_name("y" * 90, max_length=30).name)


def test_an_unusable_name_raises_rather_than_inventing_one():
    with pytest.raises(UnusableName):
        translate_name("!!! ***")


def test_the_output_is_always_api_safe():
    import re
    for ugly in ("A.b.C", "  padded  ", "emoji 🚀 name", "UPPER", "a--b__c"):
        t = translate_name(ugly)
        assert re.fullmatch(r"[a-z][a-z0-9_]*", t.name), (ugly, t.name)
