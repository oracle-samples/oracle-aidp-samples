"""Topological wave computation. Pure."""
from plan.waves import compute_waves


def e(dependent, dependency):
    return {"from": dependent, "to": dependency}


def test_no_edges_means_one_wave():
    out = compute_waves(["A", "B"], [])
    assert out["waves"] == [["A", "B"]]
    assert out["cycles"] == []


def test_dependency_lands_before_its_dependent():
    out = compute_waves(["V", "T"], [e("V", "T")])
    assert out["waves"] == [["T"], ["V"]]


def test_three_level_chain():
    out = compute_waves(["A", "B", "C"], [e("C", "B"), e("B", "A")])
    assert out["waves"] == [["A"], ["B"], ["C"]]


def test_diamond_collapses_to_three_waves():
    out = compute_waves(["TOP", "L", "R", "BASE"],
                        [e("TOP", "L"), e("TOP", "R"), e("L", "BASE"), e("R", "BASE")])
    assert out["waves"] == [["BASE"], ["L", "R"], ["TOP"]]


def test_within_a_wave_nodes_are_sorted_deterministically():
    out = compute_waves(["Z", "A", "M"], [])
    assert out["waves"] == [["A", "M", "Z"]]


def test_custom_sort_key_orders_within_a_wave():
    sizes = {"A": 30, "B": 10, "C": 20}
    out = compute_waves(["A", "B", "C"], [], sort_key=lambda n: (sizes[n], n))
    assert out["waves"] == [["B", "C", "A"]], "smallest first"


def test_two_node_cycle_reported_not_waved():
    out = compute_waves(["A", "B"], [e("A", "B"), e("B", "A")])
    assert out["waves"] == []
    assert sorted(out["cycles"][0]) == ["A", "B"]


def test_cycle_isolated_and_the_rest_still_planned():
    out = compute_waves(["A", "B", "OK"], [e("A", "B"), e("B", "A")])
    assert out["waves"] == [["OK"]]
    assert sorted(out["cycles"][0]) == ["A", "B"]


def test_self_edge_is_ignored_not_a_cycle():
    out = compute_waves(["A"], [e("A", "A")])
    assert out["waves"] == [["A"]]
    assert out["cycles"] == []


def test_edges_to_unknown_nodes_are_ignored():
    out = compute_waves(["A"], [e("A", "GHOST")])
    assert out["waves"] == [["A"]]
