"""Tests for rule quality (E3) and temporal sensitivity (E4) -- no Spark needed."""

from datetime import datetime

import pytest

from src.evaluation.quality import (
    Event,
    amie_rule_key,
    bracketing_windows,
    evaluate_events,
    load_reference_rules,
    precision_recall_f1,
    rule_key,
)

T = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
W = "http://ex.org/worksAt"
C0, O0 = "http://ex.org/Class0", "http://ex.org/Org0"


def test_rule_key_is_order_independent():
    assert rule_key(["b", "a"], ["c"]) == rule_key(["a", "b"], ["c"]) == "a,b => c"


def test_amie_rule_key_matches_tsarm_key():
    amie = f"?a  <{T}>  <{C0}>   => ?a  <{W}>  <{O0}>"
    assert amie_rule_key(amie) == rule_key([f"{T}={C0}"], [f"{W}={O0}"])
    # two body atoms, same variable
    two = f"?a <{T}> <{C0}>  ?a <{W}> <http://ex.org/Org1>  => ?a <{W}> <{O0}>"
    assert amie_rule_key(two) == rule_key([f"{T}={C0}", f"{W}=http://ex.org/Org1"], [f"{W}={O0}"])


@pytest.mark.parametrize(
    "rule",
    [
        "?b  <http://p>  ?a   => ?a  <http://q>  ?b",  # variables, no constants
        "?a <http://p> <http://x>  ?b <http://q> <http://y> => ?a <http://r> <http://z>",  # 2 subjects
        "no arrow here",
    ],
)
def test_amie_rules_not_comparable(rule):
    assert amie_rule_key(rule) is None


def test_precision_recall_f1():
    q = precision_recall_f1(["r1", "r2", "r3", "r4"], ["r1", "r2", "r5"])
    assert (q.n_mined, q.n_true, q.n_correct) == (4, 3, 2)
    assert q.precision == pytest.approx(0.5)
    assert q.recall == pytest.approx(2 / 3)
    assert q.f1 == pytest.approx(2 * 0.5 * (2 / 3) / (0.5 + 2 / 3))
    empty = precision_recall_f1([], ["r1"])
    assert (empty.precision, empty.recall, empty.f1) == (0.0, 0.0, 0.0)


def test_load_reference_rules(tmp_path):
    p = tmp_path / "ref.csv"
    p.write_text(
        "# validated by hand, 2026-11\n"
        "antecedent,consequent\n"
        "b;a,c\n"
        "x,y\n"
        ",missing\n"
    )
    assert load_reference_rules(p) == {"a,b => c", "x => y"}


# --- E4 ---------------------------------------------------------------------

YEARS = {w: (datetime(2020 + w, 1, 1), datetime(2020 + w, 1, 1)) for w in range(4)}


def test_bracketing_windows():
    assert bracketing_windows(YEARS, datetime(2022, 1, 1)) == (1, 2)
    assert bracketing_windows(YEARS, datetime(2021, 6, 1)) == (1, 2)
    assert bracketing_windows(YEARS, datetime(2019, 1, 1)) == (None, 0)
    # overlapping width-2 windows: w0 = 2020-2021, w1 = 2021-2022, w2 = 2022-2023
    wide = {0: (datetime(2020, 1, 1), datetime(2021, 1, 1)),
            1: (datetime(2021, 1, 1), datetime(2022, 1, 1)),
            2: (datetime(2022, 1, 1), datetime(2023, 1, 1))}
    assert bracketing_windows(wide, datetime(2021, 6, 1)) == (0, 2)


def test_temporal_sensitivity():
    traj = {
        "decaying": {0: 1.0, 1: 1.0, 2: 0.2, 3: 0.2},  # collapses at 2022
        "emerging": {0: 0.1, 1: 0.1, 2: 0.6, 3: 0.9},  # appears at 2022
        "flat": {0: 0.8, 1: 0.8, 2: 0.79, 3: 0.8},
    }
    events = [
        Event("decaying", datetime(2022, 1, 1), "decay"),
        Event("emerging", datetime(2022, 1, 1), "emergence"),
        Event("flat", datetime(2022, 1, 1), "decay"),  # change -0.01 < eps: missed
        Event("unknown", datetime(2022, 1, 1), "decay"),  # not mined: missed
        Event("decaying", datetime(2030, 1, 1), "decay"),  # outside windows: missed
    ]
    ts, outcomes = evaluate_events(events, traj, YEARS, epsilon=0.05)
    assert [o.detected for o in outcomes] == [True, True, False, False, False]
    assert ts == pytest.approx(2 / 5)
    assert outcomes[0].window_before == 1 and outcomes[0].window_after == 2
    assert "1.000 -> 0.200" in outcomes[0].reason
    assert outcomes[3].reason == "rule not among mined candidates"


def test_temporal_sensitivity_rejects_bad_input():
    with pytest.raises(ValueError):
        evaluate_events([], {}, YEARS)
    with pytest.raises(ValueError):
        evaluate_events([Event("r", datetime(2022, 1, 1), "growth")], {}, YEARS)
