from pathlib import Path

import pandas as pd


def test_expected_wins_fixture_is_present_and_nonempty():
    p = Path("tests/fixtures/expected_wins_gold_silver.csv")
    assert p.exists(), "expected wins fixture missing"

    df = pd.read_csv(p, dtype=str).fillna("")
    assert len(df) > 0, "expected wins fixture should not be empty"

    # must contain at least one reference identifier or a matched title
    has_any_anchor = (
        df.get("ref_isrc", "").astype(str).str.strip().ne("")
        | df.get("ref_iswc", "").astype(str).str.strip().ne("")
        | df.get("matched_title", "").astype(str).str.strip().ne("")
    )
    assert bool(has_any_anchor.any()), "expected wins fixture should include anchors"


def test_expected_wins_min_count_is_recorded():
    p = Path("tests/fixtures/expected_wins_min_count.txt")
    assert p.exists(), "expected wins min count file missing"
    n = int(p.read_text(encoding="utf-8").strip())
    assert n >= 92
