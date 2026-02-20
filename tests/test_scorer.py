import json
from pathlib import Path

import pandas as pd

from scripts.score_rows import build_ref_index, load_config, score_one


def test_fixtures():
    root = Path(__file__).resolve().parents[1]
    cfg = load_config(root / "config" / "scoring_config.json")

    # Minimal reference truth stub for deterministic tests
    ref = pd.DataFrame(
        [
            {"title_norm": "eleanor rigby", "isrc": "", "iswc": "", "evidence_tokens": "john lennon;paul mccartney"},
            {"title_norm": "deja vu", "isrc": "", "iswc": "", "evidence_tokens": ""},
            {"title_norm": "macetando", "isrc": "", "iswc": "", "evidence_tokens": ""},
            {"title_norm": "diana", "isrc": "", "iswc": "", "evidence_tokens": ""},
            {"title_norm": "beijinho no ombro", "isrc": "", "iswc": "", "evidence_tokens": ""},
            {"title_norm": "me chama", "isrc": "", "iswc": "", "evidence_tokens": ""},
            {"title_norm": "vida loka", "isrc": "", "iswc": "", "evidence_tokens": ""},
        ]
    )
    ref_idx = build_ref_index(ref)

    fixtures = json.loads((root / "tests" / "fixtures.json").read_text(encoding="utf-8"))

    for fx in fixtures:
        res = score_one(fx["row"], ref_idx, cfg)
        assert (
            res.tier == fx["expected_tier"]
        ), f"{fx['name']} expected {fx['expected_tier']} got {res.tier} flags={res.evidence_flags}"
