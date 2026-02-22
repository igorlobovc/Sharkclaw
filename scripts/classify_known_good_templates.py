#!/usr/bin/env python3
"""Classify known-good templates into expected playlogs vs non-playlog sheets.

Guardrails: audit-only. No ZIP, no scoring/sweep/threshold/entity-promotion changes.

Reads:
- ~/Desktop/TempClaw/_canon2_20260221_1925/report_package/known_good_template_set.csv

Writes (into same report_package):
- known_good_templates_classified.csv
- known_good_templates_expected_playlog.csv
- known_good_templates_non_playlog.csv
- known_good_templates_classified_summary.txt

Classifier (cheap + deterministic OR logic):
Y if ANY:
1) sheet_name contains playlog-ish tokens
2) headers contain ANY title signal AND ANY contributor signal
N otherwise

Note: template CSV already includes normalized headers in col_headers_normalized
(delimited by '|').
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


SHEET_PLAYLOG_RX = re.compile(
    r"\b("
    r"relatorio|ubem|repertorio|exibicao|tocou|"
    r"mcs|cue|cuesheet|music\s*cue\s*sheet|sincronizacao|"
    r"programacao|playlist|log"
    r")\b"
)

TITLE_SIGNAL_RX = re.compile(r"\b(obra|musica|titulo|repertorio|faixa|track|isrc)\b")
CONTRIB_SIGNAL_RX = re.compile(
    r"\b(artista|interprete|autor|compositor|autores\s+da\s+musica|compositores|titular|titulares)\b"
)


def _norm(s: str) -> str:
    import unicodedata

    s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.casefold()


def classify(sheet_name: str, _file_path: str, col_headers_norm: str) -> tuple[str, str]:
    sh = _norm(sheet_name)
    cols = _norm(col_headers_norm)

    # Rule 1 (spec): sheet_name contains playlog-ish tokens
    if SHEET_PLAYLOG_RX.search(sh):
        return "Y", "sheet_token"

    # normalized headers are pipe-delimited; treat as one blob
    if TITLE_SIGNAL_RX.search(cols) and CONTRIB_SIGNAL_RX.search(cols):
        return "Y", "header_title+contrib"

    return "N", "no_signal"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--in",
        dest="in_path",
        default=str(Path("~/Desktop/TempClaw/_canon2_20260221_1925/report_package/known_good_template_set.csv").expanduser()),
    )
    ap.add_argument(
        "--out-dir",
        default=str(Path("~/Desktop/TempClaw/_canon2_20260221_1925/report_package").expanduser()),
    )
    args = ap.parse_args()

    inp = Path(args.in_path).expanduser()
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(inp, dtype=str, low_memory=False).fillna("")

    expected = []
    reason = []
    for _, r in df.iterrows():
        y, why = classify(
            r.get("sheet_name", ""),
            r.get("file_path", ""),
            r.get("col_headers_normalized", ""),
        )
        expected.append(y)
        reason.append(why)

    out = df.copy()
    out["expected_playlog"] = expected
    out["playlog_reason"] = reason

    classified_path = out_dir / "known_good_templates_classified.csv"
    out.to_csv(classified_path, index=False)

    df_y = out[out["expected_playlog"] == "Y"].copy()
    df_n = out[out["expected_playlog"] != "Y"].copy()

    y_path = out_dir / "known_good_templates_expected_playlog.csv"
    n_path = out_dir / "known_good_templates_non_playlog.csv"
    df_y.to_csv(y_path, index=False)
    df_n.to_csv(n_path, index=False)

    # summary
    lines = []
    lines.append(f"templates_total={len(out)}")
    lines.append(f"expected_playlog=Y count={len(df_y)}")
    lines.append(f"expected_playlog=N count={len(df_n)}")
    lines.append("")
    lines.append("Reasons (expected_playlog=Y):")
    for k, v in df_y["playlog_reason"].value_counts().to_dict().items():
        lines.append(f"  {k}: {v}")
    lines.append("")
    lines.append("Reasons (expected_playlog=N):")
    for k, v in df_n["playlog_reason"].value_counts().to_dict().items():
        lines.append(f"  {k}: {v}")

    summary_path = out_dir / "known_good_templates_classified_summary.txt"
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote: {classified_path}")
    print(f"Wrote: {y_path}")
    print(f"Wrote: {n_path}")
    print(f"Wrote: {summary_path}")


if __name__ == "__main__":
    main()
