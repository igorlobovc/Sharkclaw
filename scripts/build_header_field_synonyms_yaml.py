#!/usr/bin/env python3
"""Populate config/header_field_synonyms.yaml from header_synonym_inventory.csv.

For each field, include the top N header_norm values by count.
Also merges a small set of critical headers.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


CRITICAL = {
    "title": [
        "autores da musica",
        "titulares",
        "repertorio",
        "repertório",
        "obra",
        "musica",
        "música",
        "obra musica",
        "exibicao",
        "exibição",
        "tocou",
    ],
    "author": ["autores da musica", "autores", "autor", "compositor", "compositores"],
    "rightsholder_owner": ["titulares", "titular", "direitos", "editora", "editoras", "publisher", "owner"],
}


def norm_list(items: list[str]) -> list[str]:
    # basic normalize similar to field_detection.norm_header (avoid importing)
    import re, unicodedata

    def strip_accents(s: str) -> str:
        s = unicodedata.normalize("NFKD", s)
        return "".join(ch for ch in s if not unicodedata.combining(ch))

    def norm(s: str) -> str:
        s = strip_accents(str(s).casefold())
        s = re.sub(r"[^0-9a-z]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    out = []
    seen = set()
    for x in items:
        nx = norm(x)
        if not nx or nx in seen:
            continue
        seen.add(nx)
        out.append(nx)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--top-n", type=int, default=50)
    args = ap.parse_args()

    inv = pd.read_csv(args.inventory, dtype=str, low_memory=False).fillna("")
    inv["count"] = pd.to_numeric(inv.get("count", 0), errors="coerce").fillna(0).astype(int)

    fields = [
        "title",
        "artist",
        "author",
        "date",
        "program",
        "network",
        "episode",
        "isrc",
        "iswc",
        "amount_value",
        "currency",
        "percent_split",
        "rightsholder_owner",
        "publisher_editor",
        "work_id",
        "recording_id",
    ]

    lines = ["# Auto-generated from header_synonym_inventory.csv\n"]
    for f in fields:
        sub = inv[inv["field"] == f].copy()
        headers = sub.sort_values("count", ascending=False)["header_norm"].head(args.top_n).tolist()
        headers = norm_list(headers + CRITICAL.get(f, []))
        lines.append(f"{f}:")
        for h in headers:
            lines.append(f"  - {h}")
        lines.append("")

    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote: {out}")


if __name__ == "__main__":
    main()
