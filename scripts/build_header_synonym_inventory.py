#!/usr/bin/env python3
"""Build a ranked inventory of header synonyms from the known-good template set.

Input:
- known_good_template_set.csv

Output:
- header_synonym_inventory.csv with columns:
  field, header_norm, raw_header, provider_guess, count

We map headers to target fields using regex heuristics (Portuguese variants).
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


FIELD_RX = {
    "title": re.compile(r"\b(obra|titulo|t[íi]tulo|musica|m[úu]sica|faixa|track|repertorio|repert[óo]rio)\b", re.I),
    "artist": re.compile(r"\b(artista|interprete|int[ée]rprete|banda)\b", re.I),
    "author": re.compile(r"\b(autor|autores|compositor|compositores|composer)\b", re.I),
    "date_time": re.compile(r"\b(data|hora|exib|exibi|timestamp|time)\b", re.I),
    "program": re.compile(r"\b(programa|show|episodio|epis[óo]dio|capitulo|cap[íi]tulo)\b", re.I),
    "network": re.compile(r"\b(canal|emissora|network|station)\b", re.I),
    "isrc": re.compile(r"\bisrc\b", re.I),
    "iswc": re.compile(r"\biswc\b", re.I),
    "amount_value": re.compile(r"\b(valor|amount|value|preco|pre[çc]o|pagamento)\b", re.I),
    "currency": re.compile(r"\b(moeda|currency|brl|usd|eur)\b", re.I),
    "percent_split": re.compile(r"\b(percent|%|split|particip)\b", re.I),
    "rightsholder_owner": re.compile(r"\b(titular|titulares|direitos|owner|propriet|sociedade|editora|editoras|publisher)\b", re.I),
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--templates", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.templates, dtype=str, low_memory=False).fillna("")

    rows = []
    for _, r in df.iterrows():
        prov = r.get("provider_guess", "")
        raw = str(r.get("example_header_row", ""))
        norm = str(r.get("col_headers_normalized", ""))
        raw_parts = raw.split("|") if raw else []
        norm_parts = norm.split("|") if norm else []

        for raw_h, norm_h in zip(raw_parts, norm_parts):
            if not norm_h:
                continue
            field = "other"
            for k, rx in FIELD_RX.items():
                if rx.search(norm_h):
                    field = k
                    break
            rows.append(
                {
                    "field": field,
                    "header_norm": norm_h,
                    "raw_header": raw_h,
                    "provider_guess": prov,
                }
            )

    out_df = pd.DataFrame(rows)
    inv = (
        out_df.groupby(["field", "header_norm", "raw_header", "provider_guess"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values(["field", "count"], ascending=[True, False])
    )

    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    inv.to_csv(out, index=False)
    print(f"Wrote: {out} rows={len(inv)}")


if __name__ == "__main__":
    main()
