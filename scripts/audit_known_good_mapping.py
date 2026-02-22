#!/usr/bin/env python3
"""Audit known-good templates against field resolver.

Inputs:
- known_good_template_set.csv
- config/header_field_synonyms.yaml

Outputs:
- known_good_mapping_audit.csv
- known_good_mapping_audit_summary.txt
- known_good_top_entity_hits.csv (entity detection using detected columns)

This is audit-only.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from scripts.field_detection import load_synonyms_yaml, resolve_fields


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--templates", required=True)
    ap.add_argument("--synonyms", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-summary", required=True)
    ap.add_argument("--out-entity", required=True)
    ap.add_argument("--max-rows", type=int, default=20000)
    args = ap.parse_args()

    tmpl = pd.read_csv(args.templates, dtype=str, low_memory=False).fillna("")
    syn = load_synonyms_yaml(Path(args.synonyms))

    audit_rows = []
    fails = []

    # entity hits (very small: use top entities list)
    from scripts.audit_top_entities_frequency import default_entities, entity_match_in_text

    entities = default_entities()
    ent_hits = Counter()
    ent_files = defaultdict(set)
    ent_fields = defaultdict(Counter)

    for _, r in tmpl.iterrows():
        prov = r.get("provider_guess", "")
        fp = Path(r.get("file_path", ""))
        sh = r.get("sheet_name", "")
        if not fp.exists():
            continue

        try:
            xl = pd.ExcelFile(fp)
            df = xl.parse(sh, dtype=str, nrows=args.max_rows).fillna("")
        except Exception as e:
            audit_rows.append(
                {
                    "provider_guess": prov,
                    "file_path": str(fp),
                    "sheet": sh,
                    "has_title": 0,
                    "has_artist": 0,
                    "has_author": 0,
                    "detected_title_cols": "",
                    "detected_artist_cols": "",
                    "detected_author_cols": "",
                    "notes": f"parse_error:{type(e).__name__}",
                }
            )
            continue

        headers = [str(c) for c in df.columns]
        res = resolve_fields(headers, syn)

        title_cols = [c.column for c in res.get("title", [])[:3]]
        artist_cols = [c.column for c in res.get("artist", [])[:3]]
        author_cols = [c.column for c in res.get("author", [])[:3]]

        has_title = 1 if title_cols else 0
        has_artist = 1 if artist_cols else 0
        has_author = 1 if author_cols else 0

        notes = ""
        if not has_title:
            notes = "missing_title_detect"
        if not (has_artist or has_author):
            notes = (notes + ";" if notes else "") + "missing_artist_author_detect"

        audit_rows.append(
            {
                "provider_guess": prov,
                "file_path": str(fp),
                "sheet": sh,
                "has_title": has_title,
                "has_artist": has_artist,
                "has_author": has_author,
                "detected_title_cols": "|".join(title_cols),
                "detected_artist_cols": "|".join(artist_cols),
                "detected_author_cols": "|".join(author_cols),
                "notes": notes,
            }
        )

        if notes:
            fails.append((prov, fp.name, sh, notes))

        # entity scan using detected cols only
        scan_cols = list(dict.fromkeys((artist_cols + author_cols)))
        if scan_cols:
            for ent in entities:
                hit_cols = []
                for c in scan_cols:
                    try:
                        ser = df[c].astype(str)
                    except Exception:
                        continue
                    for v in ser.tolist():
                        if not str(v).strip():
                            continue
                        if entity_match_in_text(ent, v):
                            ent_hits[ent.entity_norm] += 1
                            ent_files[ent.entity_norm].add(str(fp))
                            ent_fields[ent.entity_norm][c] += 1
                            hit_cols.append(c)
                            break

    out_csv = Path(args.out_csv).expanduser()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(audit_rows).to_csv(out_csv, index=False)

    # summary
    df_a = pd.DataFrame(audit_rows)
    total = len(df_a)
    good = int(((df_a["has_title"] == 1) & ((df_a["has_artist"] == 1) | (df_a["has_author"] == 1))).sum())
    pct = (good / total) if total else 0

    lines = []
    lines.append(f"templates_total={total}")
    lines.append(f"templates_title_and_artist_or_author={good}")
    lines.append(f"pct={pct:.3f}")
    lines.append("")
    lines.append("Top failing templates (first 20):")
    for x in fails[:20]:
        lines.append(str(x))

    out_sum = Path(args.out_summary).expanduser()
    out_sum.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # entity hits output
    ent_rows = []
    for ent, cnt in ent_hits.most_common():
        files = sorted(ent_files.get(ent, set()))
        ent_rows.append(
            {
                "entity_norm": ent,
                "hit_count": int(cnt),
                "files_hit_count": len(files),
                "hit_field_breakdown": ",".join(f"{k}:{v}" for k, v in ent_fields[ent].most_common(10)),
                "top_files": ";".join([Path(f).name for f in files[:10]]),
            }
        )

    out_ent = Path(args.out_entity).expanduser()
    pd.DataFrame(ent_rows).to_csv(out_ent, index=False)

    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_sum}")
    print(f"Wrote: {out_ent}")


if __name__ == "__main__":
    main()
