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

from scripts.field_detection import load_synonyms_yaml, norm_header, resolve_fields


def header_quality_score(headers: list[str], syn: dict[str, list[str]]) -> int:
    """Score a header row; higher is better.

    Heuristics:
    - Penalize Unnamed / numeric-like headers
    - Reward presence of synonym matches or common playlog tokens
    """

    syn_flat = set()
    for v in syn.values():
        for s in v:
            syn_flat.add(norm_header(s))

    score = 0
    for h in headers:
        hn = norm_header(h)
        if not hn:
            continue
        if hn.startswith("unnamed"):
            score -= 5
            continue
        if hn.replace(" ", "").isdigit():
            score -= 2
        score += 1
        # reward exact synonym match
        if hn in syn_flat:
            score += 6
        # reward common header tokens
        toks = set(hn.split())
        if toks & {"obra", "musica", "titulo", "repertorio", "isrc", "autor", "autores", "compositor", "interprete", "artista", "programa", "data", "canal"}:
            score += 3

    return score


def should_detect_header_row(headers: list[str], *, has_title: bool, has_people: bool) -> bool:
    if not (has_title and has_people):
        return True
    if not headers:
        return True
    unnamed = sum(1 for h in headers if norm_header(h).startswith("unnamed"))
    if unnamed / max(1, len(headers)) >= 0.6:
        return True
    return False


def detect_best_header_row(xl: pd.ExcelFile, sheet: str, syn: dict[str, list[str]], *, scan_rows: int = 120) -> tuple[int | None, int]:
    """Scan first scan_rows with header=None and pick the best row to use as header."""

    try:
        df0 = xl.parse(sheet, header=None, dtype=str, nrows=scan_rows).fillna("")
    except Exception:
        return None, 0

    best_idx: int | None = None
    best_score = 0

    for i in range(len(df0)):
        row = [str(x) for x in df0.iloc[i].tolist()]
        sc = header_quality_score(row, syn)
        if sc > best_score:
            best_score = sc
            best_idx = int(i)

    return best_idx, int(best_score)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--templates", required=True)
    ap.add_argument("--synonyms", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-summary", required=True)
    ap.add_argument("--out-entity", required=True)
    ap.add_argument("--max-rows", type=int, default=20000)
    ap.add_argument("--header-scan-rows", type=int, default=120)
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

        header_mode_used = "default0"
        detected_header_row_index: int | None = None
        detected_header_quality_score = ""
        raw_headers_used: list[str] = []
        normalized_headers_used: list[str] = []

        try:
            xl = pd.ExcelFile(fp)
            # first try default header=0
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
                    "header_mode_used": header_mode_used,
                    "detected_header_row_index": "",
                    "detected_header_quality_score": "",
                    "raw_headers_used": "",
                    "normalized_headers_used": "",
                    "notes": f"parse_error:{type(e).__name__}",
                }
            )
            continue

        headers0 = [str(c) for c in df.columns]
        res0 = resolve_fields(headers0, syn)

        title_cols0 = [c.column for c in res0.get("title", [])[:3]]
        artist_cols0 = [c.column for c in res0.get("artist", [])[:3]]
        author_cols0 = [c.column for c in res0.get("author", [])[:3]]
        has_title0 = 1 if title_cols0 else 0
        has_people0 = 1 if (artist_cols0 or author_cols0) else 0

        # If default headers look wrong or mapping fails, detect better header row
        if should_detect_header_row(headers0, has_title=bool(has_title0), has_people=bool(has_people0)):
            best_idx, best_sc = detect_best_header_row(xl, sh, syn, scan_rows=args.header_scan_rows)
            if best_idx is not None and best_sc > header_quality_score(headers0, syn):
                try:
                    df2 = xl.parse(sh, dtype=str, header=best_idx, nrows=args.max_rows).fillna("")
                    df = df2
                    headers = [str(c) for c in df2.columns]
                    res = resolve_fields(headers, syn)
                    header_mode_used = "detected_row"
                    detected_header_row_index = int(best_idx)
                    detected_header_quality_score = str(int(best_sc))
                    raw_headers_used = headers
                    normalized_headers_used = [norm_header(h) for h in headers]
                except Exception:
                    headers = headers0
                    res = res0
                    raw_headers_used = headers0
                    normalized_headers_used = [norm_header(h) for h in headers0]
            else:
                headers = headers0
                res = res0
                raw_headers_used = headers0
                normalized_headers_used = [norm_header(h) for h in headers0]
        else:
            headers = headers0
            res = res0
            raw_headers_used = headers0
            normalized_headers_used = [norm_header(h) for h in headers0]

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
                "header_mode_used": header_mode_used,
                "detected_header_row_index": str(detected_header_row_index) if detected_header_row_index is not None else "",
                "detected_header_quality_score": detected_header_quality_score,
                "raw_headers_used": "|".join(raw_headers_used[:60]),
                "normalized_headers_used": "|".join(normalized_headers_used[:60]),
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
