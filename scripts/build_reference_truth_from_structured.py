#!/usr/bin/env python3
"""Build a local-only Reference_Truth.csv from structured sources (CSV/XLSX) first.

Inputs (local paths):
- Processed/Eligible_Catalog.csv (already distilled; includes isrc and basis)
- Processed/Curated_Titles_Authors.csv (titles + author codes/names)
- Processed/Supplier_Matches_Sure.csv (curated sure matches; seed tokens)
- Processed/Eligible_Artists.txt (big list of artist/owner tokens)

Outputs (local-only):
- runs/reference/reference_truth_structured.csv
- runs/reference/reference_truth_structured_summary.json

We deliberately avoid committing data; only scripts/docs go to git.
"""

import json
import re
from pathlib import Path
from collections import Counter

import pandas as pd


def norm(s: str) -> str:
    s = str(s or '').strip().lower()
    s = re.sub(r"\s+", " ", s)
    import unicodedata
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    return s


def load_eligible_artists(path: Path, top=5000):
    c = Counter()
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            t=line.strip()
            if not t or t.startswith('*'):
                continue
            c[norm(t)] += 1
    # keep non-empty
    toks=[t for t,_ in c.most_common(top) if t and t not in {'nan','none','null'}]
    return toks


def main():
    base = Path('/Users/igorcunha/Desktop/Estelita_backup/Estelita/Processed')
    out_dir = Path('runs/reference')
    out_dir.mkdir(parents=True, exist_ok=True)

    eligible = pd.read_csv(base/'Eligible_Catalog.csv')
    curated = pd.read_csv(base/'Curated_Titles_Authors.csv')
    sure = pd.read_csv(base/'Supplier_Matches_Sure.csv')

    # Normalize
    if 'title' in eligible.columns:
        eligible['title_norm'] = eligible['title'].map(norm)
    if 'artist' in eligible.columns:
        eligible['artist_norm'] = eligible['artist'].astype(str).map(norm)

    curated['title_norm'] = curated['title'].map(norm)
    curated['author_norm'] = curated['author'].astype(str).map(norm)

    # seed high-stakes tokens from sure matches
    tokens = set()
    if 'artist' in sure.columns:
        for a in sure['artist'].dropna().astype(str).tolist():
            na = norm(a)
            if na and na not in {'nan','none','null'}:
                tokens.add(na)

    # add eligible artists list
    artists_path = base/'Eligible_Artists.txt'
    if artists_path.exists():
        for t in load_eligible_artists(artists_path, top=8000):
            tokens.add(t)

    # Build truth table: join eligible with curated titles
    truth = eligible.merge(curated[['title','author','title_norm','author_norm']].drop_duplicates('title_norm'),
                           on='title_norm', how='left', suffixes=('','_cur'))

    truth.rename(columns={'title':'eligible_title','artist':'eligible_artist'}, inplace=True)

    truth['has_curated_title'] = truth['title'].notna() if 'title' in truth.columns else truth['title_norm'].isin(curated['title_norm'])

    # persist
    out_csv = out_dir/'reference_truth_structured.csv'
    truth.to_csv(out_csv, index=False)

    summary = {
        'rows': int(len(truth)),
        'unique_titles': int(truth['title_norm'].nunique()),
        'unique_isrc': int(truth['isrc'].nunique()) if 'isrc' in truth.columns else None,
        'curated_join_nonnull_pct': float(truth['author'].notna().mean()) if 'author' in truth.columns else None,
        'token_count': int(len(tokens)),
        'sample_tokens': sorted(list(tokens))[:50],
    }
    (out_dir/'reference_truth_structured_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
