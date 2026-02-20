#!/usr/bin/env python3
"""Parse report-style XLSX exported from ECAD/UBEM-like systems.

These files have a header block (TITULAR etc.) then a table header row.
We detect the table header row and export normalized CSV.

Usage:
  python3 scripts/parse_estelita_reports.py --in <xlsx> --out <csv>
"""

import argparse
from pathlib import Path
import pandas as pd


def find_header_row(df: pd.DataFrame, min_nonnull=3, max_scan=120):
    for i in range(min(max_scan, len(df))):
        row = df.iloc[i]
        nonnull = row.notna().sum()
        if nonnull < min_nonnull:
            continue
        # require at least one reasonable text cell
        for v in row.tolist():
            s = str(v)
            if any(ch.isalpha() for ch in s) and len(s) <= 80:
                return i
    return None


def parse_report_xlsx(path: Path, nrows_scan=2500):
    xl = pd.ExcelFile(path)
    sh = xl.sheet_names[0]
    raw = xl.parse(sh, header=None, nrows=nrows_scan)
    hdr = find_header_row(raw)
    if hdr is None:
        raise RuntimeError(f"Could not detect header row in {path}")

    header = raw.iloc[hdr].astype(str).tolist()
    cols = []
    for j, h in enumerate(header):
        h = (h or '').strip()
        if h == '' or h.lower() == 'nan':
            h = f'col{j}'
        cols.append(h)

    df = raw.iloc[hdr + 1:].copy()
    df.columns = cols
    # drop fully empty rows
    df = df.dropna(how='all')
    return sh, hdr, df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in', dest='inp', required=True)
    ap.add_argument('--out', dest='out', required=True)
    args = ap.parse_args()

    inp = Path(args.inp)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    sh, hdr, df = parse_report_xlsx(inp)
    df.to_csv(out, index=False)
    print(f"parsed {inp.name}: sheet={sh} header_row={hdr} rows={len(df)} cols={len(df.columns)} -> {out}")


if __name__ == '__main__':
    main()
