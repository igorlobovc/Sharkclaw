#!/usr/bin/env python3
"""Parse FONOGRAMAS PDF text export into track + participant tables.

Pipeline:
- Use pdftotext to extract text
- Detect ISRC blocks (e.g., BR-TVW-13-00013)
- Skip report headers/legends
- Associate participant rows until next ISRC

This is a heuristic state machine; we refine using samples.
"""

import re
import argparse
from pathlib import Path
import pandas as pd

ISRC_RE = re.compile(r"\b[A-Z]{2}-[A-Z0-9]{3}-\d{2}-\d{5}\b")
PCT_RE = re.compile(r"\b\d{1,3}[.,]\d{4}\b")  # e.g., 10,0000

HEADER_NOISE = [
    'RELATÓRIO ANALÍTICO',
    'ASSOCIAÇÃO:',
    'TITULAR:',
    'CATEGORIA:',
    'SUBCATEGORIA: TODAS',
    'CÓD. ECAD',
    'ISRC/GRA',
    'SITUACAO',
    'TÍTULO PRINCIPAL',
    'PSEUDÔNIMO',
    'PART. (%)',
]


def is_noise(line: str) -> bool:
    u = line.strip().upper()
    if not u:
        return True
    return any(tok in u for tok in HEADER_NOISE)


def parse_lines(lines):
    tracks=[]
    parts=[]
    cur=None

    def flush():
        nonlocal cur
        if cur:
            tracks.append(cur)
            cur=None

    i=0
    while i < len(lines):
        line=lines[i].strip()
        if is_noise(line):
            i+=1
            continue

        m=ISRC_RE.search(line)
        if m:
            # new track
            flush()
            isrc=m.group(0)
            # lookback a few lines for possible ecad work code + producer label
            lookback=lines[max(0,i-5):i]
            nums=[x.strip() for x in lookback if x.strip().isdigit()]
            work_ecad=nums[-1] if nums else None
            producer=None
            for x in reversed(lookback):
                if x.strip() and not x.strip().isdigit() and len(x.strip())<=40:
                    producer=x.strip()
                    break
            # lookahead window for situacao + title
            la=lines[i:i+20]
            situ=None
            title=None
            # find LIBERADO or similar
            for j,x in enumerate(la):
                if x.strip().upper() in {'LIBERADO','BLOQUEADO','SUSPENSO'}:
                    situ=x.strip().upper();
                    # title is likely next non-empty non-noise token after status
                    for y in la[j+1:]:
                        yy=y.strip()
                        if not yy or is_noise(yy):
                            continue
                        if ISRC_RE.search(yy):
                            break
                        # skip pure numbers
                        if yy.isdigit():
                            continue
                        title=yy
                        break
                    break
            cur={
                'isrc': isrc,
                'work_ecad_code': work_ecad,
                'producer_label': producer,
                'situacao': situ,
                'title': title,
                'start_line': i+1,
            }
            i+=1
            continue

        # participant rows: heuristic = number + name + optional pseudo + optional pct
        if cur:
            if line.isdigit():
                # potential participant id
                pid=line
                # next tokens for names
                name=None
                pseudo=None
                pct=None
                # read next few non-noise lines
                j=i+1
                buf=[]
                while j < len(lines) and len(buf) < 8:
                    t=lines[j].strip()
                    if not t or is_noise(t):
                        j+=1
                        continue
                    if ISRC_RE.search(t):
                        break
                    buf.append(t)
                    j+=1
                # name = first non-numeric
                for t in buf:
                    if not t.isdigit() and not PCT_RE.search(t):
                        name=t; break
                # pseudo = next non-numeric non-pct different from name
                if name:
                    for t in buf[buf.index(name)+1:]:
                        if t.isdigit() or PCT_RE.search(t):
                            continue
                        pseudo=t
                        break
                for t in buf:
                    pm=PCT_RE.search(t)
                    if pm:
                        pct=pm.group(0)
                        break

                parts.append({
                    'isrc': cur['isrc'],
                    'participant_ecad': pid,
                    'formal_name': name,
                    'pseudonimo': pseudo,
                    'share_pct_raw': pct,
                })
            
        i+=1

    flush()
    return pd.DataFrame(tracks), pd.DataFrame(parts)


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--text', required=True)
    ap.add_argument('--out-dir', required=True)
    args=ap.parse_args()

    text_path=Path(args.text)
    out_dir=Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    lines=text_path.read_text(encoding='utf-8', errors='ignore').splitlines()
    tracks, parts = parse_lines(lines)

    tracks.to_csv(out_dir/'fonogramas_tracks.csv', index=False)
    parts.to_csv(out_dir/'fonogramas_participants.csv', index=False)

    print(f"tracks={len(tracks)} parts={len(parts)} out={out_dir}")


if __name__=='__main__':
    main()
