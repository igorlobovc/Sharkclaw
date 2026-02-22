#!/usr/bin/env python3
"""Shared header/field detection resolver.

- Normalizes headers: casefold + strip accents + non-alnum->space + collapse whitespace
- Matches normalized headers against synonyms in config/header_field_synonyms.yaml
- Returns ranked candidate columns per field.

YAML format (simple):
field:
  - header_norm
  - ...

This parser supports only that subset.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path


_NON_ALNUM = re.compile(r"[^0-9a-z]+")
_WS = re.compile(r"\s+")


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def norm_header(s: str) -> str:
    s = "" if s is None else str(s)
    s = strip_accents(s.casefold())
    s = _NON_ALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


def load_synonyms_yaml(path: Path) -> dict[str, list[str]]:
    """Parse minimal YAML mapping to list of strings."""

    txt = path.read_text(encoding="utf-8").splitlines()
    out: dict[str, list[str]] = {}
    cur_key: str | None = None
    for raw in txt:
        line = raw.rstrip("\n")
        if not line.strip() or line.strip().startswith("#"):
            continue
        if re.match(r"^[A-Za-z0-9_]+:\s*$", line.strip()):
            cur_key = line.strip()[:-1]
            out[cur_key] = []
            continue
        if line.strip().startswith("-") and cur_key:
            item = line.strip()[1:].strip().strip('"').strip("'")
            if item:
                out[cur_key].append(item)
    return out


@dataclass
class Candidate:
    field: str
    column: str
    header_norm: str
    score: int


def resolve_fields(headers: list[str], synonyms: dict[str, list[str]]) -> dict[str, list[Candidate]]:
    """Return candidates per field.

    Scoring:
    - exact synonym match: 100
    - substring match (synonym token set subset): 60

    We keep multiple candidates.
    """

    headers_norm = [(h, norm_header(h)) for h in headers]

    result: dict[str, list[Candidate]] = {k: [] for k in synonyms.keys()}

    for field, syns in synonyms.items():
        syns_norm = [norm_header(s) for s in syns]
        syn_set = set(syns_norm)

        for raw_h, hn in headers_norm:
            if not hn:
                continue
            if hn in syn_set:
                result[field].append(Candidate(field, raw_h, hn, 100))
                continue
            # token subset heuristic
            htoks = set(hn.split())
            best = 0
            for s in syns_norm:
                stoks = set(s.split())
                if stoks and stoks.issubset(htoks):
                    best = max(best, 60)
            if best:
                result[field].append(Candidate(field, raw_h, hn, best))

        # sort candidates by score desc then header length
        result[field].sort(key=lambda c: (c.score, len(c.header_norm)), reverse=True)

    return result
