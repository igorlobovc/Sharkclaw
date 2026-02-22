#!/usr/bin/env python3
"""Shared loader + matcher for Estelita entity overrides.

Used by:
- slice_scored_by_sure_terms.py (review slice)
- run_catalog_sweep.py (catalog-wide selection)
- build_match_report.py / build_review_queues.py (reports/queues)

Matching rules
- Normalize: casefold, strip accents, collapse whitespace, strip surrounding punctuation.
- Token-boundary: no substring matches.
- Multi-token entities: require all tokens present in the target field.

Also supports per-entity controls:
- requires_coevidence=1: keep only if TITLE_EXACT or ARTIST_TOKEN_OVERLAP or any ID evidence.
- per_term_cap: keep only top N per entity (best-first by rank).
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


_PUNCT_EDGE = re.compile(r"^[\W_]+|[\W_]+$")
_WS = re.compile(r"\s+")


def norm_text(s: str) -> str:
    s = "" if s is None else str(s)
    s = strip_accents(s.casefold().strip())
    s = _PUNCT_EDGE.sub("", s)
    s = _WS.sub(" ", s)
    return s


def tokenize_norm(s: str) -> list[str]:
    s = norm_text(s)
    return [t for t in re.split(r"[^0-9a-z]+", s) if t]


@dataclass(frozen=True)
class EntityOverride:
    entity_raw: str
    entity_norm: str
    entity_type: str  # PERSON|ORG|PSEUDONYM
    priority: int
    requires_coevidence: int
    per_term_cap: int | None
    notes: str

    @property
    def tokens(self) -> tuple[str, ...]:
        return tuple(tokenize_norm(self.entity_norm))


def load_entity_overrides(path: Path) -> list[EntityOverride]:
    df = pd.read_csv(path, dtype=str, low_memory=False).fillna("")
    df.columns = [c.strip().lower() for c in df.columns]

    out: list[EntityOverride] = []
    for _, r in df.iterrows():
        raw = str(r.get("entity_raw", "")).strip()
        en = str(r.get("entity_norm", "")).strip()
        if not raw or not en:
            continue
        et = str(r.get("entity_type", "PERSON")).strip().upper() or "PERSON"
        pr = int(str(r.get("priority", "0")).strip() or 0)
        rc = int(str(r.get("requires_coevidence", "0")).strip() or 0)
        cap_s = str(r.get("per_term_cap", "")).strip()
        cap = int(cap_s) if cap_s else None
        notes = str(r.get("notes", "")).strip()
        out.append(
            EntityOverride(
                entity_raw=raw,
                entity_norm=norm_text(en),
                entity_type=et,
                priority=pr,
                requires_coevidence=rc,
                per_term_cap=cap,
                notes=notes,
            )
        )

    # de-dupe by entity_norm
    uniq = {}
    for o in out:
        uniq[o.entity_norm] = o
    return list(uniq.values())


def field_token_set(value: str) -> set[str]:
    return set(tokenize_norm(value))


def entity_matches_field(entity: EntityOverride, field_value: str) -> bool:
    toks = entity.tokens
    if not toks:
        return False
    s = field_token_set(field_value)
    # require all tokens
    return all(t in s for t in toks)


def compute_entity_override_hits(
    df: pd.DataFrame,
    overrides: list[EntityOverride],
    *,
    search_fields: list[str],
    evidence_field_aliases: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (rows_with_added_cols, per-entity stats dataframe).

    Adds columns:
    - entity_override_hit (0/1)
    - entity_override_best_priority
    - entity_override_entities (semicolon list)
    - entity_override_hit_fields (semicolon 'entity@field')

    Does NOT change tier here.
    """

    evidence_field_aliases = evidence_field_aliases or []

    out = df.copy()

    # normalize search field availability
    available_fields = [f for f in search_fields if f in out.columns]
    available_fields += [f for f in evidence_field_aliases if f in out.columns and f not in available_fields]

    hit_entities: list[list[str]] = [[] for _ in range(len(out))]
    hit_fields: list[list[str]] = [[] for _ in range(len(out))]
    best_priority = [0] * len(out)

    # Precompute token sets per field for each row to make matching cheaper
    field_tokens = {}
    for f in available_fields:
        field_tokens[f] = out[f].astype(str).map(field_token_set)

    # Per-entity counters
    stats = {}

    for ent in overrides:
        ent_toks = set(ent.tokens)
        if not ent_toks:
            continue

        hit_mask = pd.Series([False] * len(out))
        field_breakdown = {f: 0 for f in available_fields}

        for f in available_fields:
            m = field_tokens[f].map(lambda s: ent_toks.issubset(s))
            if bool(m.any()):
                hit_mask = hit_mask | m
                field_breakdown[f] += int(m.sum())

                # record hits row-wise
                idxs = m[m].index.tolist()
                for i in idxs:
                    hit_entities[i].append(ent.entity_norm)
                    hit_fields[i].append(f"{ent.entity_norm}@{f}")
                    if ent.priority > best_priority[i]:
                        best_priority[i] = ent.priority

        total_hits = int(hit_mask.sum())
        if total_hits:
            stats[ent.entity_norm] = {
                "entity_norm": ent.entity_norm,
                "entity_type": ent.entity_type,
                "priority": ent.priority,
                "requires_coevidence": ent.requires_coevidence,
                "per_term_cap": ent.per_term_cap or "",
                "hit_count": total_hits,
                "hit_field_breakdown": ",".join(f"{k}:{v}" for k, v in field_breakdown.items() if v),
            }

    out["entity_override_hit"] = [1 if x else 0 for x in hit_entities]
    out["entity_override_best_priority"] = best_priority
    out["entity_override_entities"] = [";".join(sorted(set(x))) for x in hit_entities]
    out["entity_override_hit_fields"] = [";".join(x) for x in hit_fields]

    stats_df = pd.DataFrame(list(stats.values())) if stats else pd.DataFrame(
        columns=[
            "entity_norm",
            "entity_type",
            "priority",
            "requires_coevidence",
            "per_term_cap",
            "hit_count",
            "hit_field_breakdown",
        ]
    )

    return out, stats_df


def apply_noisy_entity_controls(
    df: pd.DataFrame,
    overrides: list[EntityOverride],
    *,
    rank_cols: list[str],
) -> pd.DataFrame:
    """Apply requires_coevidence + per_term_cap for entities with those controls."""

    out = df.copy()

    # evidence bools
    flags = out.get("evidence_flags", "").astype(str)
    has_title_exact = flags.str.contains("TITLE_EXACT", case=False, na=False)
    has_artist_overlap = flags.str.contains("ARTIST_TOKEN_OVERLAP", case=False, na=False)
    has_id = (
        out.get("isrc", "").astype(str).str.strip().ne("")
        | out.get("iswc", "").astype(str).str.strip().ne("")
        | out.get("ref_isrc", "").astype(str).str.strip().ne("")
        | out.get("ref_iswc", "").astype(str).str.strip().ne("")
    )
    coevidence_ok = has_title_exact | has_artist_overlap | has_id

    # apply per controlled entity
    for ent in overrides:
        if not (ent.requires_coevidence or ent.per_term_cap):
            continue

        # identify rows that hit this entity
        m = out.get("entity_override_entities", "").astype(str).str.contains(re.escape(ent.entity_norm), na=False)
        if not int(m.sum()):
            continue

        idx = out.index[m]
        d = out.loc[idx].copy()

        if ent.requires_coevidence:
            d = d[coevidence_ok.loc[idx]]

        # drop all entity-hit rows first, then re-add capped
        out = out.drop(index=idx)

        if len(d) == 0:
            continue

        # sort best-first
        d = d.sort_values(rank_cols, ascending=[False] * len(rank_cols))

        if ent.per_term_cap:
            d = d.head(ent.per_term_cap)

        out = pd.concat([out, d], ignore_index=False)

    return out
