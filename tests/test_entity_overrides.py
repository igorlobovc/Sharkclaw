import pandas as pd

from scripts.entity_overrides import (
    EntityOverride,
    compute_entity_override_hits,
    entity_matches_field,
    load_entity_overrides,
    norm_text,
)


def test_norm_text_basic():
    assert norm_text("  Dudu Falcão ") == "dudu falcao"
    assert norm_text("(Tagore)") == "tagore"


def test_token_boundary_all_tokens_required():
    ent = EntityOverride(
        entity_raw="cordel do fogo",
        entity_norm="cordel do fogo",
        entity_type="PERSON",
        priority=1,
        requires_coevidence=1,
        per_term_cap=5000,
        notes="",
    )
    assert entity_matches_field(ent, "Cordel do Fogo Encantado") is True
    assert entity_matches_field(ent, "Cordel") is False


def test_load_entity_overrides(tmp_path):
    p = tmp_path / "ov.csv"
    p.write_text(
        "entity_raw,entity_norm,entity_type,priority,requires_coevidence,per_term_cap,notes\n"
        "Tagore,tagore,PERSON,5,0,,x\n",
        encoding="utf-8",
    )
    ovs = load_entity_overrides(p)
    assert len(ovs) == 1
    assert ovs[0].entity_norm == "tagore"
    assert ovs[0].priority == 5


def test_compute_entity_override_hits_fields():
    df = pd.DataFrame(
        {
            "artist": ["Tagore", "X"],
            "author": ["", "Dudu Falcao"],
            "publisher": ["Editora Estelita", ""],
            "evidence_flags": ["", ""],
        }
    )
    ovs = [
        EntityOverride("tagore", "tagore", "PERSON", 5, 0, None, ""),
        EntityOverride("dudu falcao", "dudu falcao", "PERSON", 5, 0, None, ""),
        EntityOverride("editora estelita", "editora estelita", "ORG", 5, 0, None, ""),
    ]

    out, stats = compute_entity_override_hits(
        df,
        ovs,
        search_fields=["artist", "author", "publisher"],
        evidence_field_aliases=["evidence_flags"],
    )

    assert out["entity_override_hit"].tolist() == [1, 1]
    assert out["entity_override_best_priority"].tolist() == [5, 5]
    assert "tagore" in out.loc[0, "entity_override_entities"]
    assert "dudu falcao" in out.loc[1, "entity_override_entities"]
    assert len(stats) == 3
