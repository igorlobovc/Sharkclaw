import pandas as pd

from scripts.slice_scored_by_sure_terms import norm, load_sure_terms


def test_norm_casefold_accents_whitespace():
    assert norm("  ÁéÍõ  ") == "aeio"
    assert norm("Zero   Quatro") == "zero quatro"
    assert norm("YAGO O PRÓPRIO") == "yago o proprio"


def test_load_sure_terms_headered_kind_mapping(tmp_path):
    p = tmp_path / "sure.csv"
    p.write_text(
        "term,kind,tier\n"
        "BALMAIN,title,KNOWN\n"
        "Tagore,person,GOLD\n"
        "Editora Estelita,entity,GOLD\n",
        encoding="utf-8",
    )

    terms = load_sure_terms(p)
    m = {(t.term, t.term_type) for t in terms}
    assert ("BALMAIN", "TITLE") in m
    assert ("Tagore", "PERSON") in m
    assert ("Editora Estelita", "ORG") in m


def test_load_sure_terms_one_column_backcompat(tmp_path):
    p = tmp_path / "sure_onecol.txt"
    p.write_text("BALMAIN\nTAGORE\n", encoding="utf-8")
    terms = load_sure_terms(p)
    assert all(t.term_type == "TITLE" for t in terms)


def test_multifield_matching_logic(tmp_path):
    # Minimal scored-like dataset
    scored = pd.DataFrame(
        {
            "source_file": ["f1", "f2", "f3"],
            "source_sheet": ["s", "s", "s"],
            "source_row": [1, 2, 3],
            "title": ["random", "random", "balmain"],
            "artist": ["tagore", "someone", "x"],
            "author": ["x", "zero quatro", "x"],
            "publisher": ["editora estelita", "x", "x"],
            "match_tier": ["NoMatch", "Silver", "Gold"],
            "evidence_flags": ["", "", ""],
        }
    )

    sure = tmp_path / "sure.csv"
    sure.write_text(
        "term,kind\n"
        "Tagore,person\n"
        "Zero Quatro,person\n"
        "Editora Estelita,entity\n"
        "Balmain,title\n",
        encoding="utf-8",
    )

    sure_terms = load_sure_terms(sure)
    # ensure we got all types
    assert {t.term_type for t in sure_terms} == {"TITLE", "PERSON", "ORG"}

    # emulate the bucket selection by checking normalization + substring existence
    # (main script uses regex+contains; we just validate that norm maps are compatible)
    assert any(t.term_norm == "tagore" and t.term_type == "PERSON" for t in sure_terms)
    assert any(t.term_norm == "zero quatro" and t.term_type == "PERSON" for t in sure_terms)
    assert any(t.term_norm == "editora estelita" and t.term_type == "ORG" for t in sure_terms)
    assert any(t.term_norm == "balmain" and t.term_type == "TITLE" for t in sure_terms)

    # sanity: the terms appear in corresponding fields after normalization
    assert "tagore" in norm(scored.loc[0, "artist"])
    assert "zero quatro" in norm(scored.loc[1, "author"])
    assert "editora estelita" in norm(scored.loc[0, "publisher"])
    assert "balmain" in norm(scored.loc[2, "title"])
