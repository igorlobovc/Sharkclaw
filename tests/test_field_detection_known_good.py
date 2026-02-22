import pandas as pd
import pytest

from scripts.field_detection import load_synonyms_yaml, resolve_fields


SYN_PATH = "config/header_field_synonyms.yaml"

# Picked from known_good_template_set.csv (canonical lanes). These paths are stable on this machine.
TEMPLATES = [
    (
        "ubem",
        "/Users/igorcunha/.openclaw/workspace/estelita_unified_audiovisual/dedup/unique/3ce67aed0293c96067e027a4a2052e256895a300.xlsx",
        "Relatório UBEM - Canais Globo",
    ),
    (
        "sbt",
        "/Users/igorcunha/Desktop/Estelita_backup/Estelita/Raw/Fornecedores/SBT/Unificado SBT nov dez 23 e jan fev mar 24.xls",
        "Planilha1",
    ),
]


def test_field_resolver_finds_title_and_artist_or_author_when_present():
    from pathlib import Path

    syn = load_synonyms_yaml(Path(SYN_PATH))

    for prov, fp, sheet in TEMPLATES:
        from pathlib import Path as _Path

        if not _Path(fp).exists():
            pytest.skip(f"known-good workbook not present on this machine: {fp}")

        xl = pd.ExcelFile(fp)
        if sheet not in xl.sheet_names:
            # Sheet names can drift across deduped copies; resolver test only needs *a* sheet.
            sheet = xl.sheet_names[0]
        df = xl.parse(sheet, dtype=str, nrows=50).fillna("")
        headers = [str(c) for c in df.columns]
        res = resolve_fields(headers, syn)

        title = res.get("title", [])
        artist = res.get("artist", [])
        author = res.get("author", [])

        assert len(title) >= 1, f"{prov} should have title columns detected"
        assert (len(artist) >= 1) or (len(author) >= 1), f"{prov} should have artist or author detected"


def test_portuguese_header_synonyms_present_in_yaml():
    syn = load_synonyms_yaml(__import__("pathlib").Path(SYN_PATH))
    # ensure critical Portuguese tokens are represented in title/author/rightsholder lists
    all_syn = set(sum(syn.values(), []))
    for must in ["autores da musica", "repertorio", "titulares", "obra", "musica"]:
        assert must in all_syn, f"missing synonym: {must}"
