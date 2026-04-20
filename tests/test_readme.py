from pathlib import Path


README = Path(__file__).resolve().parents[1] / "README.md"


def test_readme_lists_phase1_commands_and_non_goals():
    text = README.read_text(encoding="utf-8")
    assert "ri init" in text
    assert "ri batch new" in text
    assert "ri batch backfill" in text
    assert "ri cache rebuild" in text
    assert "ri audit" in text
    assert "Non-goals for P0" in text
    assert 'pip install "research-infra @ git+https://github.com/wkdbxhue/research-infra.git@v0.1.1"' in text
    assert "docs/use-guide.md" in text
