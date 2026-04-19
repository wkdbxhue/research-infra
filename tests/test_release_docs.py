from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
CHANGELOG = ROOT / "CHANGELOG.md"
UPGRADE_DOC = ROOT / "docs" / "upgrade-0.1.0.md"


def test_pyproject_declares_release_version():
    text = PYPROJECT.read_text(encoding="utf-8")
    assert 'version = "0.1.0"' in text


def test_upgrade_doc_mentions_opt_in_upgrade():
    text = UPGRADE_DOC.read_text(encoding="utf-8")
    assert "opt-in upgrade" in text


def test_changelog_includes_release_entry():
    text = CHANGELOG.read_text(encoding="utf-8")
    assert "## 0.1.0 - 2026-04-19" in text
