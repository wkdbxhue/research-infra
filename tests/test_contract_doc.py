from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "contract.md"


def test_contract_doc_contains_required_sections():
    text = DOC.read_text(encoding="utf-8")
    assert "# Research Infra Contract" in text
    assert "## Canonical Artifacts" in text
    assert "## Source Layout" in text
    assert "## Disposable Cache" in text
    assert "## Batch Layout" in text
    assert "## Audit Semantics" in text
    assert "## Compatibility Policy" in text


def test_contract_doc_describes_engines_boundary():
    text = DOC.read_text(encoding="utf-8")
    assert "`src/models/`: lineage-visible model wrappers and exact model entrypoints" in text
    assert "`src/engines/`: reusable project-specific algorithm components" in text
    assert "`src/engines/` subdirectories are project-owned" in text
