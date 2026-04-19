from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "contract.md"


def test_contract_doc_contains_required_sections():
    text = DOC.read_text(encoding="utf-8")
    assert "# Research Infra Contract" in text
    assert "## Canonical Artifacts" in text
    assert "## Disposable Cache" in text
    assert "## Batch Layout" in text
    assert "## Audit Semantics" in text
    assert "## Compatibility Policy" in text
