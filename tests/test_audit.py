from pathlib import Path

from research_infra.audit import audit_results_tree


def test_audit_accepts_minimal_fixture():
    findings = audit_results_tree(Path("/home/research-infra/tests/fixtures/minimal_project/results"))
    assert findings == []


def test_audit_ignores_non_batch_entries(tmp_path: Path):
    results_root = tmp_path / "results"
    results_root.mkdir()
    (results_root / "E50014_driver.log").write_text("driver log\n", encoding="utf-8")
    (results_root / "E50016_manual_fill").mkdir()

    batch_dir = results_root / "E50015"
    batch_dir.mkdir()

    findings = audit_results_tree(results_root)

    assert findings == [f"missing batch.json: {batch_dir}"]
