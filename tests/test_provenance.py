from pathlib import Path

from research_infra.provenance import write_environment_evidence


def test_write_environment_evidence_creates_json_file(tmp_path: Path):
    target = tmp_path / "env.json"
    payload = write_environment_evidence(target, python_version="3.10.9", infra_version="0.1.0")
    assert target.exists()
    assert payload["python_version"] == "3.10.9"
    assert payload["infra_version"] == "0.1.0"
