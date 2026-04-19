import json
import subprocess
from pathlib import Path

from research_infra.provenance import collect_git_provenance, write_environment_evidence


def test_write_environment_evidence_creates_json_file(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("research_infra.provenance.platform.platform", lambda: "TestOS-1.0")
    target = tmp_path / "env.json"
    payload = write_environment_evidence(target, python_version="3.10.9", infra_version="0.1.0")
    assert target.exists()
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "python_version": "3.10.9",
        "infra_version": "0.1.0",
        "platform": "TestOS-1.0",
    }
    assert payload["python_version"] == "3.10.9"
    assert payload["infra_version"] == "0.1.0"
    assert payload["platform"] == "TestOS-1.0"


def test_collect_git_provenance_uses_single_status_snapshot(tmp_path: Path, monkeypatch):
    calls: list[list[str]] = []
    status = "\n".join(
        [
            "# branch.oid 0123456789abcdef0123456789abcdef01234567",
            "# branch.head main",
            "1 M. N... 100644 100644 100644 1234567890abcdef1234567890abcdef12345678 1234567890abcdef1234567890abcdef12345678 file.txt",
        ]
    )

    def fake_check_output(cmd, text):
        calls.append(cmd)
        assert text is True
        return status

    monkeypatch.setattr(subprocess, "check_output", fake_check_output)

    payload = collect_git_provenance(tmp_path)

    assert calls == [["git", "-C", str(tmp_path), "status", "--porcelain=v2", "--branch"]]
    assert payload == {
        "commit": "0123456789abcdef0123456789abcdef01234567",
        "dirty": True,
        "branch": "main",
    }


def test_collect_git_provenance_normalizes_detached_head(tmp_path: Path, monkeypatch):
    status = "\n".join(
        [
            "# branch.oid 0123456789abcdef0123456789abcdef01234567",
            "# branch.head (detached)",
        ]
    )

    def fake_check_output(cmd, text):
        assert cmd == ["git", "-C", str(tmp_path), "status", "--porcelain=v2", "--branch"]
        assert text is True
        return status

    monkeypatch.setattr(subprocess, "check_output", fake_check_output)

    payload = collect_git_provenance(tmp_path)

    assert payload == {
        "commit": "0123456789abcdef0123456789abcdef01234567",
        "dirty": False,
        "branch": "HEAD",
    }
