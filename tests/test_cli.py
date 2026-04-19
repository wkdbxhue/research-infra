import json
from pathlib import Path
from subprocess import run

from research_infra.audit import audit_results_tree
from research_infra.schema import BatchMeta


def test_audit_accepts_minimal_fixture():
    findings = audit_results_tree(Path("/home/research-infra/tests/fixtures/minimal_project/results"))
    assert findings == []


def test_cli_cache_rebuild_command(tmp_path: Path):
    result = run(
        [
            "python",
            "-m",
            "research_infra.cli",
            "cache",
            "rebuild",
            "--results-root",
            "/home/research-infra/tests/fixtures/minimal_project/results",
            "--db-path",
            str(tmp_path / "registry.duckdb"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_cli_init_command(tmp_path: Path):
    result = run(
        ["python", "-m", "research_infra.cli", "init", "--workspace", str(tmp_path), "--json"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert (tmp_path / "results").exists()


def test_cli_freeze_command(tmp_path: Path):
    result = run(
        ["python", "-m", "research_infra.cli", "freeze", "--workspace", str(tmp_path), "--policy", "backfill-only"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert (tmp_path / "results/project_freeze.yml").exists()


def test_cli_audit_command(tmp_path: Path):
    clean_workspace = Path("/home/research-infra/tests/fixtures/minimal_project")
    clean_result = run(
        [
            "python",
            "-m",
            "research_infra.cli",
            "audit",
            "--workspace",
            str(clean_workspace),
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert clean_result.returncode == 0
    payload = json.loads(clean_result.stdout)
    assert payload == {
        "findings": [],
        "workspace": str(clean_workspace),
    }

    dirty_results_root = tmp_path / "results" / "E50001"
    dirty_results_root.mkdir(parents=True)
    dirty_result = run(
        [
            "python",
            "-m",
            "research_infra.cli",
            "audit",
            "--workspace",
            str(tmp_path),
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert dirty_result.returncode == 1
    dirty_payload = json.loads(dirty_result.stdout)
    assert dirty_payload["workspace"] == str(tmp_path)
    assert dirty_payload["findings"]
    assert dirty_payload["findings"][0].startswith("missing batch.json:")


def test_cli_batch_backfill_command(tmp_path: Path):
    existing_batch_dir = tmp_path / "results" / "E50001"
    existing_batch_dir.mkdir(parents=True)
    existing_batch_json = existing_batch_dir / "batch.json"
    existing_batch_json.write_text("sentinel", encoding="utf-8")

    new_batch_dir = tmp_path / "results" / "E50002"
    new_batch_dir.mkdir(parents=True)

    result = run(
        [
            "python",
            "-m",
            "research_infra.cli",
            "batch",
            "backfill",
            "--workspace",
            str(tmp_path),
            "--results-root",
            "results",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert existing_batch_json.read_text(encoding="utf-8") == "sentinel"

    payload = json.loads((new_batch_dir / "batch.json").read_text(encoding="utf-8"))
    BatchMeta.model_validate(payload)
    assert payload == {
        "experiment_id": "E50002",
        "batch_id": "E50002",
        "batch_type": "backfill",
        "created_at": "1970-01-01T00:00:00+00:00",
        "models": ["UNKNOWN"],
        "instances": {"UNKNOWN": []},
        "git": {"commit": None, "dirty": True},
        "provenance": {"infra_version": "0.1.0", "backfilled": True},
    }
