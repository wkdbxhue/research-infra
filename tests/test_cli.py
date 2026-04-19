import os
import json
from pathlib import Path
from subprocess import run

from research_infra.audit import audit_results_tree
from research_infra.schema import BatchMeta


CLI_ENV = {
    **os.environ,
    "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
}


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
        env=CLI_ENV,
        text=True,
    )
    assert result.returncode == 0


def test_cli_init_command(tmp_path: Path):
    result = run(
        ["python", "-m", "research_infra.cli", "init", "--workspace", str(tmp_path), "--json"],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
    )
    assert result.returncode == 0
    assert (tmp_path / "results").exists()


def test_cli_freeze_command(tmp_path: Path):
    result = run(
        ["python", "-m", "research_infra.cli", "freeze", "--workspace", str(tmp_path), "--policy", "backfill-only"],
        check=False,
        capture_output=True,
        env=CLI_ENV,
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
        env=CLI_ENV,
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
        env=CLI_ENV,
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
        env=CLI_ENV,
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
        "environment": {},
        "provenance": {"infra_version": "0.1.0", "backfilled": True},
    }


def test_cli_batch_backfill_omits_invalid_upgrade_without_flag(tmp_path: Path):
    batch_dir = tmp_path / "results" / "E50006"
    batch_dir.mkdir(parents=True)
    legacy_payload = '{"command": "python main.py", "total_trials": 10}\n'
    (batch_dir / "batch.json").write_text(legacy_payload, encoding="utf-8")

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
        env=CLI_ENV,
        text=True,
    )

    assert result.returncode == 0
    assert (batch_dir / "batch.json").read_text(encoding="utf-8") == legacy_payload
    assert not (batch_dir / "batch.legacy.json").exists()


def test_cli_batch_backfill_upgrade_invalid_keeps_valid_payload(tmp_path: Path):
    batch_dir = tmp_path / "results" / "E50003"
    batch_dir.mkdir(parents=True)
    valid_payload = {
        "experiment_id": "E50003",
        "batch_id": "E50003",
        "batch_type": "backfill",
        "created_at": "1970-01-01T00:00:00+00:00",
        "models": ["UNKNOWN"],
        "instances": {"UNKNOWN": []},
        "git": {"commit": None, "dirty": True},
        "environment": {},
        "provenance": {"infra_version": "0.1.0", "backfilled": True},
    }
    (batch_dir / "batch.json").write_text(json.dumps(valid_payload), encoding="utf-8")

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
            "--upgrade-invalid",
        ],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
    )

    assert result.returncode == 0
    assert json.loads((batch_dir / "batch.json").read_text(encoding="utf-8")) == valid_payload
    assert not (batch_dir / "batch.legacy.json").exists()


def test_cli_batch_backfill_upgrade_invalid_rewrites_legacy_payload(tmp_path: Path):
    batch_dir = tmp_path / "results" / "E50004"
    batch_dir.mkdir(parents=True)
    legacy_payload = {
        "experiment_id": "legacy-run",
        "models": ["M00004"],
        "instances": {"M00004": ["small-03"]},
        "git": {"commit": "b" * 40, "dirty": False, "branch": "legacy"},
        "environment": {"python": "3.11"},
    }
    (batch_dir / "batch.json").write_text(json.dumps(legacy_payload), encoding="utf-8")

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
            "--upgrade-invalid",
        ],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
    )

    assert result.returncode == 0
    assert json.loads((batch_dir / "batch.legacy.json").read_text(encoding="utf-8")) == legacy_payload
    payload = json.loads((batch_dir / "batch.json").read_text(encoding="utf-8"))
    BatchMeta.model_validate(payload)
    assert payload["experiment_id"] == "E50004"
    assert payload["batch_id"] == "E50004"
    assert payload["git"] == {"commit": "b" * 40, "dirty": False, "branch": "legacy"}
    assert payload["environment"] == {"python": "3.11"}
    assert payload["provenance"]["legacy_backup"] == "batch.legacy.json"


def test_cli_batch_backfill_upgrade_invalid_rewrites_malformed_payload(tmp_path: Path):
    batch_dir = tmp_path / "results" / "E50005"
    batch_dir.mkdir(parents=True)
    malformed_payload = "legacy-batch\nmissing-json\n"
    (batch_dir / "batch.json").write_text(malformed_payload, encoding="utf-8")

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
            "--upgrade-invalid",
        ],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
    )

    assert result.returncode == 0
    assert (batch_dir / "batch.legacy.json").read_text(encoding="utf-8") == malformed_payload
    payload = json.loads((batch_dir / "batch.json").read_text(encoding="utf-8"))
    BatchMeta.model_validate(payload)
    assert payload["experiment_id"] == "E50005"
    assert payload["batch_id"] == "E50005"
    assert payload["models"] == ["UNKNOWN"]
    assert payload["instances"] == {"UNKNOWN": []}
    assert payload["git"] == {"commit": None, "dirty": True}
    assert payload["environment"] == {}
    assert payload["provenance"]["legacy_backup"] == "batch.legacy.json"


def test_cli_batch_backfill_upgrade_invalid_refuses_damaged_canonical_payload(tmp_path: Path):
    batch_dir = tmp_path / "results" / "E50007"
    batch_dir.mkdir(parents=True)
    raw_text = json.dumps(
        {
            "experiment_id": "broken",
            "batch_id": "E50007",
            "batch_type": "backfill",
            "created_at": "1970-01-01T00:00:00+00:00",
            "models": ["UNKNOWN"],
            "instances": {"UNKNOWN": []},
            "git": {"commit": None, "dirty": True},
            "environment": {},
            "provenance": {"infra_version": "0.1.0", "backfilled": True},
        }
    )
    (batch_dir / "batch.json").write_text(raw_text, encoding="utf-8")

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
            "--upgrade-invalid",
        ],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
    )

    assert result.returncode != 0
    assert (batch_dir / "batch.json").read_text(encoding="utf-8") == raw_text
    assert not (batch_dir / "batch.legacy.json").exists()
