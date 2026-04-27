import os
import json
import csv
from pathlib import Path
from subprocess import run

import duckdb
import yaml

from research_infra.audit import audit_results_tree
from research_infra.schema import BatchMeta


CLI_ENV = {
    **os.environ,
    "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
}


def _write_batch_with_index(results_root: Path, batch_id: str = "E50001") -> None:
    batch_dir = results_root / batch_id
    batch_dir.mkdir(parents=True)
    (batch_dir / "batch.json").write_text(
        json.dumps(
            {
                "experiment_id": batch_id,
                "batch_id": batch_id,
                "batch_type": "original",
                "created_at": "1970-01-01T00:00:00+00:00",
                "models": ["M00001"],
                "instances": {"M00001": ["small-00"]},
                "git": {"commit": None, "dirty": True},
                "environment": {},
                "provenance": {"infra_version": "0.1.0"},
            }
        ),
        encoding="utf-8",
    )
    with (batch_dir / "index.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "run_id",
                "model_name",
                "instance_name",
                "param_alpha",
                "objective",
                "runtime",
                "gap",
                "status",
                "validation_feasible",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "R000001",
                "model_name": "M00001",
                "instance_name": "small-00",
                "param_alpha": "0.4",
                "objective": "123.5",
                "runtime": "9.75",
                "gap": "0.0",
                "status": "2",
                "validation_feasible": "True",
            }
        )


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


def test_cli_cache_rebuild_command_resolves_relative_paths_from_workspace(tmp_path: Path):
    workspace = tmp_path / "workspace"
    _write_batch_with_index(workspace / "results")
    db_path = workspace / "cache" / "registry.duckdb"

    result = run(
        [
            "python",
            "-m",
            "research_infra.cli",
            "cache",
            "rebuild",
            "--workspace",
            str(workspace),
            "--results-root",
            "results",
            "--db-path",
            "cache/registry.duckdb",
        ],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
        cwd="/home",
    )

    assert result.returncode == 0
    assert db_path.exists()
    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute("select batch_id, model_name, instance_name from runs").fetchall()
    assert rows == [("E50001", "M00001", "small-00")]


def test_cli_init_command(tmp_path: Path):
    result = run(
        ["python", "-m", "research_infra.cli", "init", "--workspace", str(tmp_path), "--json"],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
    )
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload == {
        "workspace": str(tmp_path),
        "created": [
            "results",
            "results/_cache",
            "docs",
            "src/models",
            "src/config",
            "src/engines",
        ],
    }
    for rel in payload["created"]:
        assert (tmp_path / rel).is_dir()


def test_cli_freeze_command(tmp_path: Path):
    result = run(
        ["python", "-m", "research_infra.cli", "freeze", "--workspace", str(tmp_path), "--policy", "backfill-only"],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
    )
    assert result.returncode == 0
    assert yaml.safe_load((tmp_path / "results/project_freeze.yml").read_text(encoding="utf-8")) == {
        "frozen": True,
        "policy": "backfill-only",
        "writes_allowed": [
            "batch backfill",
            "reproducibility documentation",
        ],
    }


def test_cli_freeze_command_rejects_unknown_policy(tmp_path: Path):
    result = run(
        ["python", "-m", "research_infra.cli", "freeze", "--workspace", str(tmp_path), "--policy", "custom"],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
    )
    assert result.returncode != 0
    assert "invalid choice" in result.stderr
    assert not (tmp_path / "results/project_freeze.yml").exists()


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
        "provenance": {"infra_version": "0.1.1", "backfilled": True},
    }


def test_cli_batch_backfill_ignores_non_directory_entries(tmp_path: Path):
    results_root = tmp_path / "results"
    results_root.mkdir(parents=True)
    (results_root / "E50014_driver.log").write_text("driver log\n", encoding="utf-8")

    batch_dir = results_root / "E50014"
    batch_dir.mkdir()

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
    assert (results_root / "E50014_driver.log").read_text(encoding="utf-8") == "driver log\n"
    payload = json.loads((batch_dir / "batch.json").read_text(encoding="utf-8"))
    BatchMeta.model_validate(payload)
    assert payload["experiment_id"] == "E50014"


def test_cli_batch_backfill_ignores_non_batch_directories(tmp_path: Path):
    results_root = tmp_path / "results"
    manual_dir = results_root / "E50016_manual_fill"
    manual_dir.mkdir(parents=True)
    batch_dir = results_root / "E50016"
    batch_dir.mkdir()

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
    assert not (manual_dir / "batch.json").exists()
    payload = json.loads((batch_dir / "batch.json").read_text(encoding="utf-8"))
    BatchMeta.model_validate(payload)
    assert payload["experiment_id"] == "E50016"


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
        "command": "python main.py --config avsr.yml",
        "total_trials": 8,
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


def test_cli_batch_backfill_upgrade_invalid_refuses_shared_key_only_payload(tmp_path: Path):
    batch_dir = tmp_path / "results" / "E50009"
    batch_dir.mkdir(parents=True)
    raw_text = json.dumps(
        {
            "experiment_id": "legacy-run",
            "models": ["M00004"],
            "instances": {"M00004": ["small-03"]},
            "git": {"commit": "b" * 40, "dirty": False, "branch": "legacy"},
            "environment": {"python": "3.11"},
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


def test_cli_batch_backfill_upgrade_invalid_refuses_malformed_payload(tmp_path: Path):
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

    assert result.returncode != 0
    assert (batch_dir / "batch.json").read_text(encoding="utf-8") == malformed_payload
    assert not (batch_dir / "batch.legacy.json").exists()


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


def test_cli_batch_backfill_upgrade_invalid_refuses_when_backup_exists(tmp_path: Path):
    batch_dir = tmp_path / "results" / "E50008"
    batch_dir.mkdir(parents=True)
    current_payload = '{"command": "python main.py", "total_trials": 10}\n'
    (batch_dir / "batch.json").write_text(current_payload, encoding="utf-8")
    (batch_dir / "batch.legacy.json").write_text('{"older": true}\n', encoding="utf-8")

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
    assert (batch_dir / "batch.json").read_text(encoding="utf-8") == current_payload
    assert (batch_dir / "batch.legacy.json").read_text(encoding="utf-8") == '{"older": true}\n'
