import json
from pathlib import Path

import pytest

from research_infra.batch import (
    allocate_experiment_id,
    backfill_batch_json,
    read_batch_json,
    upgrade_legacy_batch_json,
    write_batch_json,
)
from research_infra.schema import BatchMeta


def test_allocate_experiment_id_scans_result_directories(tmp_path: Path):
    (tmp_path / "results/E50001").mkdir(parents=True)
    (tmp_path / "results/E50002").mkdir(parents=True)
    assert allocate_experiment_id(tmp_path / "results", start=50001) == "E50003"


def test_write_batch_json_round_trips(tmp_path: Path):
    target = tmp_path / "results/E50003/batch.json"
    payload = {
        "experiment_id": "E50003",
        "batch_id": "E50003",
        "batch_type": "original",
        "created_at": "2026-04-19T00:00:00+00:00",
        "models": ["M00001"],
        "instances": {"M00001": ["small-00"]},
        "git": {"commit": "0" * 40, "dirty": False},
        "provenance": {"infra_version": "0.1.0"},
    }
    write_batch_json(target, payload)
    assert json.loads(target.read_text(encoding="utf-8"))["experiment_id"] == "E50003"


def test_backfill_batch_json_uses_existing_run_tree(tmp_path: Path):
    batch_dir = tmp_path / "results/E50004"
    (batch_dir / "runs/R000001").mkdir(parents=True)
    payload = backfill_batch_json(batch_dir, models=["M00002"], instances={"M00002": ["small-01"]})
    assert payload["experiment_id"] == "E50004"
    assert (batch_dir / "batch.json").exists()


def test_read_batch_json_returns_none_for_invalid_payload(tmp_path: Path):
    target = tmp_path / "results/E50005/batch.json"
    target.parent.mkdir(parents=True)
    target.write_text('{"legacy": true}\n', encoding="utf-8")

    assert read_batch_json(target.parent) is None


def test_upgrade_legacy_batch_json_preserves_backup_and_signal(tmp_path: Path):
    batch_dir = tmp_path / "results/E50006"
    batch_dir.mkdir(parents=True)
    legacy_payload = {
        "experiment_id": "not-an-eid",
        "batch_id": "E59999",
        "created_at": "2026-04-19T00:00:00+00:00",
        "models": ["M00003"],
        "instances": {"M00003": ["small-02"]},
        "git": {"commit": "a" * 40, "branch": "feature/legacy"},
        "environment": {"python": "3.12"},
    }
    (batch_dir / "batch.json").write_text(json.dumps(legacy_payload), encoding="utf-8")

    upgraded = upgrade_legacy_batch_json(batch_dir)

    assert json.loads((batch_dir / "batch.legacy.json").read_text(encoding="utf-8")) == legacy_payload
    assert upgraded["experiment_id"] == "E50006"
    assert upgraded["batch_id"] == "E59999"
    assert upgraded["models"] == ["M00003"]
    assert upgraded["instances"] == {"M00003": ["small-02"]}
    assert upgraded["git"] == {
        "commit": "a" * 40,
        "dirty": True,
        "branch": "feature/legacy",
    }
    assert upgraded["environment"] == {"python": "3.12"}
    assert upgraded["provenance"]["backfilled"] is True
    assert upgraded["provenance"]["legacy_backup"] == "batch.legacy.json"
    BatchMeta.model_validate(upgraded)


def test_upgrade_legacy_batch_json_keeps_existing_backup(tmp_path: Path):
    batch_dir = tmp_path / "results/E50007"
    batch_dir.mkdir(parents=True)
    (batch_dir / "batch.json").write_text('{"legacy": true}\n', encoding="utf-8")
    (batch_dir / "batch.legacy.json").write_text('{"older": true}\n', encoding="utf-8")

    upgrade_legacy_batch_json(batch_dir)

    assert (batch_dir / "batch.legacy.json").read_text(encoding="utf-8") == '{"older": true}\n'


def test_upgrade_legacy_batch_json_requires_existing_batch_json(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        upgrade_legacy_batch_json(tmp_path / "results/E50008")


def test_repo_gitignore_covers_generated_artifacts():
    patterns = (Path(__file__).resolve().parents[1] / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".pytest_cache/" in patterns
    assert "__pycache__/" in patterns
    assert "*.pyc" in patterns
    assert "src/*.egg-info/" in patterns
