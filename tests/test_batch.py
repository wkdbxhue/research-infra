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
        "instance_set_filter": "large_only",
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


def test_upgrade_legacy_batch_json_refuses_shared_key_only_payload(tmp_path: Path):
    batch_dir = tmp_path / "results/E50013"
    batch_dir.mkdir(parents=True)
    ambiguous_payload = {
        "experiment_id": "legacy-run",
        "models": ["M00004"],
        "instances": {"M00004": ["small-03"]},
        "git": {"commit": "b" * 40, "dirty": False, "branch": "legacy"},
        "environment": {"python": "3.11"},
    }
    raw_text = json.dumps(ambiguous_payload)
    (batch_dir / "batch.json").write_text(raw_text, encoding="utf-8")

    with pytest.raises(ValueError, match="does not look like a legacy batch"):
        upgrade_legacy_batch_json(batch_dir)

    assert (batch_dir / "batch.json").read_text(encoding="utf-8") == raw_text
    assert not (batch_dir / "batch.legacy.json").exists()


def test_upgrade_legacy_batch_json_refuses_malformed_legacy_payload(tmp_path: Path):
    batch_dir = tmp_path / "results/E50009"
    batch_dir.mkdir(parents=True)
    malformed_payload = "not-json\nlegacy=true\n"
    (batch_dir / "batch.json").write_text(malformed_payload, encoding="utf-8")

    with pytest.raises(ValueError, match="parseable JSON legacy batch"):
        upgrade_legacy_batch_json(batch_dir)

    assert (batch_dir / "batch.json").read_text(encoding="utf-8") == malformed_payload
    assert not (batch_dir / "batch.legacy.json").exists()


def test_upgrade_legacy_batch_json_refuses_when_backup_exists(tmp_path: Path):
    batch_dir = tmp_path / "results/E50007"
    batch_dir.mkdir(parents=True)
    current_payload = '{"legacy": true}\n'
    (batch_dir / "batch.json").write_text(current_payload, encoding="utf-8")
    (batch_dir / "batch.legacy.json").write_text('{"older": true}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="existing legacy backup"):
        upgrade_legacy_batch_json(batch_dir)

    assert (batch_dir / "batch.json").read_text(encoding="utf-8") == current_payload
    assert (batch_dir / "batch.legacy.json").read_text(encoding="utf-8") == '{"older": true}\n'


def test_upgrade_legacy_batch_json_requires_existing_batch_json(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        upgrade_legacy_batch_json(tmp_path / "results/E50008")


def test_upgrade_legacy_batch_json_refuses_damaged_canonical_payload(tmp_path: Path):
    batch_dir = tmp_path / "results/E50010"
    batch_dir.mkdir(parents=True)
    canonical_like_payload = {
        "experiment_id": "bad-id",
        "batch_id": "E50010",
        "batch_type": "backfill",
        "created_at": "1970-01-01T00:00:00+00:00",
        "models": ["UNKNOWN"],
        "instances": {"UNKNOWN": []},
        "git": {"commit": None, "dirty": True},
        "environment": {},
        "provenance": {"infra_version": "0.1.0", "backfilled": True},
    }
    raw_text = json.dumps(canonical_like_payload)
    (batch_dir / "batch.json").write_text(raw_text, encoding="utf-8")

    with pytest.raises(ValueError, match="does not look like a legacy batch"):
        upgrade_legacy_batch_json(batch_dir)

    assert (batch_dir / "batch.json").read_text(encoding="utf-8") == raw_text
    assert not (batch_dir / "batch.legacy.json").exists()


def test_upgrade_legacy_batch_json_refuses_malformed_canonical_like_text(tmp_path: Path):
    batch_dir = tmp_path / "results/E50011"
    batch_dir.mkdir(parents=True)
    raw_text = '{"experiment_id":"E50011","batch_type":"backfill","provenance":'
    (batch_dir / "batch.json").write_text(raw_text, encoding="utf-8")

    with pytest.raises(ValueError, match="parseable JSON legacy batch"):
        upgrade_legacy_batch_json(batch_dir)

    assert (batch_dir / "batch.json").read_text(encoding="utf-8") == raw_text
    assert not (batch_dir / "batch.legacy.json").exists()


def test_upgrade_legacy_batch_json_refuses_mixed_legacy_and_canonical_markers(tmp_path: Path):
    batch_dir = tmp_path / "results/E50012"
    batch_dir.mkdir(parents=True)
    mixed_payload = {
        "command": "python main.py",
        "total_trials": 10,
        "batch_type": "backfill",
        "provenance": {"infra_version": "0.1.0"},
    }
    raw_text = json.dumps(mixed_payload)
    (batch_dir / "batch.json").write_text(raw_text, encoding="utf-8")

    with pytest.raises(ValueError, match="does not look like a legacy batch"):
        upgrade_legacy_batch_json(batch_dir)

    assert (batch_dir / "batch.json").read_text(encoding="utf-8") == raw_text
    assert not (batch_dir / "batch.legacy.json").exists()


def test_repo_gitignore_covers_generated_artifacts():
    patterns = (Path(__file__).resolve().parents[1] / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".pytest_cache/" in patterns
    assert "__pycache__/" in patterns
    assert "*.pyc" in patterns
    assert "src/*.egg-info/" in patterns
