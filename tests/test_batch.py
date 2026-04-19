import json
from pathlib import Path

from research_infra.batch import allocate_experiment_id, backfill_batch_json, write_batch_json


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


def test_repo_gitignore_covers_generated_artifacts():
    patterns = (Path(__file__).resolve().parents[1] / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".pytest_cache/" in patterns
    assert "__pycache__/" in patterns
    assert "*.pyc" in patterns
    assert "src/*.egg-info/" in patterns
