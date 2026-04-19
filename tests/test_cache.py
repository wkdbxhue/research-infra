import csv
import json
from pathlib import Path

import duckdb

from research_infra.cache import rebuild_duckdb_cache


def _write_batch_with_index(results_root: Path, batch_id: str = "E50001") -> Path:
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
                "instances": {"M00001": ["small-00", "small-01"]},
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
        writer.writerow(
            {
                "run_id": "R000002",
                "model_name": "M00001",
                "instance_name": "small-01",
                "param_alpha": "0.6",
                "objective": "",
                "runtime": "",
                "gap": "",
                "status": "error",
                "validation_feasible": "",
            }
        )
    return batch_dir


def test_rebuild_duckdb_cache_creates_database(tmp_path: Path):
    source = Path("/home/research-infra/tests/fixtures/minimal_project/results")
    db_path = tmp_path / "registry.duckdb"
    rebuild_duckdb_cache(source, db_path)
    assert db_path.exists()


def test_rebuild_duckdb_cache_writes_expected_rows(tmp_path: Path):
    source = Path("/home/research-infra/tests/fixtures/minimal_project/results")
    db_path = tmp_path / "registry.duckdb"
    rebuild_duckdb_cache(source, db_path)

    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            "select experiment_id, batch_id, batch_type, batch_dir, model_count from batches"
        ).fetchall()

    assert rows == [
        (
            "E50001",
            "E50001",
            "original",
            "/home/research-infra/tests/fixtures/minimal_project/results/E50001",
            1,
        )
    ]


def test_rebuild_duckdb_cache_writes_runs_rows(tmp_path: Path):
    results_root = tmp_path / "results"
    _write_batch_with_index(results_root)
    db_path = tmp_path / "registry.duckdb"

    rebuild_duckdb_cache(results_root, db_path)

    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            select batch_id, experiment_id, model_name, instance_name, param_alpha,
                   objective, runtime, gap, status, validation_feasible, has_incumbent
            from runs
            order by instance_name
            """
        ).fetchall()

    assert rows == [
        ("E50001", "E50001", "M00001", "small-00", 0.4, 123.5, 9.75, 0.0, "2", True, True),
        ("E50001", "E50001", "M00001", "small-01", 0.6, None, None, None, "error", None, False),
    ]
