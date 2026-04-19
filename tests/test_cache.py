from pathlib import Path

from research_infra.cache import rebuild_duckdb_cache


def test_rebuild_duckdb_cache_creates_database(tmp_path: Path):
    source = Path("/home/research-infra/tests/fixtures/minimal_project/results")
    db_path = tmp_path / "registry.duckdb"
    rebuild_duckdb_cache(source, db_path)
    assert db_path.exists()


def test_rebuild_duckdb_cache_writes_expected_rows(tmp_path: Path):
    source = Path("/home/research-infra/tests/fixtures/minimal_project/results")
    db_path = tmp_path / "registry.duckdb"
    rebuild_duckdb_cache(source, db_path)

    import duckdb

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
