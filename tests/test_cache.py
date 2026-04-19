from pathlib import Path

from research_infra.cache import rebuild_duckdb_cache


def test_rebuild_duckdb_cache_creates_database(tmp_path: Path):
    source = Path("/home/research-infra/tests/fixtures/minimal_project/results")
    db_path = tmp_path / "registry.duckdb"
    rebuild_duckdb_cache(source, db_path)
    assert db_path.exists()
