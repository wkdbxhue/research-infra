import duckdb
from pathlib import Path

from research_infra.scan import iter_batch_rows


def rebuild_duckdb_cache(results_root: Path, db_path: Path) -> None:
    rows = list(iter_batch_rows(results_root))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute("drop table if exists batches")
    conn.execute("create table batches(experiment_id varchar, batch_id varchar, batch_type varchar, batch_dir varchar, model_count integer)")
    for row in rows:
        conn.execute(
            "insert into batches values (?, ?, ?, ?, ?)",
            [row["experiment_id"], row["batch_id"], row["batch_type"], row["batch_dir"], row["model_count"]],
        )
    conn.close()
