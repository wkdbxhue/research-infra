import csv

import duckdb
from pathlib import Path

from research_infra.scan import iter_batch_rows


def _read_text(row: dict[str, str], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = value.strip()
        if text:
            return text
    return None


def _read_float(row: dict[str, str], *keys: str) -> float | None:
    text = _read_text(row, *keys)
    if text is None or text.lower() in {"nan", "none", "null"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _read_bool(row: dict[str, str], *keys: str) -> bool | None:
    text = _read_text(row, *keys)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    return None


def _iter_run_rows(batch_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    run_rows: list[dict[str, object]] = []
    for batch_row in batch_rows:
        batch_dir = Path(str(batch_row["batch_dir"]))
        index_path = batch_dir / "index.csv"
        if not index_path.exists():
            continue
        try:
            with index_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    raw_objective = _read_float(row, "objective")
                    validation_feasible = _read_bool(row, "validation_feasible", "is_feasible")
                    has_incumbent = _read_bool(row, "has_incumbent")
                    if has_incumbent is None:
                        has_incumbent = raw_objective is not None
                    run_rows.append(
                        {
                            "batch_id": batch_row["batch_id"],
                            "experiment_id": batch_row["experiment_id"],
                            "model_name": _read_text(row, "model_name", "model", "param_model"),
                            "instance_name": _read_text(row, "instance_name", "instance", "param_instance"),
                            "param_alpha": _read_float(row, "param_alpha", "alpha"),
                            "objective": None if validation_feasible is False else raw_objective,
                            "runtime": _read_float(row, "runtime", "computation_time"),
                            "gap": _read_float(row, "gap"),
                            "status": _read_text(row, "status"),
                            "validation_feasible": validation_feasible,
                            "has_incumbent": has_incumbent,
                        }
                    )
        except (OSError, UnicodeDecodeError, csv.Error):
            continue
    return run_rows


def rebuild_duckdb_cache(results_root: Path, db_path: Path) -> None:
    batch_rows = list(iter_batch_rows(results_root))
    run_rows = _iter_run_rows(batch_rows)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("drop table if exists batches")
        conn.execute("create table batches(experiment_id varchar, batch_id varchar, batch_type varchar, batch_dir varchar, model_count integer)")
        for row in batch_rows:
            conn.execute(
                "insert into batches values (?, ?, ?, ?, ?)",
                [row["experiment_id"], row["batch_id"], row["batch_type"], row["batch_dir"], row["model_count"]],
            )
        conn.execute("drop table if exists runs")
        conn.execute(
            """
            create table runs(
                batch_id varchar,
                experiment_id varchar,
                model_name varchar,
                instance_name varchar,
                param_alpha double,
                objective double,
                runtime double,
                gap double,
                status varchar,
                validation_feasible boolean,
                has_incumbent boolean
            )
            """
        )
        for row in run_rows:
            conn.execute(
                "insert into runs values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    row["batch_id"],
                    row["experiment_id"],
                    row["model_name"],
                    row["instance_name"],
                    row["param_alpha"],
                    row["objective"],
                    row["runtime"],
                    row["gap"],
                    row["status"],
                    row["validation_feasible"],
                    row["has_incumbent"],
                ],
            )
