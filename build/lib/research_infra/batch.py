from __future__ import annotations

import json
import re
from pathlib import Path

import fcntl


EID_RE = re.compile(r"^E(\d{5})$")


def allocate_experiment_id(results_root: Path, *, start: int = 50001) -> str:
    results_root.mkdir(parents=True, exist_ok=True)
    lock_path = results_root / ".id_lock"
    with open(lock_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        highest = start - 1
        for child in results_root.iterdir():
            match = EID_RE.match(child.name)
            if match:
                highest = max(highest, int(match.group(1)))
        return f"E{highest + 1:05d}"


def write_batch_json(target: Path, payload: dict[str, object]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def backfill_batch_json(batch_dir: Path, *, models: list[str], instances: dict[str, list[str]]) -> dict[str, object]:
    payload = {
        "experiment_id": batch_dir.name,
        "batch_id": batch_dir.name,
        "batch_type": "backfill",
        "created_at": "1970-01-01T00:00:00+00:00",
        "models": models,
        "instances": instances,
        "git": {"commit": None, "dirty": True},
        "provenance": {"infra_version": "0.1.0", "backfilled": True},
    }
    write_batch_json(batch_dir / "batch.json", payload)
    return payload
