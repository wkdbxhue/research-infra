import json
from pathlib import Path
from typing import Iterator


def iter_batch_rows(results_root: Path) -> Iterator[dict[str, object]]:
    for batch_dir in sorted(results_root.glob("E*")):
        batch_json = batch_dir / "batch.json"
        if not batch_json.exists():
            continue
        payload = json.loads(batch_json.read_text(encoding="utf-8"))
        yield {
            "experiment_id": payload["experiment_id"],
            "batch_id": payload["batch_id"],
            "batch_type": payload["batch_type"],
            "batch_dir": str(batch_dir),
            "model_count": len(payload.get("models", [])),
        }
