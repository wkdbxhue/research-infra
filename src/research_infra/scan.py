import json
import re
from pathlib import Path
from typing import Iterator

from research_infra.schema import BatchMeta

EXACT_BATCH_DIR_RE = re.compile(r"^E\d{5}$")


def iter_batch_rows(results_root: Path) -> Iterator[dict[str, object]]:
    for batch_dir in sorted(results_root.glob("E*")):
        if not batch_dir.is_dir() or not EXACT_BATCH_DIR_RE.match(batch_dir.name):
            continue
        batch_json = batch_dir / "batch.json"
        if not batch_json.exists():
            continue
        try:
            payload = json.loads(batch_json.read_text(encoding="utf-8"))
            batch = BatchMeta.model_validate(payload)
            yield {
                "experiment_id": batch.experiment_id,
                "batch_id": batch.batch_id,
                "batch_type": batch.batch_type.value,
                "batch_dir": str(batch_dir),
                "model_count": len(batch.models),
            }
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
