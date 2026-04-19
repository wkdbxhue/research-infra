import json
import re
from pathlib import Path

from pydantic import ValidationError

from research_infra.schema import BatchMeta

EXACT_BATCH_DIR_RE = re.compile(r"^E\d{5}$")


def audit_results_tree(results_root: Path) -> list[str]:
    findings: list[str] = []
    for batch_dir in sorted(results_root.glob("E*")):
        if not batch_dir.is_dir() or not EXACT_BATCH_DIR_RE.match(batch_dir.name):
            continue
        batch_json = batch_dir / "batch.json"
        if not batch_json.exists():
            findings.append(f"missing batch.json: {batch_dir}")
            continue
        try:
            payload = json.loads(batch_json.read_text(encoding="utf-8"))
            BatchMeta.model_validate(payload)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
            findings.append(f"invalid batch.json: {batch_json} ({exc})")
    return findings
