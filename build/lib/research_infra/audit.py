import json
from pathlib import Path

from pydantic import ValidationError

from research_infra.schema import BatchMeta


def audit_results_tree(results_root: Path) -> list[str]:
    findings: list[str] = []
    for batch_dir in sorted(results_root.glob("E*")):
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
