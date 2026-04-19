from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import fcntl

from research_infra.schema import BatchMeta

EID_RE = re.compile(r"^E(\d{5})$")
INFRA_VERSION = "0.1.0"
EPOCH_TIMESTAMP = "1970-01-01T00:00:00+00:00"
LEGACY_BACKUP_NAME = "batch.legacy.json"
CANONICAL_KEYS = {
    "experiment_id",
    "batch_id",
    "batch_type",
    "created_at",
    "models",
    "instances",
    "git",
    "environment",
    "provenance",
}
CANONICAL_ONLY_KEYS = {"batch_type", "provenance"}
LEGACY_ONLY_KEYS = {"command", "total_trials", "execution_policy", "migration"}
LEGACY_COMPAT_KEYS = {
    "experiment_id",
    "batch_id",
    "created_at",
    "models",
    "instances",
    "git",
    "environment",
}


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


def read_batch_json(batch_dir: Path) -> dict[str, object] | None:
    target = batch_dir / "batch.json"
    if not target.exists():
        return None
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
        BatchMeta.model_validate(payload)
    except (json.JSONDecodeError, ValueError, TypeError):
        return None
    return payload


def _valid_eid(value: Any) -> str | None:
    if isinstance(value, str) and EID_RE.match(value):
        return value
    return None


def _usable_models(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    models = [item for item in value if isinstance(item, str) and item]
    return models or None


def _usable_instances(value: Any) -> dict[str, list[str]] | None:
    if not isinstance(value, dict):
        return None
    instances: dict[str, list[str]] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            continue
        if not isinstance(item, list):
            continue
        items = [entry for entry in item if isinstance(entry, str)]
        instances[key] = items
    return instances or None


def _legacy_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _is_legacy_shaped_payload(payload: dict[str, Any]) -> bool:
    keys = set(payload)
    if keys & CANONICAL_ONLY_KEYS:
        return False
    legacy_markers = keys & LEGACY_ONLY_KEYS
    if not legacy_markers:
        return False
    return keys <= (LEGACY_COMPAT_KEYS | LEGACY_ONLY_KEYS)


def _canonical_backfill_payload(
    batch_dir: Path,
    *,
    models: list[str],
    instances: dict[str, list[str]],
    legacy_payload: dict[str, Any] | None = None,
    from_legacy_backup: bool = False,
) -> dict[str, object]:
    legacy_payload = legacy_payload or {}
    legacy_git = _legacy_mapping(legacy_payload.get("git"))
    git_payload: dict[str, object] = {
        "commit": legacy_git.get("commit") if isinstance(legacy_git.get("commit"), str) else None,
        "dirty": legacy_git.get("dirty") if isinstance(legacy_git.get("dirty"), bool) else True,
    }
    if isinstance(legacy_git.get("branch"), str):
        git_payload["branch"] = legacy_git.get("branch")
    payload: dict[str, object] = {
        "experiment_id": _valid_eid(legacy_payload.get("experiment_id")) or batch_dir.name,
        "batch_id": _valid_eid(legacy_payload.get("batch_id")) or batch_dir.name,
        "batch_type": "backfill",
        "created_at": legacy_payload.get("created_at")
        if isinstance(legacy_payload.get("created_at"), str) and legacy_payload.get("created_at")
        else EPOCH_TIMESTAMP,
        "models": models,
        "instances": instances,
        "git": git_payload,
        "environment": _legacy_mapping(legacy_payload.get("environment")),
        "provenance": {
            "infra_version": INFRA_VERSION,
            "backfilled": True,
        },
    }
    if from_legacy_backup:
        payload["provenance"]["legacy_backup"] = LEGACY_BACKUP_NAME
        payload["provenance"]["legacy_source"] = "upgraded-invalid-batch-json"
    return payload


def backfill_batch_json(batch_dir: Path, *, models: list[str], instances: dict[str, list[str]]) -> dict[str, object]:
    payload = _canonical_backfill_payload(batch_dir, models=models, instances=instances)
    write_batch_json(batch_dir / "batch.json", payload)
    return payload


def upgrade_legacy_batch_json(batch_dir: Path) -> dict[str, object]:
    target = batch_dir / "batch.json"
    if not target.exists():
        raise FileNotFoundError(target)

    raw_text = target.read_text(encoding="utf-8")
    backup_target = batch_dir / LEGACY_BACKUP_NAME
    if backup_target.exists():
        raise ValueError(f"existing legacy backup at {backup_target}; refusing to overwrite invalid batch.json")
    try:
        parsed_payload = json.loads(raw_text)
    except json.JSONDecodeError:
        raise ValueError(f"{target} must be a parseable JSON legacy batch; refusing upgrade")
    if not isinstance(parsed_payload, dict) or not _is_legacy_shaped_payload(parsed_payload):
        raise ValueError(f"{target} does not look like a legacy batch; refusing upgrade")
    legacy_payload = parsed_payload

    backup_target.write_text(raw_text, encoding="utf-8")

    payload = _canonical_backfill_payload(
        batch_dir,
        models=_usable_models(legacy_payload.get("models")) or ["UNKNOWN"],
        instances=_usable_instances(legacy_payload.get("instances")) or {"UNKNOWN": []},
        legacy_payload=legacy_payload,
        from_legacy_backup=True,
    )
    BatchMeta.model_validate(payload)
    write_batch_json(target, payload)
    return payload
