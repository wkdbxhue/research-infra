import yaml
from pathlib import Path

SCAFFOLD_DIRS = (
    "results",
    "results/_cache",
    "docs",
    "src/models",
    "src/config",
    "src/engines",
)

FREEZE_POLICY_WRITES_ALLOWED = {
    "backfill-only": [
        "batch backfill",
        "reproducibility documentation",
    ],
}
SUPPORTED_FREEZE_POLICIES = tuple(FREEZE_POLICY_WRITES_ALLOWED)


def init_workspace(workspace: Path) -> dict[str, object]:
    created: list[str] = []
    for rel in SCAFFOLD_DIRS:
        target = workspace / rel
        if not target.exists():
            target.mkdir(parents=True, exist_ok=True)
            created.append(rel)
    return {"workspace": str(workspace), "created": created}


def write_freeze_file(workspace: Path, policy: str) -> Path:
    if policy not in FREEZE_POLICY_WRITES_ALLOWED:
        raise ValueError(f"unsupported freeze policy: {policy}")
    target = workspace / "results/project_freeze.yml"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {"frozen": True, "policy": policy}
    payload["writes_allowed"] = FREEZE_POLICY_WRITES_ALLOWED[policy]
    target.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return target
