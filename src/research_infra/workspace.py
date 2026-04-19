import yaml
from pathlib import Path


def init_workspace(workspace: Path) -> dict[str, object]:
    created: list[str] = []
    for rel in ["results", "results/_cache", "docs", "src/models", "src/config"]:
        target = workspace / rel
        if not target.exists():
            target.mkdir(parents=True, exist_ok=True)
            created.append(rel)
    return {"workspace": str(workspace), "created": created}


def write_freeze_file(workspace: Path, policy: str) -> Path:
    target = workspace / "results/project_freeze.yml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump({"frozen": True, "policy": policy}, sort_keys=False), encoding="utf-8")
    return target
