from __future__ import annotations

import json
import platform
import subprocess
from pathlib import Path


def collect_git_provenance(repo_root: Path) -> dict[str, object]:
    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "HEAD"], text=True).strip()
    dirty = bool(subprocess.check_output(["git", "-C", str(repo_root), "status", "--porcelain"], text=True).strip())
    branch = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--abbrev-ref", "HEAD"], text=True).strip()
    return {"commit": commit, "dirty": dirty, "branch": branch}


def write_environment_evidence(target: Path, *, python_version: str, infra_version: str) -> dict[str, object]:
    payload = {
        "python_version": python_version,
        "infra_version": infra_version,
        "platform": platform.platform(),
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload
