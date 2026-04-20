from __future__ import annotations

import json
import platform
import subprocess
from pathlib import Path


def collect_git_provenance(repo_root: Path) -> dict[str, object]:
    status = subprocess.check_output(
        ["git", "-C", str(repo_root), "status", "--porcelain=v2", "--branch"],
        text=True,
    )

    commit: str | None = None
    branch: str | None = None
    dirty = False

    for line in status.splitlines():
        if line.startswith("# branch.oid "):
            commit = line.removeprefix("# branch.oid ").strip()
            if commit == "(initial)":
                commit = None
        elif line.startswith("# branch.head "):
            branch = line.removeprefix("# branch.head ").strip()
            if branch in {"detached", "(detached)"}:
                branch = "HEAD"
        elif line and not line.startswith("#"):
            dirty = True

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
