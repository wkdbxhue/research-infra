import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path


def _default_codex_home() -> Path:
    return Path.home() / ".codex"


def _workspace_hash(workspace: Path) -> str:
    return hashlib.sha256(str(workspace).encode("utf-8")).hexdigest()[:16]


def _workspace_slug(workspace: Path, workspace_hash: str) -> str:
    base = workspace.name or "workspace"
    safe_base = re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-") or "workspace"
    return f"{safe_base}-{workspace_hash}"


@dataclass(frozen=True)
class TokenSaviorLayout:
    provider: str
    workspace: Path
    workspace_hash: str
    workspace_slug: str
    global_db: Path
    workspace_db: Path
    checkpoint_db: Path
    workspace_manifest: Path

    def manifest_payload(self) -> dict[str, str]:
        payload = self.to_dict()
        payload.pop("workspace_manifest")
        return payload

    def to_dict(self) -> dict[str, str]:
        return {
            "provider": self.provider,
            "workspace": str(self.workspace),
            "workspace_hash": self.workspace_hash,
            "workspace_slug": self.workspace_slug,
            "global_db": str(self.global_db),
            "workspace_db": str(self.workspace_db),
            "checkpoint_db": str(self.checkpoint_db),
            "workspace_manifest": str(self.workspace_manifest),
        }


def resolve_token_savior_layout(workspace: Path, codex_home: Path | None = None) -> TokenSaviorLayout:
    resolved_workspace = workspace.resolve()
    resolved_codex_home = (codex_home or _default_codex_home()).resolve()
    root = resolved_codex_home / "token-savior"
    workspace_hash = _workspace_hash(resolved_workspace)
    workspace_slug = _workspace_slug(resolved_workspace, workspace_hash)
    workspace_dir = root / "ws" / workspace_slug
    checkpoint_dir = root / "ckpt" / workspace_slug
    return TokenSaviorLayout(
        provider="token-savior",
        workspace=resolved_workspace,
        workspace_hash=workspace_hash,
        workspace_slug=workspace_slug,
        global_db=root / "global.sqlite",
        workspace_db=workspace_dir / "memory.sqlite",
        checkpoint_db=checkpoint_dir / "checkpoint.sqlite",
        workspace_manifest=workspace_dir / "workspace.json",
    )


def bootstrap_token_savior_layout(workspace: Path, codex_home: Path | None = None) -> TokenSaviorLayout:
    layout = resolve_token_savior_layout(workspace, codex_home=codex_home)
    layout.global_db.parent.mkdir(parents=True, exist_ok=True)
    layout.workspace_db.parent.mkdir(parents=True, exist_ok=True)
    layout.checkpoint_db.parent.mkdir(parents=True, exist_ok=True)
    layout.workspace_manifest.write_text(
        json.dumps(layout.manifest_payload(), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return layout
