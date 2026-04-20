import json
from pathlib import Path

from research_infra.memory import bootstrap_token_savior_layout, resolve_token_savior_layout


def test_resolve_token_savior_layout_uses_separate_global_workspace_and_checkpoint_roots(tmp_path: Path):
    codex_home = tmp_path / "codex-home"
    workspace = tmp_path / "workspace"
    layout = resolve_token_savior_layout(workspace, codex_home=codex_home)

    assert layout.provider == "token-savior"
    assert layout.workspace == workspace.resolve()
    assert layout.global_db == codex_home / "token-savior" / "global.sqlite"
    assert layout.workspace_db.parent.parent == codex_home / "token-savior" / "ws"
    assert layout.checkpoint_db.parent.parent == codex_home / "token-savior" / "ckpt"
    assert layout.workspace_db.name == "memory.sqlite"
    assert layout.checkpoint_db.name == "checkpoint.sqlite"
    assert layout.workspace_slug.startswith("workspace-")
    assert len(layout.workspace_hash) == 16


def test_bootstrap_token_savior_layout_creates_machine_local_manifest_only(tmp_path: Path):
    codex_home = tmp_path / "codex-home"
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    layout = bootstrap_token_savior_layout(workspace, codex_home=codex_home)

    assert layout.global_db.parent.exists()
    assert layout.workspace_db.parent.exists()
    assert layout.checkpoint_db.parent.exists()
    manifest = layout.workspace_manifest
    assert manifest.exists()
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload == {
        "provider": "token-savior",
        "workspace": str(workspace.resolve()),
        "workspace_hash": layout.workspace_hash,
        "workspace_slug": layout.workspace_slug,
        "global_db": str(layout.global_db),
        "workspace_db": str(layout.workspace_db),
        "checkpoint_db": str(layout.checkpoint_db),
    }
    assert not (workspace / ".research-infra").exists()
