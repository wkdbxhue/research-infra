import json
import os
from pathlib import Path
from subprocess import run


CLI_ENV = {
    **os.environ,
    "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
}


def test_cli_memory_show_reports_two_layer_layout(tmp_path: Path):
    workspace = tmp_path / "workspace"
    codex_home = tmp_path / "codex-home"
    result = run(
        [
            "python",
            "-m",
            "research_infra.cli",
            "memory",
            "show",
            "--workspace",
            str(workspace),
            "--codex-home",
            str(codex_home),
            "--json",
        ],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["provider"] == "token-savior"
    assert payload["workspace"] == str(workspace.resolve())
    assert payload["global_db"] == str(codex_home / "token-savior" / "global.sqlite")
    assert payload["workspace_db"].startswith(str(codex_home / "token-savior" / "ws"))
    assert payload["checkpoint_db"].startswith(str(codex_home / "token-savior" / "ckpt"))
    assert not (codex_home / "token-savior").exists()


def test_cli_memory_init_bootstraps_machine_local_layout(tmp_path: Path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    codex_home = tmp_path / "codex-home"
    result = run(
        [
            "python",
            "-m",
            "research_infra.cli",
            "memory",
            "init",
            "--workspace",
            str(workspace),
            "--codex-home",
            str(codex_home),
            "--json",
        ],
        check=False,
        capture_output=True,
        env=CLI_ENV,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["created"] == [
        str(codex_home / "token-savior"),
        payload["workspace_db_dir"],
        payload["checkpoint_db_dir"],
    ]
    assert Path(payload["global_db"]).parent.exists()
    assert Path(payload["workspace_db_dir"]).exists()
    assert Path(payload["checkpoint_db_dir"]).exists()
    assert Path(payload["workspace_manifest"]).exists()
    assert not (workspace / ".research-infra").exists()
