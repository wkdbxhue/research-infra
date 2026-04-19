from pathlib import Path
from subprocess import run

from research_infra.audit import audit_results_tree


def test_audit_accepts_minimal_fixture():
    findings = audit_results_tree(Path("/home/research-infra/tests/fixtures/minimal_project/results"))
    assert findings == []


def test_cli_cache_rebuild_command(tmp_path: Path):
    result = run(
        [
            "python",
            "-m",
            "research_infra.cli",
            "cache",
            "rebuild",
            "--results-root",
            "/home/research-infra/tests/fixtures/minimal_project/results",
            "--db-path",
            str(tmp_path / "registry.duckdb"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_cli_init_command(tmp_path: Path):
    result = run(
        ["python", "-m", "research_infra.cli", "init", "--workspace", str(tmp_path), "--json"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert (tmp_path / "results").exists()


def test_cli_freeze_command(tmp_path: Path):
    result = run(
        ["python", "-m", "research_infra.cli", "freeze", "--workspace", str(tmp_path), "--policy", "backfill-only"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert (tmp_path / "results/project_freeze.yml").exists()


def test_cli_audit_command():
    result = run(
        [
            "python",
            "-m",
            "research_infra.cli",
            "audit",
            "--workspace",
            "/home/research-infra/tests/fixtures/minimal_project",
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_cli_batch_backfill_command(tmp_path: Path):
    batch_dir = tmp_path / "results" / "E50001"
    batch_dir.mkdir(parents=True)

    result = run(
        [
            "python",
            "-m",
            "research_infra.cli",
            "batch",
            "backfill",
            "--workspace",
            str(tmp_path),
            "--results-root",
            "results",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert (batch_dir / "batch.json").exists()
