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
