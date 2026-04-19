from pathlib import Path


def audit_results_tree(results_root: Path) -> list[str]:
    findings: list[str] = []
    for batch_dir in sorted(results_root.glob("E*")):
        if not (batch_dir / "batch.json").exists():
            findings.append(f"missing batch.json: {batch_dir}")
    return findings
