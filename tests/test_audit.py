from pathlib import Path
from subprocess import run

from research_infra.audit import audit_results_tree


def test_audit_accepts_minimal_fixture():
    findings = audit_results_tree(Path("/home/research-infra/tests/fixtures/minimal_project/results"))
    assert findings == []
