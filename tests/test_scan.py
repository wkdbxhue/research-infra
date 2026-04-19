from pathlib import Path

from research_infra.scan import iter_batch_rows


def test_iter_batch_rows_reads_batch_json_fixture():
    rows = list(iter_batch_rows(Path("/home/research-infra/tests/fixtures/minimal_project/results")))
    assert rows[0]["experiment_id"] == "E50001"
