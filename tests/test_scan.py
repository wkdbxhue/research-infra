from pathlib import Path

from research_infra.scan import iter_batch_rows


def test_iter_batch_rows_reads_batch_json_fixture():
    rows = list(iter_batch_rows(Path("/home/research-infra/tests/fixtures/minimal_project/results")))
    assert rows[0] == {
        "experiment_id": "E50001",
        "batch_id": "E50001",
        "batch_type": "original",
        "batch_dir": "/home/research-infra/tests/fixtures/minimal_project/results/E50001",
        "model_count": 1,
    }


def test_iter_batch_rows_skips_malformed_batches(tmp_path: Path):
    good_batch = tmp_path / "E50001"
    bad_batch = tmp_path / "E50002"
    good_batch.mkdir()
    bad_batch.mkdir()
    (good_batch / "batch.json").write_text(
        """{
  "experiment_id": "E50001",
  "batch_id": "E50001",
  "batch_type": "original",
  "models": ["M00001"]
}
""",
        encoding="utf-8",
    )
    (bad_batch / "batch.json").write_text("{", encoding="utf-8")

    rows = list(iter_batch_rows(tmp_path))

    assert rows == [
        {
            "experiment_id": "E50001",
            "batch_id": "E50001",
            "batch_type": "original",
            "batch_dir": str(good_batch),
            "model_count": 1,
        }
    ]
