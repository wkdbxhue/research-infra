from pathlib import Path

import pytest
from pydantic import ValidationError

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
  "created_at": "2026-04-19T00:00:00+00:00",
  "models": ["M00001"],
  "instances": {"M00001": ["small-00"]},
  "git": {"commit": "0123456789abcdef0123456789abcdef01234567", "dirty": false},
  "provenance": {"infra_version": "0.0.0"}
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


def test_iter_batch_rows_raises_on_schema_invalid_batch(tmp_path: Path):
    batch_dir = tmp_path / "E50001"
    batch_dir.mkdir()
    (batch_dir / "batch.json").write_text(
        """{
  "experiment_id": "E50001",
  "batch_id": "E50001",
  "created_at": "2026-04-19T00:00:00+00:00",
  "models": ["M00001"],
  "instances": {"M00001": ["small-00"]},
  "git": {"commit": "0123456789abcdef0123456789abcdef01234567", "dirty": false},
  "provenance": {"infra_version": "0.0.0"}
}
""",
        encoding="utf-8",
    )

    with pytest.raises(ValidationError):
        list(iter_batch_rows(tmp_path))


def test_iter_batch_rows_raises_on_wrong_typed_models(tmp_path: Path):
    batch_dir = tmp_path / "E50001"
    batch_dir.mkdir()
    (batch_dir / "batch.json").write_text(
        """{
  "experiment_id": "E50001",
  "batch_id": "E50001",
  "batch_type": "original",
  "created_at": "2026-04-19T00:00:00+00:00",
  "models": "M00001",
  "instances": {"M00001": ["small-00"]},
  "git": {"commit": "0123456789abcdef0123456789abcdef01234567", "dirty": false},
  "provenance": {"infra_version": "0.0.0"}
}
""",
        encoding="utf-8",
    )

    with pytest.raises(ValidationError):
        list(iter_batch_rows(tmp_path))
