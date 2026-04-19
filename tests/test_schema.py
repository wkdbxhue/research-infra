import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from research_infra.schema import BatchMeta, BatchType, GitProvenance


ROOT = Path(__file__).resolve().parents[1]


def test_batch_type_enum_is_closed():
    assert BatchType.ORIGINAL.value == "original"
    assert BatchType.BACKFILL.value == "backfill"


def test_batch_meta_rejects_missing_models():
    with pytest.raises(ValidationError):
        BatchMeta(
            experiment_id="E50001",
            batch_id="E50001",
            batch_type="original",
            created_at="2026-04-19T00:00:00+00:00",
            models=[],
            instances={},
            git=GitProvenance(commit="0" * 40, dirty=False),
            environment={},
            provenance={"infra_version": "0.1.0"},
        )


def test_batch_meta_parses_canonical_fixture():
    payload = json.loads(
        (ROOT / "tests/fixtures/minimal_project/results/E50001/batch.json").read_text(
            encoding="utf-8"
        )
    )

    batch = BatchMeta.model_validate(payload)

    assert batch.experiment_id == "E50001"
