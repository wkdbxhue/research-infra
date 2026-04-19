import pytest
from pydantic import ValidationError

from research_infra.schema import BatchMeta, BatchType, GitProvenance


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
            provenance={"infra_version": "0.1.0"},
        )
