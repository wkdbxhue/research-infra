import json
from pathlib import Path


ROOT = Path("/home/research-infra")


def test_minimal_contract_fixture_exists():
    batch_dir = ROOT / "tests/fixtures/minimal_project/results/E50001"
    assert (batch_dir / "batch.json").exists()
    assert (batch_dir / "runs/R000001/params.json").exists()
    assert (batch_dir / "runs/R000001/solution.json").exists()

    payload = json.loads((batch_dir / "batch.json").read_text(encoding="utf-8"))
    assert payload["experiment_id"] == "E50001"
    assert payload["batch_type"] == "original"
    assert payload["git"]["dirty"] is False


def test_cache_fixture_is_separate_from_canonical_batch_tree():
    cache_dir = ROOT / "tests/fixtures/minimal_project/results/_cache"
    assert cache_dir.exists()
    assert not (cache_dir / "batch.json").exists()
