from pathlib import Path


USE_GUIDE = Path(__file__).resolve().parents[1] / "docs" / "use-guide.md"
EXPECTED_USE_GUIDE = """# research-infra use guide

## New project bootstrap

Install the pinned release from GitHub:

```bash
pip install "research-infra @ git+https://github.com/wkdbxhue/research-infra.git@v0.1.1"
```

Pin the same tag in `pyproject.toml`:

```toml
dependencies = [
  "research-infra @ git+https://github.com/wkdbxhue/research-infra.git@v0.1.1"
]
```

Initialize the workspace root:

```bash
ri init --workspace .
```

This creates:

- `results/`
- `results/_cache/`
- `docs/`
- `src/models/`
- `src/config/`

Bootstrap the machine-local two-layer memory layout for `token-savior`:

```bash
ri memory init --workspace . --json
```

This creates machine-local state under `~/.codex/token-savior/` and keeps the repo tree clean.

## Core commands

- `ri memory show --workspace . --json`
- `ri audit --workspace . --json`
- `ri cache rebuild --workspace . --results-root results --db-path results/_cache/registry.duckdb`
- `ri freeze --workspace . --policy backfill-only`

## Existing project retrofit

Backfill canonical batch metadata in an existing results tree:

```bash
ri batch backfill --workspace . --results-root results --upgrade-invalid
```

Use retrofit when the repo already has `results/E#####/` history and needs canonical `batch.json` files without restructuring the runtime code.
"""


def test_use_guide_matches_the_new_project_bootstrap_contract():
    assert USE_GUIDE.read_text(encoding="utf-8") == EXPECTED_USE_GUIDE
