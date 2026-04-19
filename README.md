# research-infra

Filesystem-first OR research infrastructure.

## Planned P1 Commands

- `ri init`
- `ri batch new`
- `ri batch backfill`
- `ri cache rebuild`
- `ri audit`

## Contract Summary

- canonical truth in filesystem artifacts
- cache is rebuildable
- machine-written batch metadata uses `batch.json`

## Non-goals for P0

- no shared reporting module
- no shared statistics module
- no template generator
- no patch-model workflow logic
