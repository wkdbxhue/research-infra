# Research Infra Contract

## Canonical Artifacts

- `src/config/*.yml`: hand-authored config truth
- `src/models/*.py`: model code plus `MODEL_META` truth
- `results/E#####/batch.json`: machine-authored batch truth
- `results/E#####/runs/R######/*`: run truth
- `results/paper*.yml`: hand-authored reporting manifests

## Source Layout

- `src/models/`: lineage-visible model wrappers and exact model entrypoints
- `src/config/`: hand-authored config truth
- `src/engines/`: reusable project-specific algorithm components
- `src/utils/`: project support code that is not model lineage or engine logic
- `src/engines/` subdirectories are project-owned and are not forced by `ri init`

## Disposable Cache

- `results/_cache/*.duckdb`: rebuildable
- `results/_cache/*.parquet`: rebuildable
- cache files are never identifiers of record

## Batch Layout

```text
results/E50001/
  batch.json
  runs/
    R000001/
      params.json
      solution.json
      model.log
      error.txt
```

## Audit Semantics

- audit checks canonical files only
- audit does not compare FS truth to a manual registry CSV
- audit may warn on legacy compatibility files, but cannot require them

## Compatibility Policy

- old repos may keep `model_registry.csv` as documentation during migration
- no new shared feature may depend on manual registry CSV updates
- `batch.json` remains the canonical machine-written metadata format
