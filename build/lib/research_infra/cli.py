import argparse
import json
from pathlib import Path

from research_infra.audit import audit_results_tree
from research_infra.batch import backfill_batch_json
from research_infra.cache import rebuild_duckdb_cache
from research_infra.workspace import init_workspace, write_freeze_file


def main() -> int:
    parser = argparse.ArgumentParser(prog="ri")
    sub = parser.add_subparsers(dest="command", required=True)

    init_parser = sub.add_parser("init")
    init_parser.add_argument("--workspace", required=True)
    init_parser.add_argument("--json", action="store_true")

    cache_parser = sub.add_parser("cache")
    cache_sub = cache_parser.add_subparsers(dest="cache_command", required=True)
    rebuild_parser = cache_sub.add_parser("rebuild")
    rebuild_parser.add_argument("--workspace", required=False)
    rebuild_parser.add_argument("--results-root", required=True)
    rebuild_parser.add_argument("--db-path", required=True)

    audit_parser = sub.add_parser("audit")
    audit_parser.add_argument("--workspace", required=True)
    audit_parser.add_argument("--json", action="store_true")

    batch_parser = sub.add_parser("batch")
    batch_sub = batch_parser.add_subparsers(dest="batch_command", required=True)
    backfill_parser = batch_sub.add_parser("backfill")
    backfill_parser.add_argument("--workspace", required=True)
    backfill_parser.add_argument("--results-root", required=True)

    freeze_parser = sub.add_parser("freeze")
    freeze_parser.add_argument("--workspace", required=True)
    freeze_parser.add_argument("--policy", required=True)

    args = parser.parse_args()
    if args.command == "init":
        summary = init_workspace(Path(args.workspace))
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    if args.command == "cache" and args.cache_command == "rebuild":
        rebuild_duckdb_cache(Path(args.results_root), Path(args.db_path))
        return 0
    if args.command == "audit":
        findings = audit_results_tree(Path(args.workspace) / "results")
        if args.json:
            print(json.dumps({"workspace": args.workspace, "findings": findings}, indent=2, sort_keys=True))
            return 1 if findings else 0
        for finding in findings:
            print(finding)
        return 1 if findings else 0
    if args.command == "batch" and args.batch_command == "backfill":
        results_root = Path(args.workspace) / args.results_root
        for batch_dir in sorted(results_root.glob("E*")):
            if not (batch_dir / "batch.json").exists():
                backfill_batch_json(batch_dir, models=["UNKNOWN"], instances={"UNKNOWN": []})
        return 0
    if args.command == "freeze":
        write_freeze_file(Path(args.workspace), args.policy)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
