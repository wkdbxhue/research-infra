import argparse
import json
from pathlib import Path

from research_infra.audit import audit_results_tree
from research_infra.batch import backfill_batch_json, read_batch_json, upgrade_legacy_batch_json
from research_infra.cache import rebuild_duckdb_cache
from research_infra.memory import bootstrap_token_savior_layout, resolve_token_savior_layout
from research_infra.scan import EXACT_BATCH_DIR_RE
from research_infra.workspace import SUPPORTED_FREEZE_POLICIES, init_workspace, write_freeze_file


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
    backfill_parser.add_argument("--upgrade-invalid", action="store_true")

    freeze_parser = sub.add_parser("freeze")
    freeze_parser.add_argument("--workspace", required=True)
    freeze_parser.add_argument("--policy", required=True, choices=SUPPORTED_FREEZE_POLICIES)

    memory_parser = sub.add_parser("memory")
    memory_sub = memory_parser.add_subparsers(dest="memory_command", required=True)

    memory_show_parser = memory_sub.add_parser("show")
    memory_show_parser.add_argument("--workspace", required=True)
    memory_show_parser.add_argument("--codex-home", required=False)
    memory_show_parser.add_argument("--json", action="store_true")

    memory_init_parser = memory_sub.add_parser("init")
    memory_init_parser.add_argument("--workspace", required=True)
    memory_init_parser.add_argument("--codex-home", required=False)
    memory_init_parser.add_argument("--json", action="store_true")

    args = parser.parse_args()
    if args.command == "init":
        summary = init_workspace(Path(args.workspace))
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    if args.command == "cache" and args.cache_command == "rebuild":
        results_root = Path(args.results_root)
        db_path = Path(args.db_path)
        if not results_root.is_absolute() and args.workspace:
            results_root = Path(args.workspace) / results_root
        if not db_path.is_absolute() and args.workspace:
            db_path = Path(args.workspace) / db_path
        rebuild_duckdb_cache(results_root, db_path)
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
            if not batch_dir.is_dir() or not EXACT_BATCH_DIR_RE.match(batch_dir.name):
                continue
            batch_json = batch_dir / "batch.json"
            if not batch_json.exists():
                backfill_batch_json(batch_dir, models=["UNKNOWN"], instances={"UNKNOWN": []})
                continue
            if args.upgrade_invalid and read_batch_json(batch_dir) is None:
                upgrade_legacy_batch_json(batch_dir)
        return 0
    if args.command == "freeze":
        write_freeze_file(Path(args.workspace), args.policy)
        return 0
    if args.command == "memory" and args.memory_command == "show":
        layout = resolve_token_savior_layout(
            Path(args.workspace),
            codex_home=Path(args.codex_home) if args.codex_home else None,
        )
        payload = layout.to_dict()
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0
        print(payload["workspace_manifest"])
        return 0
    if args.command == "memory" and args.memory_command == "init":
        codex_home = Path(args.codex_home) if args.codex_home else None
        layout = resolve_token_savior_layout(Path(args.workspace), codex_home=codex_home)
        created: list[str] = []
        for target in [layout.global_db.parent, layout.workspace_db.parent, layout.checkpoint_db.parent]:
            if not target.exists():
                target.mkdir(parents=True, exist_ok=True)
                created.append(str(target))
        layout = bootstrap_token_savior_layout(Path(args.workspace), codex_home=codex_home)
        payload = layout.to_dict()
        payload["created"] = created
        payload["workspace_db_dir"] = str(layout.workspace_db.parent)
        payload["checkpoint_db_dir"] = str(layout.checkpoint_db.parent)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0
        print(payload["workspace_manifest"])
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
