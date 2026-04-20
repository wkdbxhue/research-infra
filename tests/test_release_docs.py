from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
INIT_FILE = ROOT / "src" / "research_infra" / "__init__.py"
BATCH_FILE = ROOT / "src" / "research_infra" / "batch.py"
CHANGELOG = ROOT / "CHANGELOG.md"
UPGRADE_DOC = ROOT / "docs" / "upgrade-0.1.1.md"
RELEASE_VERSION = "0.1.1"
CHANGELOG_ENTRY = "## 0.1.1 - 2026-04-20"
UPGRADE_TITLE = "# research-infra 0.1.1 upgrade"
EXPECTED_CHANGELOG = """# Changelog

## 0.1.1 - 2026-04-20

- fix `ri freeze` so `backfill-only` writes the full freeze manifest contract
- reject unsupported freeze policies at the CLI boundary
- add a new-project-first use guide with GitHub tag-pinned install

## 0.1.0 - 2026-04-19

- initial filesystem-first contract release
- mvp CLI: init, batch backfill, cache rebuild, audit, freeze
- no reporting or stats modules yet
"""
EXPECTED_UPGRADE_DOC = """# research-infra 0.1.1 upgrade

This is a patch release.

- keep `v0.1.0` for historical reproduction
- use `v0.1.1` for new installs
- `ri freeze --policy backfill-only` now writes the full freeze manifest contract
- unsupported freeze policies are rejected
"""


def _extract(pattern: str, text: str, *, label: str) -> str:
    match = re.search(pattern, text, re.MULTILINE)
    assert match is not None, f"missing {label}"
    return match.group(1)


def test_release_artifacts_share_the_same_version():
    versions = {
        "pyproject.toml": _extract(r'^version = "([^"]+)"$', PYPROJECT.read_text(encoding="utf-8"), label="pyproject version"),
        "src/research_infra/__init__.py": _extract(
            r'^__version__ = "([^"]+)"$', INIT_FILE.read_text(encoding="utf-8"), label="package version"
        ),
        "src/research_infra/batch.py": _extract(
            r'^INFRA_VERSION = "([^"]+)"$', BATCH_FILE.read_text(encoding="utf-8"), label="batch infra version"
        ),
        "CHANGELOG.md": _extract(r"^## ([0-9]+\.[0-9]+\.[0-9]+) - \d{4}-\d{2}-\d{2}$", CHANGELOG.read_text(encoding="utf-8"), label="changelog entry"),
        "docs/upgrade-0.1.1.md": _extract(r"^# research-infra ([0-9]+\.[0-9]+\.[0-9]+) upgrade$", UPGRADE_DOC.read_text(encoding="utf-8"), label="upgrade doc title"),
    }

    assert set(versions.values()) == {RELEASE_VERSION}
    assert CHANGELOG_ENTRY in CHANGELOG.read_text(encoding="utf-8")
    assert UPGRADE_TITLE in UPGRADE_DOC.read_text(encoding="utf-8")


def test_release_docs_pin_the_opt_in_contract_text():
    changelog_text = CHANGELOG.read_text(encoding="utf-8")
    upgrade_text = UPGRADE_DOC.read_text(encoding="utf-8")

    assert changelog_text == EXPECTED_CHANGELOG
    assert upgrade_text == EXPECTED_UPGRADE_DOC
