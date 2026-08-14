#!/usr/bin/env python3
"""Schema/migration equivalence checker for the schema-migration-check workflow.

Compares pg_dump --schema-only outputs as normalized statement multisets, so
object ordering and formatting differences never matter.

Subcommands:
  diff <base.sql> <head.sql>
      Classify the schema change between the base branch and the PR head.
      Exit 0:  schemas identical (no migration needed)
      Exit 10: only added CREATE INDEX statements (startup-backfill territory,
               allowed in patch releases without a manifest entry)
      Exit 20: real schema change (a migration script + manifest entry is
               required, and the equivalence check must pass)

  compare <base_migrated.sql> <head.sql>
      Strict equivalence: base schema + migration script(s) + core boot must
      reproduce the fresh-install schema exactly.
      Exit 0 on equality, exit 1 (with the differing statements) otherwise.

  changed-scripts <manifest.json> <changed-file> [<changed-file> ...]
      Print, in manifest (i.e. version) order, the manifest-referenced .sql
      scripts among the files changed by the PR. This is the apply order for
      the equivalence check.

  validate-new-entries <head-manifest.json> <base-manifest.json|NONE> <plugin-x.y>
      Every manifest entry added by the PR (pluginVersion present in head but
      not in base; pass NONE when the base branch has no manifest) must be
      keyed with the PR branch's own plugin X.Y from build.gradle — a PR can
      only ship a migration for the version it belongs to.
      Exit 0 when all new entries match, exit 1 otherwise.
"""

import json
import os
import re
import sys
from collections import Counter

# Statement prefixes that carry no schema meaning in a --schema-only,
# --no-owner, --no-privileges dump.
SKIP_PREFIXES = (
    "SET ",
    "SELECT pg_catalog.set_config",
    "COMMENT ON ",
)


def statements(path):
    """Parse a pg_dump file into a multiset of normalized SQL statements."""
    stmts = Counter()
    current = []
    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("--"):
                continue
            current.append(line)
            if line.endswith(";"):
                stmt = re.sub(r"\s+", " ", " ".join(current)).strip()
                current = []
                if stmt.startswith(SKIP_PREFIXES):
                    continue
                stmts[stmt] += 1
    return stmts


def print_statements(title, stmts):
    if stmts:
        print(f"\n{title}")
        for s in sorted(stmts):
            print(f"  {s}")


def diff_sets(base_path, head_path):
    base, head = statements(base_path), statements(head_path)
    added = head - base
    removed = base - head
    return added, removed


def cmd_diff(base_path, head_path):
    added, removed = diff_sets(base_path, head_path)
    if not added and not removed:
        print("Schemas are identical: this PR does not change the DB schema.")
        return 0
    print_statements("Statements only in the PR head schema (added):", added)
    print_statements("Statements only in the base schema (removed):", removed)
    index_only = not removed and all(
        re.match(r"CREATE (UNIQUE )?INDEX ", s) for s in added
    )
    if index_only:
        print(
            "\nOnly additive CREATE INDEX changes: allowed without a migration "
            "manifest entry, PROVIDED the index is added to the startup "
            "backfill list (GeneralQueries.createTablesIfNotExists) so "
            "existing databases receive it on boot."
        )
        return 10
    print(
        "\nThis PR changes the DB schema: a migration-scripts/vX.Y.sql script "
        "and manifest.json entry are required in this PR."
    )
    return 20


def cmd_compare(migrated_path, head_path):
    added, removed = diff_sets(migrated_path, head_path)
    if not added and not removed:
        print(
            "Equivalence check passed: base schema + migration script(s) "
            "reproduces the fresh-install schema."
        )
        return 0
    print(
        "Equivalence check FAILED: applying the migration script(s) to the "
        "base schema does not reproduce the fresh-install schema. The "
        "migration script is incomplete or wrong."
    )
    print_statements("In fresh install but missing after migration:", added)
    print_statements("After migration but not in fresh install:", removed)
    return 1


def cmd_changed_scripts(manifest_path, changed_files):
    with open(manifest_path) as f:
        manifest = json.load(f)
    changed_basenames = {
        os.path.basename(p) for p in changed_files if p.endswith(".sql")
    }
    for entry in manifest["migrations"]:
        if entry["sql"] in changed_basenames:
            print(entry["sql"])
    return 0


def cmd_validate_new_entries(head_path, base_path, plugin_xy):
    with open(head_path) as f:
        head_versions = [e["pluginVersion"] for e in json.load(f)["migrations"]]
    base_versions = set()
    if base_path != "NONE":
        with open(base_path) as f:
            base_versions = {
                e["pluginVersion"] for e in json.load(f)["migrations"]
            }
    new_versions = [v for v in head_versions if v not in base_versions]
    wrong = [v for v in new_versions if v != plugin_xy]
    if wrong:
        print(
            f"New manifest entries keyed {wrong}, but this branch's plugin "
            f"version (build.gradle) is {plugin_xy}. A PR can only ship a "
            "migration for its own plugin X.Y - the core pairing is decided "
            "at release time, not here."
        )
        return 1
    if new_versions:
        print(f"New manifest entries {new_versions} match plugin version {plugin_xy}.")
    return 0


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == "diff" and len(args) == 2:
        return cmd_diff(*args)
    if cmd == "compare" and len(args) == 2:
        return cmd_compare(*args)
    if cmd == "changed-scripts" and len(args) >= 1:
        return cmd_changed_scripts(args[0], args[1:])
    if cmd == "validate-new-entries" and len(args) == 3:
        return cmd_validate_new_entries(*args)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
