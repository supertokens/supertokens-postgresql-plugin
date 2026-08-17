#!/usr/bin/env python3
"""Schema/migration equivalence checker for the schema-migration-check workflow.

Compares pg_dump --schema-only outputs as normalized statement multisets, so
object ordering and formatting differences never matter.

Subcommands:
  diff <base.sql> <head.sql>
      Classify the schema change between the base branch and the PR head.
      Exit 0:  schemas identical (no migration needed)
      Exit 10: index-only change (only CREATE INDEX statements added and/or
               removed) - the ONLY kind of change a patch release may carry
               (still needs a manifest entry + script)
      Exit 20: any other schema change (minor releases only)
      In both non-zero cases a manifest entry + script are required and the
      equivalence check must pass.

  compare <base_migrated.sql> <head.sql>
      Strict equivalence: base schema + migration script(s) + core boot must
      reproduce the fresh-install schema exactly.
      Exit 0 on equality, exit 1 (with the differing statements) otherwise.

  changed-scripts <manifest.json> <changed-file> [<changed-file> ...]
      Print, in manifest (i.e. version) order, the manifest-referenced .sql
      scripts among the files changed by the PR. This is the apply order for
      the equivalence check.

  validate-new-entries <head-manifest.json> <base-manifest.json|NONE> <plugin-x.y.z>
      Every manifest entry added by the PR (pluginVersion present in head but
      not in base; pass NONE when the base branch has no manifest) must be
      keyed with the PR branch's own plugin X.Y.Z from build.gradle — a PR
      can only ship a migration for the version it belongs to. Also validates
      the manifest as a whole: entries sorted strictly ascending, no
      duplicates, `sql` named v<pluginVersion>.sql and present on disk.
      Exit 0 when valid, exit 1 otherwise.

  patch-script-check <sql-file> [<sql-file> ...]
      Patch releases (Z > 0) may only ADD or DROP indexes, and the script must
      do so without locking: every statement in the given scripts must be a
      `CREATE [UNIQUE] INDEX CONCURRENTLY IF NOT EXISTS ...` or a
      `DROP INDEX CONCURRENTLY IF EXISTS ...`. (The workflow
      separately requires the schema diff to be index-only and proves the new
      core also creates the indexes itself at startup, which is what makes
      the change safe for an in-place patch rollout.)
      Exit 0 when every statement conforms, exit 1 otherwise.

  check-changelog <CHANGELOG.md> <version-x.y.z> <sql-file> [<sql-file> ...]
      The changelog section for the given version (`## [x.y.z]`) must have a
      `### Migration` heading and mention every given migration script file
      name, so a release's migration is discoverable from its release notes.
      Used for the plugin's own CHANGELOG at PR time and, by the core release
      workflow, for supertokens-core's CHANGELOG at release time.
      Exit 0 when satisfied, exit 1 otherwise.
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
    index_only = all(
        re.match(r"CREATE (UNIQUE )?INDEX ", s) for s in list(added) + list(removed)
    )
    if index_only:
        print(
            "\nIndex-only change (indexes added and/or removed). A "
            "migration-scripts/vX.Y.Z.sql script (CREATE INDEX CONCURRENTLY IF "
            "NOT EXISTS / DROP INDEX CONCURRENTLY IF EXISTS statements) and a "
            "manifest.json entry keyed by this branch's plugin version are "
            "required; the same create/drop must also be in the startup "
            "backfill list (GeneralQueries.createTablesIfNotExists) so "
            "existing databases receive it on boot. This is the only kind of "
            "schema change a patch release may carry."
        )
        return 10
    print(
        "\nThis PR changes the DB schema: a migration-scripts/vX.Y.Z.sql script "
        "and manifest.json entry keyed by this branch's plugin version are "
        "required in this PR. Such changes may only ship in a MINOR release "
        "(patch releases may only add/drop indexes)."
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


PATCH_STATEMENT_RE = re.compile(
    r"^(CREATE (UNIQUE )?INDEX CONCURRENTLY IF NOT EXISTS \S+ ON |DROP INDEX CONCURRENTLY IF EXISTS \S+)",
    re.I,
)


def cmd_patch_script_check(sql_files):
    bad = []
    for path in sql_files:
        for stmt in statements(path):
            if not PATCH_STATEMENT_RE.match(stmt):
                bad.append((path, stmt))
    if bad:
        print(
            "Patch releases may only ADD or DROP indexes, CONCURRENTLY: every "
            "statement in a patch migration script must be "
            "`CREATE [UNIQUE] INDEX CONCURRENTLY IF NOT EXISTS ...` or "
            "`DROP INDEX CONCURRENTLY IF EXISTS ...`. Non-conforming statements:"
        )
        for path, stmt in bad:
            print(f"  {path}: {stmt}")
        print(
            "Anything else (columns, tables, constraints, non-concurrent index "
            "builds/drops) must ship in a minor release."
        )
        return 1
    print(f"Patch script check passed for {', '.join(sql_files)}: CREATE/DROP INDEX CONCURRENTLY only.")
    return 0


VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


def version_key(version):
    m = VERSION_RE.match(version)
    if not m:
        raise ValueError(f"not an X.Y.Z version: {version!r}")
    return tuple(int(g) for g in m.groups())


def cmd_validate_new_entries(head_path, base_path, plugin_xyz):
    if not VERSION_RE.match(plugin_xyz):
        print(f"Plugin version {plugin_xyz!r} (from build.gradle) is not X.Y.Z.")
        return 1
    with open(head_path) as f:
        head_entries = json.load(f)["migrations"]
    head_versions = [e["pluginVersion"] for e in head_entries]

    # Whole-manifest invariants: X.Y.Z keys, strictly ascending, unique,
    # script named after the key and present next to the manifest.
    problems = []
    for e in head_entries:
        v = e["pluginVersion"]
        if not VERSION_RE.match(v):
            problems.append(f"entry {v!r}: pluginVersion is not X.Y.Z")
            continue
        if not VERSION_RE.match(e.get("coreVersion", "")):
            problems.append(f"entry {v}: coreVersion is not X.Y.Z")
        expected_sql = f"v{v}.sql"
        if e.get("sql") != expected_sql:
            problems.append(f"entry {v}: sql must be {expected_sql}, got {e.get('sql')!r}")
        elif not os.path.exists(os.path.join(os.path.dirname(head_path), expected_sql)):
            problems.append(f"entry {v}: {expected_sql} does not exist")
    if not problems:
        keys = [version_key(v) for v in head_versions]
        if keys != sorted(set(keys)):
            problems.append(
                "entries must be sorted by pluginVersion strictly ascending "
                f"(got {head_versions})"
            )
    if problems:
        print("manifest.json is invalid:")
        for p in problems:
            print(f"  - {p}")
        return 1

    base_versions = set()
    if base_path != "NONE":
        with open(base_path) as f:
            base_versions = {
                e["pluginVersion"] for e in json.load(f)["migrations"]
            }
    new_entries = [e for e in head_entries if e["pluginVersion"] not in base_versions]
    new_versions = [e["pluginVersion"] for e in new_entries]
    wrong = [v for v in new_versions if v != plugin_xyz]
    if wrong:
        print(
            f"New manifest entries keyed {wrong}, but this branch's plugin "
            f"version (build.gradle) is {plugin_xyz}. A PR can only ship a "
            "migration for its own plugin X.Y.Z - the core pairing is decided "
            "at release time, not here."
        )
        return 1
    if new_versions:
        kind = "patch" if version_key(plugin_xyz)[2] != 0 else "minor"
        print(
            f"New manifest entries {new_versions} match plugin version "
            f"{plugin_xyz} ({kind} release)."
        )
    return 0


def cmd_check_changelog(changelog_path, version, sql_files):
    with open(changelog_path) as f:
        lines = f.read().splitlines()
    heading = re.compile(r"^##\s+\[?" + re.escape(version) + r"\]?(\s|$)")
    start = next((i for i, l in enumerate(lines) if heading.match(l)), None)
    if start is None:
        print(
            f"{changelog_path}: no `## [{version}]` section found. Add the "
            f"release section with a `### Migration` subsection linking "
            f"{', '.join(sql_files)}."
        )
        return 1
    end = next(
        (i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")),
        len(lines),
    )
    section = "\n".join(lines[start:end])
    problems = []
    if not re.search(r"^###\s+Migration", section, re.M):
        problems.append("missing a `### Migration` subsection")
    for sql in sql_files:
        if os.path.basename(sql) not in section:
            problems.append(f"does not mention the migration script `{sql}`")
    if problems:
        print(f"{changelog_path}: the `## [{version}]` section " + "; ".join(problems) + ".")
        print(
            "Every release that ships a DB migration must document it in its "
            "changelog section under `### Migration`, referencing the "
            "canonical script (e.g. `migration-scripts/vX.Y.Z.sql` in "
            "supertokens-postgresql-plugin) so the migration is discoverable "
            "from the release notes."
        )
        return 1
    print(f"{changelog_path}: `## [{version}]` documents the migration ({', '.join(sql_files)}).")
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
    if cmd == "patch-script-check" and len(args) >= 1:
        return cmd_patch_script_check(args)
    if cmd == "validate-new-entries" and len(args) == 3:
        return cmd_validate_new_entries(*args)
    if cmd == "check-changelog" and len(args) >= 3:
        return cmd_check_changelog(args[0], args[1], args[2:])
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
