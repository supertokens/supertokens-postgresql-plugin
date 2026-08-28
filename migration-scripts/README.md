# Migration scripts

Canonical, per-version PostgreSQL migration scripts. `manifest.json` is the
machine-readable registry: **one entry per plugin X.Y.Z version whose release
contains DB schema changes** — minor *and* patch releases. A plugin version
with no entry ships no migration.

**This directory is the single source of truth for DB migrations** across the
whole release pipeline — the core release workflow and the SaaS backend both
key off it. The full flow is documented in
[The full pipeline](#the-full-pipeline) below; `supertokens-core` and
`supertokens-backend-apis` link here rather than re-describing it.

Entries are keyed by the full **plugin** version (X.Y.Z), not core version,
because that is the only version a PR can know: which core a plugin release
pairs with is decided at release time (by the core release workflow's inputs),
and that workflow does the translation — it derives its `has-db-migration`
flag from whether the exact plugin X.Y.Z being released has an entry here.
`coreVersion` on an entry is informational: the core X.Y.Z whose SaaS upgrade
applies the migration, filled in when the pairing becomes known.

## The rule

Every PR that changes the DB schema — in any way the schema diff can see,
including index-only changes — MUST, **in the same PR**:

1. add a migration script named after this branch's plugin version
   (`vX.Y.Z.sql`, `version` in `build.gradle`; e.g. `v9.8.0.sql` for plugin
   9.8.0, `v9.7.2.sql` for plugin 9.7.2),
2. add a `manifest.json` entry keyed by that same version (`sql` must equal
   `v<pluginVersion>.sql`), and
3. document it in `CHANGELOG.md` under the release's `## [X.Y.Z]` section, in
   a `### Migration` subsection that references the script.

What a release may *contain* depends on its kind:

- **Minor releases (X.Y.0)** may carry any schema change: new columns,
  tables, constraint/PK changes, data transforms, drops. The SaaS applies the
  script when a deployment moves to the new core (teleport), before the new
  core boots.
- **Patch releases (X.Y.Z, Z > 0)** may only **add or drop indexes** —
  nothing else — and the script must do it with `CREATE INDEX CONCURRENTLY IF
  NOT EXISTS` / `DROP INDEX CONCURRENTLY IF EXISTS` (each statement its own
  transaction, no write lock). The same change must also be in
  `GeneralQueries.createTablesIfNotExists`'s startup backfill list (`CREATE
  INDEX IF NOT EXISTS` / `DROP INDEX IF EXISTS`). The reason is mechanical: a
  patch rolls out
  on the SaaS as an *in-place* refresh of the running X.Y core (same ECS
  service, new image), and that path has no migration hook — the only code
  that runs against every customer database is the core's own boot. CI
  enforces all three parts: the schema diff must be index-only (indexes
  added and/or removed), every statement in the script must be a `CREATE
  [UNIQUE] INDEX CONCURRENTLY IF NOT EXISTS` or `DROP INDEX CONCURRENTLY IF
  EXISTS`, and booting the new core on the un-migrated base schema must
  reproduce the fresh-install schema (proving the startup backfill). The
  script exists so that self-hosters can pre-build/drop heavy indexes
  `CONCURRENTLY` before upgrading (making the startup DDL a no-op) and so the
  SaaS teleport chain stays complete. Anything else — columns, tables,
  constraints, non-concurrent index builds/drops — must bump the minor
  version.

## Manifest entry fields

| field                       | meaning                                                                                                            |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `pluginVersion`             | Plugin X.Y.Z that ships this migration — the machine key. Must equal the PR branch's `build.gradle` version (CI-enforced). Keep entries sorted ascending. |
| `coreVersion`               | Informational: core X.Y.Z whose SaaS upgrade applies this migration (matches the backend-apis CORE_MIGRATIONS key). |
| `sql`                       | The migration script file in this directory; must be `v<pluginVersion>.sql` (CI-enforced).                          |
| `custom`                    | `true` when the SaaS side cannot be code-generated from the script and must be implemented by hand: multi-step migrations mixing transactional DDL with `CONCURRENTLY` builds, data transforms, etc. Not needed for plain transactional scripts, nor for patch scripts made only of `CREATE/DROP INDEX CONCURRENTLY` (the generator emits those statement-by-statement). Always note the run instructions in the file's header. |
| `requiresBackfillMigration` | `true` when the release also needs the SaaS upgrade orchestrator's user-record backfill arm.                       |
| `notes`                     | One-line summary of what changed.                                                                                  |

## Who consumes this

1. **The core release workflow** (`supertokens-core`'s `do-release.yml`)
   derives its `has-db-migration` flag from the presence of an entry for the
   exact plugin X.Y.Z being released, so a migration can't be silently
   forgotten at release time. It also requires the core's own `CHANGELOG.md`
   section for the released core version to document the migration
   (`### Migration` + the script name) — same rule as here, same checker.
2. **The SaaS backend** (`supertokens-backend-apis`): each entry here must
   have a matching entry (keyed by core X.Y.Z) in `CORE_MIGRATIONS`
   (`apps/backend-api/ts/helpers/multi-tenancy/rds/migration/index.ts`) before
   the SaaS rollout of that core version can run — releases with
   `hasDBMigration: true` and no registered migration defer the rollout with
   an alert and a `core_version_metadata.saas_rollout_pending` marker.
3. **Self-hosted users** can apply `vX.Y.Z.sql` files in ascending plugin
   version order when upgrading across core versions — use each entry's
   `coreVersion` to map a core upgrade to the scripts it needs (see also the
   `### Migration` sections in the changelogs). Patch-release scripts are
   optional pre-work: the core makes the same index changes on boot, the
   script just lets you do it `CONCURRENTLY` beforehand.

Pre-7.0 migrations are not represented here; they predate this convention and
live only in the SaaS legacy migration path.

## CI enforcement

The `DB schema / migration manifest check` workflow
(`.github/workflows/schema-migration-check.yml`) enforces the rule above on
every PR, mechanically: it boots the core with the base-branch plugin and with
the PR-head plugin against fresh databases, diffs the `pg_dump`'d schemas, and

- passes silently when the schemas are identical;
- for any schema change (index-only ones included), requires this PR to also
  change `manifest.json` plus a referenced script, with any new entry keyed
  by this branch's `build.gradle` plugin X.Y.Z, then **verifies the script**:
  it applies the changed script(s) to the base schema, boots the new core on
  the migrated database (the same order as a real SaaS upgrade), and requires
  the result to be statement-for-statement identical to the fresh-install
  schema. A wrong or incomplete migration script fails the check, not just a
  missing one;
- on a patch branch (Z > 0), additionally requires the diff to be
  index-only, requires every statement of the script to be a `CREATE
  [UNIQUE] INDEX CONCURRENTLY IF NOT EXISTS` or `DROP INDEX CONCURRENTLY IF
  EXISTS`, and **proves the startup
  backfill**: it restores the base schema into a fresh database, boots the
  new core on it *without* running the script, and requires the result to
  equal the fresh-install schema too;
- requires `CHANGELOG.md`'s `## [X.Y.Z]` section for this branch's version
  to contain a `### Migration` subsection that references the script.

Escape hatch: add the `skip-schema-check` label to the PR (intended for old
release branches where `core,master` no longer builds against this plugin).

## The full pipeline

How a schema change travels from a plugin PR to the SuperTokens SaaS, with no
step depending on anyone remembering anything:

### 1. Plugin PR (this repo)

A PR that changes the DB schema must, **in the same PR**, add the migration
script, a `manifest.json` entry keyed by the branch's own plugin X.Y.Z, and a
`### Migration` changelog note. The `schema-migration-check` CI enforces this
mechanically (schema diff between base and head via real core boots) and
verifies the script itself: base schema + script + a boot of the new core
must reproduce the fresh-install schema statement-for-statement. On a patch
branch it additionally requires an index-only diff, a `CREATE/DROP INDEX
CONCURRENTLY`-only script, and proves the startup backfill (base schema + a
boot of the new core, no script, must reproduce it as well).

### 2. Core release (`supertokens-core`, `do-release.yml`)

The release pairs a core X.Y.Z with a plugin X.Y.Z. Its
`derive-db-migration-flag` job reads this manifest at the paired plugin SHA,
looks up the exact plugin version being released and derives
`hasDBMigration` — nobody types it. When true, the same job requires the
core's `CHANGELOG.md` section for the released core version to document the
migration (`### Migration` referencing the script) and fails the release
early otherwise — a one-line fix, and it happens before anything is
published. The flag rides both `PATCH /core` calls to api.supertokens.io
(`_release-mark-passed.yml` on `testPassed`, `addReleaseTag` on
`release: true`).

When the flag is true, the `open-backend-apis-migration-pr` job also opens a
PR against `supertokens-backend-apis` carrying the SaaS side of the migration:
code-generated from the script here for plain-SQL entries and for
`CREATE/DROP INDEX CONCURRENTLY`-only patch scripts
(`apps/backend-api/scripts/generate-core-migration.mjs`), or a
`PENDING-<coreX.Y.Z>.md` doc for `custom: true` entries that must be
implemented by hand.

**The core release itself (docker images, tags, jars) is never blocked by any
of this.**

### 3. SaaS backend (`supertokens-backend-apis`)

The SaaS applies migrations from its `CORE_MIGRATIONS` registry
(`apps/backend-api/ts/helpers/multi-tenancy/rds/migration/`, keyed by core
X.Y.Z) when a customer deployment is upgraded/teleported to a new core
version: every registered migration strictly newer than the source core's
full version and up to the target's is run, in order, before the target core
boots. Patch-release migrations (index-only) are therefore applied on
teleport too, while an in-place patch rollout relies on the core applying
the index changes at boot. On a release with `hasDBMigration: true` whose version is
missing from that registry:

- the core is still marked released, but the SaaS (ECS) rollout is **skipped**;
- `core_version_metadata.saas_rollout_pending` is set, a Slack alert fires,
  and a cronjob repeats the reminder every 6h until resolved;
- the manual rollout endpoint (`POST /manual/multi-tenancy/core/release`)
  hard-rejects until the registry knows the version.

### 4. Recovery / completion

Review and merge the auto-opened backend-apis PR → deploy backend-apis →
trigger the rollout via `POST /manual/multi-tenancy/core/release`. A
successful rollout clears the pending marker and the reminder stops. (When
the registry already knew the version at release time, the rollout just runs
as part of the release with no deferral.)

### Remaining human steps, by design

Code review of the generated backend-apis PR (it runs DDL on customer
databases), the backend-apis deploy, and implementing `custom: true`
migrations by hand. Everything else — detecting schema changes, verifying
scripts, deriving the flag, deferring the rollout, nagging, and drafting the
SaaS-side code — is mechanical.

The other files in this directory (`dump_old_canonical.sql`,
`dump_new_canonical.sql`, `migration-backfill.sql`) belong to the 12.0 schema
rework's dual-write/backfill tooling, not to this per-version registry.
