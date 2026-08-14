# Migration scripts

Canonical, per-version PostgreSQL migration scripts. `manifest.json` is the
machine-readable registry: **one entry per plugin X.Y version whose release
contains DB schema changes**. A plugin version with no entry ships no
migration.

**This directory is the single source of truth for DB migrations** across the
whole release pipeline — the core release workflow and the SaaS backend both
key off it. The full flow is documented in
[The full pipeline](#the-full-pipeline) below; `supertokens-core` and
`supertokens-backend-apis` link here rather than re-describing it.

Entries are keyed by **plugin** version, not core version, because that is the
only version a PR can know: which core a plugin release pairs with is decided
at release time (by the core release workflow's inputs), and that workflow
does the translation — it derives its `has-db-migration` flag from whether the
plugin X.Y being released has an entry here. `coreVersion` on an entry is
informational: the core X.Y whose SaaS upgrade applies the migration, filled
in when the pairing becomes known.

## The rule

- **Minor releases (X.Y bump)** carry schema changes: new columns, tables,
  constraint/PK changes, data transforms. Every such release MUST add a
  migration script and a manifest entry keyed by this branch's plugin X.Y
  (`version` in `build.gradle`) **in the same PR as the schema change**.
  Name new scripts after the plugin version (e.g. `v9.8.sql` for plugin 9.8);
  the manifest's `sql` field is the authoritative reference either way.
- **Patch releases (X.Y.Z bump)** may only contain additive, idempotent DDL
  that the core's startup backfill handles itself (`CREATE INDEX IF NOT
  EXISTS` entries in `GeneralQueries.createTablesIfNotExists`'s backfill
  list). Those need no manifest entry. Anything else must bump X.Y.

## Manifest entry fields

| field                       | meaning                                                                                                            |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `pluginVersion`             | Plugin X.Y that ships this migration — the machine key. Must equal the PR branch's `build.gradle` X.Y (CI-enforced). Keep entries sorted ascending. |
| `coreVersion`               | Informational: core X.Y whose SaaS upgrade applies this migration (matches the backend-apis CORE_MIGRATIONS key).  |
| `sql`                       | The migration script file in this directory.                                                                       |
| `custom`                    | `true` when running the SQL file top-to-bottom in one transaction is NOT sufficient (see the file's comments).     |
| `requiresBackfillMigration` | `true` when the release also needs the SaaS upgrade orchestrator's user-record backfill arm.                       |
| `notes`                     | One-line summary of what changed.                                                                                  |

## Who consumes this

1. **The core release workflow** (`supertokens-core`'s `do-release.yml`)
   derives its `has-db-migration` flag from the presence of an entry for the
   plugin X.Y being released, so a migration can't be silently forgotten at
   release time.
2. **The SaaS backend** (`supertokens-backend-apis`): each entry here must
   have a matching entry in `CORE_MIGRATIONS`
   (`apps/backend-api/ts/helpers/multi-tenancy/rds/migration/index.ts`) before
   the SaaS rollout of that core version can run — releases with
   `hasDBMigration: true` and no registered migration defer the rollout with
   an alert and a `core_version_metadata.saas_rollout_pending` marker.
3. **Self-hosted users** can apply `vX.Y.sql` files in order when upgrading
   across core versions (see also the per-version upgrade notes in the docs).

Pre-7.0 migrations are not represented here; they predate this convention and
live only in the SaaS legacy migration path.

## CI enforcement

The `DB schema / migration manifest check` workflow
(`.github/workflows/schema-migration-check.yml`) enforces the rule above on
every PR, mechanically: it boots the core with the base-branch plugin and with
the PR-head plugin against fresh databases, diffs the `pg_dump`'d schemas, and

- passes silently when the schemas are identical;
- allows additive `CREATE INDEX`-only diffs without a manifest entry (they
  must go through the startup backfill list instead);
- for any other schema change, requires this PR to also change
  `manifest.json` plus a referenced script, with any new entry keyed by this
  branch's `build.gradle` plugin X.Y, then **verifies the script**:
  it applies the changed script(s) to the base schema, boots the new core on
  the migrated database (the same order as a real SaaS upgrade), and requires
  the result to be statement-for-statement identical to the fresh-install
  schema. A wrong or incomplete migration script fails the check, not just a
  missing one.

Escape hatch: add the `skip-schema-check` label to the PR (intended for old
release branches where `core,master` no longer builds against this plugin).

## The full pipeline

How a schema change travels from a plugin PR to the SuperTokens SaaS, with no
step depending on anyone remembering anything:

### 1. Plugin PR (this repo)

A PR that changes the DB schema must, **in the same PR**, add the migration
script and a `manifest.json` entry keyed by the branch's own plugin X.Y. The
`schema-migration-check` CI enforces this mechanically (schema diff between
base and head via real core boots) and verifies the script itself: base
schema + script + a boot of the new core must reproduce the fresh-install
schema statement-for-statement. Index-only additive changes go through the
core's startup backfill instead and need no entry.

### 2. Core release (`supertokens-core`, `do-release.yml`)

The release pairs a core version with a plugin version. Its
`derive-db-migration-flag` job reads this manifest at the paired plugin SHA
and derives `hasDBMigration` — nobody types it. The flag rides both
`PATCH /core` calls to api.supertokens.io (`_release-mark-passed.yml` on
`testPassed`, `addReleaseTag` on `release: true`).

When the flag is true, the `open-backend-apis-migration-pr` job also opens a
PR against `supertokens-backend-apis` carrying the SaaS side of the migration:
code-generated from the script here for plain-SQL entries
(`apps/backend-api/scripts/generate-core-migration.mjs`), or a
`PENDING-<coreX.Y>.md` doc for `custom: true` entries that must be
implemented by hand.

**The core release itself (docker images, tags, jars) is never blocked by any
of this.**

### 3. SaaS backend (`supertokens-backend-apis`)

The SaaS applies migrations from its `CORE_MIGRATIONS` registry
(`apps/backend-api/ts/helpers/multi-tenancy/rds/migration/`) when a customer
deployment is upgraded/teleported to a new core version. On a release with
`hasDBMigration: true` whose version is missing from that registry:

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
