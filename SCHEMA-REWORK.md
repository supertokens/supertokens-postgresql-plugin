# Schema Rework — PostgreSQL Plugin

This document contains the database-layer detail for the schema rework shipped in
`supertokens-postgresql-plugin 9.5.0` together with `supertokens-core 12.0.0` and
`supertokens-plugin-interface 8.6.0`.

For the end-to-end operator runbook (migration modes, cutover procedure, backfill monitoring) see
`SCHEMA-REWORK.md` in the `supertokens-core` repository.

---

## Release summary (9.5.0)

### Background

Two related shifts drive the schema rework in this release:

1. **Lower the transaction isolation level from SERIALIZABLE to READ_COMMITTED.** Under SERIALIZABLE the planner
   accumulated SI-locks on the auth-recipe and account-info paths whenever multiple sessions touched
   overlapping users (linking, makePrimary, updateEmail, addUserIdToTenant), driving avoidable
   `could not serialize access` retry storms and tail-latency growth on busy tenants. READ_COMMITTED removes
   that contention class outright. To make that safe, the correctness work that SERIALIZABLE was doing for us
   implicitly is now done explicitly.

2. **Move invariants from application code into the database schema.** Account info uniqueness — one email per
   tenant per primary group, one phone per tenant per primary group, one (`third_party_id`,
   `third_party_user_id`) per tenant per primary group — used to be enforced by Java helpers reading the old
   `*_user_to_tenant` and `all_auth_recipe_users` projections and rejecting conflicts before writing. Under
   READ_COMMITTED those reads can become stale between check and write. The new reservation tables encode each
   invariant as a real primary-key / unique constraint, and conflict detection becomes a single
   `INSERT … ON CONFLICT … RETURNING` round-trip that the database resolves atomically. The Java side either
   gets the row it asked for or gets back the conflicting `primary_user_id` and translates that into the
   appropriate exception. No more read-then-write windows on the conflict-detection path.

The `LockedUser` pattern (acquired via `UserLockingQueries` against `app_id_to_user_id`) is the third leg of
the stool: where serialization on a specific user is genuinely required — linking, unlinking, email change,
tenant add — the lock is taken once at the top of the transaction and passed through the call as a typed
token. Result: each individual operation does less work on the database, hot paths no longer fight with each
other in the planner, and the correctness story is auditable at compile time.

### Added

- New schema: `recipe_user_account_infos`, `recipe_user_tenants`, `primary_user_tenants` with full index set.
- New columns on `app_id_to_user_id`: `time_joined`, `primary_or_recipe_user_time_joined` (both
  `BIGINT NOT NULL DEFAULT 0`) plus four pagination indexes scoped to app
  (`app_id_to_user_id_pagination_index1..4`).
- `ON UPDATE CASCADE` on FKs that reference `app_id_to_user_id(app_id, user_id)` (prep for a future CASCADE-FK
  rework).
- `migration_mode` config field (`SUPERTOKENS_MIGRATION_MODE`). Default `LEGACY`. Editable per-tenant via the
  standard core-config CRUD path.
- `UserLockingQueries`, `LockedUserImpl`, `LockFailure` — `SELECT ... FOR UPDATE` pattern targeting
  `app_id_to_user_id` with two-round, deterministic-ordered locking and a TOCTOU expansion guard.
- `AccountInfoQueries` (1500+ lines) — reservation-table mutations plus new-path conflict detection for
  `makePrimaryUser` / `linkAccounts` / `updateEmail` / `updatePhone` / `addUserIdToTenant`.
- `MigrationBackfillQueries` — per-user batch backfill, pending count, completeness verification.
- `migration-scripts/migration-backfill.sql` — idempotent set-based offline backfill, callable via
  `psql -f`. `migration-scripts/dump_old_canonical.sql` and
  `migration-scripts/dump_new_canonical.sql` produce a parity-comparable canonical dump from
  the old and new table sets for end-to-end verification.
- New tests: `BackfillTest`, `BackfillConcurrencyTest`, `BackfillIntegrationTest`, `MigrationModeTest`,
  `DualWriteConsistencyTest`, `LegacyModeRaceTest`, `ReservationTableIntegrityTest`, plus per-recipe `*RaceTest`
  files (`Passwordless`, `ThirdParty`, `WebAuthn`, `Unlink`, `DeleteUser`, `RaceCondition`, `Multitenancy`).

### Changed

- All read paths for account-info / tenant-membership / pagination / dashboard search now branch on
  `migration_mode` between the new reservation tables and `_legacy` helpers that hit the old
  `*_user_to_tenant` and `all_auth_recipe_users` tables.
- All write paths short-circuit when `!writesToOldTables()` / `!writesToNewTables()`.
- Locking moved from `all_auth_recipe_users` to `app_id_to_user_id` everywhere.
- `getUsers()` (dashboard search), `listUsersByAccountInfo`, active-user counts, `getPrimaryUserInfo*`,
  tenant-scoped `doesUserIdExist`, ThirdParty / WebAuthn / Passwordless / EmailPassword by-email/phone lookups
  rewritten against `recipe_user_tenants`.
- Startup DDL now runs as a single atomic batch with autocommit forced off, and the ~51-table existence probe
  collapsed to a single `pg_tables` query.

### Fixed

- `Primery` → `Primary` typo (internal-only).
- ThirdParty email/phone queries now include `third_party_id = ''` to use the account-info index fully.
- Free JDBC `Array` objects explicitly after use.
- Bulk-import write paths fixed for reservation-table coexistence.

### Migration notes

- Schema is forward-compatible with the legacy table set. The new tables are created on first boot of this
  version; old tables are untouched. The new code defaults to `LEGACY` mode (read+write old, ignore new) so an
  upgrade-only deploy is functionally identical to the prior version.
- Removing the old tables (`all_auth_recipe_users`, `*_user_to_tenant`) is a separate future release — do not
  drop them as part of this upgrade.
- See `SCHEMA-REWORK.md` in `supertokens-core` for the end-to-end cutover runbook.
