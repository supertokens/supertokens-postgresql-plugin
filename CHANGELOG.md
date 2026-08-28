# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [9.8.0]

- Implements the plugin-interface activity-log storage contract: retention parameter, transactional insert, unfolded-activity check, and last-active rollup.
- Changes the `activity_log.payload` column from `TEXT` to `JSONB`. Fresh installs create it as `JSONB`;
  pre-existing `TEXT` columns are migrated automatically at startup.

### Migration

Applied automatically at startup (idempotent; a non-JSON row aborts it loudly rather than dropping data).
The rewrite takes an `ACCESS EXCLUSIVE` lock, so on large `activity_log` tables you may pre-apply it
before upgrading to control the timing:

```sql
ALTER TABLE activity_log ALTER COLUMN payload TYPE JSONB USING payload::jsonb;
```

## [9.7.1]

- Makes the dashboard user search (email/phone/provider) sargable: `ILIKE` scans on `account_info_value`
  become `LIKE lower(?) || '%'` prefix matches via a `text_pattern_ops` opclass swap plus two partial indexes.

### Migration

Created/swapped automatically at startup; on large `recipe_user_tenants` tables pre-create them with
`CREATE INDEX CONCURRENTLY` before upgrading to avoid a lock (note the transient two-index window on the account-info family):

```sql
-- opclass swap of the account-info index (create the successor concurrently, then drop the predecessor)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_recipe_user_tenants_account_info_pattern ON recipe_user_tenants
  (app_id, tenant_id, account_info_type, account_info_value text_pattern_ops);
DROP INDEX CONCURRENTLY IF EXISTS idx_recipe_user_tenants_account_info;
-- partial indexes for the email-domain and case-insensitive provider arms
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_recipe_user_tenants_search_domain ON recipe_user_tenants
  (app_id, tenant_id, lower(split_part(account_info_value, '@', 2)) text_pattern_ops)
  WHERE account_info_type = 'email';
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_recipe_user_tenants_search_tparty ON recipe_user_tenants
  (app_id, tenant_id, lower(account_info_value) text_pattern_ops)
  WHERE account_info_type = 'tparty';
```


## [9.7.0]

- Fixes a connection pool leak in `UserIdMappingQueries.createBulkUserIdMapping`, which never returned its
  pooled connection and could exhaust the pool on large bulk imports
- Fixes `ActiveUsersQueries.getLastActiveByMultipleUserIds` reading its `IN (...)` batch result with
  `if` instead of `while`, so all but one requested user read as never-active
- Runs `UserRolesQueries.deleteRole` inside a transaction with a `FOR UPDATE` row lock, matching the other
  role queries, so it can no longer interleave with a concurrent assign-role flow
- Removes redundant recipe-level `recipe_user_tenants` inserts in `addUserIdToTenant_Transaction`; those rows
  are already written by `Start.addUserIdToTenant_Transaction`. No behavior change.
- Replaces the per-token `oauth_m2m_tokens` stats table with an hourly bucketed `oauth_m2m_token_stats`
  rollup, fixing M2M token undercounting for bursty issuers (additive DDL; existing rows migrated on upgrade)
- Decorrelates the tenant-removal reservation cleanup to scan the primary user's group members instead of the
  whole app's `recipe_user_tenants` (result set unchanged).
- Implements the approximate tenant user-count storage contract added in plugin-interface
  (`computeTenantUserCountAnchor` and `countTenantUsersJoinedSince` on `AuthRecipeSQLStorage`): an opt-in fast
  path that serves an exact-for-creations per-tenant user count in ms via a snapshot anchor plus a "joined
  since" delta, instead of the multi-second exact merge. The exact-count SQL is unchanged. Adds
  `ApproximateTenantUserCountTest`.
- Adds `app_id` to the join in the legacy WebAuthN email lookup
  (`getPrimaryUserIdForAppUsingEmail_Transaction`) so it can no longer match another app's
  `all_auth_recipe_users` row carrying the same `user_id`.
- Fixes `listUserIdsByMultipleThirdPartyInfo_Transaction` matching the cross-product of its inputs instead of 
  the requested `(third_party_id, third_party_user_id)` pairs.
- Adds support for plugin interface version 9.0
- Adds nullable `prev_refresh_token_hash_2` (`VARCHAR(128)`) and `refresh_token_rotated_at` (`BIGINT`, ms epoch)
  columns to the `session_info` table to record refresh-token rotation state. Both `NULL` means "no rotation
  recorded" (no backfill required).
- Adds `prevRefreshTokenHash2` and `refreshTokenRotatedAt` params to `updateSessionInfo_Transaction` and reads
  the new columns back into `SessionInfo`.

### Migration

Make sure the core is already upgraded to the version that supports plugin interface 9.0 before migrating.

```sql
ALTER TABLE session_info ADD COLUMN prev_refresh_token_hash_2 VARCHAR(128);
ALTER TABLE session_info ADD COLUMN refresh_token_rotated_at BIGINT;
```


## [9.6.2]

- Adds two additive indexes on `oauth_sessions`, `(app_id, client_id)` and `(app_id, session_handle)`, so the
  revoke paths no longer scan the whole table on session-heavy deployments. The table is keyed by `gid` only,
  so `deleteOAuthSessionByClientId` (`DELETE ... WHERE app_id = ? AND client_id = ?` — revoke-all-for-client,
  and the FK-cascade path when an oauth client is deleted) and `deleteOAuthSessionBySessionHandle`
  (`DELETE ... WHERE app_id = ? AND session_handle = ?` — revoke on SuperTokens-session logout) previously did
  a sequential scan per call. No query or behaviour changes; both deletes now use an index scan.
- Adds `OAuthSessionRevokeIndexRegressionTest`: seeds ~50k oauth sessions across many clients directly with SQL
  and asserts on `EXPLAIN (FORMAT JSON)` that both revoke-by-client and revoke-by-session-handle deletes plan
  an index scan on the new indexes, and (teeth) that dropping the indexes forces a sequential scan. Skippable
  locally via `SKIP_SCALE_REGRESSION_TESTS=true`
- Adds two secondary indexes on `recipe_user_account_infos` — `(app_id, primary_user_id)` and
  `(app_id, account_info_type, account_info_value)` — so the reservation-cleanup subqueries and the
  third-party/webauthn sign-in lookups no longer seq-scan the whole app's rows. Created on fresh databases
  and backfilled at startup (see Migration below).
- Restores the `app_id` condition on the nested subqueries of two `AccountInfoQueries` reservation-cleanup
  statements (tenant-removal cleanup and `updateAccountInfo_Transaction`'s tenant delete), which had dropped
  it and so forced app-wide table scans. Results unchanged.
- Pins `getUsersCount_new`'s `D - L` statement to its streaming merge join
  (`SET LOCAL enable_hashjoin = off`) so the planner can no longer flip to a hash join that spills the
  app-wide table to disk when its row estimates drift. Count result unchanged.
- Makes the user-listing and bulk-import keyset pagination cursors sargable by adding a redundant
  leading-sort-column bound to the cursor predicate, so deep pages seek straight to the cursor instead of
  scanning the pagination index from the top. Applies to `getUsers_new`, `getUsers_legacy`, and the
  bulk-import listing; rows, order and cursor tokens are unchanged.

### Migration

Adds additive indexes, created on fresh databases and backfilled on existing ones at startup via

``` sql

CREATE INDEX IF NOT EXISTS idx_recipe_user_account_infos_app_primary_user ON recipe_user_account_infos 
(app_id, primary_user_id);

CREATE INDEX IF NOT EXISTS idx_recipe_user_account_infos_account_info ON recipe_user_account_infos 
(app_id, account_info_type, account_info_value);

CREATE INDEX IF NOT EXISTS oauth_session_client_id_index on oauth_sessions (app_id, client_id);

CREATE INDEX IF NOT EXISTS oauth_session_session_handle_index on oauth_sessions (app_id, session_handle);
```

No table or column changes. **Operators of large deployments should pre-create these indexes with
`CREATE INDEX CONCURRENTLY` before upgrading**, so the startup DDL is a no-op and does not hold a table lock
during a long index build.

## [9.6.1]

- Implements `updateTimeJoinedForPrimaryUsers_Transaction` (new in plugin-interface `8.7.1`) by delegating to
  the existing internal batch query, which normalizes `primary_or_recipe_user_time_joined` to the linked-group
  minimum across every table carrying the column (respecting migration-mode branching). This lets callers that
  insert linked members without normalizing — notably bulk import — restore the invariant that user-list
  pagination relies on.
- Fixes the reservation-table backfill getting stuck on users removed from all tenants: their `time_joined`
  now falls back to the per-app recipe table instead of staying 0, which kept them permanently in the
  pending set and looped the backfill cron forever
- Fixes activity log partition maintenance failing forever when rows for a not-yet-created month landed in the
  DEFAULT partition (e.g. after the core was paused across a month boundary): the rows are now moved into the
  newly created monthly partition, and DEFAULT rows older than the retention window are purged
- Rewrites the migrated-schema paginated user listing (`getUsers_new`, plain and cursor variants) to stream
  over the pagination indexes: instead of joining `app_id_to_user_id` to `recipe_user_tenants` and
  `GROUP BY`-ing every user of the tenant before the `LIMIT` can apply, the two non-search variants now use
  `SELECT DISTINCT ... WHERE EXISTS (...)`, moving the cursor filter from a `HAVING` on `MIN(...)` to a plain
  indexable `WHERE`. Pagination cost now scales with page size instead of tenant size. Rows, ordering and
  cursor semantics are unchanged (relies on the same per-group `primary_or_recipe_user_time_joined` invariant
  the dashboard-search branch already depends on)
- Rewrites the migrated-schema unfiltered per-tenant user count (`getUsersCount_new`) to avoid the
  tenant-wide hash-aggregating join that spilled to disk on very large tenants. The count is now computed as
  the `D - L + G` decomposition (distinct recipe users in the tenant, minus those that are linked-or-primary,
  plus the distinct primary users present in the tenant), where each term is an index-only streaming scan
  over one of the three additive indexes noted in the Migration section below. The unfiltered app-scoped
  count now streams a `GROUP BY (primary_or_recipe_user_time_joined, primary_or_recipe_user_id)` off
  `app_id_to_user_id_pagination_index2` instead of hash-aggregating. Counts are unchanged; the
  recipe-id-filtered variants keep their existing queries.
- Adds `MigratedUserScaleRegressionTest`: plan-shape regression tests over a ~200k-user fixture seeded directly
  with SQL, asserting on `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` that the rewritten pagination feeds only a
  small multiple of the page size into its top `Unique` node (vs the old query aggregating the whole tenant)
  and that the `D - L + G` and app-scoped counts write zero temp blocks and use no `HashAggregate` / `Hash Join`
  at `work_mem = 64kB` (vs the old join + `GROUP BY` spilling), plus new-vs-old result equality. Heavy fixture;
  runs in CI, skippable locally via `SKIP_SCALE_REGRESSION_TESTS=true`

### Migration

Adds three additive indexes, created on fresh databases and backfilled on existing ones at startup via

``` sql

CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_tenant_recipe_user on recipe_user_tenants (app_id, tenant_id, 
recipe_user_id);

CREATE INDEX IF NOT EXISTS app_id_to_user_id_linked_flag_index on app_id_to_user_id (app_id, user_id, 
is_linked_or_is_a_primary_user);

CREATE INDEX IF NOT EXISTS idx_primary_user_tenants_tenant_primary on primary_user_tenants (app_id, tenant_id, 
primary_user_id);

```

No table or column changes. **Operators of very large deployments should pre-create these three indexes with
`CREATE INDEX CONCURRENTLY` before upgrading**, so the startup DDL is a no-op and does not hold a table lock
during a long index build.

## [9.6.0]

- Fixes `isUserIdBeingUsedInNonAuthRecipe` to use O(1) existence probes (`SELECT 1 ... LIMIT 1`) for sessions,
  user roles and TOTP devices instead of loading every matching row
- Fixes the raw database password being embedded verbatim in the HikariCP connection pool name (which is
  included in Hikari log lines, exception messages and telemetry exports): the pool id now uses a truncated
  SHA-256 hash of the password instead. Also masks the password on the OpenTelemetry log appender path.
- Adds `countUsersActiveSinceGroupedByDay` (implements the new `ActiveUsersStorage` method): the MAU series is
  now computed with a single bucketed query instead of one `COUNT(*)` per day threshold
- Adds a composite `(app_id, last_active_time)` index on `user_last_active`, created on fresh databases and
  backfilled (best-effort, outside the main DDL transaction) on databases provisioned before it existed

## [9.5.6]

- Adds `SKIP LOCKED` to the backfill batch locking query to improve performance

## [9.5.5]

- Fix no-op account info updates

## [9.5.4]

- Upgrade dependencies to fix vulnerabilities in dependencies

## [9.5.3]

- Fixes users signed up while `migration_mode` is `LEGACY` being permanently skipped by the reservation-tables
  backfill: their `app_id_to_user_id` rows now keep the `time_joined = 0` sentinel (real timestamps stay in the
  legacy tables), so they remain in the backfill pending set until their reservation rows are created
- Makes `migration_mode` a connection-pool property so that changing it on an existing tenant (via the multitenancy CRUD endpoint) takes effect on the live storage instance without a core restart. Previously the storage layer reused the existing instance because `migration_mode` did not affect the pool identity, leaving the persisted mode dormant until the next restart. `migration_mode` is now normalised to a canonical value (absent → `LEGACY`, upper-cased) so it contributes consistently to `connectionPoolId`.
- Fixes `linkAccounts` to be idempotent when the recipe user is already the target primary user: in non-`LEGACY`
  migration modes this case now returns `accountsAlreadyLinked=true` (matching pre-12.0 and `LEGACY` behaviour)
  instead of throwing `CannotLinkSinceRecipeUserIdAlreadyLinkedWithAnotherPrimaryUserIdException`

## [9.5.2]

- Adds the `activity_log` table: an append-only audit log, range-partitioned by `created_at` into one partition per UTC month
- Adds `ActivityLogStorage.maintainActivityLogPartitions()` — pre-creates upcoming month partitions and drops a partition once its whole month is older than 31 days
- `activity_log.payload` is stored as `TEXT`

### Migration

The `activity_log` table DDL is in [SCHEMA-REWORK.md](SCHEMA-REWORK.md#pre-flight).

## [9.5.1]

- Fixes possible fan-out with makePrimary

## [9.5.0]

- Adds reservation tables: `recipe_user_account_infos`, `recipe_user_tenants`, `primary_user_tenants`
- Adds `time_joined` and `primary_or_recipe_user_time_joined` columns to `app_id_to_user_id` with four new pagination indexes
- Adds `UserLockingQueries` and `LockedUser` token pattern (`SELECT ... FOR UPDATE` on `app_id_to_user_id`)
- Adds `AccountInfoQueries` for reservation-table conflict detection on `makePrimaryUser`, `linkAccounts`, `updateEmail`, `addUserIdToTenant`
- Adds `MigrationBackfillQueries` for online per-user batch backfill
- Adds offline migration SQL scripts in `migration-scripts/`
- Rewrites read/write paths to branch on `migration_mode` between legacy and new tables
- Collapses startup DDL into a single atomic batch with a single `pg_tables` existence probe
- Fixes ThirdParty email/phone queries to include `third_party_id = ''` for full index use

### Migration

Safe to upgrade: new tables are created on first boot; old tables are untouched; defaults to `LEGACY` mode.
See [SCHEMA-REWORK.md](SCHEMA-REWORK.md) for the full schema details and the end-to-end cutover runbook; the full DDL is in its [Pre-flight](SCHEMA-REWORK.md#pre-flight) section.

## [9.4.2]

- Fixes concurrency issue with oauth refresh token

## [9.4.1]

- Fixes env var reading with specific types
- Updates dependencies for testing

## [9.4.0]

- Adds support for bulk query APIs for usermetadata and TOTP

## [9.3.2]

- Regenerates `implementationDependencies.json`

## [9.3.1]

- Bump version

## [9.3.0]

- Adds SAML support

### Migration

```sql
CREATE TABLE IF NOT EXISTS saml_clients (
    app_id VARCHAR(64) NOT NULL DEFAULT 'public',
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',
    client_id VARCHAR(256) NOT NULL,
    client_secret TEXT,
    sso_login_url TEXT NOT NULL,
    redirect_uris TEXT NOT NULL,
    default_redirect_uri TEXT NOT NULL,
    idp_entity_id VARCHAR(256) NOT NULL,
    idp_signing_certificate TEXT NOT NULL,
    allow_idp_initiated_login BOOLEAN NOT NULL DEFAULT FALSE,
    enable_request_signing BOOLEAN NOT NULL DEFAULT FALSE,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT saml_clients_pkey PRIMARY KEY(app_id, tenant_id, client_id),
    CONSTRAINT saml_clients_idp_entity_id_key UNIQUE (app_id, tenant_id, idp_entity_id),
    CONSTRAINT saml_clients_app_id_fkey FOREIGN KEY(app_id) REFERENCES apps (app_id) ON DELETE CASCADE,
    CONSTRAINT saml_clients_tenant_id_fkey FOREIGN KEY(app_id, tenant_id) REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS saml_clients_app_id_tenant_id_index ON saml_clients (app_id, tenant_id);

CREATE TABLE IF NOT EXISTS saml_relay_state (
    app_id VARCHAR(64) NOT NULL DEFAULT 'public',
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',
    relay_state VARCHAR(256) NOT NULL,
    client_id VARCHAR(256) NOT NULL,
    state TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    CONSTRAINT saml_relay_state_pkey PRIMARY KEY(app_id, tenant_id, relay_state),
    CONSTRAINT saml_relay_state_app_id_fkey FOREIGN KEY(app_id) REFERENCES apps (app_id) ON DELETE CASCADE,
    CONSTRAINT saml_relay_state_tenant_id_fkey FOREIGN KEY(app_id, tenant_id) REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS saml_relay_state_app_id_tenant_id_index ON saml_relay_state (app_id, tenant_id);
CREATE INDEX IF NOT EXISTS saml_relay_state_expires_at_index ON saml_relay_state (expires_at);

CREATE TABLE IF NOT EXISTS saml_claims (
    app_id VARCHAR(64) NOT NULL DEFAULT 'public',
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',
    client_id VARCHAR(256) NOT NULL,
    code VARCHAR(256) NOT NULL,
    claims TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    CONSTRAINT saml_claims_pkey PRIMARY KEY(app_id, tenant_id, code),
    CONSTRAINT saml_claims_app_id_fkey FOREIGN KEY(app_id) REFERENCES apps (app_id) ON DELETE CASCADE,
    CONSTRAINT saml_claims_tenant_id_fkey FOREIGN KEY(app_id, tenant_id) REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS saml_claims_app_id_tenant_id_index ON saml_claims (app_id, tenant_id);
CREATE INDEX IF NOT EXISTS saml_claims_expires_at_index ON saml_claims (expires_at);
```

## [9.2.0]

- Adds docker support for opentelemetry javaagent

## [9.1.0]

- Sends hikari logs to opentelemetry
- Updates config json from env

## [9.0.4]

- Adds back `implementationDependencies.json` file, but now it is generated by the build process

## [9.0.3]

- Fixes BatchUpdateException checks and error handling to prevent bulk import users stuck in `PROCESSING` state

## [9.0.2]
 
- Fixes `AuthRecipe#getUserByAccountInfo` to consider the tenantId instead of the appId when fetching the webauthn user
- Changes dependency structure to avoid multiple dependency declarations for the same library

## [9.0.1]

- Upgrades the embedded tomcat 11.0.6 and logback classic to 1.5.13 because of security vulnerabilities

## [9.0.0]

- Migrates to github actions
- Updates JRE version to 21

## [8.1.3]
 
- Adds tcpKeepAlive to the connection pool config

## [8.1.2]

- Adds user_id index to the user roles table

### Migration

```sql
CREATE INDEX IF NOT EXISTS user_roles_app_id_user_id_index ON user_roles (app_id, user_id);
```

## [8.1.1]

- Adds more null and empty checks for bulk migration

## [8.1.0]

- Adds support for webauthn (passkeys)
- Adds additional indexing for `emailverification_verified_emails`

### Migration

```sql
CREATE INDEX IF NOT EXISTS emailverification_verified_emails_app_id_email_index ON emailverification_verified_emails
(app_id, email);

CREATE TABLE IF NOT EXISTS webauthn_account_recovery_tokens (
    app_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    tenant_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    user_id CHAR(36) NOT NULL,
    email VARCHAR(256) NOT NULL,
    token VARCHAR(256) NOT NULL,
    expires_at BIGINT NOT NULL,
    CONSTRAINT webauthn_account_recovery_token_pkey PRIMARY KEY (app_id, tenant_id, user_id, token),
    CONSTRAINT webauthn_account_recovery_token_user_id_fkey FOREIGN KEY (app_id, tenant_id, user_id) REFERENCES 
    all_auth_recipe_users(app_id, tenant_id, user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS webauthn_credentials (
    id VARCHAR(256) NOT NULL,
    app_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    rp_id VARCHAR(256) NOT NULL,
    user_id CHAR(36),
    counter BIGINT NOT NULL,
    public_key BYTEA NOT NULL,
    transports TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT webauthn_credentials_pkey PRIMARY KEY (app_id, rp_id, id),
    CONSTRAINT webauthn_credentials_user_id_fkey FOREIGN KEY (app_id, user_id) REFERENCES webauthn_users
    (app_id, user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS webauthn_generated_options (
    app_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    tenant_id VARCHAR(64) DEFAULT 'public'NOT NULL,
    id CHAR(36) NOT NULL,
    challenge VARCHAR(256) NOT NULL,
    email VARCHAR(256),
    rp_id VARCHAR(256) NOT NULL,
    rp_name VARCHAR(256) NOT NULL,
    origin VARCHAR(256) NOT NULL,
    expires_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    user_presence_required BOOLEAN DEFAULT false NOT NULL,
    user_verification VARCHAR(12) DEFAULT 'preferred' NOT NULL,
    CONSTRAINT webauthn_generated_options_pkey PRIMARY KEY (app_id, tenant_id, id),
    CONSTRAINT webauthn_generated_options_tenant_id_fkey FOREIGN KEY (app_id, tenant_id) REFERENCES tenants
    (app_id, tenant_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS webauthn_user_to_tenant (
    app_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    tenant_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    user_id CHAR(36) NOT NULL,
    email VARCHAR(256) NOT NULL,
    CONSTRAINT webauthn_user_to_tenant_email_key UNIQUE (app_id, tenant_id, email),
    CONSTRAINT webauthn_user_to_tenant_pkey PRIMARY KEY (app_id, tenant_id, user_id),
    CONSTRAINT webauthn_user_to_tenant_user_id_fkey FOREIGN KEY (app_id, tenant_id, user_id) REFERENCES 
    all_auth_recipe_users(app_id, tenant_id, user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS webauthn_users (
    app_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    user_id CHAR(36) NOT NULL,
    email VARCHAR(256) NOT NULL,
    rp_id VARCHAR(256) NOT NULL,
    time_joined BIGINT NOT NULL,
    CONSTRAINT webauthn_users_pkey PRIMARY KEY (app_id, user_id),
    CONSTRAINT webauthn_users_user_id_fkey FOREIGN KEY (app_id, user_id) REFERENCES app_id_to_user_id(app_id, 
    user_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS webauthn_user_to_tenant_email_index ON webauthn_user_to_tenant (app_id, email);
CREATE INDEX IF NOT EXISTS webauthn_user_challenges_expires_at_index ON webauthn_generated_options (app_id, tenant_id, expires_at);
CREATE INDEX IF NOT EXISTS webauthn_credentials_user_id_index ON webauthn_credentials (user_id);
CREATE INDEX IF NOT EXISTS webauthn_account_recovery_token_token_index ON webauthn_account_recovery_tokens (app_id, tenant_id, token);
CREATE INDEX IF NOT EXISTS webauthn_account_recovery_token_expires_at_index ON webauthn_account_recovery_tokens (expires_at DESC);
CREATE INDEX IF NOT EXISTS webauthn_account_recovery_token_email_index ON webauthn_account_recovery_tokens (app_id, tenant_id, email);
```

## [8.0.3]

- Fixes `StorageTransactionLogicException` in bulk import when not using userRoles and totpDevices in import json.
- Adds `USE_STRUCTURED_LOGGING` environment variable to control the logging format.

## [8.0.2]

- Fixes `NullPointerException` in user search API

## [8.0.1]

- Fixes slow queries for account linking

### Migration

```sql
CREATE INDEX IF NOT EXISTS emailpassword_users_email_index ON emailpassword_users (app_id, email);
CREATE INDEX IF NOT EXISTS emailpassword_user_to_tenant_email_index ON emailpassword_user_to_tenant (app_id, tenant_id, email);

CREATE INDEX IF NOT EXISTS passwordless_users_email_index ON passwordless_users (app_id, email);
CREATE INDEX IF NOT EXISTS passwordless_users_phone_number_index ON passwordless_users (app_id, phone_number);
CREATE INDEX IF NOT EXISTS passwordless_user_to_tenant_email_index ON passwordless_user_to_tenant (app_id, tenant_id, email);
CREATE INDEX IF NOT EXISTS passwordless_user_to_tenant_phone_number_index ON passwordless_user_to_tenant (app_id, tenant_id, phone_number);

CREATE INDEX IF NOT EXISTS thirdparty_user_to_tenant_third_party_user_id_index ON thirdparty_user_to_tenant (app_id, tenant_id, third_party_id, third_party_user_id);
```

## [8.0.0]

- Adds tables and queries for Bulk Import
- Optimize getUserIdMappingWithEitherSuperTokensUserIdOrExternalUserId query
- Adds indexing for `session_info` table on `user_id, app_id` columns

### Migration

```sql
"CREATE TABLE IF NOT EXISTS bulk_import_users (
                id CHAR(36),
                app_id VARCHAR(64) NOT NULL DEFAULT 'public',
                primary_user_id VARCHAR(36),
                raw_data TEXT NOT NULL,
                status VARCHAR(128) DEFAULT 'NEW',
                error_msg TEXT,
                created_at BIGINT NOT NULL, 
                updated_at BIGINT NOT NULL, 
                CONSTRAINT bulk_import_users_pkey PRIMARY KEY(app_id, id),
                CONSTRAINT bulk_import_users__app_id_fkey FOREIGN KEY(app_id) REFERENCES apps(app_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS bulk_import_users_status_updated_at_index ON bulk_import_users (app_id, status, updated_at);

CREATE INDEX IF NOT EXISTS bulk_import_users_pagination_index1 ON bulk_import_users (app_id, status, created_at DESC,
 id DESC);
 
CREATE INDEX IF NOT EXISTS bulk_import_users_pagination_index2 ON bulk_import_users (app_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS session_info_user_id_app_id_index ON session_info (user_id, app_id);
```

## [7.2.0] - 2024-10-03

- Compatible with plugin interface version 6.3
- Adds support for OAuthStorage

### Migration

```sql
CREATE TABLE IF NOT EXISTS oauth_clients (
    app_id VARCHAR(64),
    client_id VARCHAR(255) NOT NULL,
    is_client_credentials_only BOOLEAN NOT NULL,
    PRIMARY KEY (app_id, client_id),
    FOREIGN KEY(app_id) REFERENCES apps(app_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oauth_sessions (
    gid VARCHAR(255),
    app_id VARCHAR(64) DEFAULT 'public',
    client_id VARCHAR(255) NOT NULL,
    session_handle VARCHAR(128),
    external_refresh_token VARCHAR(255) UNIQUE,
    internal_refresh_token VARCHAR(255) UNIQUE,
    jti TEXT NOT NULL,
    exp BIGINT NOT NULL,
    PRIMARY KEY (gid),
    FOREIGN KEY(app_id, client_id) REFERENCES oauth_clients(app_id, client_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS oauth_session_exp_index ON oauth_sessions(exp DESC);
CREATE INDEX IF NOT EXISTS oauth_session_external_refresh_token_index ON oauth_sessions(app_id, external_refresh_token DESC);

CREATE TABLE IF NOT EXISTS oauth_m2m_tokens (
    app_id VARCHAR(64) DEFAULT 'public',
    client_id VARCHAR(255) NOT NULL,
    iat BIGINT NOT NULL,
    exp BIGINT NOT NULL,
    PRIMARY KEY (app_id, client_id, iat),
    FOREIGN KEY(app_id, client_id) REFERENCES oauth_clients(app_id, client_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS oauth_m2m_token_iat_index ON oauth_m2m_tokens(iat DESC, app_id DESC);
CREATE INDEX IF NOT EXISTS oauth_m2m_token_exp_index ON oauth_m2m_tokens(exp DESC);

CREATE TABLE IF NOT EXISTS oauth_logout_challenges (
    app_id VARCHAR(64) DEFAULT 'public',
    challenge VARCHAR(128) NOT NULL,
    client_id VARCHAR(255) NOT NULL,
    post_logout_redirect_uri VARCHAR(1024),
    session_handle VARCHAR(128),
    state VARCHAR(128),
    time_created BIGINT NOT NULL,
    PRIMARY KEY (app_id, challenge),
    FOREIGN KEY(app_id, client_id) REFERENCES oauth_clients(app_id, client_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS oauth_logout_challenges_time_created_index ON oauth_logout_challenges(time_created DESC);
```

## [7.1.3] - 2024-09-04

- Adds index on `last_active_time` for `user_last_active` table to improve the performance of MAU computation.

### Migration

```sql
CREATE INDEX IF NOT EXISTS user_last_active_last_active_time_index ON user_last_active (last_active_time DESC, app_id DESC);
```

## [7.1.2] - 2024-09-02

- Optimizes users count query

## [7.1.1] - 2024-08-08

- Fixes tests that check for `Internal Error` in 500 status responses

## [7.1.0]

- Compatible with plugin interface version 6.2
- Adds implementation for a new method `getConfigFieldsInfo` to fetch the plugin config fields.
- Adds `DashboardInfo` annotations to the config properties in `PostgreSQLConfig`
- Adds `null` state for `firstFactors` by adding `is_first_factors_null` field in `tenant_configs` table. The value of
  this column is only applicable when there are no entries in the `tenant_first_factors` table for the tenant.

### Migration

```sql
ALTER TABLE tenant_configs ADD COLUMN IF NOT EXISTS is_first_factors_null BOOLEAN DEFAULT TRUE;
ALTER TABLE tenant_configs ALTER COLUMN is_first_factors_null DROP DEFAULT;
```

## [7.0.1] - 2024-04-17

- Fixes issues with partial failures during tenant creation

## [7.0.0] - 2024-03-13

- Replace `TotpNotEnabledError` with `UnknownUserIdTotpError`.
- Support for MFA recipe
- Adds `firstFactors` and `requiredSecondaryFactors` for tenant config.
- Adds a new `useStaticKey` param to `updateSessionInfo_Transaction`
    - This enables smooth switching between `useDynamicAccessTokenSigningKey` settings by allowing refresh calls to
      change the signing key type of a session

### Migration

Make sure the core is already upgraded to version 8.0.0 before migrating

```sql
ALTER TABLE totp_user_devices ADD COLUMN IF NOT EXISTS created_at BIGINT default 0;
ALTER TABLE totp_user_devices 
  ALTER COLUMN created_at DROP DEFAULT;
```

## [6.0.0] - 2024-03-05

- Implements `deleteAllUserRoleAssociationsForRole`
- Drops `(app_id, role)` foreign key constraint on `user_roles` table

### Migration

```sql
ALTER TABLE user_roles DROP CONSTRAINT IF EXISTS user_roles_role_fkey;
```

## [5.0.8] - 2024-02-19

- Fixes vulnerabilities in dependencies

## [5.0.7] - 2024-01-25

- Fixes the issue where passwords were inadvertently logged in the logs.
- Adds tests to check connection pool behaviour.
- Adds `postgresql_idle_connection_timeout` and `postgresql_minimum_idle_connections` configs to control active
  connections to the database.

## [5.0.6] - 2023-12-05

- Validates db config types in `canBeUsed` function

## [5.0.5] - 2023-11-23

- Fixes call to `getPrimaryUserInfoForUserIds_Transaction` in `listPrimaryUsersByThirdPartyInfo_Transaction`

## [5.0.4] - 2023-11-23

- Adds `app_id_to_user_id_primary_user_id_index` index on `app_id_to_user_id` table

### Migration

Run the following sql script:

```sql
CREATE INDEX IF NOT EXISTS app_id_to_user_id_primary_user_id_index ON app_id_to_user_id (primary_or_recipe_user_id, app_id);
```

## [5.0.3] - 2023-11-10

- Fixes issue with email verification with user id mapping

## [5.0.2] - 2023-11-01

- Fixes `verified` in `loginMethods` for users with userId mapping

## [5.0.1] - 2023-10-12

- Fixes user info from primary user id query
- Fixes `deviceIdHash` issue

## [5.0.0] - 2023-09-19

### Changes

- Support for Account Linking
    - Adds columns `primary_or_recipe_user_id`, `is_linked_or_is_a_primary_user`
      and `primary_or_recipe_user_time_joined` to `all_auth_recipe_users` table
    - Adds columns `primary_or_recipe_user_id` and `is_linked_or_is_a_primary_user` to `app_id_to_user_id` table
    - Removes index `all_auth_recipe_users_pagination_index` and addes `all_auth_recipe_users_pagination_index1`,
      `all_auth_recipe_users_pagination_index2`, `all_auth_recipe_users_pagination_index3` and
      `all_auth_recipe_users_pagination_index4` indexes instead on `all_auth_recipe_users` table
    - Adds `all_auth_recipe_users_recipe_id_index` on `all_auth_recipe_users` table
    - Adds `all_auth_recipe_users_primary_user_id_index` on `all_auth_recipe_users` table
    - Adds `email` column to `emailpassword_pswd_reset_tokens` table
    - Changes `user_id` foreign key constraint on `emailpassword_pswd_reset_tokens` to `app_id_to_user_id` table

### Migration

1. Ensure that the core is already upgraded to the version 6.0.13 (CDI version 3.0)
2. Stop the core instance(s)
3. Run the migration script
   ```sql
    ALTER TABLE all_auth_recipe_users
      ADD COLUMN primary_or_recipe_user_id CHAR(36) NOT NULL DEFAULT ('0');

    ALTER TABLE all_auth_recipe_users
      ADD COLUMN is_linked_or_is_a_primary_user BOOLEAN NOT NULL DEFAULT FALSE;

    ALTER TABLE all_auth_recipe_users
      ADD COLUMN primary_or_recipe_user_time_joined BIGINT NOT NULL DEFAULT 0;

    UPDATE all_auth_recipe_users
      SET primary_or_recipe_user_id = user_id
      WHERE primary_or_recipe_user_id = '0';

    UPDATE all_auth_recipe_users
      SET primary_or_recipe_user_time_joined = time_joined
      WHERE primary_or_recipe_user_time_joined = 0;

    ALTER TABLE all_auth_recipe_users
      ADD CONSTRAINT all_auth_recipe_users_primary_or_recipe_user_id_fkey
        FOREIGN KEY (app_id, primary_or_recipe_user_id)
        REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE;

    ALTER TABLE all_auth_recipe_users
      ALTER primary_or_recipe_user_id DROP DEFAULT;

    ALTER TABLE app_id_to_user_id
      ADD COLUMN primary_or_recipe_user_id CHAR(36) NOT NULL DEFAULT ('0');

    ALTER TABLE app_id_to_user_id
      ADD COLUMN is_linked_or_is_a_primary_user BOOLEAN NOT NULL DEFAULT FALSE;

    UPDATE app_id_to_user_id
      SET primary_or_recipe_user_id = user_id
      WHERE primary_or_recipe_user_id = '0';

    ALTER TABLE app_id_to_user_id
      ADD CONSTRAINT app_id_to_user_id_primary_or_recipe_user_id_fkey
        FOREIGN KEY (app_id, primary_or_recipe_user_id)
        REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE;

    ALTER TABLE app_id_to_user_id
        ALTER primary_or_recipe_user_id DROP DEFAULT;

    DROP INDEX all_auth_recipe_users_pagination_index;

    CREATE INDEX all_auth_recipe_users_pagination_index1 ON all_auth_recipe_users (
      app_id, tenant_id, primary_or_recipe_user_time_joined DESC, primary_or_recipe_user_id DESC);

    CREATE INDEX all_auth_recipe_users_pagination_index2 ON all_auth_recipe_users (
      app_id, tenant_id, primary_or_recipe_user_time_joined ASC, primary_or_recipe_user_id DESC);

    CREATE INDEX all_auth_recipe_users_pagination_index3 ON all_auth_recipe_users (
      recipe_id, app_id, tenant_id, primary_or_recipe_user_time_joined DESC, primary_or_recipe_user_id DESC);

    CREATE INDEX all_auth_recipe_users_pagination_index4 ON all_auth_recipe_users (
      recipe_id, app_id, tenant_id, primary_or_recipe_user_time_joined ASC, primary_or_recipe_user_id DESC);

    CREATE INDEX all_auth_recipe_users_primary_user_id_index ON all_auth_recipe_users (primary_or_recipe_user_id, app_id);

    CREATE INDEX all_auth_recipe_users_recipe_id_index ON all_auth_recipe_users (app_id, recipe_id, tenant_id);

    ALTER TABLE emailpassword_pswd_reset_tokens DROP CONSTRAINT IF EXISTS emailpassword_pswd_reset_tokens_user_id_fkey;

    ALTER TABLE emailpassword_pswd_reset_tokens ADD CONSTRAINT emailpassword_pswd_reset_tokens_user_id_fkey FOREIGN KEY (app_id, user_id) REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE;

    ALTER TABLE emailpassword_pswd_reset_tokens ADD COLUMN email VARCHAR(256);
   ```
4. Run the new instance(s) of the core (version 7.0.0)

## [4.0.2]

- Fixes null pointer issue when user belongs to no tenant.

## [4.0.1] - 2023-07-11

- Fixes duplicate users in users search queries when user is associated to multiple tenants

## [4.0.0] - 2023-06-02

### Changes

- Support for multitenancy
    - New tables `apps` and `tenants` have been added.
    - Schema of tables have been changed, adding `app_id` and `tenant_id` columns in tables and constraints & indexes
      have been modified to include this columns.
    - New user tables have been added to map users to apps and tenants.
    - New tables for multitenancy have been added.
- Increased transaction retry count to 50 from 20.

### Migration

1. Ensure that the core is already upgraded to version 5.0.0 (CDI version 2.21)
2. Stop the core instance(s)
3. Run the following migration script

    ```sql
    -- General Tables

    CREATE TABLE IF NOT EXISTS apps  (
      app_id VARCHAR(64) NOT NULL DEFAULT 'public',
      created_at_time BIGINT,
      CONSTRAINT apps_pkey PRIMARY KEY(app_id)
    );

    INSERT INTO apps (app_id, created_at_time) 
      VALUES ('public', 0) ON CONFLICT DO NOTHING;

    ------------------------------------------------------------

    CREATE TABLE IF NOT EXISTS tenants (
      app_id VARCHAR(64) NOT NULL DEFAULT 'public',
      tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',
      created_at_time BIGINT ,
      CONSTRAINT tenants_pkey
        PRIMARY KEY (app_id, tenant_id),
      CONSTRAINT tenants_app_id_fkey FOREIGN KEY(app_id)
        REFERENCES apps (app_id) ON DELETE CASCADE
    );

    INSERT INTO tenants (app_id, tenant_id, created_at_time) 
      VALUES ('public', 'public', 0) ON CONFLICT DO NOTHING;

    CREATE INDEX IF NOT EXISTS tenants_app_id_index ON tenants (app_id);

    ------------------------------------------------------------

    ALTER TABLE key_value
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public',
      ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE key_value
      DROP CONSTRAINT key_value_pkey;

    ALTER TABLE key_value
      ADD CONSTRAINT key_value_pkey 
        PRIMARY KEY (app_id, tenant_id, name);

    ALTER TABLE key_value
      DROP CONSTRAINT IF EXISTS key_value_tenant_id_fkey;

    ALTER TABLE key_value
      ADD CONSTRAINT key_value_tenant_id_fkey 
        FOREIGN KEY (app_id, tenant_id)
        REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS key_value_tenant_id_index ON key_value (app_id, tenant_id);

    ------------------------------------------------------------

    CREATE TABLE IF NOT EXISTS app_id_to_user_id (
      app_id VARCHAR(64) NOT NULL DEFAULT 'public',
      user_id CHAR(36) NOT NULL,
      recipe_id VARCHAR(128) NOT NULL,
      CONSTRAINT app_id_to_user_id_pkey
        PRIMARY KEY (app_id, user_id),
      CONSTRAINT app_id_to_user_id_app_id_fkey
        FOREIGN KEY(app_id) REFERENCES apps (app_id) ON DELETE CASCADE
    );

    INSERT INTO app_id_to_user_id (user_id, recipe_id) 
      SELECT user_id, recipe_id
      FROM all_auth_recipe_users ON CONFLICT DO NOTHING;

    CREATE INDEX IF NOT EXISTS app_id_to_user_id_app_id_index ON app_id_to_user_id (app_id);

    ------------------------------------------------------------

    ALTER TABLE all_auth_recipe_users
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public',
      ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE all_auth_recipe_users
      DROP CONSTRAINT all_auth_recipe_users_pkey CASCADE;

    ALTER TABLE all_auth_recipe_users
      ADD CONSTRAINT all_auth_recipe_users_pkey 
        PRIMARY KEY (app_id, tenant_id, user_id);

    ALTER TABLE all_auth_recipe_users
      DROP CONSTRAINT IF EXISTS all_auth_recipe_users_tenant_id_fkey;

    ALTER TABLE all_auth_recipe_users
      ADD CONSTRAINT all_auth_recipe_users_tenant_id_fkey 
        FOREIGN KEY (app_id, tenant_id)
        REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE;

    ALTER TABLE all_auth_recipe_users
      DROP CONSTRAINT IF EXISTS all_auth_recipe_users_user_id_fkey;

    ALTER TABLE all_auth_recipe_users
      ADD CONSTRAINT all_auth_recipe_users_user_id_fkey 
        FOREIGN KEY (app_id, user_id)
        REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE;

    DROP INDEX all_auth_recipe_users_pagination_index;

    CREATE INDEX all_auth_recipe_users_pagination_index ON all_auth_recipe_users (time_joined DESC, user_id DESC, tenant_id DESC, app_id DESC);

    CREATE INDEX IF NOT EXISTS all_auth_recipe_user_id_index ON all_auth_recipe_users (app_id, user_id);

    CREATE INDEX IF NOT EXISTS all_auth_recipe_tenant_id_index ON all_auth_recipe_users (app_id, tenant_id);

    -- Multitenancy

    CREATE TABLE IF NOT EXISTS tenant_configs (
      connection_uri_domain VARCHAR(256) DEFAULT '',
      app_id VARCHAR(64) DEFAULT 'public',
      tenant_id VARCHAR(64) DEFAULT 'public',
      core_config TEXT,
      email_password_enabled BOOLEAN,
      passwordless_enabled BOOLEAN,
      third_party_enabled BOOLEAN,
      CONSTRAINT tenant_configs_pkey
        PRIMARY KEY (connection_uri_domain, app_id, tenant_id)
    );

    ------------------------------------------------------------

    CREATE TABLE IF NOT EXISTS tenant_thirdparty_providers (
      connection_uri_domain VARCHAR(256) DEFAULT '',
      app_id VARCHAR(64) DEFAULT 'public',
      tenant_id VARCHAR(64) DEFAULT 'public',
      third_party_id VARCHAR(28) NOT NULL,
      name VARCHAR(64),
      authorization_endpoint TEXT,
      authorization_endpoint_query_params TEXT,
      token_endpoint TEXT,
      token_endpoint_body_params TEXT,
      user_info_endpoint TEXT,
      user_info_endpoint_query_params TEXT,
      user_info_endpoint_headers TEXT,
      jwks_uri TEXT,
      oidc_discovery_endpoint TEXT,
      require_email BOOLEAN,
      user_info_map_from_id_token_payload_user_id VARCHAR(64),
      user_info_map_from_id_token_payload_email VARCHAR(64),
      user_info_map_from_id_token_payload_email_verified VARCHAR(64),
      user_info_map_from_user_info_endpoint_user_id VARCHAR(64),
      user_info_map_from_user_info_endpoint_email VARCHAR(64),
      user_info_map_from_user_info_endpoint_email_verified VARCHAR(64),
      CONSTRAINT tenant_thirdparty_providers_pkey
        PRIMARY KEY (connection_uri_domain, app_id, tenant_id, third_party_id),
      CONSTRAINT tenant_thirdparty_providers_tenant_id_fkey
        FOREIGN KEY(connection_uri_domain, app_id, tenant_id)
        REFERENCES tenant_configs (connection_uri_domain, app_id, tenant_id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS tenant_thirdparty_providers_tenant_id_index ON tenant_thirdparty_providers (connection_uri_domain, app_id, tenant_id);

    ------------------------------------------------------------

    CREATE TABLE IF NOT EXISTS tenant_thirdparty_provider_clients (
      connection_uri_domain VARCHAR(256) DEFAULT '',
      app_id VARCHAR(64) DEFAULT 'public',
      tenant_id VARCHAR(64) DEFAULT 'public',
      third_party_id VARCHAR(28) NOT NULL,
      client_type VARCHAR(64) NOT NULL DEFAULT '',
      client_id VARCHAR(256) NOT NULL,
      client_secret TEXT,
      scope VARCHAR(128)[],
      force_pkce BOOLEAN,
      additional_config TEXT,
      CONSTRAINT tenant_thirdparty_provider_clients_pkey
        PRIMARY KEY (connection_uri_domain, app_id, tenant_id, third_party_id, client_type),
      CONSTRAINT tenant_thirdparty_provider_clients_third_party_id_fkey
        FOREIGN KEY (connection_uri_domain, app_id, tenant_id, third_party_id)
        REFERENCES tenant_thirdparty_providers (connection_uri_domain, app_id, tenant_id, third_party_id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS tenant_thirdparty_provider_clients_third_party_id_index ON tenant_thirdparty_provider_clients (connection_uri_domain, app_id, tenant_id, third_party_id);

    -- Session

    ALTER TABLE session_info
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public',
      ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE session_info
      DROP CONSTRAINT session_info_pkey CASCADE;

    ALTER TABLE session_info
      ADD CONSTRAINT session_info_pkey 
        PRIMARY KEY (app_id, tenant_id, session_handle);

    ALTER TABLE session_info
      DROP CONSTRAINT IF EXISTS session_info_tenant_id_fkey;

    ALTER TABLE session_info
      ADD CONSTRAINT session_info_tenant_id_fkey 
        FOREIGN KEY (app_id, tenant_id)
        REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS session_expiry_index ON session_info (expires_at);

    CREATE INDEX IF NOT EXISTS session_info_tenant_id_index ON session_info (app_id, tenant_id);

    ------------------------------------------------------------

    ALTER TABLE session_access_token_signing_keys
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE session_access_token_signing_keys
      DROP CONSTRAINT session_access_token_signing_keys_pkey CASCADE;

    ALTER TABLE session_access_token_signing_keys
      ADD CONSTRAINT session_access_token_signing_keys_pkey 
        PRIMARY KEY (app_id, created_at_time);

    ALTER TABLE session_access_token_signing_keys
      DROP CONSTRAINT IF EXISTS session_access_token_signing_keys_app_id_fkey;

    ALTER TABLE session_access_token_signing_keys
      ADD CONSTRAINT session_access_token_signing_keys_app_id_fkey 
        FOREIGN KEY (app_id)
        REFERENCES apps (app_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS access_token_signing_keys_app_id_index ON session_access_token_signing_keys (app_id);

    -- JWT

    ALTER TABLE jwt_signing_keys
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE jwt_signing_keys
      DROP CONSTRAINT jwt_signing_keys_pkey CASCADE;

    ALTER TABLE jwt_signing_keys
      ADD CONSTRAINT jwt_signing_keys_pkey 
        PRIMARY KEY (app_id, key_id);

    ALTER TABLE jwt_signing_keys
      DROP CONSTRAINT IF EXISTS jwt_signing_keys_app_id_fkey;

    ALTER TABLE jwt_signing_keys
      ADD CONSTRAINT jwt_signing_keys_app_id_fkey 
        FOREIGN KEY (app_id)
        REFERENCES apps (app_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS jwt_signing_keys_app_id_index ON jwt_signing_keys (app_id);

    -- EmailVerification

    ALTER TABLE emailverification_verified_emails
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE emailverification_verified_emails
      DROP CONSTRAINT emailverification_verified_emails_pkey CASCADE;

    ALTER TABLE emailverification_verified_emails
      ADD CONSTRAINT emailverification_verified_emails_pkey 
        PRIMARY KEY (app_id, user_id, email);

    ALTER TABLE emailverification_verified_emails
      DROP CONSTRAINT IF EXISTS emailverification_verified_emails_app_id_fkey;

    ALTER TABLE emailverification_verified_emails
      ADD CONSTRAINT emailverification_verified_emails_app_id_fkey 
        FOREIGN KEY (app_id)
        REFERENCES apps (app_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS emailverification_verified_emails_app_id_index ON emailverification_verified_emails (app_id);

    ------------------------------------------------------------

    ALTER TABLE emailverification_tokens
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public',
      ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE emailverification_tokens
      DROP CONSTRAINT emailverification_tokens_pkey CASCADE;

    ALTER TABLE emailverification_tokens
      ADD CONSTRAINT emailverification_tokens_pkey 
        PRIMARY KEY (app_id, tenant_id, user_id, email, token);

    ALTER TABLE emailverification_tokens
      DROP CONSTRAINT IF EXISTS emailverification_tokens_tenant_id_fkey;

    ALTER TABLE emailverification_tokens
      ADD CONSTRAINT emailverification_tokens_tenant_id_fkey 
        FOREIGN KEY (app_id, tenant_id)
        REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS emailverification_tokens_tenant_id_index ON emailverification_tokens (app_id, tenant_id);

    -- EmailPassword

    ALTER TABLE emailpassword_users
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE emailpassword_users
      DROP CONSTRAINT emailpassword_users_pkey CASCADE;

    ALTER TABLE emailpassword_users
      DROP CONSTRAINT IF EXISTS emailpassword_users_email_key CASCADE;

    ALTER TABLE emailpassword_users
      ADD CONSTRAINT emailpassword_users_pkey 
        PRIMARY KEY (app_id, user_id);

    ALTER TABLE emailpassword_users
      DROP CONSTRAINT IF EXISTS emailpassword_users_user_id_fkey;

    ALTER TABLE emailpassword_users
      ADD CONSTRAINT emailpassword_users_user_id_fkey 
        FOREIGN KEY (app_id, user_id)
        REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE;

    ------------------------------------------------------------

    CREATE TABLE IF NOT EXISTS emailpassword_user_to_tenant (
      app_id VARCHAR(64) DEFAULT 'public',
      tenant_id VARCHAR(64) DEFAULT 'public',
      user_id CHAR(36) NOT NULL,
      email VARCHAR(256) NOT NULL,
      CONSTRAINT emailpassword_user_to_tenant_email_key
        UNIQUE (app_id, tenant_id, email),
      CONSTRAINT emailpassword_user_to_tenant_pkey
        PRIMARY KEY (app_id, tenant_id, user_id),
      CONSTRAINT emailpassword_user_to_tenant_user_id_fkey
        FOREIGN KEY (app_id, tenant_id, user_id)
        REFERENCES all_auth_recipe_users (app_id, tenant_id, user_id) ON DELETE CASCADE
    );

    ALTER TABLE emailpassword_user_to_tenant
      DROP CONSTRAINT IF EXISTS emailpassword_user_to_tenant_email_key;

    ALTER TABLE emailpassword_user_to_tenant
      ADD CONSTRAINT emailpassword_user_to_tenant_email_key
        UNIQUE (app_id, tenant_id, email);

    ALTER TABLE emailpassword_user_to_tenant
      DROP CONSTRAINT IF EXISTS emailpassword_user_to_tenant_user_id_fkey;

    ALTER TABLE emailpassword_user_to_tenant
      ADD CONSTRAINT emailpassword_user_to_tenant_user_id_fkey
        FOREIGN KEY (app_id, tenant_id, user_id)
        REFERENCES all_auth_recipe_users (app_id, tenant_id, user_id) ON DELETE CASCADE;

    INSERT INTO emailpassword_user_to_tenant (user_id, email)
      SELECT user_id, email FROM emailpassword_users ON CONFLICT DO NOTHING;

    ------------------------------------------------------------

    ALTER TABLE emailpassword_pswd_reset_tokens
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE emailpassword_pswd_reset_tokens
      DROP CONSTRAINT emailpassword_pswd_reset_tokens_pkey CASCADE;

    ALTER TABLE emailpassword_pswd_reset_tokens
      ADD CONSTRAINT emailpassword_pswd_reset_tokens_pkey 
        PRIMARY KEY (app_id, user_id, token);

    ALTER TABLE emailpassword_pswd_reset_tokens
      DROP CONSTRAINT IF EXISTS emailpassword_pswd_reset_tokens_user_id_fkey;

    ALTER TABLE emailpassword_pswd_reset_tokens
      ADD CONSTRAINT emailpassword_pswd_reset_tokens_user_id_fkey 
        FOREIGN KEY (app_id, user_id)
        REFERENCES emailpassword_users (app_id, user_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS emailpassword_pswd_reset_tokens_user_id_index ON emailpassword_pswd_reset_tokens (app_id, user_id);

    -- Passwordless

    ALTER TABLE passwordless_users
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE passwordless_users
      DROP CONSTRAINT passwordless_users_pkey CASCADE;

    ALTER TABLE passwordless_users
      ADD CONSTRAINT passwordless_users_pkey 
        PRIMARY KEY (app_id, user_id);

    ALTER TABLE passwordless_users
      DROP CONSTRAINT IF EXISTS passwordless_users_email_key;

    ALTER TABLE passwordless_users
      DROP CONSTRAINT IF EXISTS passwordless_users_phone_number_key;

    ALTER TABLE passwordless_users
      DROP CONSTRAINT IF EXISTS passwordless_users_user_id_fkey;

    ALTER TABLE passwordless_users
      ADD CONSTRAINT passwordless_users_user_id_fkey 
        FOREIGN KEY (app_id, user_id)
        REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE;

    ------------------------------------------------------------

    CREATE TABLE IF NOT EXISTS passwordless_user_to_tenant (
      app_id VARCHAR(64) DEFAULT 'public',
      tenant_id VARCHAR(64) DEFAULT 'public',
      user_id CHAR(36) NOT NULL,
      email VARCHAR(256),
      phone_number VARCHAR(256),
      CONSTRAINT passwordless_user_to_tenant_email_key
        UNIQUE (app_id, tenant_id, email),
      CONSTRAINT passwordless_user_to_tenant_phone_number_key
        UNIQUE (app_id, tenant_id, phone_number),
      CONSTRAINT passwordless_user_to_tenant_pkey
        PRIMARY KEY (app_id, tenant_id, user_id),
      CONSTRAINT passwordless_user_to_tenant_user_id_fkey
        FOREIGN KEY (app_id, tenant_id, user_id)
        REFERENCES all_auth_recipe_users (app_id, tenant_id, user_id) ON DELETE CASCADE
    );

    ALTER TABLE passwordless_user_to_tenant
      DROP CONSTRAINT IF EXISTS passwordless_user_to_tenant_user_id_fkey;

    ALTER TABLE passwordless_user_to_tenant
      ADD CONSTRAINT passwordless_user_to_tenant_user_id_fkey
        FOREIGN KEY (app_id, tenant_id, user_id)
        REFERENCES all_auth_recipe_users (app_id, tenant_id, user_id) ON DELETE CASCADE;

    INSERT INTO passwordless_user_to_tenant (user_id, email, phone_number)
      SELECT user_id, email, phone_number FROM passwordless_users ON CONFLICT DO NOTHING;

    ------------------------------------------------------------

    ALTER TABLE passwordless_devices
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public',
      ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE passwordless_devices
      DROP CONSTRAINT passwordless_devices_pkey CASCADE;

    ALTER TABLE passwordless_devices
      ADD CONSTRAINT passwordless_devices_pkey 
        PRIMARY KEY (app_id, tenant_id, device_id_hash);

    ALTER TABLE passwordless_devices
      DROP CONSTRAINT IF EXISTS passwordless_devices_tenant_id_fkey;

    ALTER TABLE passwordless_devices
      ADD CONSTRAINT passwordless_devices_tenant_id_fkey 
        FOREIGN KEY (app_id, tenant_id)
        REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE;

    DROP INDEX IF EXISTS passwordless_devices_email_index;

    CREATE INDEX IF NOT EXISTS passwordless_devices_email_index ON passwordless_devices (app_id, tenant_id, email);

    DROP INDEX IF EXISTS passwordless_devices_phone_number_index;

    CREATE INDEX IF NOT EXISTS passwordless_devices_phone_number_index ON passwordless_devices (app_id, tenant_id, phone_number);

    CREATE INDEX IF NOT EXISTS passwordless_devices_tenant_id_index ON passwordless_devices (app_id, tenant_id);

    ------------------------------------------------------------

    ALTER TABLE passwordless_codes
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public',
      ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE passwordless_codes
      DROP CONSTRAINT passwordless_codes_pkey CASCADE;

    ALTER TABLE passwordless_codes
      ADD CONSTRAINT passwordless_codes_pkey 
        PRIMARY KEY (app_id, tenant_id, code_id);

    ALTER TABLE passwordless_codes
      DROP CONSTRAINT IF EXISTS passwordless_codes_device_id_hash_fkey;

    ALTER TABLE passwordless_codes
      ADD CONSTRAINT passwordless_codes_device_id_hash_fkey 
        FOREIGN KEY (app_id, tenant_id, device_id_hash)
        REFERENCES passwordless_devices (app_id, tenant_id, device_id_hash) ON DELETE CASCADE;

    ALTER TABLE passwordless_codes
      DROP CONSTRAINT passwordless_codes_link_code_hash_key;

    ALTER TABLE passwordless_codes
      DROP CONSTRAINT IF EXISTS passwordless_codes_link_code_hash_key;

    ALTER TABLE passwordless_codes
      ADD CONSTRAINT passwordless_codes_link_code_hash_key
        UNIQUE (app_id, tenant_id, link_code_hash);

    DROP INDEX IF EXISTS passwordless_codes_created_at_index;

    CREATE INDEX IF NOT EXISTS passwordless_codes_created_at_index ON passwordless_codes (app_id, tenant_id, created_at);

    DROP INDEX IF EXISTS passwordless_codes_device_id_hash_index;
    CREATE INDEX IF NOT EXISTS passwordless_codes_device_id_hash_index ON passwordless_codes (app_id, tenant_id, device_id_hash);

    -- ThirdParty

    ALTER TABLE thirdparty_users
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE thirdparty_users
      DROP CONSTRAINT thirdparty_users_pkey CASCADE;

    ALTER TABLE thirdparty_users
      DROP CONSTRAINT IF EXISTS thirdparty_users_user_id_key CASCADE;

    ALTER TABLE thirdparty_users
      ADD CONSTRAINT thirdparty_users_pkey 
        PRIMARY KEY (app_id, user_id);

    ALTER TABLE thirdparty_users
      DROP CONSTRAINT IF EXISTS thirdparty_users_user_id_fkey;

    ALTER TABLE thirdparty_users
      ADD CONSTRAINT thirdparty_users_user_id_fkey 
        FOREIGN KEY (app_id, user_id)
        REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE;

    DROP INDEX IF EXISTS thirdparty_users_thirdparty_user_id_index;

    CREATE INDEX IF NOT EXISTS thirdparty_users_thirdparty_user_id_index ON thirdparty_users (app_id, third_party_id, third_party_user_id);

    DROP INDEX IF EXISTS thirdparty_users_email_index;

    CREATE INDEX IF NOT EXISTS thirdparty_users_email_index ON thirdparty_users (app_id, email);

    ------------------------------------------------------------

    CREATE TABLE IF NOT EXISTS thirdparty_user_to_tenant (
      app_id VARCHAR(64) DEFAULT 'public',
      tenant_id VARCHAR(64) DEFAULT 'public',
      user_id CHAR(36) NOT NULL,
      third_party_id VARCHAR(28) NOT NULL,
      third_party_user_id VARCHAR(256) NOT NULL,
      CONSTRAINT thirdparty_user_to_tenant_third_party_user_id_key
        UNIQUE (app_id, tenant_id, third_party_id, third_party_user_id),
      CONSTRAINT thirdparty_user_to_tenant_pkey
        PRIMARY KEY (app_id, tenant_id, user_id),
      CONSTRAINT thirdparty_user_to_tenant_user_id_fkey
        FOREIGN KEY (app_id, tenant_id, user_id)
        REFERENCES all_auth_recipe_users (app_id, tenant_id, user_id) ON DELETE CASCADE
    );

    ALTER TABLE thirdparty_user_to_tenant
      DROP CONSTRAINT IF EXISTS thirdparty_user_to_tenant_third_party_user_id_key;

    ALTER TABLE thirdparty_user_to_tenant
      ADD CONSTRAINT thirdparty_user_to_tenant_third_party_user_id_key
        UNIQUE (app_id, tenant_id, third_party_id, third_party_user_id);

    ALTER TABLE thirdparty_user_to_tenant
      DROP CONSTRAINT IF EXISTS thirdparty_user_to_tenant_user_id_fkey;

    ALTER TABLE thirdparty_user_to_tenant
      ADD CONSTRAINT thirdparty_user_to_tenant_user_id_fkey
        FOREIGN KEY (app_id, tenant_id, user_id)
        REFERENCES all_auth_recipe_users (app_id, tenant_id, user_id) ON DELETE CASCADE;

    INSERT INTO thirdparty_user_to_tenant (user_id, third_party_id, third_party_user_id)
      SELECT user_id, third_party_id, third_party_user_id FROM thirdparty_users ON CONFLICT DO NOTHING;

    -- UserIdMapping

    ALTER TABLE userid_mapping
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE userid_mapping
      DROP CONSTRAINT IF EXISTS userid_mapping_pkey CASCADE;

    ALTER TABLE userid_mapping
      ADD CONSTRAINT userid_mapping_pkey 
        PRIMARY KEY (app_id, supertokens_user_id, external_user_id);

    ALTER TABLE userid_mapping
      DROP CONSTRAINT IF EXISTS userid_mapping_supertokens_user_id_key;

    ALTER TABLE userid_mapping
      ADD CONSTRAINT userid_mapping_supertokens_user_id_key
        UNIQUE (app_id, supertokens_user_id);

    ALTER TABLE userid_mapping
      DROP CONSTRAINT IF EXISTS userid_mapping_external_user_id_key;

    ALTER TABLE userid_mapping
      ADD CONSTRAINT userid_mapping_external_user_id_key
        UNIQUE (app_id, external_user_id);

    ALTER TABLE userid_mapping
      DROP CONSTRAINT IF EXISTS userid_mapping_supertokens_user_id_fkey;

    ALTER TABLE userid_mapping
      ADD CONSTRAINT userid_mapping_supertokens_user_id_fkey 
        FOREIGN KEY (app_id, supertokens_user_id)
        REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS userid_mapping_supertokens_user_id_index ON userid_mapping (app_id, supertokens_user_id);

    -- UserRoles

    ALTER TABLE roles
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE roles
      DROP CONSTRAINT roles_pkey CASCADE;

    ALTER TABLE roles
      ADD CONSTRAINT roles_pkey 
        PRIMARY KEY (app_id, role);

    ALTER TABLE roles
      DROP CONSTRAINT IF EXISTS roles_app_id_fkey;

    ALTER TABLE roles
      ADD CONSTRAINT roles_app_id_fkey 
        FOREIGN KEY (app_id)
        REFERENCES apps (app_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS roles_app_id_index ON roles (app_id);

    ------------------------------------------------------------

    ALTER TABLE role_permissions
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE role_permissions
      DROP CONSTRAINT role_permissions_pkey CASCADE;

    ALTER TABLE role_permissions
      ADD CONSTRAINT role_permissions_pkey 
        PRIMARY KEY (app_id, role, permission);

    ALTER TABLE role_permissions
      DROP CONSTRAINT IF EXISTS role_permissions_role_fkey;

    ALTER TABLE role_permissions
      ADD CONSTRAINT role_permissions_role_fkey 
        FOREIGN KEY (app_id, role)
        REFERENCES roles (app_id, role) ON DELETE CASCADE;

    DROP INDEX IF EXISTS role_permissions_permission_index;

    CREATE INDEX IF NOT EXISTS role_permissions_permission_index ON role_permissions (app_id, permission);

    CREATE INDEX IF NOT EXISTS role_permissions_role_index ON role_permissions (app_id, role);

    ------------------------------------------------------------

    ALTER TABLE user_roles
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public',
      ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE user_roles
      DROP CONSTRAINT user_roles_pkey CASCADE;

    ALTER TABLE user_roles
      ADD CONSTRAINT user_roles_pkey 
        PRIMARY KEY (app_id, tenant_id, user_id, role);

    ALTER TABLE user_roles
      DROP CONSTRAINT IF EXISTS user_roles_tenant_id_fkey;

    ALTER TABLE user_roles
      ADD CONSTRAINT user_roles_tenant_id_fkey 
        FOREIGN KEY (app_id, tenant_id)
        REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE;

    ALTER TABLE user_roles
      DROP CONSTRAINT IF EXISTS user_roles_role_fkey;

    ALTER TABLE user_roles
      ADD CONSTRAINT user_roles_role_fkey 
        FOREIGN KEY (app_id, role)
        REFERENCES roles (app_id, role) ON DELETE CASCADE;

    DROP INDEX IF EXISTS user_roles_role_index;

    CREATE INDEX IF NOT EXISTS user_roles_role_index ON user_roles (app_id, tenant_id, role);

    CREATE INDEX IF NOT EXISTS user_roles_tenant_id_index ON user_roles (app_id, tenant_id);

    CREATE INDEX IF NOT EXISTS user_roles_app_id_role_index ON user_roles (app_id, role);

    -- UserMetadata

    ALTER TABLE user_metadata
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE user_metadata
      DROP CONSTRAINT user_metadata_pkey CASCADE;

    ALTER TABLE user_metadata
      ADD CONSTRAINT user_metadata_pkey 
        PRIMARY KEY (app_id, user_id);

    ALTER TABLE user_metadata
      DROP CONSTRAINT IF EXISTS user_metadata_app_id_fkey;

    ALTER TABLE user_metadata
      ADD CONSTRAINT user_metadata_app_id_fkey 
        FOREIGN KEY (app_id)
        REFERENCES apps (app_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS user_metadata_app_id_index ON user_metadata (app_id);

    -- Dashboard

    ALTER TABLE dashboard_users
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE dashboard_users
      DROP CONSTRAINT dashboard_users_pkey CASCADE;

    ALTER TABLE dashboard_users
      ADD CONSTRAINT dashboard_users_pkey 
        PRIMARY KEY (app_id, user_id);

    ALTER TABLE dashboard_users
      DROP CONSTRAINT IF EXISTS dashboard_users_email_key;

    ALTER TABLE dashboard_users
      ADD CONSTRAINT dashboard_users_email_key
        UNIQUE (app_id, email);

    ALTER TABLE dashboard_users
      DROP CONSTRAINT IF EXISTS dashboard_users_app_id_fkey;

    ALTER TABLE dashboard_users
      ADD CONSTRAINT dashboard_users_app_id_fkey 
        FOREIGN KEY (app_id)
        REFERENCES apps (app_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS dashboard_users_app_id_index ON dashboard_users (app_id);

    ------------------------------------------------------------

    ALTER TABLE dashboard_user_sessions
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE dashboard_user_sessions
      DROP CONSTRAINT dashboard_user_sessions_pkey CASCADE;

    ALTER TABLE dashboard_user_sessions
      ADD CONSTRAINT dashboard_user_sessions_pkey 
        PRIMARY KEY (app_id, session_id);

    ALTER TABLE dashboard_user_sessions
      DROP CONSTRAINT IF EXISTS dashboard_user_sessions_user_id_fkey;

    ALTER TABLE dashboard_user_sessions
      ADD CONSTRAINT dashboard_user_sessions_user_id_fkey 
        FOREIGN KEY (app_id, user_id)
        REFERENCES dashboard_users (app_id, user_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS dashboard_user_sessions_user_id_index ON dashboard_user_sessions (app_id, user_id);

    -- TOTP

    ALTER TABLE totp_users
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE totp_users
      DROP CONSTRAINT totp_users_pkey CASCADE;

    ALTER TABLE totp_users
      ADD CONSTRAINT totp_users_pkey 
        PRIMARY KEY (app_id, user_id);

    ALTER TABLE totp_users
      DROP CONSTRAINT IF EXISTS totp_users_app_id_fkey;

    ALTER TABLE totp_users
      ADD CONSTRAINT totp_users_app_id_fkey 
        FOREIGN KEY (app_id)
        REFERENCES apps (app_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS totp_users_app_id_index ON totp_users (app_id);

    ------------------------------------------------------------

    ALTER TABLE totp_user_devices
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE totp_user_devices
      DROP CONSTRAINT totp_user_devices_pkey;

    ALTER TABLE totp_user_devices
      ADD CONSTRAINT totp_user_devices_pkey 
        PRIMARY KEY (app_id, user_id, device_name);

    ALTER TABLE totp_user_devices
      DROP CONSTRAINT IF EXISTS totp_user_devices_user_id_fkey;

    ALTER TABLE totp_user_devices
      ADD CONSTRAINT totp_user_devices_user_id_fkey 
        FOREIGN KEY (app_id, user_id)
        REFERENCES totp_users (app_id, user_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS totp_user_devices_user_id_index ON totp_user_devices (app_id, user_id);

    ------------------------------------------------------------

    ALTER TABLE totp_used_codes
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public',
      ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE totp_used_codes
      DROP CONSTRAINT totp_used_codes_pkey CASCADE;

    ALTER TABLE totp_used_codes
      ADD CONSTRAINT totp_used_codes_pkey 
        PRIMARY KEY (app_id, tenant_id, user_id, created_time_ms);

    ALTER TABLE totp_used_codes
      DROP CONSTRAINT IF EXISTS totp_used_codes_user_id_fkey;

    ALTER TABLE totp_used_codes
      ADD CONSTRAINT totp_used_codes_user_id_fkey 
        FOREIGN KEY (app_id, user_id)
        REFERENCES totp_users (app_id, user_id) ON DELETE CASCADE;

    ALTER TABLE totp_used_codes
      DROP CONSTRAINT IF EXISTS totp_used_codes_tenant_id_fkey;

    ALTER TABLE totp_used_codes
      ADD CONSTRAINT totp_used_codes_tenant_id_fkey 
        FOREIGN KEY (app_id, tenant_id)
        REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE;

    DROP INDEX IF EXISTS totp_used_codes_expiry_time_ms_index;

    CREATE INDEX IF NOT EXISTS totp_used_codes_expiry_time_ms_index ON totp_used_codes (app_id, tenant_id, expiry_time_ms);

    CREATE INDEX IF NOT EXISTS totp_used_codes_user_id_index ON totp_used_codes (app_id, user_id);

    CREATE INDEX IF NOT EXISTS totp_used_codes_tenant_id_index ON totp_used_codes (app_id, tenant_id);

    -- ActiveUsers

    ALTER TABLE user_last_active
      ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';

    ALTER TABLE user_last_active
      DROP CONSTRAINT user_last_active_pkey CASCADE;

    ALTER TABLE user_last_active
      ADD CONSTRAINT user_last_active_pkey 
        PRIMARY KEY (app_id, user_id);

    ALTER TABLE user_last_active
      DROP CONSTRAINT IF EXISTS user_last_active_app_id_fkey;

    ALTER TABLE user_last_active
      ADD CONSTRAINT user_last_active_app_id_fkey 
        FOREIGN KEY (app_id)
        REFERENCES apps (app_id) ON DELETE CASCADE;

    CREATE INDEX IF NOT EXISTS user_last_active_app_id_index ON user_last_active (app_id);

    ```

4. Start the new instance(s) of the core (version 6.0.0)

## [3.0.0] - 2023-04-05

- Adds `use_static_key` `BOOLEAN` column into `session_info`
- Adds support for plugin inteface version 2.23

### Migration

- If using `access_token_signing_key_dynamic` false in the core:
    - ```sql
  ALTER TABLE session_info ADD COLUMN use_static_key BOOLEAN NOT NULL DEFAULT(true);
  ALTER TABLE session_info ALTER COLUMN use_static_key DROP DEFAULT;
    ```
    - ```sql
    INSERT INTO jwt_signing_keys(key_id, key_string, algorithm, created_at)
      select CONCAT('s-', created_at_time) as key_id, value as key_string, 'RS256' as algorithm, created_at_time as created_at
      from session_access_token_signing_keys;
    ```
- If using `access_token_signing_key_dynamic` true (or not set) in the core:
    - ```sql
  ALTER TABLE session_info ADD COLUMN use_static_key BOOLEAN NOT NULL DEFAULT(false);
  ALTER TABLE session_info ALTER COLUMN use_static_key DROP DEFAULT;
    ```

## [2.4.0] - 2023-03-30

- Support for Dashboard Search

## [2.3.0] - 2023-03-27

- Support for TOTP recipe
- Support for active users

### Database changes

- Add new tables for TOTP recipe:
    - `totp_users` that stores the users that have enabled TOTP
    - `totp_user_devices` that stores devices (each device has its own secret) for each user
    - `totp_used_codes` that stores used codes for each user. This is to implement rate limiting and prevent replay
      attacks.
- Add `user_last_active` table to store the last active time of a user.

## [2.2.0] - 2023-02-21

- Adds support for Dashboard recipe

## [2.1.0] - 2022-11-07

- Updates dependencies as per: https://github.com/supertokens/supertokens-core/issues/525

## [2.0.0] - 2022-09-19

- Updates the `third_party_user_id` column in the `thirdparty_users` table from `VARCHAR(128)` to `VARCHAR(256)` to
  resolve https://github.com/supertokens/supertokens-core/issues/306

- Adds support for user migration
    - Updates the `password_hash` column in the `emailpassword_users` table from `VARCHAR(128)` to `VARCHAR(256)` to
      support more types of password hashes.

- For legacy users who are self hosting the SuperTokens core run the following command to update your database with the
  changes:
  `ALTER TABLE thirdparty_users ALTER COLUMN third_party_user_id TYPE VARCHAR(256); ALTER TABLE emailpassword_users ALTER COLUMN password_hash TYPE VARCHAR(256);`

## [1.20.0] - 2022-08-18

- Adds log level feature and compatibility with plugin interface 2.18

## [1.19.0] - 2022-08-10

- Adds compatibility with plugin interface 2.17

## [1.18.0] - 2022-07-25

- Adds support for UserIdMapping recipe

## [1.17.0] - 2022-06-07

- Compatibility with plugin interface 2.15 - returns only non expired session handles for a user

## [1.16.0] - 2022-05-05

- Adds support for UserRoles recipe

## [1.15.0] - 2022-03-04

- Adds support for the new usermetadata recipe
- Fixes https://github.com/supertokens/supertokens-postgresql-plugin/issues/34

## [1.14.0] - 2022-02-23

- Adds an index on device_id_hash to the codes table.
- Using lower transaction isolation level while creating passwordless device with code

## [1.13.2] - 2022-02-19

- Refactor Query Mechanism to avoid Memory Leaks
- Adds debug statement to help fix error of passwordless code creation procedure (related to https://github.
  com/supertokens/supertokens-core/issues/373).

## [1.13.1] - 2022-02-16

- Fixed https://github.com/supertokens/supertokens-core/issues/373: Catching `StorageTransactionLogicException` in
  transaction helper function for retries
- add workflow to verify if pr title follows conventional commits

## [1.13.0] - 2021-12-24

- added passwordless support

## [1.12.0] - 2021-12-19

### Added

- Delete user functionality

## [1.11.1] - 2021-10-07

### Changed

- Explicitly naming table constraints on creation (using the default Postgres names, so we don't break existing DBs)
- Using PSQLException to parse exception messages

## [1.11.0] - 2021-09-12

### Changed

- Added functions and other changes for the JWT recipe
- Updated to match 2.9 plugin interface to support multiple access token signing
  keys: https://github.com/supertokens/supertokens-core/issues/305
- Added new table to store access token signing keys (session_access_token_signing_keys)

### Breaking change:

- Changed email verification table to have user_id with max length 128

## [1.10.0] - 2021-06-20

### Changed

- Fixes https://github.com/supertokens/supertokens-core/issues/258
- Changes for pagination and count queries: https://github.com/supertokens/supertokens-core/issues/259
- Add GetThirdPartyUsersByEmail query: https://github.com/supertokens/supertokens-core/issues/277
- Add change email interface method within transaction: https://github.com/supertokens/supertokens-core/issues/275
- Added emailverification functions: https://github.com/supertokens/supertokens-core/issues/270

## [1.9.0] - 2021-06-01

### Added

- Added ability to specify a table schema: https://github.com/supertokens/supertokens-core/issues/251

## [1.8.0] - 2021-04-20

### Added

- Added ability to set table name prefix (https://github.com/supertokens/supertokens-core/issues/220)
- Added connection URI support (https://github.com/supertokens/supertokens-core/issues/221)

## [1.7.0] - 2021-02-16

### Changed

- Extracted email verification as its own recipe
- ThirdParty queries

## [1.6.0] - 2021-01-14

### Changed

- Used rowmapper interface
- Adds email verification queries
- User pagination queries

## [1.5.0] - 2020-11-06

### Added

- Support for emailpassword recipe
- Refactoring of queries to put them per recipe
- Changes base interface as per plugin interface 2.4

## [1.3.0] - 2020-05-21

### Added

- Adds check to know if in memory db should be used.

## [1.1.1] - 2020-04-08

### Fixed

- The core now waits for the PostgrSQL db to start
