-- Core 12.0: schema rework (supertokens-core PR #1275).
-- New recipe_user_account_infos / recipe_user_tenants / primary_user_tenants
-- tables, activity_log (partitioned), and FK rework to ON UPDATE CASCADE.
--
-- CUSTOM MIGRATION — do not run this file blindly in a single transaction:
--   * Step 1 (up to the marker below) is transactional and idempotent.
--   * Step 2 drops INVALID leftovers of the pagination indexes (a crashed
--     CONCURRENTLY build leaves an invalid index behind that IF NOT EXISTS
--     would wrongly treat as present).
--   * Step 3's CREATE INDEX CONCURRENTLY statements MUST run outside any
--     transaction block, each as its own statement (psql default autocommit
--     handles this; do NOT wrap the file in BEGIN/COMMIT).
-- This release also requires the user-record backfill
-- (requiresBackfillMigration in manifest.json; see migration-backfill.sql
-- and the dual-write tooling in this directory).

-- ── Step 1: transactional DDL ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS recipe_user_account_infos (
    app_id              VARCHAR(64)  NOT NULL,
    recipe_user_id      CHAR(36)     NOT NULL,
    recipe_id           VARCHAR(128) NOT NULL,
    account_info_type   VARCHAR(8)   NOT NULL,
    account_info_value  TEXT         NOT NULL,
    third_party_id      VARCHAR(28),
    third_party_user_id VARCHAR(256),
    primary_user_id     CHAR(36)     NULL,
    CONSTRAINT recipe_user_account_infos_pkey
        PRIMARY KEY (app_id, recipe_id, recipe_user_id, account_info_type, third_party_id, third_party_user_id),
    CONSTRAINT recipe_user_account_infos_tenant_id_fkey
        FOREIGN KEY (app_id)
        REFERENCES apps (app_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_recipe_user_account_infos_app_recipe_user
    ON recipe_user_account_infos (app_id, recipe_user_id);

CREATE TABLE IF NOT EXISTS recipe_user_tenants (
    app_id              VARCHAR(64)  NOT NULL,
    recipe_user_id      CHAR(36)     NOT NULL,
    tenant_id           VARCHAR(64)  NOT NULL,
    recipe_id           VARCHAR(128) NOT NULL,
    account_info_type   VARCHAR(8)   NOT NULL,
    account_info_value  TEXT         NOT NULL,
    third_party_id      VARCHAR(28),
    third_party_user_id VARCHAR(256),
    CONSTRAINT recipe_user_tenants_pkey
        PRIMARY KEY (app_id, tenant_id, recipe_id, account_info_type, third_party_id, third_party_user_id, account_info_value),
    CONSTRAINT recipe_user_tenants_tenant_id_fkey
        FOREIGN KEY (app_id, tenant_id)
        REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_tenant
    ON recipe_user_tenants (app_id, tenant_id);
CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_recipe_user_id
    ON recipe_user_tenants (app_id, recipe_user_id);
CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_account_info
    ON recipe_user_tenants (app_id, tenant_id, account_info_type, account_info_value);

CREATE TABLE IF NOT EXISTS primary_user_tenants (
    app_id             VARCHAR(64) NOT NULL,
    tenant_id          VARCHAR(64) NOT NULL,
    account_info_type  VARCHAR(8)  NOT NULL,
    account_info_value TEXT        NOT NULL,
    primary_user_id    CHAR(36)    NOT NULL,
    CONSTRAINT primary_user_tenants_pkey
        PRIMARY KEY (app_id, tenant_id, account_info_type, account_info_value),
    CONSTRAINT primary_user_tenants_app_id_fkey
        FOREIGN KEY (app_id, tenant_id)
        REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_primary_user_tenants_primary
    ON primary_user_tenants (primary_user_id);

-- activity_log audit table (core 12.0.3 / postgresql-plugin 9.5.2):
-- append-only, range-partitioned by created_at into one partition per UTC month.
CREATE TABLE IF NOT EXISTS activity_log (
    id                        BIGINT GENERATED ALWAYS AS IDENTITY,
    app_id                    VARCHAR(64)  NOT NULL DEFAULT 'public',
    tenant_id                 VARCHAR(64)  NOT NULL DEFAULT 'public',
    recipe_user_id            VARCHAR(128),
    primary_or_recipe_user_id VARCHAR(128),
    event_type                VARCHAR(64)  NOT NULL,
    status                    VARCHAR(128),
    auth_principal            VARCHAR(256),
    identifier                VARCHAR(256),
    created_at                BIGINT       NOT NULL,
    payload                   TEXT
) PARTITION BY RANGE (created_at);

-- DEFAULT partition is a backstop; the core pre-creates the current/next month at boot
-- and the CleanupActivityLogPartitions cron maintains the monthly partitions thereafter.
CREATE TABLE IF NOT EXISTS activity_log_default PARTITION OF activity_log DEFAULT;

CREATE INDEX IF NOT EXISTS activity_log_created_at_brin ON activity_log USING brin (created_at);

ALTER TABLE app_id_to_user_id
    ADD COLUMN IF NOT EXISTS time_joined BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS primary_or_recipe_user_time_joined BIGINT NOT NULL DEFAULT 0;

ALTER TABLE app_id_to_user_id
    DROP CONSTRAINT app_id_to_user_id_primary_or_recipe_user_id_fkey;
ALTER TABLE app_id_to_user_id
    ADD CONSTRAINT app_id_to_user_id_primary_or_recipe_user_id_fkey
    FOREIGN KEY (app_id, primary_or_recipe_user_id)
    REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE all_auth_recipe_users
    DROP CONSTRAINT all_auth_recipe_users_primary_or_recipe_user_id_fkey;
ALTER TABLE all_auth_recipe_users
    ADD CONSTRAINT all_auth_recipe_users_primary_or_recipe_user_id_fkey
    FOREIGN KEY (app_id, primary_or_recipe_user_id)
    REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE all_auth_recipe_users
    DROP CONSTRAINT all_auth_recipe_users_user_id_fkey;
ALTER TABLE all_auth_recipe_users
    ADD CONSTRAINT all_auth_recipe_users_user_id_fkey
    FOREIGN KEY (app_id, user_id)
    REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE emailpassword_users
    DROP CONSTRAINT emailpassword_users_user_id_fkey;
ALTER TABLE emailpassword_users
    ADD CONSTRAINT emailpassword_users_user_id_fkey
    FOREIGN KEY (app_id, user_id)
    REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE thirdparty_users
    DROP CONSTRAINT thirdparty_users_user_id_fkey;
ALTER TABLE thirdparty_users
    ADD CONSTRAINT thirdparty_users_user_id_fkey
    FOREIGN KEY (app_id, user_id)
    REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE passwordless_users
    DROP CONSTRAINT passwordless_users_user_id_fkey;
ALTER TABLE passwordless_users
    ADD CONSTRAINT passwordless_users_user_id_fkey
    FOREIGN KEY (app_id, user_id)
    REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE webauthn_users
    DROP CONSTRAINT webauthn_users_user_id_fkey;
ALTER TABLE webauthn_users
    ADD CONSTRAINT webauthn_users_user_id_fkey
    FOREIGN KEY (app_id, user_id)
    REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE webauthn_account_recovery_tokens
    DROP CONSTRAINT webauthn_account_recovery_token_user_id_fkey;
ALTER TABLE webauthn_account_recovery_tokens
    ADD CONSTRAINT webauthn_account_recovery_token_user_id_fkey
    FOREIGN KEY (app_id, user_id)
    REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE userid_mapping
    DROP CONSTRAINT userid_mapping_supertokens_user_id_fkey;
ALTER TABLE userid_mapping
    ADD CONSTRAINT userid_mapping_supertokens_user_id_fkey
    FOREIGN KEY (app_id, supertokens_user_id)
    REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE ON UPDATE CASCADE;

-- ── Step 2: drop INVALID leftovers of the pagination indexes ─────────

DO $$
DECLARE idx TEXT;
BEGIN
    FOR idx IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname IN (
            'app_id_to_user_id_pagination_index1',
            'app_id_to_user_id_pagination_index2',
            'app_id_to_user_id_pagination_index3',
            'app_id_to_user_id_pagination_index4'
        ) AND NOT i.indisvalid
    LOOP
        EXECUTE 'DROP INDEX ' || quote_ident(idx);
    END LOOP;
END $$;

-- ── Step 3: pagination indexes, CONCURRENTLY, outside any transaction ─

CREATE INDEX CONCURRENTLY IF NOT EXISTS app_id_to_user_id_pagination_index1 ON app_id_to_user_id
    (app_id, primary_or_recipe_user_time_joined DESC, primary_or_recipe_user_id DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS app_id_to_user_id_pagination_index2 ON app_id_to_user_id
    (app_id, primary_or_recipe_user_time_joined ASC, primary_or_recipe_user_id DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS app_id_to_user_id_pagination_index3 ON app_id_to_user_id
    (recipe_id, app_id, primary_or_recipe_user_time_joined DESC, primary_or_recipe_user_id DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS app_id_to_user_id_pagination_index4 ON app_id_to_user_id
    (recipe_id, app_id, primary_or_recipe_user_time_joined ASC, primary_or_recipe_user_id DESC);
