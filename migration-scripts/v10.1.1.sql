-- Plugin 10.1.1: index on webauthn_credentials (app_id, id).
--
-- The app-wide credential id uniqueness check at registration looks credentials up by id alone, which the
-- (app_id, rp_id, id) primary key cannot serve, so without this index it scans every credential of the app.
-- (Sign in and the counter update are rp-scoped on 10.x and use the primary key.)
--
-- PATCH-RELEASE MIGRATION, applied by the core itself at startup: the new core creates this index in
-- GeneralQueries.createTablesIfNotExists' backfill list, so an in-place upgrade needs no manual step. This
-- script lets large deployments build it CONCURRENTLY (outside any transaction, no write lock) BEFORE
-- upgrading, making the startup DDL a no-op, and lets the SaaS teleport chain do the same.
--
-- Do not run this file inside a transaction block: CREATE INDEX CONCURRENTLY cannot run in one. Run it
-- with psql autocommit. The statement is idempotent.

CREATE INDEX CONCURRENTLY IF NOT EXISTS webauthn_credentials_app_id_id_index ON webauthn_credentials (app_id, id);
