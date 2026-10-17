-- Plugin 9.9.2: index on webauthn_credentials (app_id, id).
--
-- WebAuthn credential lookups by id alone (sign in, the counter update, and the app-wide credential id
-- uniqueness check at registration) cannot use the (app_id, rp_id, id) primary key, so without this index
-- each of them scans every credential of the app.
--
-- PATCH-RELEASE MIGRATION, applied by the core itself at startup: the new core creates this index in
-- GeneralQueries.createTablesIfNotExists' backfill list, so an in-place upgrade needs no manual step. This
-- script lets large deployments build it CONCURRENTLY (outside any transaction, no write lock) BEFORE
-- upgrading, making the startup DDL a no-op, and lets the SaaS teleport chain do the same.
--
-- Do not run this file inside a transaction block: CREATE INDEX CONCURRENTLY cannot run in one. Run it
-- with psql autocommit. The statement is idempotent.

CREATE INDEX CONCURRENTLY IF NOT EXISTS webauthn_credentials_app_id_id_index ON webauthn_credentials (app_id, id);
