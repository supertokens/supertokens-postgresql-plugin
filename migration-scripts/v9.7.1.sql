-- Plugin 9.7.1 (core 12.1.1): sargable dashboard user search indexes on
-- recipe_user_tenants (opclass swap of the account-info index to
-- text_pattern_ops + two partial indexes for the email-domain and provider
-- search arms).
--
-- PATCH-RELEASE MIGRATION, applied by the core itself at startup: the new
-- core creates/swaps these indexes in GeneralQueries.createTablesIfNotExists'
-- backfill list, so an in-place upgrade needs no manual step. This script
-- exists so that large deployments can build the indexes CONCURRENTLY
-- (outside any transaction, no write lock) BEFORE upgrading, making the
-- startup DDL a no-op, and so the SaaS teleport chain can do the same.
--
-- CUSTOM MIGRATION - do not run this file in a single transaction:
-- CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction block. Run
-- it with psql autocommit (each statement is its own transaction). Every
-- statement is idempotent; note the transient two-index window on the
-- account-info family between the CREATE and the DROP.

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
