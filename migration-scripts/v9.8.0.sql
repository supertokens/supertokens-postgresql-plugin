-- Plugin 9.8.0 (core 12.2.0): whole-table extended statistics for the two
-- dashboard-search index expressions on recipe_user_tenants (email-domain and
-- provider arms) plus a one-time ANALYZE, so the planner stops misestimating
-- those arms and rejecting the 9.7.1 partial indexes on large tables.
--
-- Applied by the plugin itself at startup (PostgreSQL >= 14; skipped on older
-- versions), so an in-place upgrade needs no manual step. This script exists
-- so that large deployments can run it BEFORE upgrading - the first dashboard
-- search after the upgrade then doesn't wait on the ANALYZE - and so the SaaS
-- teleport chain can do the same. Safe to run online: ANALYZE samples the
-- table, it does not scan it. Every statement is idempotent.

CREATE STATISTICS IF NOT EXISTS st_recipe_user_tenants_search_domain
  ON (lower(split_part(account_info_value, '@', 2))) FROM recipe_user_tenants;
CREATE STATISTICS IF NOT EXISTS st_recipe_user_tenants_search_tparty
  ON (lower(account_info_value)) FROM recipe_user_tenants;
ANALYZE recipe_user_tenants;
