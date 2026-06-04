-- =============================================================================
-- SuperTokens Reservation-Tables Backfill
-- =============================================================================
-- Populates the new reservation tables (recipe_user_account_infos,
-- recipe_user_tenants, primary_user_tenants) plus the time_joined columns on
-- app_id_to_user_id, from the legacy table set (all_auth_recipe_users,
-- emailpassword_users, passwordless_users, thirdparty_users, webauthn_users).
--
-- Idempotent. All inserts use ON CONFLICT DO NOTHING; the time_joined update
-- only touches rows where time_joined = 0.
--
-- Set-based: one statement per target table. Safe for any data volume but
-- runs in a single transaction. For very large datasets you may want to
-- split per-app and run in batches; see the per-user variant in
-- MigrationBackfillQueries.java for that pattern.
--
-- Default table names assumed. If you've configured custom table-name
-- prefixes via core config, search-and-replace before running.
--
-- USAGE
--   psql -v app_id="'my-app'" -f migration-backfill.sql   -- single app
--   psql -v app_id="''"        -f migration-backfill.sql   -- all apps
--   psql                       -f migration-backfill.sql   -- all apps (default)
--
-- The script provides a default of '' for app_id, so unset == all apps.
-- =============================================================================

\if :{?app_id}
\else
  \set app_id ''
\endif

BEGIN;

-- -----------------------------------------------------------------------------
-- Step 1: backfill app_id_to_user_id.time_joined and
--         app_id_to_user_id.primary_or_recipe_user_time_joined.
--
-- Only touches rows where time_joined = 0 (the sentinel value indicating an
-- un-backfilled legacy row). Re-running is a no-op.
-- -----------------------------------------------------------------------------
UPDATE app_id_to_user_id a
SET time_joined = COALESCE((
        SELECT MIN(time_joined)
        FROM all_auth_recipe_users u
        WHERE u.app_id = a.app_id AND u.user_id = a.user_id
    ), 0),
    primary_or_recipe_user_time_joined = COALESCE((
        SELECT MIN(primary_or_recipe_user_time_joined)
        FROM all_auth_recipe_users u
        WHERE u.app_id = a.app_id AND u.user_id = a.user_id
    ), 0)
WHERE a.time_joined = 0
  AND (NULLIF(:'app_id', '') IS NULL OR a.app_id = :'app_id');

-- -----------------------------------------------------------------------------
-- Step 2: backfill recipe_user_account_infos
--
-- One INSERT per recipe variant. Padding columns (third_party_id,
-- third_party_user_id) use '' for non-thirdparty rows, matching the live
-- write path. account_info_type values are lowercase: 'email', 'phone',
-- 'tparty' — these match RecipeUserAccountInfoQueries / the cron path.
--
-- primary_user_id is NULL for users that aren't part of a linked group;
-- otherwise it's the primary id from app_id_to_user_id.
-- -----------------------------------------------------------------------------

-- 2a) emailpassword
INSERT INTO recipe_user_account_infos
    (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,
     third_party_id, third_party_user_id, primary_user_id)
SELECT ep.app_id, ep.user_id, 'emailpassword', 'email', ep.email, '', '',
       CASE WHEN a.is_linked_or_is_a_primary_user
            THEN a.primary_or_recipe_user_id
            ELSE NULL
       END
FROM emailpassword_users ep
JOIN app_id_to_user_id a
  ON ep.app_id = a.app_id AND ep.user_id = a.user_id
WHERE NULLIF(:'app_id', '') IS NULL OR ep.app_id = :'app_id'
ON CONFLICT DO NOTHING;

-- 2b) passwordless email
INSERT INTO recipe_user_account_infos
    (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,
     third_party_id, third_party_user_id, primary_user_id)
SELECT pu.app_id, pu.user_id, 'passwordless', 'email', pu.email, '', '',
       CASE WHEN a.is_linked_or_is_a_primary_user
            THEN a.primary_or_recipe_user_id
            ELSE NULL
       END
FROM passwordless_users pu
JOIN app_id_to_user_id a
  ON pu.app_id = a.app_id AND pu.user_id = a.user_id
WHERE pu.email IS NOT NULL
  AND (NULLIF(:'app_id', '') IS NULL OR pu.app_id = :'app_id')
ON CONFLICT DO NOTHING;

-- 2c) passwordless phone
INSERT INTO recipe_user_account_infos
    (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,
     third_party_id, third_party_user_id, primary_user_id)
SELECT pu.app_id, pu.user_id, 'passwordless', 'phone', pu.phone_number, '', '',
       CASE WHEN a.is_linked_or_is_a_primary_user
            THEN a.primary_or_recipe_user_id
            ELSE NULL
       END
FROM passwordless_users pu
JOIN app_id_to_user_id a
  ON pu.app_id = a.app_id AND pu.user_id = a.user_id
WHERE pu.phone_number IS NOT NULL
  AND (NULLIF(:'app_id', '') IS NULL OR pu.app_id = :'app_id')
ON CONFLICT DO NOTHING;

-- 2d) thirdparty — email row (carries the third_party identifiers)
INSERT INTO recipe_user_account_infos
    (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,
     third_party_id, third_party_user_id, primary_user_id)
SELECT tp.app_id, tp.user_id, 'thirdparty', 'email', tp.email,
       tp.third_party_id, tp.third_party_user_id,
       CASE WHEN a.is_linked_or_is_a_primary_user
            THEN a.primary_or_recipe_user_id
            ELSE NULL
       END
FROM thirdparty_users tp
JOIN app_id_to_user_id a
  ON tp.app_id = a.app_id AND tp.user_id = a.user_id
WHERE NULLIF(:'app_id', '') IS NULL OR tp.app_id = :'app_id'
ON CONFLICT DO NOTHING;

-- 2e) thirdparty — composite tparty row (id::userId), padding cols empty
INSERT INTO recipe_user_account_infos
    (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,
     third_party_id, third_party_user_id, primary_user_id)
SELECT tp.app_id, tp.user_id, 'thirdparty', 'tparty',
       tp.third_party_id || '::' || tp.third_party_user_id, '', '',
       CASE WHEN a.is_linked_or_is_a_primary_user
            THEN a.primary_or_recipe_user_id
            ELSE NULL
       END
FROM thirdparty_users tp
JOIN app_id_to_user_id a
  ON tp.app_id = a.app_id AND tp.user_id = a.user_id
WHERE NULLIF(:'app_id', '') IS NULL OR tp.app_id = :'app_id'
ON CONFLICT DO NOTHING;

-- 2f) webauthn
INSERT INTO recipe_user_account_infos
    (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,
     third_party_id, third_party_user_id, primary_user_id)
SELECT wu.app_id, wu.user_id, 'webauthn', 'email', wu.email, '', '',
       CASE WHEN a.is_linked_or_is_a_primary_user
            THEN a.primary_or_recipe_user_id
            ELSE NULL
       END
FROM webauthn_users wu
JOIN app_id_to_user_id a
  ON wu.app_id = a.app_id AND wu.user_id = a.user_id
WHERE NULLIF(:'app_id', '') IS NULL OR wu.app_id = :'app_id'
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- Step 3: backfill recipe_user_tenants from (users × account_infos)
-- -----------------------------------------------------------------------------
INSERT INTO recipe_user_tenants
    (app_id, recipe_user_id, tenant_id, recipe_id,
     account_info_type, account_info_value,
     third_party_id, third_party_user_id)
SELECT u.app_id, u.user_id, u.tenant_id, u.recipe_id,
       rai.account_info_type, rai.account_info_value,
       rai.third_party_id, rai.third_party_user_id
FROM all_auth_recipe_users u
JOIN recipe_user_account_infos rai
  ON u.app_id = rai.app_id AND u.user_id = rai.recipe_user_id
WHERE NULLIF(:'app_id', '') IS NULL OR u.app_id = :'app_id'
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- Step 4: backfill primary_user_tenants — only for linked / primary users.
--
-- The DISTINCT collapses per-recipe duplicates (a primary user with three
-- linked login methods on tenant T should produce one row per
-- (account_info_type, account_info_value) pair on that tenant, not three).
-- -----------------------------------------------------------------------------
INSERT INTO primary_user_tenants
    (app_id, tenant_id, primary_user_id, account_info_type, account_info_value)
SELECT DISTINCT rt.app_id, rt.tenant_id,
       a.primary_or_recipe_user_id,
       rt.account_info_type, rt.account_info_value
FROM recipe_user_tenants rt
JOIN app_id_to_user_id a
  ON rt.app_id = a.app_id AND rt.recipe_user_id = a.user_id
WHERE a.is_linked_or_is_a_primary_user = TRUE
  AND (NULLIF(:'app_id', '') IS NULL OR a.app_id = :'app_id')
ON CONFLICT DO NOTHING;

COMMIT;

-- =============================================================================
-- Verification queries — run after backfill to sanity-check completeness.
-- (Uncomment to run as part of the script, or run separately.)
-- =============================================================================

-- -- All users have time_joined populated
-- SELECT COUNT(*) AS users_missing_time_joined
-- FROM app_id_to_user_id
-- WHERE time_joined = 0
--   AND (NULLIF(:'app_id', '') IS NULL OR app_id = :'app_id');
-- -- Expected: 0

-- -- All recipe users have account info
-- SELECT COUNT(*) AS users_missing_account_info
-- FROM app_id_to_user_id a
-- LEFT JOIN recipe_user_account_infos rai
--   ON a.app_id = rai.app_id AND a.user_id = rai.recipe_user_id
-- WHERE rai.recipe_user_id IS NULL
--   AND (NULLIF(:'app_id', '') IS NULL OR a.app_id = :'app_id');
-- -- Expected: 0

-- -- All linked users have primary reservations
-- SELECT COUNT(*) AS linked_users_missing_primary_reservation
-- FROM app_id_to_user_id a
-- WHERE a.is_linked_or_is_a_primary_user = TRUE
--   AND (NULLIF(:'app_id', '') IS NULL OR a.app_id = :'app_id')
--   AND NOT EXISTS (
--     SELECT 1 FROM primary_user_tenants pt
--     WHERE pt.app_id = a.app_id
--       AND pt.primary_user_id = a.primary_or_recipe_user_id
--   );
-- -- Expected: 0
