-- =============================================================================
-- Canonical Projection — LEGACY (old) tables
-- =============================================================================
-- Emits a CSV row per (user, tenant, account_info) with stable, content-derived
-- identifiers in place of the raw user_id/app_id/timestamps. Two databases that
-- contain logically equivalent users will produce byte-identical CSV output.
--
-- Output columns (line-comparable with dump_new_canonical.sql):
--   canonical_user_id, canonical_primary_id, tenant_id, recipe_id,
--   account_info_type, account_info_value, tp_id, tp_user_id, is_primary
--
-- USAGE
--   psql -v app_id="'my-app'" -f dump_old_canonical.sql > old.csv
--   psql                       -f dump_old_canonical.sql > old.csv  -- all apps
-- =============================================================================

\if :{?app_id}
\else
  \set app_id ''
\endif

COPY (
WITH
-- Synthesize the same per-recipe account_info rows that the backfill
-- produces, reading directly from the recipe-specific old tables.
synth_account_infos AS (
    SELECT ep.app_id, ep.user_id, 'emailpassword' AS recipe_id,
           'email' AS account_info_type, ep.email AS account_info_value,
           '' AS tp_id, '' AS tp_user_id
    FROM emailpassword_users ep
    UNION ALL
    SELECT pu.app_id, pu.user_id, 'passwordless', 'email', pu.email, '', ''
    FROM passwordless_users pu WHERE pu.email IS NOT NULL
    UNION ALL
    SELECT pu.app_id, pu.user_id, 'passwordless', 'phone', pu.phone_number, '', ''
    FROM passwordless_users pu WHERE pu.phone_number IS NOT NULL
    UNION ALL
    SELECT tp.app_id, tp.user_id, 'thirdparty', 'email', tp.email,
           tp.third_party_id, tp.third_party_user_id
    FROM thirdparty_users tp
    UNION ALL
    SELECT tp.app_id, tp.user_id, 'thirdparty', 'tparty',
           tp.third_party_id || '::' || tp.third_party_user_id, '', ''
    FROM thirdparty_users tp
    UNION ALL
    SELECT wu.app_id, wu.user_id, 'webauthn', 'email', wu.email, '', ''
    FROM webauthn_users wu
),
scoped_infos AS (
    SELECT * FROM synth_account_infos
    WHERE NULLIF(:'app_id', '') IS NULL OR app_id = :'app_id'
),
-- One stable hash per (app_id, user_id) derived from ALL of that user's
-- account_info rows. The hash is order-independent (string_agg with ORDER BY).
user_canonical AS (
    SELECT app_id, user_id,
           md5(string_agg(
               recipe_id || '|' || account_info_type || '|' ||
               COALESCE(account_info_value, '') || '|' ||
               tp_id || '|' || tp_user_id,
               E'\n'
               ORDER BY account_info_type, account_info_value, tp_id, tp_user_id
           )) AS canonical_id
    FROM scoped_infos
    GROUP BY app_id, user_id
)
SELECT
    uc.canonical_id                                       AS canonical_user_id,
    pc.canonical_id                                       AS canonical_primary_id,
    aru.tenant_id,
    aru.recipe_id,
    si.account_info_type,
    si.account_info_value,
    si.tp_id,
    si.tp_user_id,
    a.is_linked_or_is_a_primary_user                      AS is_primary
FROM all_auth_recipe_users aru
JOIN app_id_to_user_id a
  ON aru.app_id = a.app_id AND aru.user_id = a.user_id
JOIN scoped_infos si
  ON si.app_id = aru.app_id AND si.user_id = aru.user_id
JOIN user_canonical uc
  ON uc.app_id = aru.app_id AND uc.user_id = aru.user_id
LEFT JOIN user_canonical pc
  ON pc.app_id = a.app_id AND pc.user_id = a.primary_or_recipe_user_id
WHERE NULLIF(:'app_id', '') IS NULL OR aru.app_id = :'app_id'
ORDER BY
    canonical_user_id,
    aru.recipe_id,
    aru.tenant_id,
    si.account_info_type,
    si.account_info_value,
    si.tp_id,
    si.tp_user_id
) TO STDOUT WITH (FORMAT csv, HEADER true);
