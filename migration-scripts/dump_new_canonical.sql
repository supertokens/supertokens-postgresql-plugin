-- =============================================================================
-- Canonical Projection — Reservation (new) tables
-- =============================================================================
-- Mirror of dump_old_canonical.sql, reading from the new table set
-- (recipe_user_account_infos, recipe_user_tenants). Output schema is
-- byte-identical so the two CSVs can be diffed line-for-line.
--
-- Output columns:
--   canonical_user_id, canonical_primary_id, tenant_id, recipe_id,
--   account_info_type, account_info_value, tp_id, tp_user_id, is_primary
--
-- USAGE
--   psql -v app_id="'my-app'" -f dump_new_canonical.sql > new.csv
--   psql                       -f dump_new_canonical.sql > new.csv  -- all apps
-- =============================================================================

\if :{?app_id}
\else
  \set app_id ''
\endif

COPY (
WITH
scoped_infos AS (
    SELECT app_id, recipe_user_id AS user_id, recipe_id,
           account_info_type, account_info_value,
           third_party_id    AS tp_id,
           third_party_user_id AS tp_user_id,
           primary_user_id
    FROM recipe_user_account_infos
    WHERE NULLIF(:'app_id', '') IS NULL OR app_id = :'app_id'
),
-- Same hash function as dump_old_canonical.sql — same inputs produce same id.
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
),
-- A user is "primary" (in the linked-group sense) iff any of its
-- recipe_user_account_infos rows have a non-null primary_user_id.
user_primary_flag AS (
    SELECT app_id, user_id,
           bool_or(primary_user_id IS NOT NULL) AS is_primary,
           MAX(primary_user_id)                  AS primary_user_id
    FROM scoped_infos
    GROUP BY app_id, user_id
)
SELECT
    uc.canonical_id                                       AS canonical_user_id,
    pc.canonical_id                                       AS canonical_primary_id,
    rt.tenant_id,
    rt.recipe_id,
    rt.account_info_type,
    rt.account_info_value,
    rt.third_party_id                                     AS tp_id,
    rt.third_party_user_id                                AS tp_user_id,
    upf.is_primary
FROM recipe_user_tenants rt
JOIN user_canonical uc
  ON uc.app_id = rt.app_id AND uc.user_id = rt.recipe_user_id
JOIN user_primary_flag upf
  ON upf.app_id = rt.app_id AND upf.user_id = rt.recipe_user_id
LEFT JOIN user_canonical pc
  ON pc.app_id = rt.app_id AND pc.user_id = upf.primary_user_id
WHERE NULLIF(:'app_id', '') IS NULL OR rt.app_id = :'app_id'
ORDER BY
    canonical_user_id,
    rt.recipe_id,
    rt.tenant_id,
    rt.account_info_type,
    rt.account_info_value,
    rt.third_party_id,
    rt.third_party_user_id
) TO STDOUT WITH (FORMAT csv, HEADER true);
