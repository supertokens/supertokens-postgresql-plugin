-- Plugin 9.9.0 (core 12.3.0): activity_log.payload moves from TEXT to JSONB.
-- The activity-log lifecycle/activity ledger now writes structured JSON payloads,
-- and JSONB rejects malformed JSON at write time. Applied to the partitioned
-- activity_log parent, so PostgreSQL rewrites every child partition with the same cast.
--
-- Applied by the plugin itself at startup (guarded: skipped when payload is already
-- JSONB), so an in-place upgrade needs no manual step. This script exists so that large
-- deployments can run it BEFORE upgrading, and so the SaaS teleport chain can apply it.
--
-- WARNING: ALTER COLUMN ... TYPE rewrites the whole table under an ACCESS EXCLUSIVE lock.
-- On a large activity_log run this ahead of the upgrade to avoid a startup stall. All
-- historical payloads are NULL, so the cast is a pure type rewrite (no value parsing),
-- but the table rewrite still happens. USING payload::jsonb would fail loudly on any
-- non-JSON text rather than silently dropping it.

ALTER TABLE activity_log ALTER COLUMN payload TYPE JSONB USING payload::jsonb;
