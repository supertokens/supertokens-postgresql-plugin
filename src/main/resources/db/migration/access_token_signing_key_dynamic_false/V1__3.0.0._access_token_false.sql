ALTER TABLE session_info ADD COLUMN use_static_key BOOLEAN NOT NULL DEFAULT(true);
ALTER TABLE session_info ALTER COLUMN use_static_key DROP DEFAULT;
INSERT INTO jwt_signing_keys(key_id, key_string, algorithm, created_at)
  select CONCAT('s-', created_at_time) as key_id, value as key_string, 'RS256' as algorithm, created_at_time as created_at
  from session_access_token_signing_keys;