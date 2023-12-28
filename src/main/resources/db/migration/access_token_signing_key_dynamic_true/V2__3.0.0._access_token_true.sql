ALTER TABLE session_info ADD COLUMN use_static_key BOOLEAN NOT NULL DEFAULT(false);
ALTER TABLE session_info ALTER COLUMN use_static_key DROP DEFAULT;