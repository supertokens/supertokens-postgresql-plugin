-- Plugin 9.7.0 (core 12.1.0): refresh token rotation columns on session_info.

ALTER TABLE session_info ADD COLUMN prev_refresh_token_hash_2 VARCHAR(128);
ALTER TABLE session_info ADD COLUMN refresh_token_rotated_at BIGINT;
