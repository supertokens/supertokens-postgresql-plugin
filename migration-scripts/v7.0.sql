-- Plugin 7.0 (core 9.0): created_at on TOTP devices.

ALTER TABLE totp_user_devices ADD COLUMN IF NOT EXISTS created_at BIGINT default 0;
ALTER TABLE totp_user_devices ALTER COLUMN created_at DROP DEFAULT;
