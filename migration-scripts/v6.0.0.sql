-- Plugin 6.0.0 (core 8.0.0): roles no longer need to exist before being assigned to users.

ALTER TABLE user_roles DROP CONSTRAINT IF EXISTS user_roles_role_fkey;
