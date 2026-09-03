-- Plugin 8.1.0 (core 10.1.0): WebAuthn tables (10.1.0) and emailverification / user_roles
-- indexes (10.1.2). Tables are ordered so webauthn_users exists before
-- webauthn_credentials references it.

CREATE INDEX IF NOT EXISTS emailverification_verified_emails_app_id_email_index ON emailverification_verified_emails (app_id, email);

CREATE TABLE IF NOT EXISTS webauthn_users (
    app_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    user_id CHAR(36) NOT NULL,
    email VARCHAR(256) NOT NULL,
    rp_id VARCHAR(256) NOT NULL,
    time_joined BIGINT NOT NULL,
    CONSTRAINT webauthn_users_pkey PRIMARY KEY (app_id, user_id),
    CONSTRAINT webauthn_users_user_id_fkey FOREIGN KEY (app_id, user_id)
        REFERENCES app_id_to_user_id (app_id, user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS webauthn_user_to_tenant (
    app_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    tenant_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    user_id CHAR(36) NOT NULL,
    email VARCHAR(256) NOT NULL,
    CONSTRAINT webauthn_user_to_tenant_email_key UNIQUE (app_id, tenant_id, email),
    CONSTRAINT webauthn_user_to_tenant_pkey PRIMARY KEY (app_id, tenant_id, user_id),
    CONSTRAINT webauthn_user_to_tenant_user_id_fkey FOREIGN KEY (app_id, tenant_id, user_id)
        REFERENCES all_auth_recipe_users (app_id, tenant_id, user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS webauthn_credentials (
    id VARCHAR(256) NOT NULL,
    app_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    rp_id VARCHAR(256) NOT NULL,
    user_id CHAR(36),
    counter BIGINT NOT NULL,
    public_key BYTEA NOT NULL,
    transports TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT webauthn_credentials_pkey PRIMARY KEY (app_id, rp_id, id),
    CONSTRAINT webauthn_credentials_user_id_fkey FOREIGN KEY (app_id, user_id)
        REFERENCES webauthn_users (app_id, user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS webauthn_generated_options (
    app_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    tenant_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    id CHAR(36) NOT NULL,
    challenge VARCHAR(256) NOT NULL,
    email VARCHAR(256),
    rp_id VARCHAR(256) NOT NULL,
    rp_name VARCHAR(256) NOT NULL,
    origin VARCHAR(256) NOT NULL,
    expires_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    user_presence_required BOOLEAN DEFAULT false NOT NULL,
    user_verification VARCHAR(12) DEFAULT 'preferred' NOT NULL,
    CONSTRAINT webauthn_generated_options_pkey PRIMARY KEY (app_id, tenant_id, id),
    CONSTRAINT webauthn_generated_options_tenant_id_fkey FOREIGN KEY (app_id, tenant_id)
        REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS webauthn_account_recovery_tokens (
    app_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    tenant_id VARCHAR(64) DEFAULT 'public' NOT NULL,
    user_id CHAR(36) NOT NULL,
    email VARCHAR(256) NOT NULL,
    token VARCHAR(256) NOT NULL,
    expires_at BIGINT NOT NULL,
    CONSTRAINT webauthn_account_recovery_token_pkey PRIMARY KEY (app_id, tenant_id, user_id, token),
    CONSTRAINT webauthn_account_recovery_token_user_id_fkey FOREIGN KEY (app_id, tenant_id, user_id)
        REFERENCES all_auth_recipe_users (app_id, tenant_id, user_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS webauthn_user_to_tenant_email_index ON webauthn_user_to_tenant (app_id, email);
CREATE INDEX IF NOT EXISTS webauthn_user_challenges_expires_at_index ON webauthn_generated_options (app_id, tenant_id, expires_at);
CREATE INDEX IF NOT EXISTS webauthn_credentials_user_id_index ON webauthn_credentials (user_id);
CREATE INDEX IF NOT EXISTS webauthn_account_recovery_token_token_index ON webauthn_account_recovery_tokens (app_id, tenant_id, token);
CREATE INDEX IF NOT EXISTS webauthn_account_recovery_token_expires_at_index ON webauthn_account_recovery_tokens (expires_at DESC);
CREATE INDEX IF NOT EXISTS webauthn_account_recovery_token_email_index ON webauthn_account_recovery_tokens (app_id, tenant_id, email);

CREATE INDEX IF NOT EXISTS user_roles_app_id_user_id_index ON user_roles (app_id, user_id);
