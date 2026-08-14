-- Core 11.3: SAML tables.

CREATE TABLE IF NOT EXISTS saml_clients (
    app_id VARCHAR(64) NOT NULL DEFAULT 'public',
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',
    client_id VARCHAR(256) NOT NULL,
    client_secret TEXT,
    sso_login_url TEXT NOT NULL,
    redirect_uris TEXT NOT NULL,
    default_redirect_uri TEXT NOT NULL,
    idp_entity_id VARCHAR(256) NOT NULL,
    idp_signing_certificate TEXT NOT NULL,
    allow_idp_initiated_login BOOLEAN NOT NULL DEFAULT FALSE,
    enable_request_signing BOOLEAN NOT NULL DEFAULT FALSE,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT saml_clients_pkey PRIMARY KEY(app_id, tenant_id, client_id),
    CONSTRAINT saml_clients_idp_entity_id_key UNIQUE (app_id, tenant_id, idp_entity_id),
    CONSTRAINT saml_clients_app_id_fkey FOREIGN KEY(app_id) REFERENCES apps (app_id) ON DELETE CASCADE,
    CONSTRAINT saml_clients_tenant_id_fkey FOREIGN KEY(app_id, tenant_id) REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS saml_clients_app_id_tenant_id_index ON saml_clients (app_id, tenant_id);

CREATE TABLE IF NOT EXISTS saml_relay_state (
    app_id VARCHAR(64) NOT NULL DEFAULT 'public',
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',
    relay_state VARCHAR(256) NOT NULL,
    client_id VARCHAR(256) NOT NULL,
    state TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    CONSTRAINT saml_relay_state_pkey PRIMARY KEY(app_id, tenant_id, relay_state),
    CONSTRAINT saml_relay_state_app_id_fkey FOREIGN KEY(app_id) REFERENCES apps (app_id) ON DELETE CASCADE,
    CONSTRAINT saml_relay_state_tenant_id_fkey FOREIGN KEY(app_id, tenant_id) REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS saml_relay_state_app_id_tenant_id_index ON saml_relay_state (app_id, tenant_id);
CREATE INDEX IF NOT EXISTS saml_relay_state_expires_at_index ON saml_relay_state (expires_at);

CREATE TABLE IF NOT EXISTS saml_claims (
    app_id VARCHAR(64) NOT NULL DEFAULT 'public',
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',
    client_id VARCHAR(256) NOT NULL,
    code VARCHAR(256) NOT NULL,
    claims TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    CONSTRAINT saml_claims_pkey PRIMARY KEY(app_id, tenant_id, code),
    CONSTRAINT saml_claims_app_id_fkey FOREIGN KEY(app_id) REFERENCES apps (app_id) ON DELETE CASCADE,
    CONSTRAINT saml_claims_tenant_id_fkey FOREIGN KEY(app_id, tenant_id) REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS saml_claims_app_id_tenant_id_index ON saml_claims (app_id, tenant_id);
CREATE INDEX IF NOT EXISTS saml_claims_expires_at_index ON saml_claims (expires_at);
