-- Plugin 9.9.1 (core 12.3.1): SAML callback replay hardening (GHSA-3r7x-rhvc-fw28).
--
-- Adds the AuthnRequest ID and ACS URL to saml_relay_state so the callback can bind a response to the
-- login it answers (InResponseTo) and pin its Destination/Recipient, and adds saml_consumed_assertions
-- so each signed assertion is accepted at most once (this closes the IdP-initiated replay window).
--
-- Applied by the plugin itself at startup. This script lets large deployments apply it ahead of the
-- upgrade and lets the SaaS teleport chain apply it. All statements are additive and online-safe.

ALTER TABLE saml_relay_state ADD COLUMN IF NOT EXISTS in_response_to VARCHAR(256);
ALTER TABLE saml_relay_state ADD COLUMN IF NOT EXISTS acs_url TEXT;

CREATE TABLE IF NOT EXISTS saml_consumed_assertions (
    app_id VARCHAR(64) NOT NULL DEFAULT 'public',
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',
    assertion_id VARCHAR(256) NOT NULL,
    expires_at BIGINT NOT NULL,
    CONSTRAINT saml_consumed_assertions_pkey PRIMARY KEY(app_id, tenant_id, assertion_id),
    CONSTRAINT saml_consumed_assertions_app_id_fkey FOREIGN KEY(app_id) REFERENCES apps (app_id) ON DELETE CASCADE,
    CONSTRAINT saml_consumed_assertions_tenant_id_fkey FOREIGN KEY(app_id, tenant_id) REFERENCES tenants (app_id, tenant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS saml_consumed_assertions_expires_at_index ON saml_consumed_assertions (expires_at);

-- SAML IdP-initiated ACS pinning (GHSA-3r7x follow-up): a nullable allow-list of ACS URLs on the
-- SAML client. NULL = not configured (legacy behaviour preserved). Additive and online-safe.
ALTER TABLE saml_clients ADD COLUMN IF NOT EXISTS acs_urls TEXT;
