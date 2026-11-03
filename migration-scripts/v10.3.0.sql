-- Plugin 10.3.0 (core 12.5.0): optional SAML client attribute mapping.
--
-- A nullable saml_clients.attribute_mapping column holding the client's attribute mapping as JSON object
-- text. NULL = not configured (existing clients are unchanged).
--
-- Applied by the plugin itself at startup. This script lets large deployments apply it ahead of the
-- upgrade and lets the SaaS teleport chain apply it. Additive and online-safe.

ALTER TABLE saml_clients ADD COLUMN IF NOT EXISTS attribute_mapping TEXT;
