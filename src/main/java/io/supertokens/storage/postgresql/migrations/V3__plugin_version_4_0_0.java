/*
 *    Copyright (c) 2023, VRAI Labs and/or its affiliates. All rights reserved.
 *
 *    This software is licensed under the Apache License, Version 2.0 (the
 *    "License") as published by the Apache Software Foundation.
 *
 *    You may not use this file except in compliance with the License. You may
 *    obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *    License for the specific language governing permissions and limitations
 *    under the License.
 */

package io.supertokens.storage.postgresql.migrations;

import io.supertokens.storage.postgresql.MigrationContextManager;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.utils.Utils;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Statement;
import java.util.Map;

import static io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.STARTING_MIGRATION;
import static io.supertokens.storage.postgresql.ProcessState.getInstance;

public class V3__plugin_version_4_0_0 extends BaseJavaMigration {


    @Override
    public void migrate(Context context) throws Exception {
        Map<String, String> ph = context.getConfiguration().getPlaceholders();
        Start start = MigrationContextManager.getContext(ph.get("process_id"));
        getInstance(start).addState(STARTING_MIGRATION, null);
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + ph.get("apps_table") + "  ( " +
                    "  app_id VARCHAR(64) NOT NULL DEFAULT 'public', " +
                    "  created_at_time BIGINT, " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("apps_table"), null,
                    "pkey")  + " PRIMARY KEY(app_id) " +
                    ");");

            statement.execute("INSERT INTO " + ph.get("apps_table") +
                    " (app_id, created_at_time) VALUES ('public', 0) ON CONFLICT DO NOTHING;");
        }

        // Migrate tenants table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + ph.get("tenants_table") + " ( " +
                    "  app_id VARCHAR(64) NOT NULL DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) NOT NULL DEFAULT 'public', " +
                    "  created_at_time BIGINT, " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("tenants_table"), null,
                            "pkey") + " PRIMARY KEY (app_id, tenant_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("tenants_table"), "app_id",
                            "fkey") + " FOREIGN KEY(app_id) " +
                    "    REFERENCES " + ph.get("apps_table") + " (app_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("INSERT INTO " + ph.get("tenants_table") +
                    " (app_id, tenant_id, created_at_time) VALUES ('public', 'public', 0) ON CONFLICT DO NOTHING;");

            statement.execute("CREATE INDEX IF NOT EXISTS tenants_app_id_index ON " +
                    ph.get("tenants_table") + " (app_id);");
        }

        // Migrate key_value table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("key_value_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("key_value_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("key_value_table"), null,
                    "pkey"));

            statement.execute("ALTER TABLE " + ph.get("key_value_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("key_value_table"), null,
                            "pkey") +
                    " PRIMARY KEY (app_id, tenant_id, name);");

            statement.execute("ALTER TABLE " + ph.get("key_value_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get("key_value_table"), "tenant_id", "fkey"));

            statement.execute("ALTER TABLE " + ph.get("key_value_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("key_value_table"), "tenant_id", "fkey") +
                    " FOREIGN KEY (app_id, tenant_id) REFERENCES " + ph.get("tenants_table") +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS key_value_tenant_id_index" +
                    " ON " + ph.get("key_value_table") + " (app_id, tenant_id);");
        }

        // Migrate app_id_to_user_id table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + ph.get("app_id_to_user_id_table") + " ( " +
                    "  app_id VARCHAR(64) NOT NULL DEFAULT 'public', " +
                    "  user_id CHAR(36) NOT NULL, " +
                    "  recipe_id VARCHAR(128) NOT NULL, " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("app_id_to_user_id_table"), null,
                    "pkey") + " PRIMARY KEY (app_id, user_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("app_id_to_user_id_table"), null,
                    "fkey") + " FOREIGN KEY(app_id) " +
                    "    REFERENCES " + ph.get("apps_table") + " (app_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("INSERT INTO " + ph.get("app_id_to_user_id_table") +
                    " (user_id, recipe_id) " +
                    "SELECT user_id, recipe_id FROM " + ph.get("all_auth_recipe_users_table") + " ON CONFLICT DO NOTHING;");

            statement.execute("CREATE INDEX IF NOT EXISTS " + ph.get("app_id_to_user_id_table") +
                    "_app_id_index ON " + ph.get("app_id_to_user_id_table") + " (app_id);");
        }

        // Migrate all_auth_recipe_users table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "all_auth_recipe_users_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "all_auth_recipe_users_table"), null, "pkey") +
                    " PRIMARY KEY (app_id, tenant_id, user_id);");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "all_auth_recipe_users_table"), "tenant_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("all_auth_recipe_users_table"), "tenant_id", "fkey") +
                    " FOREIGN KEY (app_id, tenant_id) REFERENCES " + ph.get("tenants_table") +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get("all_auth_recipe_users_table"), "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("all_auth_recipe_users_table"), "user_id", "fkey") +
                    " FOREIGN KEY (app_id, user_id) REFERENCES " + ph.get("app_id_to_user_id_table") +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS all_auth_recipe_users_pagination_index;");

            statement.execute("CREATE INDEX all_auth_recipe_users_pagination_index ON " + ph.get("all_auth_recipe_users_table") +
                    " (time_joined DESC, user_id DESC, tenant_id DESC, app_id DESC);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_user_id_index ON " + ph.get("all_auth_recipe_users_table") +
                    " (app_id, user_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_tenant_id_index ON " + ph.get("all_auth_recipe_users_table") +
                    " (app_id, tenant_id);");
        }

        // Migrate tenant_configs table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + ph.get("tenant_configs_table") + " ( " +
                    "  connection_uri_domain VARCHAR(256) DEFAULT '', " +
                    "  app_id VARCHAR(64) DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) DEFAULT 'public', " +
                    "  core_config TEXT, " +
                    "  email_password_enabled BOOLEAN, " +
                    "  passwordless_enabled BOOLEAN, " +
                    "  third_party_enabled BOOLEAN, " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("tenant_configs_table"), null,
                    "pkey") +
                    " PRIMARY KEY (connection_uri_domain, app_id, tenant_id) " +
                    ");");
        }

        // Migrate tenant_thirdparty_providers table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + ph.get("tenant_thirdparty_providers_table") + " ( " +
                    "  connection_uri_domain VARCHAR(256) DEFAULT '', " +
                    "  app_id VARCHAR(64) DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) DEFAULT 'public', " +
                    "  third_party_id VARCHAR(28) NOT NULL, " +
                    "  name VARCHAR(64), " +
                    "  authorization_endpoint TEXT, " +
                    "  authorization_endpoint_query_params TEXT, " +
                    "  token_endpoint TEXT, " +
                    "  token_endpoint_body_params TEXT, " +
                    "  user_info_endpoint TEXT, " +
                    "  user_info_endpoint_query_params TEXT, " +
                    "  user_info_endpoint_headers TEXT, " +
                    "  jwks_uri TEXT, " +
                    "  oidc_discovery_endpoint TEXT, " +
                    "  require_email BOOLEAN, " +
                    "  user_info_map_from_id_token_payload_user_id VARCHAR(64), " +
                    "  user_info_map_from_id_token_payload_email VARCHAR(64), " +
                    "  user_info_map_from_id_token_payload_email_verified VARCHAR(64), " +
                    "  user_info_map_from_user_info_endpoint_user_id VARCHAR(64), " +
                    "  user_info_map_from_user_info_endpoint_email VARCHAR(64), " +
                    "  user_info_map_from_user_info_endpoint_email_verified VARCHAR(64), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "tenant_thirdparty_providers_table"), null, "pkey") +
                    " PRIMARY KEY (connection_uri_domain, app_id, tenant_id, third_party_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("tenant_thirdparty_providers_table"), "tenant_id", "fkey") +
                    " FOREIGN KEY(connection_uri_domain, app_id, tenant_id) " +
                    "    REFERENCES " + ph.get("tenant_configs_table") + " (connection_uri_domain, app_id, tenant_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("CREATE INDEX IF NOT EXISTS tenant_thirdparty_providers_tenant_id_index ON " + ph.get("tenant_thirdparty_providers_table") +
                    " (connection_uri_domain, app_id, tenant_id);");
        }

        // Migrate tenant_thirdparty_provider_clients table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + ph.get("tenant_thirdparty_provider_clients_table") + " ( " +
                    "  connection_uri_domain VARCHAR(256) DEFAULT '', " +
                    "  app_id VARCHAR(64) DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) DEFAULT 'public', " +
                    "  third_party_id VARCHAR(28) NOT NULL, " +
                    "  client_type VARCHAR(64) NOT NULL DEFAULT '', " +
                    "  client_id VARCHAR(256) NOT NULL, " +
                    "  client_secret TEXT, " +
                    "  scope VARCHAR(128)[], " +
                    "  force_pkce BOOLEAN, " +
                    "  additional_config TEXT, " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "tenant_thirdparty_provider_clients_table"), null, "pkey") +
                    " PRIMARY KEY (connection_uri_domain, app_id, tenant_id, third_party_id, client_type), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("tenant_thirdparty_provider_clients_table"), "third_party_id", "fkey") +
                    " FOREIGN KEY (connection_uri_domain, app_id, tenant_id, third_party_id) " +
                    "    REFERENCES " + ph.get("tenant_thirdparty_providers_table") + " (connection_uri_domain, app_id, tenant_id, third_party_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("CREATE INDEX IF NOT EXISTS tenant_thirdparty_provider_clients_third_party_id_index ON " + ph.get("tenant_thirdparty_provider_clients_table") +
                    " (connection_uri_domain, app_id, tenant_id, third_party_id);");
        }

        // Migrate session_info table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("session_info_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("session_info_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("session_info_table"), null,
                    "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("session_info_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("session_info_table"), null,
                    "pkey") +
                    " PRIMARY KEY (app_id, tenant_id, session_handle);");

            statement.execute("ALTER TABLE " + ph.get("session_info_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "session_info_table"), "tenant_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("session_info_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("session_info_table"), "tenant_id", "fkey") +
                    " FOREIGN KEY (app_id, tenant_id) REFERENCES " + ph.get("tenants_table") +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS session_expiry_index ON " + ph.get("session_info_table") +
                    " (expires_at);");

            statement.execute("CREATE INDEX IF NOT EXISTS session_info_tenant_id_index ON " + ph.get("session_info_table") +
                    " (app_id, tenant_id);");
        }
        // Migrate session_access_token_signing_keys table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("session_access_token_signing_keys_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("session_access_token_signing_keys_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "session_access_token_signing_keys_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("session_access_token_signing_keys_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "session_access_token_signing_keys_table"), null, "pkey") +
                    " PRIMARY KEY (app_id, created_at_time);");

            statement.execute("ALTER TABLE " + ph.get("session_access_token_signing_keys_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get("session_access_token_signing_keys_table"), "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("session_access_token_signing_keys_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("session_access_token_signing_keys_table"), "app_id", "fkey") +
                    " FOREIGN KEY (app_id) REFERENCES " + ph.get("apps_table") +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS access_token_signing_keys_app_id_index ON " +
                    ph.get("session_access_token_signing_keys_table") + " (app_id);");
        }

        // Migrate jwt_signing_keys table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("jwt_signing_keys_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("jwt_signing_keys_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("jwt_signing_keys_table"),
                    null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("jwt_signing_keys_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("jwt_signing_keys_table"),
                    null, "pkey") +
                    " PRIMARY KEY (app_id, key_id);");

            statement.execute("ALTER TABLE " + ph.get("jwt_signing_keys_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get("jwt_signing_keys_table"), "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("jwt_signing_keys_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("jwt_signing_keys_table"), "app_id", "fkey") +
                    " FOREIGN KEY (app_id) REFERENCES " + ph.get("apps_table") +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS jwt_signing_keys_app_id_index ON " +
                    ph.get("jwt_signing_keys_table") + " (app_id);");
        }

        // Migrate emailverification_verified_emails table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("emailverification_verified_emails_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("emailverification_verified_emails_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "emailverification_verified_emails_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("emailverification_verified_emails_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "emailverification_verified_emails_table"), null, "pkey") +
                    " PRIMARY KEY (app_id, user_id, email);");

            statement.execute("ALTER TABLE " + ph.get("emailverification_verified_emails_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get("emailverification_verified_emails_table"), "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("emailverification_verified_emails_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("emailverification_verified_emails_table"), "app_id", "fkey") +
                    " FOREIGN KEY (app_id) REFERENCES " + ph.get("apps_table") +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS emailverification_verified_emails_app_id_index ON " +
                    ph.get("emailverification_verified_emails_table") + " (app_id);");
        }

        // Migrate emailverification_tokens table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("emailverification_tokens_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("emailverification_tokens_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "emailverification_tokens_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("emailverification_tokens_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "emailverification_tokens_table"), null, "pkey") +
                    " PRIMARY KEY (app_id, tenant_id, user_id, email, token);");

            statement.execute("ALTER TABLE " + ph.get("emailverification_tokens_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get("emailverification_tokens_table"), "tenant_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("emailverification_tokens_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("emailverification_tokens_table"), "tenant_id", "fkey") +
                    " FOREIGN KEY (app_id, tenant_id) REFERENCES " + ph.get("tenants_table") +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS emailverification_tokens_tenant_id_index ON " +
                    ph.get("emailverification_tokens_table") + " (app_id, tenant_id);");
        }

        // Migrate emailpassword_users table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("emailpassword_users_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_users_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("emailpassword_users_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "emailpassword_users_table"), null, "key") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("emailpassword_users_table"), null, "pkey") +
                    " PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get("emailpassword_users_table"), "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("emailpassword_users_table"), "user_id", "fkey") +
                    " FOREIGN KEY (app_id, user_id) REFERENCES " + ph.get("app_id_to_user_id_table") +
                    " (app_id, user_id) ON DELETE CASCADE;");
        }

        // Migrate emailpassword_user_to_tenant table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + ph.get("emailpassword_user_to_tenant_table") + " ( " +
                    "  app_id VARCHAR(64) DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) DEFAULT 'public', " +
                    "  user_id CHAR(36) NOT NULL, " +
                    "  email VARCHAR(256) NOT NULL, " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("emailpassword_user_to_tenant_table"),
                    "email", "key") +
                    "    UNIQUE (app_id, tenant_id, email), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("emailpassword_user_to_tenant_table"), null, "pkey") +
                    "    PRIMARY KEY (app_id, tenant_id, user_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("emailpassword_user_to_tenant_table"), "user_id", "pkey") +
                    "    FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "    REFERENCES " + ph.get("all_auth_recipe_users_table") +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_user_to_tenant_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("emailpassword_user_to_tenant_table"), "email", "key") + ";");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_user_to_tenant_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "emailpassword_user_to_tenant_table"), "email", "key") +
                    "   UNIQUE (app_id, tenant_id, email);");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_user_to_tenant_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "emailpassword_user_to_tenant_table"), "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_user_to_tenant_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "emailpassword_user_to_tenant_table"), "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "   REFERENCES " + ph.get("all_auth_recipe_users_table") +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE;");


            statement.execute("INSERT INTO " + ph.get("emailpassword_user_to_tenant_table") +
                    " (user_id, email) " +
                    "   SELECT user_id, email FROM " + ph.get("emailpassword_users_table") +
                    " ON CONFLICT DO NOTHING;");
        }

        // Migrate emailpassword_pswd_reset_tokens table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("emailpassword_pswd_reset_tokens_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_pswd_reset_tokens_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "emailpassword_pswd_reset_tokens_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_pswd_reset_tokens_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "emailpassword_pswd_reset_tokens_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id, token);");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_pswd_reset_tokens_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "emailpassword_pswd_reset_tokens_table"), "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_pswd_reset_tokens_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "emailpassword_pswd_reset_tokens_table"), "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + ph.get("emailpassword_users_table") +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS emailpassword_pswd_reset_tokens_user_id_index ON " +
                    ph.get("emailpassword_pswd_reset_tokens_table") + " (app_id, user_id);");
        }

        // Migrate passwordless_users table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("passwordless_users_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("passwordless_users_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_users_table"),
                    null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("passwordless_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_users_table"),
                    null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + ph.get("passwordless_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("passwordless_users_table"), "email", "key") + ";");

            statement.execute("ALTER TABLE " + ph.get("passwordless_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "passwordless_users_table"), "phone_number", "key") + ";");

            statement.execute("ALTER TABLE " + ph.get("passwordless_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "passwordless_users_table"), "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("passwordless_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_users_table"),
                    "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + ph.get("app_id_to_user_id_table") +
                    " (app_id, user_id) ON DELETE CASCADE;");
        }

        // Migrate passwordless_user_to_tenant table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + ph.get("passwordless_user_to_tenant_table") + " ( " +
                    "  app_id VARCHAR(64) DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) DEFAULT 'public', " +
                    "  user_id CHAR(36) NOT NULL, " +
                    "  email VARCHAR(256), " +
                    "  phone_number VARCHAR(256), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_user_to_tenant_table"),
                    "email", "key") +
                    "    UNIQUE (app_id, tenant_id, email), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_user_to_tenant_table"),
                    "phone_number", "key") +
                    "    UNIQUE (app_id, tenant_id, phone_number), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_user_to_tenant_table"),
                    null, "pkey") +
                    "    PRIMARY KEY (app_id, tenant_id, user_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_user_to_tenant_table"),
                    "user_id", "fkey") +
                    "    FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "    REFERENCES " + ph.get("all_auth_recipe_users_table") +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("ALTER TABLE " + ph.get("passwordless_user_to_tenant_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("passwordless_user_to_tenant_table"), "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("passwordless_user_to_tenant_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "passwordless_user_to_tenant_table"), "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "   REFERENCES " + ph.get("all_auth_recipe_users_table") +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE;");

            statement.execute("INSERT INTO " + ph.get("passwordless_user_to_tenant_table") +
                    " (user_id, email, phone_number) " +
                    "   SELECT user_id, email, phone_number FROM " +
                    ph.get("passwordless_users_table") +
                    " ON CONFLICT DO NOTHING;");
        }

        // Migrate passwordless_devices table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("passwordless_devices_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("passwordless_devices_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "passwordless_devices_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("passwordless_devices_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_devices_table"),
                    null, "pkey") +
                    "   PRIMARY KEY (app_id, tenant_id, device_id_hash);");

            statement.execute("ALTER TABLE " + ph.get("passwordless_devices_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("passwordless_devices_table"), "tenant_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("passwordless_devices_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_devices_table"),
                    "tenant_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id) " +
                    "   REFERENCES " + ph.get("tenants_table") +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS passwordless_devices_email_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS passwordless_devices_email_index ON " +
                    ph.get("passwordless_devices_table") + " (app_id, tenant_id, email);");

            statement.execute("DROP INDEX IF EXISTS passwordless_devices_phone_number_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS passwordless_devices_phone_number_index ON " +
                    ph.get("passwordless_devices_table") + " (app_id, tenant_id, phone_number);");

            statement.execute("CREATE INDEX IF NOT EXISTS passwordless_devices_tenant_id_index ON " +
                    ph.get("passwordless_devices_table") + " (app_id, tenant_id);");
        }

        // Migrate passwordless_codes table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("passwordless_codes_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("passwordless_codes_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_codes_table"),
                    null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("passwordless_codes_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_codes_table"),
                    null, "pkey") +
                    "   PRIMARY KEY (app_id, tenant_id, code_id);");

            statement.execute("ALTER TABLE " + ph.get("passwordless_codes_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("passwordless_codes_table"), "device_id_hash", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("passwordless_codes_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_codes_table"),
                    "device_id_hash", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id, device_id_hash) " +
                    "   REFERENCES " + ph.get("passwordless_devices_table") +
                    " (app_id, tenant_id, device_id_hash) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("passwordless_codes_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_codes_table"),
                    "link_code_hash", "key") + ";");

            statement.execute("ALTER TABLE " + ph.get("passwordless_codes_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("passwordless_codes_table"), "link_code_hash", "key") + ";");

            statement.execute("ALTER TABLE " + ph.get("passwordless_codes_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("passwordless_codes_table"),
                    "link_code_hash", "key") +
                    "   UNIQUE (app_id, tenant_id, link_code_hash);");

            statement.execute("DROP INDEX IF EXISTS passwordless_codes_created_at_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS passwordless_codes_created_at_index ON " +
                    ph.get("passwordless_codes_table") + " (app_id, tenant_id, created_at);");

            statement.execute("DROP INDEX IF EXISTS passwordless_codes_device_id_hash_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS passwordless_codes_device_id_hash_index ON " +
                    ph.get("passwordless_codes_table") + " (app_id, tenant_id, device_id_hash);");
        }

        // Migrate thirdparty_users table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("thirdparty_users_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("thirdparty_users_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("thirdparty_users_table"),
                    null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("thirdparty_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "thirdparty_users_table"), "user_id", "key") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("thirdparty_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("thirdparty_users_table"),
                    null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + ph.get("thirdparty_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"), ph.get(
                    "thirdparty_users_table"), "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("thirdparty_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get("thirdparty_users_table"),
                    "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + ph.get("app_id_to_user_id_table") +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS thirdparty_users_thirdparty_user_id_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS thirdparty_users_thirdparty_user_id_index ON " +
                    ph.get("thirdparty_users_table") +
                    " (app_id, third_party_id, third_party_user_id);");

            statement.execute("DROP INDEX IF EXISTS thirdparty_users_email_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS thirdparty_users_email_index ON " +
                    ph.get("thirdparty_users_table") + " (app_id, email);");
        }

        // Migrate thirdparty_user_to_tenant table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + ph.get("thirdparty_user_to_tenant_table") +
                    " ( " +
                    "  app_id VARCHAR(64) DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) DEFAULT 'public', " +
                    "  user_id CHAR(36) NOT NULL, " +
                    "  third_party_id VARCHAR(28) NOT NULL, " +
                    "  third_party_user_id VARCHAR(256) NOT NULL, " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "thirdparty_user_to_tenant_table"),
                    "user_id", "key") +
                    "    UNIQUE (app_id, tenant_id, third_party_id, third_party_user_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "thirdparty_user_to_tenant_table"),
                    null, "pkey") +
                    "    PRIMARY KEY (app_id, tenant_id, user_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(ph.get("schema"), ph.get(
                            "thirdparty_user_to_tenant_table"),
                    "user_id", "fkey") +
                    "    FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "    REFERENCES " + ph.get("all_auth_recipe_users_table") +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("ALTER TABLE " + ph.get("thirdparty_user_to_tenant_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("thirdparty_user_to_tenant_table"), "third_party_user_id", "key") + ";");

            statement.execute("ALTER TABLE " + ph.get("thirdparty_user_to_tenant_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("thirdparty_user_to_tenant_table"), "third_party_user_id", "key") +
                    "   UNIQUE (app_id, tenant_id, third_party_id, third_party_user_id);");

            statement.execute("ALTER TABLE " + ph.get("thirdparty_user_to_tenant_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("thirdparty_user_to_tenant_table"), null, "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("thirdparty_user_to_tenant_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("thirdparty_user_to_tenant_table"), "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "   REFERENCES " + ph.get("all_auth_recipe_users_table") +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE;");


            statement.execute("INSERT INTO " + ph.get("thirdparty_user_to_tenant_table") +
                    " (user_id, third_party_id, third_party_user_id) " +
                    "  SELECT user_id, third_party_id, third_party_user_id FROM " +
                    ph.get("thirdparty_users_table") + " ON CONFLICT DO NOTHING;");
        }

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("userid_mapping_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("userid_mapping_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("userid_mapping_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("userid_mapping_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("userid_mapping_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, supertokens_user_id, external_user_id);");

            statement.execute("ALTER TABLE " + ph.get("userid_mapping_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("userid_mapping_table"), "supertokens_user_id", "key") + ";");

            statement.execute("ALTER TABLE " + ph.get("userid_mapping_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("userid_mapping_table"), "supertokens_user_id", "key") +
                    "   UNIQUE (app_id, supertokens_user_id);");

            statement.execute("ALTER TABLE " + ph.get("userid_mapping_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("userid_mapping_table"), "external_user_id", "key") + ";");

            statement.execute("ALTER TABLE " + ph.get("userid_mapping_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("userid_mapping_table"), "external_user_id", "key") +
                    "   UNIQUE (app_id, external_user_id);");

            statement.execute("ALTER TABLE " + ph.get("userid_mapping_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("userid_mapping_table"), "supertokens_user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("userid_mapping_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("userid_mapping_table"), "supertokens_user_id", "fkey") +
                    "   FOREIGN KEY (app_id, supertokens_user_id) " +
                    "   REFERENCES " + ph.get("app_id_to_user_id_table") +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS userid_mapping_supertokens_user_id_index ON " +
                    ph.get("userid_mapping_table") + " (app_id, supertokens_user_id);");
        }

        // Migrate roles table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("roles_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("roles_table") +
                    " DROP CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("roles_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("roles_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("roles_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, role);");

            statement.execute("ALTER TABLE " + ph.get("roles_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("roles_table"), "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("roles_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("roles_table"), "app_id", "fkey") +
                    "   FOREIGN KEY (app_id) " +
                    "   REFERENCES " + ph.get("apps_table") +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS roles_app_id_index ON " +
                    ph.get("roles_table") + " (app_id);");
        }

        // Migrate role_permissions table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("role_permissions_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("role_permissions_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("role_permissions_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("role_permissions_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("role_permissions_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, role, permission);");

            statement.execute("ALTER TABLE " + ph.get("role_permissions_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("role_permissions_table"), "role", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("role_permissions_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("role_permissions_table"), "role", "fkey") +
                    "   FOREIGN KEY (app_id, role) " +
                    "   REFERENCES " + ph.get("roles_table") +
                    " (app_id, role) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS role_permissions_permission_index;");

            statement.execute("CREATE INDEX IF NOT EXISTS role_permissions_permission_index ON " +
                    ph.get("role_permissions_table") + " (app_id, permission);");

            statement.execute("CREATE INDEX IF NOT EXISTS role_permissions_role_index ON " +
                    ph.get("role_permissions_table") + " (app_id, role);");
        }

        // Migrate user_roles table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("user_roles_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    "ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("user_roles_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_roles_table"), null, "pkey") + " CASCADE;");


            statement.execute("ALTER TABLE " + ph.get("user_roles_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_roles_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, tenant_id, user_id, role);");


            statement.execute("ALTER TABLE " + ph.get("user_roles_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_roles_table"), "tenant_id", "fkey") + ";");


            statement.execute("ALTER TABLE " + ph.get("user_roles_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_roles_table"), "tenant_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id) " +
                    "   REFERENCES " + ph.get("tenants_table") +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("user_roles_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_roles_table"), "role", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("user_roles_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_roles_table"), "role", "fkey") +
                    "   FOREIGN KEY (app_id, role) " +
                    "   REFERENCES " + ph.get("roles_table") +
                    " (app_id, role) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS user_roles_role_index;");

            statement.execute("CREATE INDEX IF NOT EXISTS user_roles_role_index ON " +
                    ph.get("user_roles_table") + " (app_id, tenant_id, role);");

            statement.execute("CREATE INDEX IF NOT EXISTS user_roles_tenant_id_index ON " +
                    ph.get("user_roles_table") + " (app_id, tenant_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS user_roles_app_id_role_index ON " +
                    ph.get("user_roles_table") + " (app_id, role);");
        }

        // Migrate user_metadata table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("user_metadata_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            //todo
            statement.execute("ALTER TABLE " + ph.get("user_metadata_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_metadata_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("user_metadata_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_metadata_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + ph.get("user_metadata_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_metadata_table"), "app_id", "fkey") + ";");


            statement.execute("ALTER TABLE " + ph.get("user_metadata_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_metadata_table"), "app_id", "fkey") +
                    "   FOREIGN KEY (app_id) " +
                    "   REFERENCES " + ph.get("apps_table") +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS user_metadata_app_id_index ON " +
                    ph.get("user_metadata_table") + " (app_id);");
        }

        // Migrate dashboard_users table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("dashboard_users_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("dashboard_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("dashboard_users_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("dashboard_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("dashboard_users_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");


            statement.execute("ALTER TABLE " + ph.get("dashboard_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("dashboard_users_table"), "email", "key") + ";");


            statement.execute("ALTER TABLE " + ph.get("dashboard_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("dashboard_users_table"), "email", "key") +
                    "   UNIQUE (app_id, email);");


            statement.execute("ALTER TABLE " + ph.get("dashboard_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("dashboard_users_table"), "app_id", "fkey") + ";");


            statement.execute("ALTER TABLE " + ph.get("dashboard_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("dashboard_users_table"), "app_id", "fkey") +
                    "   FOREIGN KEY (app_id) " +
                    "   REFERENCES " + ph.get("apps_table") +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS dashboard_users_app_id_index ON " +
                    ph.get("dashboard_users_table") + " (app_id);");
        }

        // Migrate dashboard_user_sessions table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("dashboard_user_sessions_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("dashboard_user_sessions_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("dashboard_user_sessions_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("dashboard_user_sessions_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("dashboard_user_sessions_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, session_id);");

            statement.execute("ALTER TABLE " + ph.get("dashboard_user_sessions_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("dashboard_user_sessions_table"), "user_id", "fkey") + ";");


            statement.execute("ALTER TABLE " + ph.get("dashboard_user_sessions_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("dashboard_user_sessions_table"), "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + ph.get("dashboard_users_table") +
                    " (app_id, user_id) ON DELETE CASCADE;");


            statement.execute("CREATE INDEX IF NOT EXISTS dashboard_user_sessions_user_id_index ON " +
                    ph.get("dashboard_user_sessions_table") + " (app_id, user_id);");
        }

        // Migrate totp_users table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("totp_users_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("totp_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_users_table"), null, "pkey") + " CASCADE;");


            statement.execute("ALTER TABLE " + ph.get("totp_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_users_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");


            statement.execute("ALTER TABLE " + ph.get("totp_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_users_table"), "app_id", "fkey") + ";");


            statement.execute("ALTER TABLE " + ph.get("totp_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_users_table"), "app_id", "fkey") +
                    "   FOREIGN KEY (app_id) " +
                    "   REFERENCES " + ph.get("apps_table") +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS totp_users_app_id_index ON " +
                    ph.get("totp_users_table") + " (app_id);");
        }

        // Migrate totp_user_devices table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("totp_user_devices_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("totp_user_devices_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_user_devices_table"), null, "pkey") + ";");


            statement.execute("ALTER TABLE " + ph.get("totp_user_devices_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_user_devices_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id, device_name);");


            statement.execute("ALTER TABLE " + ph.get("totp_user_devices_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_user_devices_table"), "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("totp_user_devices_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_user_devices_table"), "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + ph.get("totp_users_table") +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS totp_user_devices_user_id_index ON " +
                    ph.get("totp_user_devices_table") + " (app_id, user_id);");
        }

        // Migrate totp_used_codes table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("totp_used_codes_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    "   ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("totp_used_codes_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_used_codes_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("totp_used_codes_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_used_codes_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, tenant_id, user_id, created_time_ms);");

            statement.execute("ALTER TABLE " + ph.get("totp_used_codes_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_used_codes_table"), "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + ph.get("totp_users_table") +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("totp_used_codes_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_used_codes_table"), "tenant_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("totp_used_codes_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("totp_used_codes_table"), "tenant_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id) " +
                    "   REFERENCES " + ph.get("tenants_table") +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS totp_used_codes_expiry_time_ms_index;");

            statement.execute("CREATE INDEX IF NOT EXISTS totp_used_codes_expiry_time_ms_index ON " +
                    ph.get("totp_used_codes_table") + " (app_id, tenant_id, expiry_time_ms);");

            statement.execute("CREATE INDEX IF NOT EXISTS totp_used_codes_user_id_index ON " +
                    ph.get("totp_used_codes_table") + " (app_id, user_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS totp_used_codes_tenant_id_index ON " +
                    ph.get("totp_used_codes_table") + " (app_id, tenant_id);");
        }

        // Migrate user_last_active table
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("user_last_active_table") +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + ph.get("user_last_active_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_last_active_table"), null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("user_last_active_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_last_active_table"), null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + ph.get("user_last_active_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_last_active_table"), "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("user_last_active_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("user_last_active_table"), "app_id", "fkey") +
                    "   FOREIGN KEY (app_id) " +
                    "   REFERENCES " + ph.get("apps_table") +
                    " (app_id) ON DELETE CASCADE;");


            statement.execute("CREATE INDEX IF NOT EXISTS user_last_active_app_id_index ON " +
                    ph.get("user_last_active_table") + " (app_id);");
        }

    }
}
