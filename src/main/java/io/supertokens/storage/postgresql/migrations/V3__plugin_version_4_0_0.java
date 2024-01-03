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
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class V3__plugin_version_4_0_0 extends BaseJavaMigration {


    @Override
    public void migrate(Context context) throws Exception {
        Map<String, String> ph = context.getConfiguration().getPlaceholders();
        Start start = MigrationContextManager.getContext(ph.get("process_id"));

        // Migrate apps table
        migrateAppsTable(context,start);

        // Migrate tenants table
        migrateTenantsTable(context,start);

        // Migrate key_value table
        migrateKeyValueTable(context,start);

        // Migrate app_id_to_user_id table
        migrateAppIdToUserIdTable(context,start);

        // Migrate all_auth_recipe_users table
        migrateAllUserTable(context,start);

        // Migrate tenant_configs table
        migrateTenantConfigTable(context,start);

        // Migrate tenant_thirdparty_providers table
        migrateTenantThirdPartyProvidersTable(context,start);

        // Migrate tenant_thirdparty_provider_clients table
        migrateTenantThirdPartyProviderClientsTable(context,start);

        // Migrate session_info table
        migrateSessionInfoTable(context, start);

        // Migrate session_access_token_signing_keys table
        migrateSessionAccessTokenSigningKeys(context, start);

        // Migrate jwt_signing_keys table
        migrateJwtSigningKeysTable(context,start);

        // Migrate emailverification_verified_emails table
        migrateEmailVerificationTable(context,start);

        // Migrate emailverification_tokens table
        migrateEmailVerificationTokensTable(context,start);

        // Migrate emailpassword_users table
        migrateEmailPasswordUsersTable(context,start);

        // Migrate emailpassword_user_to_tenant table
        migrateEmailPasswordUserToTenantTable(context,start);

        // Migrate emailpassword_pswd_reset_tokens table
        migratePasswordResetTokensTable(context,start);

        // Migrate passwordless_users table
        migratePasswordlessUsersTable(context,start);

        // Migrate passwordless_user_to_tenant table
        migratePasswordlessUserToTenantTable(context,start);

        // Migrate passwordless_devices table
        migratePasswordlessDevicesTable(context,start);

        // Migrate passwordless_codes table
        migratePasswordlessCodesTable(context,start);

        // Migrate thirdparty_users table
        migrateThirdPartyUsersTable(context,start);

        // Migrate thirdparty_user_to_tenant table
        migrateThirdPartyUserToTenantTable(context,start);

        // Migrate userid_mapping table
        migrateUserIdMappingTable(context,start);

        // Migrate roles table
        migrateRolesTable(context,start);

        // Migrate role_permissions table
        migrateUserRolesPermissionsTable(context,start);

        // Migrate user_roles table
        migrateUserRolesTable(context,start);

        // Migrate user_metadata table
        migrateUserMetadataTable(context,start);

        // Migrate dashboard_users table
        migrateDashboardUsersTable(context,start);

        // Migrate dashboard_user_sessions table
        migrateDashboardSessionsTable(context,start);

        // Migrate totp_users table
        migrateTotpUsersTable(context,start);

        // Migrate totp_user_devices table
        migrateTotpUserDevicesTable(context,start);

        // Migrate totp_used_codes table
        migrateTotpUsedCodesTable(context,start);

        // Migrate user_last_active table
        migrateUserLastActiveTable(context,start);
    }


    /*
     - adding columns: app_id
     - primary key created_at_time => (app_id,created_at_time) + fk to app_table
     - index(app_id)
     */
    private void migrateSessionAccessTokenSigningKeys(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String accessTokenSigningKeysTable = Config.getConfig(start).getAccessTokenSigningKeysTable();
        String appsTable = Config.getConfig(start).getAppsTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + accessTokenSigningKeysTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + accessTokenSigningKeysTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, accessTokenSigningKeysTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + accessTokenSigningKeysTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, accessTokenSigningKeysTable, null, "pkey") +
                    " PRIMARY KEY (app_id, created_at_time);");

            statement.execute("ALTER TABLE " + accessTokenSigningKeysTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, accessTokenSigningKeysTable,
                    "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + accessTokenSigningKeysTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, accessTokenSigningKeysTable,
                    "app_id", "fkey") +
                    " FOREIGN KEY (app_id) REFERENCES " + appsTable +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS access_token_signing_keys_app_id_index ON " +
                    accessTokenSigningKeysTable + " (app_id);");
        }
    }
    
    /*
    - adding columns: app_id, tenant_id
    - primary key session_handle => (app_id,tenant_id,session_handle) + FK to tenant table in V3
    - index(app_id,tenant_id) + index(session_expiry_index)
     */
    private void migrateSessionInfoTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String sessionInfoTable = Config.getConfig(start).getSessionInfoTable();
        String tenantsTable = Config.getConfig(start).getTenantsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + sessionInfoTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + sessionInfoTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, sessionInfoTable, null,
                    "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + sessionInfoTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, sessionInfoTable, null,
                    "pkey") +
                    " PRIMARY KEY (app_id, tenant_id, session_handle);");

            statement.execute("ALTER TABLE " + sessionInfoTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, sessionInfoTable, "tenant_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + sessionInfoTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, sessionInfoTable, "tenant_id", "fkey") +
                    " FOREIGN KEY (app_id, tenant_id) REFERENCES " + tenantsTable +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS session_expiry_index ON " + sessionInfoTable +
                    " (expires_at);");

            statement.execute("CREATE INDEX IF NOT EXISTS session_info_tenant_id_index ON " + sessionInfoTable +
                    " (app_id, tenant_id);");
        }
    }
    
    /*
     Created with V3 migration
     */
    private void migrateAppsTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String appsTable = Config.getConfig(start).getAppsTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + appsTable + "  ( " +
                    "  app_id VARCHAR(64) NOT NULL DEFAULT 'public', " +
                    "  created_at_time BIGINT, " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, appsTable, null,
                    "pkey")  + " PRIMARY KEY(app_id) " +
                    ");");

            statement.execute("INSERT INTO " + appsTable +
                    " (app_id, created_at_time) VALUES ('public', 0) ON CONFLICT DO NOTHING;");
        }
    }

    /*
    Created with V3 migration
    */
    private void migrateTenantsTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantsTable = Config.getConfig(start).getTenantsTable();
        String appsTable = Config.getConfig(start).getAppsTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + tenantsTable + " ( " +
                    "  app_id VARCHAR(64) NOT NULL DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) NOT NULL DEFAULT 'public', " +
                    "  created_at_time BIGINT, " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, tenantsTable, null,
                    "pkey") + " PRIMARY KEY (app_id, tenant_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, tenantsTable, "app_id",
                    "fkey") + " FOREIGN KEY(app_id) " +
                    "    REFERENCES " + appsTable + " (app_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("INSERT INTO " + tenantsTable +
                    " (app_id, tenant_id, created_at_time) VALUES ('public', 'public', 0) ON CONFLICT DO NOTHING;");

            statement.execute("CREATE INDEX IF NOT EXISTS tenants_app_id_index ON " +
                    tenantsTable + " (app_id);");
        }
    }

    /*
      - adding columns: app_id, tenant_id
     - primary key session_handle => (app_id,tenant_id,name) + FK to tenant table
     - index(app_id,tenant_id)
     */
    private void migrateKeyValueTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantsTable = Config.getConfig(start).getTenantsTable();
        String keyValueTable = Config.getConfig(start).getKeyValueTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + keyValueTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + keyValueTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, keyValueTable, null,
                    "pkey"));

            statement.execute("ALTER TABLE " + keyValueTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, keyValueTable, null,
                    "pkey") +
                    " PRIMARY KEY (app_id, tenant_id, name);");

            statement.execute("ALTER TABLE " + keyValueTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, keyValueTable, "tenant_id", "fkey"));

            statement.execute("ALTER TABLE " + keyValueTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, keyValueTable, "tenant_id", "fkey") +
                    " FOREIGN KEY (app_id, tenant_id) REFERENCES " + tenantsTable +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS key_value_tenant_id_index" +
                    " ON " + keyValueTable + " (app_id, tenant_id);");
        }
    }

    /*
     Created with V3 migration
     */
    private void migrateAppIdToUserIdTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String appsTable = Config.getConfig(start).getAppsTable();
        String appIdToUserIdTable = Config.getConfig(start).getAppIdToUserIdTable();
        String allUserTable = Config.getConfig(start).getUsersTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + appIdToUserIdTable + " ( " +
                    "  app_id VARCHAR(64) NOT NULL DEFAULT 'public', " +
                    "  user_id CHAR(36) NOT NULL, " +
                    "  recipe_id VARCHAR(128) NOT NULL, " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, appIdToUserIdTable, null,
                    "pkey") + " PRIMARY KEY (app_id, user_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, appIdToUserIdTable, "app_id",
                    "fkey") + " FOREIGN KEY(app_id) " +
                    "    REFERENCES " + appsTable + " (app_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("INSERT INTO " + appIdToUserIdTable +
                    " (user_id, recipe_id) " +
                    "SELECT user_id, recipe_id FROM " + allUserTable + " ON CONFLICT DO NOTHING;");

            statement.execute("CREATE INDEX IF NOT EXISTS " + appIdToUserIdTable +
                    "_app_id_index ON " + appIdToUserIdTable + " (app_id);");
        }
    }

    /*
    - adding columns: app_id, tenant_id
    - primary key session_handle => (app_id,tenant_id,name) + FK to tenant table
    - index(app_id,tenant_id),(time_joined DESC, user_id DESC, tenant_id DESC, app_id),(app_id, user_id)
    */
    private void migrateAllUserTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantsTable = Config.getConfig(start).getTenantsTable();
        String appIdToUserIdTable = Config.getConfig(start).getAppIdToUserIdTable();
        String allUserTable = Config.getConfig(start).getUsersTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + allUserTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + allUserTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, allUserTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + allUserTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, allUserTable, null, "pkey") +
                    " PRIMARY KEY (app_id, tenant_id, user_id);");

            statement.execute("ALTER TABLE " + allUserTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, allUserTable, "tenant_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + allUserTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, allUserTable, "tenant_id", "fkey") +
                    " FOREIGN KEY (app_id, tenant_id) REFERENCES " + tenantsTable +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + allUserTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, allUserTable, "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + allUserTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, allUserTable, "user_id", "fkey") +
                    " FOREIGN KEY (app_id, user_id) REFERENCES " + appIdToUserIdTable +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS all_auth_recipe_users_pagination_index;");

            statement.execute("CREATE INDEX all_auth_recipe_users_pagination_index ON " + allUserTable +
                    " (time_joined DESC, user_id DESC, tenant_id DESC, app_id DESC);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_user_id_index ON " + allUserTable +
                    " (app_id, user_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_tenant_id_index ON " + allUserTable +
                    " (app_id, tenant_id);");
        }
    }
    
    /*
     Created with V3 migration
     */
    private void migrateTenantConfigTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantConfigTable = Config.getConfig(start).getTenantConfigsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + tenantConfigTable + " ( " +
                    "  connection_uri_domain VARCHAR(256) DEFAULT '', " +
                    "  app_id VARCHAR(64) DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) DEFAULT 'public', " +
                    "  core_config TEXT, " +
                    "  email_password_enabled BOOLEAN, " +
                    "  passwordless_enabled BOOLEAN, " +
                    "  third_party_enabled BOOLEAN, " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, tenantConfigTable, null,
                    "pkey") +
                    " PRIMARY KEY (connection_uri_domain, app_id, tenant_id) " +
                    ");");
        }
    }

    /*
    Created with V3 migration
    */
    private void migrateTenantThirdPartyProvidersTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantThirdPartyProvidersTable = Config.getConfig(start).getTenantThirdPartyProvidersTable();
        String tenantConfigTable = Config.getConfig(start).getTenantConfigsTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + tenantThirdPartyProvidersTable+ " ( " +
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
                    "  CONSTRAINT " + Utils.getConstraintName(schema, tenantThirdPartyProvidersTable, null, "pkey") +
                    " PRIMARY KEY (connection_uri_domain, app_id, tenant_id, third_party_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, tenantThirdPartyProvidersTable, "tenant_id",
                    "fkey") +
                    " FOREIGN KEY(connection_uri_domain, app_id, tenant_id) " +
                    "    REFERENCES " + tenantConfigTable + " (connection_uri_domain, app_id, tenant_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("CREATE INDEX IF NOT EXISTS tenant_thirdparty_providers_tenant_id_index ON " + tenantThirdPartyProvidersTable+
                    " (connection_uri_domain, app_id, tenant_id);");
        }
    }

    /*
    Created with V3 migration
    */
    private void migrateTenantThirdPartyProviderClientsTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantThirdPartyProvidersTable = Config.getConfig(start).getTenantThirdPartyProvidersTable();
        String tenantThirdPartyProviderClientsTable = Config.getConfig(start).getTenantThirdPartyProviderClientsTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + tenantThirdPartyProviderClientsTable + " ( " +
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
                    "  CONSTRAINT " + Utils.getConstraintName(schema, tenantThirdPartyProviderClientsTable, null, "pkey") +
                    " PRIMARY KEY (connection_uri_domain, app_id, tenant_id, third_party_id, client_type), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, tenantThirdPartyProviderClientsTable, "third_party_id", "fkey") +
                    " FOREIGN KEY (connection_uri_domain, app_id, tenant_id, third_party_id) " +
                    "    REFERENCES " + tenantThirdPartyProvidersTable+ " (connection_uri_domain, app_id, tenant_id, third_party_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("CREATE INDEX IF NOT EXISTS tenant_thirdparty_provider_clients_third_party_id_index ON " + tenantThirdPartyProviderClientsTable +
                    " (connection_uri_domain, app_id, tenant_id, third_party_id);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id,key_id) + FK to apps table
    - index(app_id)
    */
    private void migrateJwtSigningKeysTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String jwtSigningKeysTable = Config.getConfig(start).getJWTSigningKeysTable();
        String appsTable = Config.getConfig(start).getAppsTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + jwtSigningKeysTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + jwtSigningKeysTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, jwtSigningKeysTable,
                    null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + jwtSigningKeysTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, jwtSigningKeysTable,
                    null, "pkey") +
                    " PRIMARY KEY (app_id, key_id);");

            statement.execute("ALTER TABLE " + jwtSigningKeysTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, jwtSigningKeysTable, "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + jwtSigningKeysTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, jwtSigningKeysTable, "app_id", "fkey") +
                    " FOREIGN KEY (app_id) REFERENCES " + appsTable +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS jwt_signing_keys_app_id_index ON " +
                    jwtSigningKeysTable + " (app_id);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id,user_id,email) + FK to apps table
    - index(app_id)
    */
    private void migrateEmailVerificationTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String emailVerificationTable = Config.getConfig(start).getEmailVerificationTable();
        String appsTable = Config.getConfig(start).getAppsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + emailVerificationTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + emailVerificationTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, emailVerificationTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + emailVerificationTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, emailVerificationTable, null, "pkey") +
                    " PRIMARY KEY (app_id, user_id, email);");

            statement.execute("ALTER TABLE " + emailVerificationTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, emailVerificationTable, "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + emailVerificationTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, emailVerificationTable, "app_id", "fkey") +
                    " FOREIGN KEY (app_id) REFERENCES " + appsTable +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS emailverification_verified_emails_app_id_index ON " +
                    emailVerificationTable + " (app_id);");
        }
    }

    /*
    - adding columns: app_id, tenant_id
    - primary key session_handle => (app_id, tenant_id, user_id, email, token) + FK to apps table
    - index((app_id, tenant_id)
    */
    private void migrateEmailVerificationTokensTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String emailVerificationTokensTable = Config.getConfig(start).getEmailVerificationTokensTable();
        String tenantsTable = Config.getConfig(start).getTenantsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + emailVerificationTokensTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + emailVerificationTokensTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, emailVerificationTokensTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + emailVerificationTokensTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, emailVerificationTokensTable, null, "pkey") +
                    " PRIMARY KEY (app_id, tenant_id, user_id, email, token);");

            statement.execute("ALTER TABLE " + emailVerificationTokensTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, emailVerificationTokensTable, "tenant_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + emailVerificationTokensTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, emailVerificationTokensTable, "tenant_id", "fkey") +
                    " FOREIGN KEY (app_id, tenant_id) REFERENCES " + tenantsTable +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS emailverification_tokens_tenant_id_index ON " +
                    emailVerificationTokensTable + " (app_id, tenant_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS emailverification_tokens_index ON " +
                    emailVerificationTokensTable + " (token_expiry);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, user_id)) + FK to appIdToUserIdTable
    */
    private void migrateEmailPasswordUsersTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String emailPasswordUsersTable = Config.getConfig(start).getEmailPasswordUsersTable();
        String appIdToUserIdTable = Config.getConfig(start).getAppIdToUserIdTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + emailPasswordUsersTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + emailPasswordUsersTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, emailPasswordUsersTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + emailPasswordUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, emailPasswordUsersTable, "email",
                    "key") + " CASCADE;");

            statement.execute("ALTER TABLE " + emailPasswordUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    emailPasswordUsersTable, null, "pkey") +
                    " PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + emailPasswordUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, emailPasswordUsersTable, "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + emailPasswordUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, emailPasswordUsersTable, "user_id", "fkey") +
                    " FOREIGN KEY (app_id, user_id) REFERENCES " + appIdToUserIdTable +
                    " (app_id, user_id) ON DELETE CASCADE;");
        }
    }

    /*
      Created with V3
     */
    private void migrateEmailPasswordUserToTenantTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String emailPasswordUsersTable = Config.getConfig(start).getEmailPasswordUsersTable();
        String emailPasswordUserToTenantTable = Config.getConfig(start).getEmailPasswordUserToTenantTable();
        String allUserTable = Config.getConfig(start).getUsersTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + emailPasswordUserToTenantTable + " ( " +
                    "  app_id VARCHAR(64) DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) DEFAULT 'public', " +
                    "  user_id CHAR(36) NOT NULL, " +
                    "  email VARCHAR(256) NOT NULL, " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, emailPasswordUserToTenantTable,
                    "email", "key") +
                    "    UNIQUE (app_id, tenant_id, email), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema,
                    emailPasswordUserToTenantTable, null, "pkey") +
                    "    PRIMARY KEY (app_id, tenant_id, user_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, emailPasswordUserToTenantTable, "user_id", "pkey") +
                    "    FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "    REFERENCES " + allUserTable +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("ALTER TABLE " + emailPasswordUserToTenantTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    emailPasswordUserToTenantTable, "email", "key") + ";");

            statement.execute("ALTER TABLE " + emailPasswordUserToTenantTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, emailPasswordUserToTenantTable, "email", "key") +
                    "   UNIQUE (app_id, tenant_id, email);");

            statement.execute("ALTER TABLE " + emailPasswordUserToTenantTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, emailPasswordUserToTenantTable, "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + emailPasswordUserToTenantTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, emailPasswordUserToTenantTable, "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "   REFERENCES " + allUserTable +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE;");


            statement.execute("INSERT INTO " + emailPasswordUserToTenantTable +
                    " (user_id, email) " +
                    "   SELECT user_id, email FROM " + emailPasswordUsersTable +
                    " ON CONFLICT DO NOTHING;");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, user_id, token) + FK to emailPasswordUsersTable
    */
    private void migratePasswordResetTokensTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String emailPasswordUsersTable = Config.getConfig(start).getEmailPasswordUsersTable();
        String passwordResetTokensTable = Config.getConfig(start).getPasswordResetTokensTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + passwordResetTokensTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + passwordResetTokensTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, passwordResetTokensTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + passwordResetTokensTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, passwordResetTokensTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id, token);");

            statement.execute("ALTER TABLE " + passwordResetTokensTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, passwordResetTokensTable,
                    "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + passwordResetTokensTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, passwordResetTokensTable, "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + emailPasswordUsersTable +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS emailpassword_pswd_reset_tokens_user_id_index ON " +
                    passwordResetTokensTable + " (app_id, user_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS emailpassword_password_reset_token_expiry_index ON " +
                    passwordResetTokensTable + " (token_expiry);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, user_id, token) + FK to appIdToUserIdTable
    */
    private void migratePasswordlessUsersTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String passwordlessUsersTable = Config.getConfig(start).getPasswordlessUsersTable();
        String appIdToUserIdTable = Config.getConfig(start).getAppIdToUserIdTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + passwordlessUsersTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + passwordlessUsersTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUsersTable,
                    null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + passwordlessUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUsersTable,
                    null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + passwordlessUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    passwordlessUsersTable, "email", "key") + ";");

            statement.execute("ALTER TABLE " + passwordlessUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, passwordlessUsersTable, "phone_number", "key") + ";");

            statement.execute("ALTER TABLE " + passwordlessUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, passwordlessUsersTable, "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + passwordlessUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUsersTable,
                    "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + appIdToUserIdTable +
                    " (app_id, user_id) ON DELETE CASCADE;");
        }
    }
    
    /*
    Created with V3
     */
    private void migratePasswordlessUserToTenantTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String passwordlessUsersTable = Config.getConfig(start).getPasswordlessUsersTable();
        String passwordlessUserToTenantTable = Config.getConfig(start).getPasswordlessUserToTenantTable();
        String allUserTable = Config.getConfig(start).getUsersTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + passwordlessUserToTenantTable+ " ( " +
                    "  app_id VARCHAR(64) DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) DEFAULT 'public', " +
                    "  user_id CHAR(36) NOT NULL, " +
                    "  email VARCHAR(256), " +
                    "  phone_number VARCHAR(256), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUserToTenantTable,
                    "email", "key") +
                    "    UNIQUE (app_id, tenant_id, email), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUserToTenantTable,
                    "phone_number", "key") +
                    "    UNIQUE (app_id, tenant_id, phone_number), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUserToTenantTable,
                    null, "pkey") +
                    "    PRIMARY KEY (app_id, tenant_id, user_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUserToTenantTable,
                    "user_id", "fkey") +
                    "    FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "    REFERENCES " + allUserTable +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("ALTER TABLE " + passwordlessUserToTenantTable+
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    passwordlessUserToTenantTable, "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + passwordlessUserToTenantTable+
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUserToTenantTable, "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "   REFERENCES " + allUserTable +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE;");

            statement.execute("INSERT INTO " + passwordlessUserToTenantTable+
                    " (user_id, email, phone_number) " +
                    "   SELECT user_id, email, phone_number FROM " +
                    passwordlessUsersTable +
                    " ON CONFLICT DO NOTHING;");
        }
    }

    /*
    - adding columns: app_id,tenant_id
    - primary key session_handle => (app_id, tenant_id, device_id_hash) + FK to tenantTable
    */
    private void migratePasswordlessDevicesTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String passwordlessDevicesTable = Config.getConfig(start).getPasswordlessDevicesTable();
        String tenantsTable = Config.getConfig(start).getTenantsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + passwordlessDevicesTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + passwordlessDevicesTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, passwordlessDevicesTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + passwordlessDevicesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, passwordlessDevicesTable,
                    null, "pkey") +
                    "   PRIMARY KEY (app_id, tenant_id, device_id_hash);");

            statement.execute("ALTER TABLE " + passwordlessDevicesTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    passwordlessDevicesTable, "tenant_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + passwordlessDevicesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, passwordlessDevicesTable,
                    "tenant_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id) " +
                    "   REFERENCES " + tenantsTable +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS passwordless_devices_email_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS passwordless_devices_email_index ON " +
                    passwordlessDevicesTable + " (app_id, tenant_id, email);");

            statement.execute("DROP INDEX IF EXISTS passwordless_devices_phone_number_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS passwordless_devices_phone_number_index ON " +
                    passwordlessDevicesTable + " (app_id, tenant_id, phone_number);");

            statement.execute("CREATE INDEX IF NOT EXISTS passwordless_devices_tenant_id_index ON " +
                    passwordlessDevicesTable + " (app_id, tenant_id);");
        }
    }

    /*
    - adding columns: app_id,tenant_id
    - primary key session_handle => (app_id, tenant_id, code_id) + FK to passwordlessDevicesTable
    */
    private void migratePasswordlessCodesTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String passwordlessDevicesTable = Config.getConfig(start).getPasswordlessDevicesTable();
        String passwordlessCodesTable = Config.getConfig(start).getPasswordlessCodesTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + passwordlessCodesTable+
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    " ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + passwordlessCodesTable+
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, passwordlessCodesTable,
                    null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + passwordlessCodesTable+
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, passwordlessCodesTable,
                    null, "pkey") +
                    "   PRIMARY KEY (app_id, tenant_id, code_id);");

            statement.execute("ALTER TABLE " + passwordlessCodesTable+
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    passwordlessCodesTable, "device_id_hash", "fkey") + ";");

            statement.execute("ALTER TABLE " + passwordlessCodesTable+
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, passwordlessCodesTable,
                    "device_id_hash", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id, device_id_hash) " +
                    "   REFERENCES " + passwordlessDevicesTable +
                    " (app_id, tenant_id, device_id_hash) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + passwordlessCodesTable+
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, passwordlessCodesTable,
                    "link_code_hash", "key") + ";");

            statement.execute("ALTER TABLE " + passwordlessCodesTable+
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    passwordlessCodesTable, "link_code_hash", "key") + ";");

            statement.execute("ALTER TABLE " + passwordlessCodesTable+
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, passwordlessCodesTable,
                    "link_code_hash", "key") +
                    "   UNIQUE (app_id, tenant_id, link_code_hash);");

            statement.execute("DROP INDEX IF EXISTS passwordless_codes_created_at_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS passwordless_codes_created_at_index ON " +
                    passwordlessCodesTable+ " (app_id, tenant_id, created_at);");

            statement.execute("DROP INDEX IF EXISTS passwordless_codes_device_id_hash_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS passwordless_codes_device_id_hash_index ON " +
                    passwordlessCodesTable+ " (app_id, tenant_id, device_id_hash);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, user_id) + FK to appIdToUserIdTable
    */
    private void migrateThirdPartyUsersTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String thirdPartyUsersTable = Config.getConfig(start).getThirdPartyUsersTable();
        String appIdToUserIdTable = Config.getConfig(start).getAppIdToUserIdTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + thirdPartyUsersTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + thirdPartyUsersTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUsersTable,
                    null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + thirdPartyUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, thirdPartyUsersTable, "user_id", "key") + " CASCADE;");

            statement.execute("ALTER TABLE " + thirdPartyUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUsersTable,
                    null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + thirdPartyUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema, thirdPartyUsersTable, "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + thirdPartyUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUsersTable,
                    "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + appIdToUserIdTable +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS thirdparty_users_thirdparty_user_id_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS thirdparty_users_thirdparty_user_id_index ON " +
                    thirdPartyUsersTable +
                    " (app_id, third_party_id, third_party_user_id);");

            statement.execute("DROP INDEX IF EXISTS thirdparty_users_email_index;");
            statement.execute("CREATE INDEX IF NOT EXISTS thirdparty_users_email_index ON " +
                    thirdPartyUsersTable + " (app_id, email);");
        }
    }
    
    /*
    Created with V3
     */
    private void migrateThirdPartyUserToTenantTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String thirdPartyUsersTable = Config.getConfig(start).getThirdPartyUsersTable();
        String thirdPartyUserToTenantTable = Config.getConfig(start).getThirdPartyUserToTenantTable();
        String allUserTable = Config.getConfig(start).getUsersTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + thirdPartyUserToTenantTable +
                    " ( " +
                    "  app_id VARCHAR(64) DEFAULT 'public', " +
                    "  tenant_id VARCHAR(64) DEFAULT 'public', " +
                    "  user_id CHAR(36) NOT NULL, " +
                    "  third_party_id VARCHAR(28) NOT NULL, " +
                    "  third_party_user_id VARCHAR(256) NOT NULL, " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUserToTenantTable,
                    "third_party_user_id", "key") +
                    "    UNIQUE (app_id, tenant_id, third_party_id, third_party_user_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUserToTenantTable,
                    null, "pkey") +
                    "    PRIMARY KEY (app_id, tenant_id, user_id), " +
                    "  CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUserToTenantTable,
                    "user_id", "fkey") +
                    "    FOREIGN KEY (app_id, tenant_id, user_id) " +
                    "    REFERENCES " + allUserTable +
                    " (app_id, tenant_id, user_id) ON DELETE CASCADE " +
                    ");");

            statement.execute("ALTER TABLE " + thirdPartyUserToTenantTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    thirdPartyUserToTenantTable, "third_party_user_id", "key") + ";");

            statement.execute("ALTER TABLE " + thirdPartyUserToTenantTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    thirdPartyUserToTenantTable, "third_party_user_id", "key") +
                    "   UNIQUE (app_id, tenant_id, third_party_id, third_party_user_id);");

            statement.execute("ALTER TABLE " + thirdPartyUserToTenantTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    thirdPartyUserToTenantTable, null, "fkey") + ";");

            statement.execute("INSERT INTO " + thirdPartyUserToTenantTable +
                    " (user_id, third_party_id, third_party_user_id) " +
                    "  SELECT user_id, third_party_id, third_party_user_id FROM " +
                    thirdPartyUsersTable + " ON CONFLICT DO NOTHING;");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, supertokens_user_id, external_user_id) + FK to appIdToUserIdTable
    */
    private void migrateUserIdMappingTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String userIdMappingTable = Config.getConfig(start).getUserIdMappingTable();
        String appIdToUserIdTable = Config.getConfig(start).getAppIdToUserIdTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + userIdMappingTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + userIdMappingTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    userIdMappingTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + userIdMappingTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    userIdMappingTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, supertokens_user_id, external_user_id);");

            statement.execute("ALTER TABLE " + userIdMappingTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    userIdMappingTable, "supertokens_user_id", "key") + ";");

            statement.execute("ALTER TABLE " + userIdMappingTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    userIdMappingTable, "supertokens_user_id", "key") +
                    "   UNIQUE (app_id, supertokens_user_id);");

            statement.execute("ALTER TABLE " + userIdMappingTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    userIdMappingTable, "external_user_id", "key") + ";");

            statement.execute("ALTER TABLE " + userIdMappingTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    userIdMappingTable, "external_user_id", "key") +
                    "   UNIQUE (app_id, external_user_id);");

            statement.execute("ALTER TABLE " + userIdMappingTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    userIdMappingTable, "supertokens_user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + userIdMappingTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    userIdMappingTable, "supertokens_user_id", "fkey") +
                    "   FOREIGN KEY (app_id, supertokens_user_id) " +
                    "   REFERENCES " + appIdToUserIdTable +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS userid_mapping_supertokens_user_id_index ON " +
                    userIdMappingTable + " (app_id, supertokens_user_id);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, role) + FK to appsTable
    */
    private void migrateRolesTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String rolesTable = Config.getConfig(start).getRolesTable();
        String appsTable = Config.getConfig(start).getAppsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + rolesTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + rolesTable +
                    " DROP CONSTRAINT " + Utils.getConstraintName(schema,
                    rolesTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + rolesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    rolesTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, role);");

            statement.execute("ALTER TABLE " + rolesTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    rolesTable, "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + rolesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    rolesTable, "app_id", "fkey") +
                    "   FOREIGN KEY (app_id) " +
                    "   REFERENCES " + appsTable +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS roles_app_id_index ON " +
                    rolesTable + " (app_id);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, role, permission) + FK to rolesTable
    */
    private void migrateUserRolesPermissionsTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String rolesTable = Config.getConfig(start).getRolesTable();
        String userRolesPermissionsTable = Config.getConfig(start).getUserRolesPermissionsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + userRolesPermissionsTable+
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + userRolesPermissionsTable+
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    userRolesPermissionsTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + userRolesPermissionsTable+
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    userRolesPermissionsTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, role, permission);");

            statement.execute("ALTER TABLE " + userRolesPermissionsTable+
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    userRolesPermissionsTable, "role", "fkey") + ";");

            statement.execute("ALTER TABLE " + userRolesPermissionsTable+
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    userRolesPermissionsTable, "role", "fkey") +
                    "   FOREIGN KEY (app_id, role) " +
                    "   REFERENCES " + rolesTable +
                    " (app_id, role) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS role_permissions_permission_index;");

            statement.execute("CREATE INDEX IF NOT EXISTS role_permissions_permission_index ON " +
                    userRolesPermissionsTable+ " (app_id, permission);");

            statement.execute("CREATE INDEX IF NOT EXISTS role_permissions_role_index ON " +
                    userRolesPermissionsTable+ " (app_id, role);");
        }
    }

    /*
    - adding columns: app_id, tenant_id
    - primary key session_handle => (app_id, tenant_id, user_id, role) + FK to tenantsTable, rolesTable
    */
    private void migrateUserRolesTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String rolesTable = Config.getConfig(start).getRolesTable();
        String UserRolesTable = Config.getConfig(start).getUserRolesTable();
        String tenantsTable = Config.getConfig(start).getTenantsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + UserRolesTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    "ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + UserRolesTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    UserRolesTable, null, "pkey") + " CASCADE;");


            statement.execute("ALTER TABLE " + UserRolesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    UserRolesTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, tenant_id, user_id, role);");


            statement.execute("ALTER TABLE " + UserRolesTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    UserRolesTable, "tenant_id", "fkey") + ";");


            statement.execute("ALTER TABLE " + UserRolesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    UserRolesTable, "tenant_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id) " +
                    "   REFERENCES " + tenantsTable +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + UserRolesTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    UserRolesTable, "role", "fkey") + ";");

            statement.execute("ALTER TABLE " + UserRolesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    UserRolesTable, "role", "fkey") +
                    "   FOREIGN KEY (app_id, role) " +
                    "   REFERENCES " + rolesTable +
                    " (app_id, role) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS user_roles_role_index;");

            statement.execute("CREATE INDEX IF NOT EXISTS user_roles_role_index ON " +
                    UserRolesTable + " (app_id, tenant_id, role);");

            statement.execute("CREATE INDEX IF NOT EXISTS user_roles_tenant_id_index ON " +
                    UserRolesTable + " (app_id, tenant_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS user_roles_app_id_role_index ON " +
                    UserRolesTable + " (app_id, role);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, user_id)) + FK to appsTable
    */
    private void migrateUserMetadataTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String userMetadataTable = Config.getConfig(start).getUserMetadataTable();
        String appsTable = Config.getConfig(start).getAppsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + userMetadataTable+
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            //todo
            statement.execute("ALTER TABLE " + userMetadataTable+
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    userMetadataTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + userMetadataTable+
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    userMetadataTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + userMetadataTable+
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    userMetadataTable, "app_id", "fkey") + ";");


            statement.execute("ALTER TABLE " + userMetadataTable+
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    userMetadataTable, "app_id", "fkey") +
                    "   FOREIGN KEY (app_id) " +
                    "   REFERENCES " + appsTable +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS user_metadata_app_id_index ON " +
                    userMetadataTable+ " (app_id);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, user_id)) + FK to appsTable
    */
    private void migrateDashboardUsersTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String dashboardUsersTable = Config.getConfig(start).getDashboardUsersTable();
        String appsTable = Config.getConfig(start).getAppsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + dashboardUsersTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + dashboardUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    dashboardUsersTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + dashboardUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    dashboardUsersTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");


            statement.execute("ALTER TABLE " + dashboardUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    dashboardUsersTable, "email", "key") + ";");


            statement.execute("ALTER TABLE " + dashboardUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    dashboardUsersTable, "email", "key") +
                    "   UNIQUE (app_id, email);");


            statement.execute("ALTER TABLE " + dashboardUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    dashboardUsersTable, "app_id", "fkey") + ";");


            statement.execute("ALTER TABLE " + dashboardUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    dashboardUsersTable, "app_id", "fkey") +
                    "   FOREIGN KEY (app_id) " +
                    "   REFERENCES " + appsTable +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS dashboard_users_app_id_index ON " +
                    dashboardUsersTable + " (app_id);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, session_id) + FK to dashboardUsersTable
    */
    private void migrateDashboardSessionsTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String dashboardUsersTable = Config.getConfig(start).getDashboardUsersTable();
        String dashboardSessionsTable = Config.getConfig(start).getDashboardSessionsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + dashboardSessionsTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + dashboardSessionsTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    dashboardSessionsTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + dashboardSessionsTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    dashboardSessionsTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, session_id);");

            statement.execute("ALTER TABLE " + dashboardSessionsTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    dashboardSessionsTable, "user_id", "fkey") + ";");


            statement.execute("ALTER TABLE " + dashboardSessionsTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    dashboardSessionsTable, "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + dashboardUsersTable +
                    " (app_id, user_id) ON DELETE CASCADE;");


            statement.execute("CREATE INDEX IF NOT EXISTS dashboard_user_sessions_user_id_index ON " +
                    dashboardSessionsTable + " (app_id, user_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS dashboard_user_sessions_expiry_index ON " +
                    dashboardSessionsTable + " (expiry);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, user_id) + FK to appsTable
    */
    private void migrateTotpUsersTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String totpUsersTable = Config.getConfig(start).getTotpUsersTable();
        String appsTable = Config.getConfig(start).getAppsTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + totpUsersTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + totpUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    totpUsersTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + totpUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    totpUsersTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + totpUsersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    totpUsersTable, "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + totpUsersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    totpUsersTable, "app_id", "fkey") +
                    "   FOREIGN KEY (app_id) " +
                    "   REFERENCES " + appsTable +
                    " (app_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS totp_users_app_id_index ON " +
                    totpUsersTable + " (app_id);");
        }
    }

    /*
    - adding columns: app_id
    - primary key session_handle => (app_id, user_id, device_name) + FK to totpUsersTable
    */
    private void migrateTotpUserDevicesTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String totpUsersTable = Config.getConfig(start).getTotpUsersTable();
        String totpUserDevicesTable = Config.getConfig(start).getTotpUserDevicesTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + totpUserDevicesTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + totpUserDevicesTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    totpUserDevicesTable, null, "pkey") + ";");


            statement.execute("ALTER TABLE " + totpUserDevicesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    totpUserDevicesTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id, device_name);");


            statement.execute("ALTER TABLE " + totpUserDevicesTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    totpUserDevicesTable, "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + totpUserDevicesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    totpUserDevicesTable, "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + totpUsersTable +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("CREATE INDEX IF NOT EXISTS totp_user_devices_user_id_index ON " +
                    totpUserDevicesTable + " (app_id, user_id);");
        }
    }

    /*
  - adding columns: app_id, tenant_id
  - primary key session_handle => (app_id, tenant_id, user_id, created_time_ms) + FK to totpUsersTable,tenantsTable
  */
    private void migrateTotpUsedCodesTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String totpUsersTable = Config.getConfig(start).getTotpUsersTable();
        String totpUsedCodesTable = Config.getConfig(start).getTotpUsedCodesTable();
        String tenantsTable = Config.getConfig(start).getTenantsTable();
        
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + totpUsedCodesTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public', " +
                    "   ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + totpUsedCodesTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    totpUsedCodesTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + totpUsedCodesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    totpUsedCodesTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, tenant_id, user_id, created_time_ms);");

            statement.execute("ALTER TABLE " + totpUsedCodesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    totpUsedCodesTable, "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + totpUsersTable +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + totpUsedCodesTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    totpUsedCodesTable, "tenant_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + totpUsedCodesTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    totpUsedCodesTable, "tenant_id", "fkey") +
                    "   FOREIGN KEY (app_id, tenant_id) " +
                    "   REFERENCES " + tenantsTable +
                    " (app_id, tenant_id) ON DELETE CASCADE;");

            statement.execute("DROP INDEX IF EXISTS totp_used_codes_expiry_time_ms_index;");

            statement.execute("CREATE INDEX IF NOT EXISTS totp_used_codes_expiry_time_ms_index ON " +
                    totpUsedCodesTable + " (app_id, tenant_id, expiry_time_ms);");

            statement.execute("CREATE INDEX IF NOT EXISTS totp_used_codes_user_id_index ON " +
                    totpUsedCodesTable + " (app_id, user_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS totp_used_codes_tenant_id_index ON " +
                    totpUsedCodesTable + " (app_id, tenant_id);");
        }
    }

    /*
    - adding columns: app_id, tenant_id
    - primary key session_handle => (app_id, user_id) + FK to appsTable
    */
    private void migrateUserLastActiveTable(Context context, Start start) throws SQLException {
        String schema = Config.getConfig(start).getTableSchema();
        String userLastActiveTable = Config.getConfig(start).getUserLastActiveTable();
        String appsTable = Config.getConfig(start).getAppsTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + userLastActiveTable +
                    " ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'public';");

            statement.execute("ALTER TABLE " + userLastActiveTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    userLastActiveTable, null, "pkey") + " CASCADE;");

            statement.execute("ALTER TABLE " + userLastActiveTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    userLastActiveTable, null, "pkey") +
                    "   PRIMARY KEY (app_id, user_id);");

            statement.execute("ALTER TABLE " + userLastActiveTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    userLastActiveTable, "app_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + userLastActiveTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    userLastActiveTable, "app_id", "fkey") +
                    "   FOREIGN KEY (app_id) " +
                    "   REFERENCES " + appsTable +
                    " (app_id) ON DELETE CASCADE;");


            statement.execute("CREATE INDEX IF NOT EXISTS user_last_active_app_id_index ON " +
                    userLastActiveTable + " (app_id);");
        }
    }
}
