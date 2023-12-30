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

package io.supertokens.storage.postgresql;

import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.output.Logging;
import io.supertokens.storage.postgresql.queries.BaselineMigrationQueries;
import org.flywaydb.core.Flyway;

import java.util.HashMap;
import java.util.Map;

public final class FlywayMigration {

    private FlywayMigration() {}

    public static void startMigration(Start start) {
        Logging.info(start, "Setting up Flyway.", true);

        try {
            String baselineVersion = BaselineMigrationQueries.getBaselineMigrationVersion(start);

            Flyway flyway = Flyway.configure()
                    .dataSource(ConnectionPool.getHikariDataSource(start))
 //                   .baselineOnMigrate(true)
 //                   .baselineVersion(baselineVersion)
                    .locations("classpath:/io/supertokens/storage/postgresql/migrations")
                    .placeholders(getPlaceholders(start))
                    .load();
            flyway.migrate();
        } catch (Exception e) {
            Logging.error(start, "Error Setting up Flyway.", true);
            Logging.error(start, e.toString(), true);
           // TODO: Find all possible exception
        }
    }

    private static Map<String, String> getPlaceholders(Start start) {
        Map<String, String> ph = new HashMap<>();

        ph.put("schema", Config.getConfig(start).getTableSchema());
        ph.put("session_info_table", Config.getConfig(start).getSessionInfoTable());
        ph.put("jwt_signing_keys_table", Config.getConfig(start).getJWTSigningKeysTable());
        ph.put("session_access_token_signing_keys_table", Config.getConfig(start).getAccessTokenSigningKeysTable());
        ph.put("access_token_signing_key_dynamic", "true");
        ph.put("apps_table", Config.getConfig(start).getAppsTable());
        ph.put("tenants_table", Config.getConfig(start).getTenantsTable());
        ph.put("key_value_table", Config.getConfig(start).getKeyValueTable());
        ph.put("app_id_to_user_id_table", Config.getConfig(start).getAppIdToUserIdTable());
        ph.put("all_auth_recipe_users_table", Config.getConfig(start).getUsersTable());
        ph.put("tenant_configs_table", Config.getConfig(start).getTenantConfigsTable());
        ph.put("tenant_thirdparty_providers_table", Config.getConfig(start).getTenantThirdPartyProvidersTable());
        ph.put("tenant_thirdparty_provider_clients_table", Config.getConfig(start).getTenantThirdPartyProviderClientsTable());
        ph.put("emailverification_verified_emails_table", Config.getConfig(start).getEmailVerificationTable());
        ph.put("emailverification_tokens_table", Config.getConfig(start).getEmailVerificationTokensTable());
        ph.put("emailpassword_users_table", Config.getConfig(start).getEmailPasswordUsersTable());
        ph.put("emailpassword_user_to_tenant_table", Config.getConfig(start).getEmailPasswordUserToTenantTable());
        ph.put("emailpassword_pswd_reset_tokens_table", Config.getConfig(start).getPasswordResetTokensTable());
        ph.put("passwordless_users_table", Config.getConfig(start).getPasswordlessUsersTable());
        ph.put("passwordless_user_to_tenant_table", Config.getConfig(start).getPasswordlessUserToTenantTable());
        ph.put("passwordless_devices_table", Config.getConfig(start).getPasswordlessDevicesTable());

        return ph;
    }
}
