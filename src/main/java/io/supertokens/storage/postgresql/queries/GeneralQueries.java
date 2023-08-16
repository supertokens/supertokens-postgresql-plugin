/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
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

package io.supertokens.storage.postgresql.queries;

import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.dashboard.DashboardSearchTags;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static io.supertokens.storage.postgresql.PreparedStatementValueSetter.NO_OP_SETTER;
import static io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.CREATING_NEW_TABLE;
import static io.supertokens.storage.postgresql.ProcessState.getInstance;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;
import static io.supertokens.storage.postgresql.queries.EmailPasswordQueries.*;
import static io.supertokens.storage.postgresql.queries.EmailVerificationQueries.*;
import static io.supertokens.storage.postgresql.queries.JWTSigningQueries.getQueryToCreateAppIdIndexForJWTSigningTable;
import static io.supertokens.storage.postgresql.queries.JWTSigningQueries.getQueryToCreateJWTSigningTable;
import static io.supertokens.storage.postgresql.queries.PasswordlessQueries.*;
import static io.supertokens.storage.postgresql.queries.SessionQueries.*;
import static io.supertokens.storage.postgresql.queries.UserMetadataQueries.getQueryToCreateAppIdIndexForUserMetadataTable;
import static io.supertokens.storage.postgresql.queries.UserMetadataQueries.getQueryToCreateUserMetadataTable;

public class GeneralQueries {

    private static boolean doesTableExists(Start start, String tableName) {
        try {
            String QUERY = "SELECT 1 FROM " + tableName + " LIMIT 1";
            execute(start, QUERY, NO_OP_SETTER, result -> null);
            return true;
        } catch (SQLException | StorageQueryException e) {
            return false;
        }
    }

    static String getQueryToCreateUsersTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String usersTable = Config.getConfig(start).getUsersTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + usersTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "user_id CHAR(36) NOT NULL,"
                + "primary_or_recipe_user_id CHAR(36) NOT NULL,"
                + "is_linked_or_is_a_primary_user BOOLEAN NOT NULL DEFAULT FALSE,"
                + "recipe_id VARCHAR(128) NOT NULL,"
                + "time_joined BIGINT NOT NULL,"
                + "primary_or_recipe_user_time_joined BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, usersTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, user_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, usersTable, "tenant_id", "fkey")
                + " FOREIGN KEY(app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantsTable() + " (app_id, tenant_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, usersTable, "user_id", "fkey")
                + " FOREIGN KEY(app_id, user_id)"
                + " REFERENCES " + Config.getConfig(start).getAppIdToUserIdTable() +
                " (app_id, user_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static String getQueryToCreateUserIdIndexForUsersTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS all_auth_recipe_user_id_index ON "
                + Config.getConfig(start).getUsersTable() + "(app_id, user_id);";
    }

    public static String getQueryToCreateTenantIdIndexForUsersTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS all_auth_recipe_tenant_id_index ON "
                + Config.getConfig(start).getUsersTable() + "(app_id, tenant_id);";
    }

    static String getQueryToCreateUserPaginationIndex(Start start) {
        return "CREATE INDEX all_auth_recipe_users_pagination_index ON " + Config.getConfig(start).getUsersTable()
                + "(primary_or_recipe_user_time_joined DESC, primary_or_recipe_user_id DESC, tenant_id DESC, app_id DESC);";
    }

    static String getQueryToCreatePrimaryUserIdIndex(Start start) {
        /*
         * Used in:
         * - does user exist
         *
         * */
        return "CREATE INDEX all_auth_recipe_users_primary_user_id_index ON " + Config.getConfig(start).getUsersTable()
                + "(app_id, primary_or_recipe_user_id);";
    }

    static String getQueryToCreatePrimaryUserIdAndTenantIndex(Start start) {
        /*
         * Used in:
         * - user count query
         *
         * */
        return "CREATE INDEX all_auth_recipe_users_primary_user_id_and_tenant_id_index ON " +
                Config.getConfig(start).getUsersTable()
                + "(app_id, tenant_id, primary_or_recipe_user_id);";
    }

    private static String getQueryToCreateAppsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String appsTable = Config.getConfig(start).getAppsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + appsTable + " ("
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "created_at_time BIGINT ,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, appsTable, null, "pkey")
                + " PRIMARY KEY(app_id)" +
                " );";
        // @formatter:on
    }

    private static String getQueryToCreateTenantsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantsTable = Config.getConfig(start).getTenantsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tenantsTable + " ("
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "created_at_time BIGINT ,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tenantsTable, null, "pkey") +
                " PRIMARY KEY(app_id, tenant_id) ,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tenantsTable, "app_id", "fkey")
                + " FOREIGN KEY(app_id)"
                + " REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreateAppIdIndexForTenantsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS tenants_app_id_index ON "
                + Config.getConfig(start).getTenantsTable() + "(app_id);";
    }

    private static String getQueryToCreateKeyValueTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String keyValueTable = Config.getConfig(start).getKeyValueTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + keyValueTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "name VARCHAR(128),"
                + "value TEXT,"
                + "created_at_time BIGINT ,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, keyValueTable, null, "pkey")
                + " PRIMARY KEY(app_id, tenant_id, name),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, keyValueTable, "tenant_id", "fkey")
                + " FOREIGN KEY(app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantsTable() + " (app_id, tenant_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreateTenantIdIndexForKeyValueTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS key_value_tenant_id_index ON "
                + Config.getConfig(start).getKeyValueTable() + "(app_id, tenant_id);";
    }

    private static String getQueryToCreateAppIdToUserIdTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String appToUserTable = Config.getConfig(start).getAppIdToUserIdTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + appToUserTable + " ("
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "user_id CHAR(36) NOT NULL,"
                + "recipe_id VARCHAR(128) NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, appToUserTable, null, "pkey")
                + " PRIMARY KEY (app_id, user_id), "
                + "CONSTRAINT " + Utils.getConstraintName(schema, appToUserTable, "app_id", "fkey")
                + " FOREIGN KEY(app_id) REFERENCES " + Config.getConfig(start).getAppsTable()
                + " (app_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreateAppIdIndexForAppIdToUserIdTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS app_id_to_user_id_app_id_index ON "
                + Config.getConfig(start).getAppIdToUserIdTable() + "(app_id);";
    }

    public static void createTablesIfNotExists(Start start) throws SQLException, StorageQueryException {
        int numberOfRetries = 0;
        boolean retry = true;
        while (retry) {
            retry = false;
            try {
                if (!doesTableExists(start, Config.getConfig(start).getAppsTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateAppsTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getTenantsTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateTenantsTable(start), NO_OP_SETTER);

                    // index
                    update(start, getQueryToCreateAppIdIndexForTenantsTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getKeyValueTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateKeyValueTable(start), NO_OP_SETTER);

                    // index
                    update(start, getQueryToCreateTenantIdIndexForKeyValueTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getAppIdToUserIdTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateAppIdToUserIdTable(start), NO_OP_SETTER);

                    // index
                    update(start, getQueryToCreateAppIdIndexForAppIdToUserIdTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getUsersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateUsersTable(start), NO_OP_SETTER);

                    // index
                    update(start, getQueryToCreatePrimaryUserIdIndex(start), NO_OP_SETTER);
                    update(start, getQueryToCreateUserPaginationIndex(start), NO_OP_SETTER);
                    update(start, getQueryToCreatePrimaryUserIdAndTenantIndex(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getUserLastActiveTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, ActiveUsersQueries.getQueryToCreateUserLastActiveTable(start), NO_OP_SETTER);

                    // Index
                    update(start, ActiveUsersQueries.getQueryToCreateAppIdIndexForUserLastActiveTable(start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getAccessTokenSigningKeysTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateAccessTokenSigningKeysTable(start), NO_OP_SETTER);

                    // index
                    update(start, getQueryToCreateAppIdIndexForAccessTokenSigningKeysTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getSessionInfoTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateSessionInfoTable(start), NO_OP_SETTER);

                    // index
                    update(start, getQueryToCreateSessionExpiryIndex(start), NO_OP_SETTER);
                    update(start, getQueryToCreateTenantIdIndexForSessionInfoTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getTenantConfigsTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, MultitenancyQueries.getQueryToCreateTenantConfigsTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getTenantThirdPartyProvidersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, MultitenancyQueries.getQueryToCreateTenantThirdPartyProvidersTable(start),
                            NO_OP_SETTER);

                    // index
                    update(start,
                            MultitenancyQueries.getQueryToCreateTenantIdIndexForTenantThirdPartyProvidersTable(start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getTenantThirdPartyProviderClientsTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, MultitenancyQueries.getQueryToCreateTenantThirdPartyProviderClientsTable(start),
                            NO_OP_SETTER);

                    // index
                    update(start,
                            MultitenancyQueries.getQueryToCreateThirdPartyIdIndexForTenantThirdPartyProviderClientsTable(
                                    start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getEmailPasswordUsersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, EmailPasswordQueries.getQueryToCreateUsersTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getEmailPasswordUserToTenantTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, EmailPasswordQueries.getQueryToCreateEmailPasswordUserToTenantTable(start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getPasswordResetTokensTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreatePasswordResetTokensTable(start), NO_OP_SETTER);
                    // index
                    update(start, getQueryToCreatePasswordResetTokenExpiryIndex(start), NO_OP_SETTER);
                    update(start, getQueryToCreateUserIdIndexForPasswordResetTokensTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getEmailVerificationTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateEmailVerificationTable(start), NO_OP_SETTER);

                    // index
                    update(start, getQueryToCreateAppIdIndexForEmailVerificationTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getEmailVerificationTokensTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateEmailVerificationTokensTable(start), NO_OP_SETTER);
                    // index
                    update(start, getQueryToCreateEmailVerificationTokenExpiryIndex(start), NO_OP_SETTER);
                    update(start, getQueryToCreateTenantIdIndexForEmailVerificationTokensTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getThirdPartyUsersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, ThirdPartyQueries.getQueryToCreateUsersTable(start), NO_OP_SETTER);
                    // index
                    update(start, ThirdPartyQueries.getQueryToThirdPartyUserEmailIndex(start), NO_OP_SETTER);
                    update(start, ThirdPartyQueries.getQueryToThirdPartyUserIdIndex(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getThirdPartyUserToTenantTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, ThirdPartyQueries.getQueryToCreateThirdPartyUserToTenantTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getJWTSigningKeysTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateJWTSigningTable(start), NO_OP_SETTER);

                    // index
                    update(start, getQueryToCreateAppIdIndexForJWTSigningTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getPasswordlessUsersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, PasswordlessQueries.getQueryToCreateUsersTable(start), NO_OP_SETTER);

                    // index
                    update(start, getQueryToCreateUserIdIndexForUsersTable(start), NO_OP_SETTER);
                    update(start, getQueryToCreateTenantIdIndexForUsersTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getPasswordlessUserToTenantTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, PasswordlessQueries.getQueryToCreatePasswordlessUserToTenantTable(start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getPasswordlessDevicesTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateDevicesTable(start), NO_OP_SETTER);
                    // index
                    update(start, getQueryToCreateDeviceEmailIndex(start), NO_OP_SETTER);
                    update(start, getQueryToCreateDevicePhoneNumberIndex(start), NO_OP_SETTER);
                    update(start, getQueryToCreateTenantIdIndexForDevicesTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getPasswordlessCodesTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateCodesTable(start), NO_OP_SETTER);
                    // index
                    update(start, getQueryToCreateCodeCreatedAtIndex(start), NO_OP_SETTER);

                }
                // This PostgreSQL specific, because it's created automatically in MySQL and it
                // doesn't support "create
                // index if not exists"
                // We missed creating this earlier for the codes table, so it may be missing
                // even if the table exists
                update(start, getQueryToCreateCodeDeviceIdHashIndex(start), NO_OP_SETTER);

                if (!doesTableExists(start, Config.getConfig(start).getUserMetadataTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateUserMetadataTable(start), NO_OP_SETTER);

                    // Index
                    update(start, getQueryToCreateAppIdIndexForUserMetadataTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getRolesTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, UserRolesQueries.getQueryToCreateRolesTable(start), NO_OP_SETTER);

                    // Index
                    update(start, UserRolesQueries.getQueryToCreateAppIdIndexForRolesTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getUserRolesPermissionsTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, UserRolesQueries.getQueryToCreateRolePermissionsTable(start), NO_OP_SETTER);
                    // index
                    update(start, UserRolesQueries.getQueryToCreateRolePermissionsPermissionIndex(start), NO_OP_SETTER);
                    update(start, UserRolesQueries.getQueryToCreateRoleIndexForRolePermissionsTable(start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getUserRolesTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, UserRolesQueries.getQueryToCreateUserRolesTable(start), NO_OP_SETTER);

                    // index
                    update(start, UserRolesQueries.getQueryToCreateUserRolesRoleIndex(start), NO_OP_SETTER);
                    update(start, UserRolesQueries.getQueryToCreateTenantIdIndexForUserRolesTable(start), NO_OP_SETTER);
                    update(start, UserRolesQueries.getQueryToCreateRoleIndexForUserRolesTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getUserIdMappingTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, UserIdMappingQueries.getQueryToCreateUserIdMappingTable(start), NO_OP_SETTER);

                    // index
                    update(start,
                            UserIdMappingQueries.getQueryToCreateSupertokensUserIdIndexForUserIdMappingTable(start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getDashboardUsersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, DashboardQueries.getQueryToCreateDashboardUsersTable(start), NO_OP_SETTER);

                    // Index
                    update(start, DashboardQueries.getQueryToCreateAppIdIndexForDashboardUsersTable(start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getDashboardSessionsTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, DashboardQueries.getQueryToCreateDashboardUserSessionsTable(start), NO_OP_SETTER);
                    // index
                    update(start, DashboardQueries.getQueryToCreateDashboardUserSessionsExpiryIndex(start),
                            NO_OP_SETTER);
                    update(start, DashboardQueries.getQueryToCreateUserIdIndexForDashboardUserSessionsTable(start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getTotpUsersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, TOTPQueries.getQueryToCreateUsersTable(start), NO_OP_SETTER);

                    // index
                    update(start, TOTPQueries.getQueryToCreateAppIdIndexForUsersTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getTotpUserDevicesTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, TOTPQueries.getQueryToCreateUserDevicesTable(start), NO_OP_SETTER);

                    // index
                    update(start, TOTPQueries.getQueryToCreateUserIdIndexForUserDevicesTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getTotpUsedCodesTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, TOTPQueries.getQueryToCreateUsedCodesTable(start), NO_OP_SETTER);
                    // index:
                    update(start, TOTPQueries.getQueryToCreateUsedCodesExpiryTimeIndex(start), NO_OP_SETTER);
                    update(start, TOTPQueries.getQueryToCreateUserIdIndexForUsedCodesTable(start), NO_OP_SETTER);
                    update(start, TOTPQueries.getQueryToCreateTenantIdIndexForUsedCodesTable(start), NO_OP_SETTER);
                }

            } catch (Exception e) {
                if (e.getMessage().contains("schema") && e.getMessage().contains("does not exist")
                        && numberOfRetries < 1) {
                    // we must create the schema and try again.
                    String schema = Config.getConfig(start).getTableSchema();
                    if (!schema.equals("public")) {
                        String query = "CREATE SCHEMA " + schema;
                        update(start, query, NO_OP_SETTER);
                        numberOfRetries++;
                        retry = true;
                        continue;
                    }
                }
                throw e;
            }
        }
    }

    @TestOnly
    public static void deleteAllTables(Start start) throws SQLException, StorageQueryException {
        if (!Start.isTesting) {
            throw new UnsupportedOperationException();
        }
        {
            String DROP_QUERY = "DROP INDEX IF EXISTS emailpassword_password_reset_token_expiry_index";
            update(start, DROP_QUERY, NO_OP_SETTER);
        }
        {
            String DROP_QUERY = "DROP INDEX IF EXISTS emailverification_tokens_index";
            update(start, DROP_QUERY, NO_OP_SETTER);
        }
        {
            String DROP_QUERY = "DROP INDEX IF EXISTS all_auth_recipe_users_pagination_index";
            update(start, DROP_QUERY, NO_OP_SETTER);
        }

        {
            String DROP_QUERY = "DROP TABLE IF EXISTS "
                    + getConfig(start).getAppsTable() + ","
                    + getConfig(start).getUserLastActiveTable() + ","
                    + getConfig(start).getTenantsTable() + ","
                    + getConfig(start).getKeyValueTable() + ","
                    + getConfig(start).getAppIdToUserIdTable() + ","
                    + getConfig(start).getUserIdMappingTable() + ","
                    + getConfig(start).getUsersTable() + ","
                    + getConfig(start).getAccessTokenSigningKeysTable() + ","
                    + getConfig(start).getTenantConfigsTable() + ","
                    + getConfig(start).getTenantThirdPartyProvidersTable() + ","
                    + getConfig(start).getTenantThirdPartyProviderClientsTable() + ","
                    + getConfig(start).getSessionInfoTable() + ","
                    + getConfig(start).getEmailPasswordUserToTenantTable() + ","
                    + getConfig(start).getEmailPasswordUsersTable() + ","
                    + getConfig(start).getPasswordResetTokensTable() + ","
                    + getConfig(start).getEmailVerificationTokensTable() + ","
                    + getConfig(start).getEmailVerificationTable() + ","
                    + getConfig(start).getThirdPartyUsersTable() + ","
                    + getConfig(start).getThirdPartyUserToTenantTable() + ","
                    + getConfig(start).getJWTSigningKeysTable() + ","
                    + getConfig(start).getPasswordlessCodesTable() + ","
                    + getConfig(start).getPasswordlessDevicesTable() + ","
                    + getConfig(start).getPasswordlessUserToTenantTable() + ","
                    + getConfig(start).getPasswordlessUsersTable() + ","
                    + getConfig(start).getUserMetadataTable() + ","
                    + getConfig(start).getRolesTable() + ","
                    + getConfig(start).getUserRolesPermissionsTable() + ","
                    + getConfig(start).getUserRolesTable() + ","
                    + getConfig(start).getDashboardUsersTable() + ","
                    + getConfig(start).getDashboardSessionsTable() + ","
                    + getConfig(start).getTotpUsedCodesTable() + "," + getConfig(start).getTotpUserDevicesTable() + ","
                    + getConfig(start).getTotpUsersTable();
            update(start, DROP_QUERY, NO_OP_SETTER);
        }
    }

    public static void setKeyValue_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier,
                                               String key, KeyValueInfo info)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getKeyValueTable()
                + "(app_id, tenant_id, name, value, created_at_time) VALUES(?, ?, ?, ?, ?) "
                + "ON CONFLICT (app_id, tenant_id, name) DO UPDATE SET value = ?, created_at_time = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, key);
            pst.setString(4, info.value);
            pst.setLong(5, info.createdAtTime);
            pst.setString(6, info.value);
            pst.setLong(7, info.createdAtTime);
        });
    }

    public static void setKeyValue(Start start, TenantIdentifier tenantIdentifier, String key, KeyValueInfo info)
            throws SQLException, StorageQueryException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            setKeyValue_Transaction(start, con, tenantIdentifier, key, info);
        }
    }

    public static KeyValueInfo getKeyValue(Start start, TenantIdentifier tenantIdentifier, String key)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT value, created_at_time FROM " + getConfig(start).getKeyValueTable()
                + " WHERE app_id = ? AND tenant_id = ? AND name = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, key);
        }, result -> {
            if (result.next()) {
                return KeyValueInfoRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static KeyValueInfo getKeyValue_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier,
                                                       String key)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT value, created_at_time FROM " + getConfig(start).getKeyValueTable()
                + " WHERE app_id = ? AND tenant_id = ? AND name = ? FOR UPDATE";

        return execute(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, key);
        }, result -> {
            if (result.next()) {
                return KeyValueInfoRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static void deleteKeyValue_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier,
                                                  String key)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getKeyValueTable()
                + " WHERE app_id = ? AND tenant_id = ? AND name = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, key);
        });
    }

    public static long getUsersCount(Start start, AppIdentifier appIdentifier, RECIPE_ID[] includeRecipeIds)
            throws SQLException, StorageQueryException {
        StringBuilder QUERY = new StringBuilder(
                "SELECT COUNT(DISTINCT primary_or_recipe_user_id) AS total FROM " +
                        getConfig(start).getUsersTable());
        QUERY.append(" WHERE app_id = ?");
        if (includeRecipeIds != null && includeRecipeIds.length > 0) {
            QUERY.append(" AND recipe_id IN (");
            for (int i = 0; i < includeRecipeIds.length; i++) {
                QUERY.append("?");
                if (i != includeRecipeIds.length - 1) {
                    // not the last element
                    QUERY.append(",");
                }
            }
            QUERY.append(")");
        }

        return execute(start, QUERY.toString(), pst -> {
            pst.setString(1, appIdentifier.getAppId());
            if (includeRecipeIds != null) {
                for (int i = 0; i < includeRecipeIds.length; i++) {
                    // i+2 cause this starts with 1 and not 0, and 1 is appId
                    pst.setString(i + 2, includeRecipeIds[i].toString());
                }
            }
        }, result -> {
            if (result.next()) {
                return result.getLong("total");
            }
            return 0L;
        });
    }

    public static long getUsersCount(Start start, TenantIdentifier tenantIdentifier, RECIPE_ID[] includeRecipeIds)
            throws SQLException, StorageQueryException {
        StringBuilder QUERY = new StringBuilder(
                "SELECT COUNT(DISTINCT primary_or_recipe_user_id) AS total FROM " + getConfig(start).getUsersTable());
        QUERY.append(" WHERE app_id = ? AND tenant_id = ?");
        if (includeRecipeIds != null && includeRecipeIds.length > 0) {
            QUERY.append(" AND recipe_id IN (");
            for (int i = 0; i < includeRecipeIds.length; i++) {
                QUERY.append("?");
                if (i != includeRecipeIds.length - 1) {
                    // not the last element
                    QUERY.append(",");
                }
            }
            QUERY.append(")");
        }

        return execute(start, QUERY.toString(), pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            if (includeRecipeIds != null) {
                for (int i = 0; i < includeRecipeIds.length; i++) {
                    // i+3 cause this starts with 1 and not 0, and 1 is appId, 2 is tenantId
                    pst.setString(i + 3, includeRecipeIds[i].toString());
                }
            }
        }, result -> {
            if (result.next()) {
                return result.getLong("total");
            }
            return 0L;
        });
    }

    public static boolean doesUserIdExist(Start start, AppIdentifier appIdentifier, String userId)
            throws SQLException, StorageQueryException {
        // We query both tables cause there is a case where a primary user ID exists, but its associated
        // recipe user ID has been deleted AND there are other recipe user IDs linked to this primary user ID already.
        String QUERY = "SELECT 1 FROM " + getConfig(start).getAppIdToUserIdTable()
                + " WHERE app_id = ? AND user_id = ? UNION SELECT 1 FROM " + getConfig(start).getUsersTable() +
                " WHERE app_id = ? AND primary_or_recipe_user_id = ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
            pst.setString(3, appIdentifier.getAppId());
            pst.setString(4, userId);
        }, ResultSet::next);
    }

    public static boolean doesUserIdExist(Start start, TenantIdentifier tenantIdentifier, String userId)
            throws SQLException, StorageQueryException {
        // We query both tables cause there is a case where a primary user ID exists, but its associated
        // recipe user ID has been deleted AND there are other recipe user IDs linked to this primary user ID already.
        String QUERY = "SELECT 1 FROM " + getConfig(start).getUsersTable()
                + " WHERE app_id = ? AND tenant_id = ? AND user_id = ? UNION SELECT 1 FROM " +
                getConfig(start).getUsersTable() +
                " WHERE app_id = ? AND tenant_id = ? AND primary_or_recipe_user_id = ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, userId);
            pst.setString(4, tenantIdentifier.getAppId());
            pst.setString(5, tenantIdentifier.getTenantId());
            pst.setString(6, userId);
        }, ResultSet::next);
    }

    public static AuthRecipeUserInfo[] getUsers(Start start, TenantIdentifier tenantIdentifier, @NotNull Integer limit,
                                                @NotNull String timeJoinedOrder,
                                                @Nullable RECIPE_ID[] includeRecipeIds, @Nullable String userId,
                                                @Nullable Long timeJoined,
                                                @Nullable DashboardSearchTags dashboardSearchTags)
            throws SQLException, StorageQueryException {

        // This list will be used to keep track of the result's order from the db
        List<String> usersFromQuery;

        if (dashboardSearchTags != null) {
            ArrayList<String> queryList = new ArrayList<>();
            {
                StringBuilder USER_SEARCH_TAG_CONDITION = new StringBuilder();

                {
                    // check if we should search through the emailpassword table
                    if (dashboardSearchTags.shouldEmailPasswordTableBeSearched()) {
                        String QUERY = "SELECT  allAuthUsersTable.*" + " FROM " + getConfig(start).getUsersTable()
                                + " AS allAuthUsersTable" +
                                " JOIN " + getConfig(start).getEmailPasswordUserToTenantTable()
                                + " AS emailpasswordTable ON allAuthUsersTable.app_id = emailpasswordTable.app_id AND "
                                + "allAuthUsersTable.tenant_id = emailpasswordTable.tenant_id AND "
                                + "allAuthUsersTable.user_id = emailpasswordTable.user_id";

                        // attach email tags to queries
                        QUERY = QUERY +
                                " WHERE (emailpasswordTable.app_id = ? AND emailpasswordTable.tenant_id = ?) AND"
                                + " (emailpasswordTable.email LIKE ? OR emailpasswordTable.email LIKE ?)";
                        queryList.add(tenantIdentifier.getAppId());
                        queryList.add(tenantIdentifier.getTenantId());
                        queryList.add(dashboardSearchTags.emails.get(0) + "%");
                        queryList.add("%@" + dashboardSearchTags.emails.get(0) + "%");
                        for (int i = 1; i < dashboardSearchTags.emails.size(); i++) {
                            QUERY += " OR emailpasswordTable.email LIKE ? OR emailpasswordTable.email LIKE ?";
                            queryList.add(dashboardSearchTags.emails.get(i) + "%");
                            queryList.add("%@" + dashboardSearchTags.emails.get(i) + "%");
                        }

                        USER_SEARCH_TAG_CONDITION.append("SELECT * FROM ( ").append(QUERY)
                                .append(" LIMIT 1000) AS emailpasswordResultTable");
                    }
                }

                {
                    // check if we should search through the thirdparty table
                    if (dashboardSearchTags.shouldThirdPartyTableBeSearched()) {
                        String QUERY = "SELECT  allAuthUsersTable.*" + " FROM " + getConfig(start).getUsersTable()
                                + " AS allAuthUsersTable"
                                + " JOIN " + getConfig(start).getThirdPartyUserToTenantTable()
                                +
                                " AS thirdPartyToTenantTable ON allAuthUsersTable.app_id = thirdPartyToTenantTable" +
                                ".app_id AND"
                                + " allAuthUsersTable.tenant_id = thirdPartyToTenantTable.tenant_id AND"
                                + " allAuthUsersTable.user_id = thirdPartyToTenantTable.user_id"
                                + " JOIN " + getConfig(start).getThirdPartyUsersTable()
                                + " AS thirdPartyTable ON thirdPartyToTenantTable.app_id = thirdPartyTable.app_id AND"
                                + " thirdPartyToTenantTable.user_id = thirdPartyTable.user_id";

                        // check if email tag is present
                        if (dashboardSearchTags.emails != null) {

                            QUERY +=
                                    " WHERE (thirdPartyToTenantTable.app_id = ? AND thirdPartyToTenantTable.tenant_id" +
                                            " = ?)"
                                            + " AND ( thirdPartyTable.email LIKE ? OR thirdPartyTable.email LIKE ?";
                            queryList.add(tenantIdentifier.getAppId());
                            queryList.add(tenantIdentifier.getTenantId());
                            queryList.add(dashboardSearchTags.emails.get(0) + "%");
                            queryList.add("%@" + dashboardSearchTags.emails.get(0) + "%");

                            for (int i = 1; i < dashboardSearchTags.emails.size(); i++) {
                                QUERY += " OR thirdPartyTable.email LIKE ? OR thirdPartyTable.email LIKE ?";
                                queryList.add(dashboardSearchTags.emails.get(i) + "%");
                                queryList.add("%@" + dashboardSearchTags.emails.get(i) + "%");
                            }

                            QUERY += " )";

                        }

                        // check if providers tag is present
                        if (dashboardSearchTags.providers != null) {
                            if (dashboardSearchTags.emails != null) {
                                QUERY += " AND ";
                            } else {
                                QUERY += " WHERE (thirdPartyToTenantTable.app_id = ? AND thirdPartyToTenantTable" +
                                        ".tenant_id = ?) AND ";
                                queryList.add(tenantIdentifier.getAppId());
                                queryList.add(tenantIdentifier.getTenantId());
                            }

                            QUERY += " ( thirdPartyTable.third_party_id LIKE ?";
                            queryList.add(dashboardSearchTags.providers.get(0) + "%");
                            for (int i = 1; i < dashboardSearchTags.providers.size(); i++) {
                                QUERY += " OR thirdPartyTable.third_party_id LIKE ?";
                                queryList.add(dashboardSearchTags.providers.get(i) + "%");
                            }

                            QUERY += " )";
                        }

                        // check if we need to append this to an existing search query
                        if (USER_SEARCH_TAG_CONDITION.length() != 0) {
                            USER_SEARCH_TAG_CONDITION.append(" UNION ").append("SELECT * FROM ( ").append(QUERY)
                                    .append(" LIMIT 1000) AS thirdPartyResultTable");

                        } else {
                            USER_SEARCH_TAG_CONDITION.append("SELECT * FROM ( ").append(QUERY)
                                    .append(" LIMIT 1000) AS thirdPartyResultTable");

                        }
                    }
                }

                {
                    // check if we should search through the passwordless table
                    if (dashboardSearchTags.shouldPasswordlessTableBeSearched()) {
                        String QUERY = "SELECT  allAuthUsersTable.*" + " FROM " + getConfig(start).getUsersTable()
                                + " AS allAuthUsersTable" +
                                " JOIN " + getConfig(start).getPasswordlessUserToTenantTable()
                                + " AS passwordlessTable ON allAuthUsersTable.app_id = passwordlessTable.app_id AND"
                                + " allAuthUsersTable.tenant_id = passwordlessTable.tenant_id AND"
                                + " allAuthUsersTable.user_id = passwordlessTable.user_id";

                        // check if email tag is present
                        if (dashboardSearchTags.emails != null) {

                            QUERY = QUERY + " WHERE (passwordlessTable.app_id = ? AND passwordlessTable.tenant_id = ?)"
                                    + " AND ( passwordlessTable.email LIKE ? OR passwordlessTable.email LIKE ?";
                            queryList.add(tenantIdentifier.getAppId());
                            queryList.add(tenantIdentifier.getTenantId());
                            queryList.add(dashboardSearchTags.emails.get(0) + "%");
                            queryList.add("%@" + dashboardSearchTags.emails.get(0) + "%");
                            for (int i = 1; i < dashboardSearchTags.emails.size(); i++) {
                                QUERY += " OR passwordlessTable.email LIKE ? OR passwordlessTable.email LIKE ?";
                                queryList.add(dashboardSearchTags.emails.get(i) + "%");
                                queryList.add("%@" + dashboardSearchTags.emails.get(i) + "%");
                            }

                            QUERY += " )";
                        }

                        // check if phone tag is present
                        if (dashboardSearchTags.phoneNumbers != null) {

                            if (dashboardSearchTags.emails != null) {
                                QUERY += " AND ";
                            } else {
                                QUERY += " WHERE (passwordlessTable.app_id = ? AND passwordlessTable.tenant_id = ?) " +
                                        "AND ";
                                queryList.add(tenantIdentifier.getAppId());
                                queryList.add(tenantIdentifier.getTenantId());
                            }

                            QUERY += " ( passwordlessTable.phone_number LIKE ?";
                            queryList.add(dashboardSearchTags.phoneNumbers.get(0) + "%");
                            for (int i = 1; i < dashboardSearchTags.phoneNumbers.size(); i++) {
                                QUERY += " OR passwordlessTable.phone_number LIKE ?";
                                queryList.add(dashboardSearchTags.phoneNumbers.get(i) + "%");
                            }

                            QUERY += " )";
                        }

                        // check if we need to append this to an existing search query
                        if (USER_SEARCH_TAG_CONDITION.length() != 0) {
                            USER_SEARCH_TAG_CONDITION.append(" UNION ").append("SELECT * FROM ( ").append(QUERY)
                                    .append(" LIMIT 1000) AS passwordlessResultTable");

                        } else {
                            USER_SEARCH_TAG_CONDITION.append("SELECT * FROM ( ").append(QUERY)
                                    .append(" LIMIT 1000) AS passwordlessResultTable");

                        }
                    }
                }

                if (USER_SEARCH_TAG_CONDITION.toString().length() == 0) {
                    usersFromQuery = new ArrayList<>();
                } else {

                    String finalQuery = "SELECT * FROM ( " + USER_SEARCH_TAG_CONDITION.toString() + " )"
                            + " AS finalResultTable ORDER BY time_joined " + timeJoinedOrder + ", primary_or_recipe_user_id DESC ";
                    usersFromQuery = execute(start, finalQuery, pst -> {
                        for (int i = 1; i <= queryList.size(); i++) {
                            pst.setString(i, queryList.get(i - 1));
                        }
                    }, result -> {
                        List<String> temp = new ArrayList<>();
                        while (result.next()) {
                            temp.add(result.getString("primary_or_recipe_user_id"));
                        }
                        return temp;
                    });
                }

            }

        } else {
            StringBuilder RECIPE_ID_CONDITION = new StringBuilder();
            if (includeRecipeIds != null && includeRecipeIds.length > 0) {
                RECIPE_ID_CONDITION.append("recipe_id IN (");
                for (int i = 0; i < includeRecipeIds.length; i++) {

                    RECIPE_ID_CONDITION.append("?");
                    if (i != includeRecipeIds.length - 1) {
                        // not the last element
                        RECIPE_ID_CONDITION.append(",");
                    }
                }
                RECIPE_ID_CONDITION.append(")");
            }

            if (timeJoined != null && userId != null) {
                String recipeIdCondition = RECIPE_ID_CONDITION.toString();
                if (!recipeIdCondition.equals("")) {
                    recipeIdCondition = recipeIdCondition + " AND";
                }
                String timeJoinedOrderSymbol = timeJoinedOrder.equals("ASC") ? ">" : "<";
                String QUERY = "SELECT primary_or_recipe_user_id FROM " + getConfig(start).getUsersTable() + " WHERE "
                        + recipeIdCondition + " (primary_or_recipe_user_time_joined " + timeJoinedOrderSymbol
                        + " ? OR (primary_or_recipe_user_time_joined = ? AND primary_or_recipe_user_id <= ?)) AND app_id = ? AND tenant_id = ?"
                        + " ORDER BY primary_or_recipe_user_time_joined " + timeJoinedOrder
                        + ", primary_or_recipe_user_id DESC LIMIT ?";
                usersFromQuery = execute(start, QUERY, pst -> {
                    if (includeRecipeIds != null) {
                        for (int i = 0; i < includeRecipeIds.length; i++) {
                            // i+1 cause this starts with 1 and not 0
                            pst.setString(i + 1, includeRecipeIds[i].toString());
                        }
                    }
                    int baseIndex = includeRecipeIds == null ? 0 : includeRecipeIds.length;
                    pst.setLong(baseIndex + 1, timeJoined);
                    pst.setLong(baseIndex + 2, timeJoined);
                    pst.setString(baseIndex + 3, userId);
                    pst.setString(baseIndex + 4, tenantIdentifier.getAppId());
                    pst.setString(baseIndex + 5, tenantIdentifier.getTenantId());
                    pst.setInt(baseIndex + 6, limit);
                }, result -> {
                    List<String> temp = new ArrayList<>();
                    while (result.next()) {
                        temp.add(result.getString("primary_or_recipe_user_id"));
                    }
                    return temp;
                });
            } else {
                String recipeIdCondition = RECIPE_ID_CONDITION.toString();
                String QUERY = "SELECT primary_or_recipe_user_id, recipe_id FROM " + getConfig(start).getUsersTable() + " WHERE ";
                if (!recipeIdCondition.equals("")) {
                    QUERY += recipeIdCondition + " AND";
                }
                QUERY += " app_id = ? AND tenant_id = ? ORDER BY primary_or_recipe_user_time_joined " + timeJoinedOrder
                        + ", primary_or_recipe_user_id DESC LIMIT ?";
                usersFromQuery = execute(start, QUERY, pst -> {
                    if (includeRecipeIds != null) {
                        for (int i = 0; i < includeRecipeIds.length; i++) {
                            // i+1 cause this starts with 1 and not 0
                            pst.setString(i + 1, includeRecipeIds[i].toString());
                        }
                    }
                    int baseIndex = includeRecipeIds == null ? 0 : includeRecipeIds.length;
                    pst.setString(baseIndex + 1, tenantIdentifier.getAppId());
                    pst.setString(baseIndex + 2, tenantIdentifier.getTenantId());
                    pst.setInt(baseIndex + 3, limit);
                }, result -> {
                    List<String> temp = new ArrayList<>();
                    while (result.next()) {
                        temp.add(result.getString("primary_or_recipe_user_id"));
                    }
                    return temp;
                });
            }
        }

        AuthRecipeUserInfo[] finalResult = new AuthRecipeUserInfo[usersFromQuery.size()];

        List<AuthRecipeUserInfo> users = getPrimaryUserInfoForUserIds(start,
                tenantIdentifier.toAppIdentifier(),
                usersFromQuery);

        // we fill in all the slots in finalResult based on their position in
        // usersFromQuery
        Map<String, AuthRecipeUserInfo> userIdToInfoMap = new HashMap<>();
        for (AuthRecipeUserInfo user : users) {
            userIdToInfoMap.put(user.id, user);
        }
        for (int i = 0; i < usersFromQuery.size(); i++) {
            if (finalResult[i] == null) {
                finalResult[i] = userIdToInfoMap.get(usersFromQuery.get(i));
            }
        }

        return finalResult;
    }

    public static void makePrimaryUser_Transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier,
                                                   String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "UPDATE " + getConfig(start).getUsersTable() +
                " SET is_linked_or_is_a_primary_user = true WHERE app_id = ? AND user_id = ?";

        update(sqlCon, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        });
    }

    public static void linkAccounts_Transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier,
                                                String recipeUserId, String primaryUserId)
            throws SQLException, StorageQueryException {
        {
            String QUERY = "UPDATE " + getConfig(start).getUsersTable() +
                    " SET is_linked_or_is_a_primary_user = true, primary_or_recipe_user_id = ? WHERE app_id = ? AND " +
                    "user_id = ?";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, primaryUserId);
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, recipeUserId);
            });
        }
        { // update primary_or_recipe_user_time_joined to min time joined
            String QUERY = "UPDATE " + getConfig(start).getUsersTable() +
                    " SET primary_or_recipe_user_time_joined = (SELECT MIN(time_joined) FROM " +
                    getConfig(start).getUsersTable() + " WHERE app_id = ? AND primary_or_recipe_user_id = ?) WHERE " +
                    " app_id = ? AND primary_or_recipe_user_id = ?";
            update(sqlCon, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, primaryUserId);
                pst.setString(3, appIdentifier.getAppId());
                pst.setString(4, primaryUserId);
            });
        }
    }

    public static void unlinkAccounts_Transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier,
                                                  String primaryUserId, String recipeUserId)
            throws SQLException, StorageQueryException {
        {
            String QUERY = "UPDATE " + getConfig(start).getUsersTable() +
                    " SET is_linked_or_is_a_primary_user = false, primary_or_recipe_user_id = ?, " +
                    "primary_or_recipe_user_time_joined = time_joined WHERE app_id = ? AND " +
                    "user_id = ?";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, recipeUserId);
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, recipeUserId);
            });
        }
        { // update primary_or_recipe_user_time_joined to min time joined
            String QUERY = "UPDATE " + getConfig(start).getUsersTable() +
                    " SET primary_or_recipe_user_time_joined = (SELECT MIN(time_joined) FROM " +
                    getConfig(start).getUsersTable() + " WHERE app_id = ? AND primary_or_recipe_user_id = ?) WHERE " +
                    " app_id = ? AND primary_or_recipe_user_id = ?";
            update(sqlCon, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, primaryUserId);
                pst.setString(3, appIdentifier.getAppId());
                pst.setString(4, primaryUserId);
            });
        }
    }

    public static AuthRecipeUserInfo[] listPrimaryUsersByPhoneNumber_Transaction(Start start, Connection sqlCon,
                                                                                 TenantIdentifier tenantIdentifier,
                                                                                 String phoneNumber)
            throws SQLException, StorageQueryException {
        // we first lock on the table based on phoneNumber and tenant - this will ensure that any other
        // query happening related to the account linking on this phone number / tenant will wait for this to finish,
        // and vice versa.

        PasswordlessQueries.lockPhoneAndTenant_Transaction(start, sqlCon, tenantIdentifier, phoneNumber);

        // now that we have locks on all the relevant tables, we can read from them safely
        return listPrimaryUsersByPhoneNumberHelper(start, sqlCon, tenantIdentifier, phoneNumber);
    }

    public static AuthRecipeUserInfo getPrimaryUsersByThirdPartyInfo_Transaction(Start start, Connection sqlCon,
                                                                                 TenantIdentifier tenantIdentifier,
                                                                                 String thirdPartyId,
                                                                                 String thirdPartyUserId)
            throws SQLException, StorageQueryException {
        // we first lock on the table based on thirdparty info and tenant - this will ensure that any other
        // query happening related to the account linking on this third party info / tenant will wait for this to
        // finish,
        // and vice versa.

        ThirdPartyQueries.lockThirdPartyInfoAndTenant_Transaction(start, sqlCon, tenantIdentifier, thirdPartyId,
                thirdPartyUserId);

        // now that we have locks on all the relevant tables, we can read from them safely
        return getPrimaryUserByThirdPartyInfoHelper(start, sqlCon, tenantIdentifier, thirdPartyId, thirdPartyUserId);
    }

    public static AuthRecipeUserInfo[] listPrimaryUsersByEmail_Transaction(Start start, Connection sqlCon,
                                                                           TenantIdentifier tenantIdentifier,
                                                                           String email)
            throws SQLException, StorageQueryException {
        // we first lock on the three tables based on email and tenant - this will ensure that any other
        // query happening related to the account linking on this email / tenant will wait for this to finish,
        // and vice versa.

        EmailPasswordQueries.lockEmailAndTenant_Transaction(start, sqlCon, tenantIdentifier, email);

        ThirdPartyQueries.lockEmailAndTenant_Transaction(start, sqlCon, tenantIdentifier, email);

        PasswordlessQueries.lockEmailAndTenant_Transaction(start, sqlCon, tenantIdentifier, email);

        // now that we have locks on all the relevant tables, we can read from them safely
        return listPrimaryUsersByEmailHelper(start, sqlCon, tenantIdentifier, email);
    }

    public static AuthRecipeUserInfo[] listPrimaryUsersByEmail(Start start, TenantIdentifier tenantIdentifier,
                                                               String email)
            throws StorageQueryException, SQLException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            return listPrimaryUsersByEmailHelper(start, con, tenantIdentifier, email);
        }
    }

    private static AuthRecipeUserInfo[] listPrimaryUsersByEmailHelper(Start start, Connection con,
                                                                      TenantIdentifier tenantIdentifier,
                                                                      String email)
            throws StorageQueryException, SQLException {
        List<String> userIds = new ArrayList<>();
        String emailPasswordUserId = EmailPasswordQueries.getPrimaryUserIdUsingEmail(start, con, tenantIdentifier,
                email);
        if (emailPasswordUserId != null) {
            userIds.add(emailPasswordUserId);
        }

        String passwordlessUserId = PasswordlessQueries.getPrimaryUserIdUsingEmail(start, con, tenantIdentifier,
                email);
        if (passwordlessUserId != null) {
            userIds.add(passwordlessUserId);
        }

        userIds.addAll(ThirdPartyQueries.getPrimaryUserIdUsingEmail(start, con, tenantIdentifier, email));

        // remove duplicates from userIds
        Set<String> userIdsSet = new HashSet<>(userIds);
        userIds = new ArrayList<>(userIdsSet);

        List<AuthRecipeUserInfo> result = getPrimaryUserInfoForUserIds(start, con, tenantIdentifier.toAppIdentifier(),
                userIds);

        // this is going to order them based on oldest that joined to newest that joined.
         result.sort(Comparator.comparingLong(o -> o.timeJoined));

        return result.toArray(new AuthRecipeUserInfo[0]);
    }

    public static AuthRecipeUserInfo[] listPrimaryUsersByPhoneNumber(Start start,
                                                                     TenantIdentifier tenantIdentifier,
                                                                     String phoneNumber)
            throws StorageQueryException, SQLException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            return listPrimaryUsersByPhoneNumberHelper(start, con, tenantIdentifier, phoneNumber);
        }
    }

    private static AuthRecipeUserInfo[] listPrimaryUsersByPhoneNumberHelper(Start start, Connection con,
                                                                            TenantIdentifier tenantIdentifier,
                                                                            String phoneNumber)
            throws StorageQueryException, SQLException {
        List<String> userIds = new ArrayList<>();

        String passwordlessUserId = PasswordlessQueries.getPrimaryUserByPhoneNumber(start, con, tenantIdentifier,
                phoneNumber);
        if (passwordlessUserId != null) {
            userIds.add(passwordlessUserId);
        }

        List<AuthRecipeUserInfo> result = getPrimaryUserInfoForUserIds(start, con, tenantIdentifier.toAppIdentifier(),
                userIds);

        // this is going to order them based on oldest that joined to newest that joined.
        result.sort(Comparator.comparingLong(o -> o.timeJoined));

        return result.toArray(new AuthRecipeUserInfo[0]);
    }

    public static AuthRecipeUserInfo getPrimaryUserByThirdPartyInfo(Start start,
                                                                    TenantIdentifier tenantIdentifier,
                                                                    String thirdPartyId,
                                                                    String thirdPartyUserId)
            throws StorageQueryException, SQLException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            return getPrimaryUserByThirdPartyInfoHelper(start, con, tenantIdentifier, thirdPartyId, thirdPartyUserId);
        }
    }

    private static AuthRecipeUserInfo getPrimaryUserByThirdPartyInfoHelper(Start start, Connection con,
                                                                           TenantIdentifier tenantIdentifier,
                                                                           String thirdPartyId,
                                                                           String thirdPartyUserId)
            throws StorageQueryException, SQLException {

        String userId = ThirdPartyQueries.getThirdPartyUserInfoUsingId(start, con, tenantIdentifier,
                thirdPartyId, thirdPartyUserId);
        if (userId != null) {
            return getPrimaryUserInfoForUserId(start, con, tenantIdentifier.toAppIdentifier(),
                    userId);
        }
        return null;
    }

    public static AuthRecipeUserInfo getPrimaryUserInfoForUserId_Transaction(Start start, Connection sqlCon,
                                                                             AppIdentifier appIdentifier, String id)
            throws SQLException, StorageQueryException {

        // We do for update on the outer query cause the outer one will lock at least all the ones
        // that the inner one locks.
        String QUERY = "SELECT * FROM " + getConfig(start).getUsersTable() +
                " WHERE primary_or_recipe_user_id IN (SELECT primary_or_recipe_user_id FROM " +
                getConfig(start).getUsersTable() +
                " WHERE user_id = ? OR primary_or_recipe_user_id = ? AND app_id = ?) AND app_id = ? FOR UPDATE";

        List<AllAuthRecipeUsersResultHolder> allAuthUsersResult = execute(sqlCon, QUERY, pst -> {
            pst.setString(1, id);
            pst.setString(2, id);
            pst.setString(3, appIdentifier.getAppId());
            pst.setString(4, appIdentifier.getAppId());
        }, result -> {
            List<AllAuthRecipeUsersResultHolder> finalResult = new ArrayList<>();
            while (result.next()) {
                finalResult.add(new AllAuthRecipeUsersResultHolder(result.getString("user_id"),
                        result.getString("tenant_id"),
                        result.getString("primary_or_recipe_user_id"),
                        result.getBoolean("is_linked_or_is_a_primary_user"),
                        result.getString("recipe_id"),
                        result.getLong("time_joined")));
            }
            return finalResult;
        });

        if (allAuthUsersResult.size() == 0) {
            return null;
        }

        Set<String> recipeUserIdsToFetch = new HashSet<>();
        for (AllAuthRecipeUsersResultHolder user : allAuthUsersResult) {
            // this will remove duplicate entries wherein a user id is shared across several tenants.
            recipeUserIdsToFetch.add(user.userId);
        }

        List<LoginMethod> loginMethods = new ArrayList<>();
        loginMethods.addAll(
                EmailPasswordQueries.getUsersInfoUsingIdList(start, sqlCon, recipeUserIdsToFetch, appIdentifier));
        loginMethods.addAll(
                ThirdPartyQueries.getUsersInfoUsingIdList(start, sqlCon, recipeUserIdsToFetch, appIdentifier));
        loginMethods.addAll(
                PasswordlessQueries.getUsersInfoUsingIdList(start, sqlCon, recipeUserIdsToFetch, appIdentifier));

        Map<String, LoginMethod> recipeUserIdToLoginMethodMap = new HashMap<>();
        for (LoginMethod loginMethod : loginMethods) {
            recipeUserIdToLoginMethodMap.put(loginMethod.recipeUserId, loginMethod);
        }

        Map<String, AuthRecipeUserInfo> userIdToAuthRecipeUserInfo = new HashMap<>();

        String pUserId = null;
        for (AllAuthRecipeUsersResultHolder authRecipeUsersResultHolder : allAuthUsersResult) {
            String recipeUserId = authRecipeUsersResultHolder.userId;
            LoginMethod loginMethod = recipeUserIdToLoginMethodMap.get(recipeUserId);
            assert (loginMethod != null);
            String primaryUserId = authRecipeUsersResultHolder.primaryOrRecipeUserId;
            pUserId = primaryUserId;
            AuthRecipeUserInfo curr = userIdToAuthRecipeUserInfo.get(primaryUserId);
            if (curr == null) {
                curr = AuthRecipeUserInfo.create(primaryUserId, authRecipeUsersResultHolder.isLinkedOrIsAPrimaryUser,
                        loginMethod);
            } else {
                curr.addLoginMethod(loginMethod);
            }
            userIdToAuthRecipeUserInfo.put(primaryUserId, curr);
        }

        assert (userIdToAuthRecipeUserInfo.size() == 1 && pUserId != null);

        return userIdToAuthRecipeUserInfo.get(pUserId);
    }

    public static String getPrimaryUserIdStrForUserId(Start start, AppIdentifier appIdentifier, String id)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT primary_or_recipe_user_id FROM " + getConfig(start).getUsersTable() +
                " WHERE user_id = ? AND app_id = ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, id);
            pst.setString(2, appIdentifier.getAppId());
        }, result -> {
            if (result.next()) {
                return result.getString("primary_or_recipe_user_id");
            }
            return null;
        });
    }

    public static AuthRecipeUserInfo getPrimaryUserInfoForUserId(Start start, AppIdentifier appIdentifier, String id)
            throws SQLException, StorageQueryException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            return getPrimaryUserInfoForUserId(start, con, appIdentifier, id);
        }
    }

    private static AuthRecipeUserInfo getPrimaryUserInfoForUserId(Start start, Connection con,
                                                                  AppIdentifier appIdentifier, String id)
            throws SQLException, StorageQueryException {
        List<String> ids = new ArrayList<>();
        ids.add(id);
        List<AuthRecipeUserInfo> result = getPrimaryUserInfoForUserIds(start, con, appIdentifier, ids);
        if (result.isEmpty()) {
            return null;
        }
        return result.get(0);
    }

    private static List<AuthRecipeUserInfo> getPrimaryUserInfoForUserIds(Start start,
                                                                         Connection con,
                                                                         AppIdentifier appIdentifier,
                                                                         List<String> userIds)
            throws StorageQueryException, SQLException {
        if (userIds.size() == 0) {
            return new ArrayList<>();
        }

        // We check both user_id and primary_or_recipe_user_id because the input may have a recipe userId
        // which is linked to a primary user ID in which case it won't be in the primary_or_recipe_user_id column,
        // or the input may have a primary user ID whose recipe user ID was removed, so it won't be in the user_id
        // column
        String QUERY = "SELECT * FROM " + getConfig(start).getUsersTable() +
                " WHERE primary_or_recipe_user_id IN (SELECT primary_or_recipe_user_id FROM " +
                getConfig(start).getUsersTable() + " WHERE (user_id IN ("
                + Utils.generateCommaSeperatedQuestionMarks(userIds.size()) +
                ") OR primary_or_recipe_user_id IN (" +
                Utils.generateCommaSeperatedQuestionMarks(userIds.size()) +
                ")) AND app_id = ?) AND app_id = ?";

        List<AllAuthRecipeUsersResultHolder> allAuthUsersResult = execute(con, QUERY, pst -> {
            // IN user_id
            int index = 1;
            for (int i = 0; i < userIds.size(); i++, index++) {
                pst.setString(index, userIds.get(i));
            }
            // IN primary_or_recipe_user_id
            for (int i = 0; i < userIds.size(); i++, index++) {
                pst.setString(index, userIds.get(i));
            }
            // for app_id
            pst.setString(index, appIdentifier.getAppId());
            pst.setString(index + 1, appIdentifier.getAppId());
        }, result -> {
            List<AllAuthRecipeUsersResultHolder> parsedResult = new ArrayList<>();
            while (result.next()) {
                parsedResult.add(new AllAuthRecipeUsersResultHolder(result.getString("user_id"),
                        result.getString("tenant_id"),
                        result.getString("primary_or_recipe_user_id"),
                        result.getBoolean("is_linked_or_is_a_primary_user"),
                        result.getString("recipe_id"),
                        result.getLong("time_joined")));
            }
            return parsedResult;
        });

        // Now we form the userIds again, but based on the user_id in the result from above.
        Set<String> recipeUserIdsToFetch = new HashSet<>();
        for (AllAuthRecipeUsersResultHolder user : allAuthUsersResult) {
            // this will remove duplicate entries wherein a user id is shared across several tenants.
            recipeUserIdsToFetch.add(user.userId);
        }

        List<LoginMethod> loginMethods = new ArrayList<>();
        loginMethods.addAll(
                EmailPasswordQueries.getUsersInfoUsingIdList(start, con, recipeUserIdsToFetch, appIdentifier));
        loginMethods.addAll(ThirdPartyQueries.getUsersInfoUsingIdList(start, con, recipeUserIdsToFetch, appIdentifier));
        loginMethods.addAll(
                PasswordlessQueries.getUsersInfoUsingIdList(start, con, recipeUserIdsToFetch, appIdentifier));

        Map<String, LoginMethod> recipeUserIdToLoginMethodMap = new HashMap<>();
        for (LoginMethod loginMethod : loginMethods) {
            recipeUserIdToLoginMethodMap.put(loginMethod.recipeUserId, loginMethod);
        }

        Map<String, AuthRecipeUserInfo> userIdToAuthRecipeUserInfo = new HashMap<>();

        for (AllAuthRecipeUsersResultHolder authRecipeUsersResultHolder : allAuthUsersResult) {
            String recipeUserId = authRecipeUsersResultHolder.userId;
            LoginMethod loginMethod = recipeUserIdToLoginMethodMap.get(recipeUserId);
            assert (loginMethod != null);
            String primaryUserId = authRecipeUsersResultHolder.primaryOrRecipeUserId;
            AuthRecipeUserInfo curr = userIdToAuthRecipeUserInfo.get(primaryUserId);
            if (curr == null) {
                curr = AuthRecipeUserInfo.create(primaryUserId, authRecipeUsersResultHolder.isLinkedOrIsAPrimaryUser,
                        loginMethod);
            } else {
                curr.addLoginMethod(loginMethod);
            }
            userIdToAuthRecipeUserInfo.put(primaryUserId, curr);
        }

        return userIdToAuthRecipeUserInfo.keySet().stream().map(userIdToAuthRecipeUserInfo::get)
                .collect(Collectors.toList());
    }

    public static String getRecipeIdForUser_Transaction(Start start, Connection sqlCon,
                                                        TenantIdentifier tenantIdentifier, String userId)
            throws SQLException, StorageQueryException {


        String QUERY = "SELECT recipe_id FROM " + getConfig(start).getAppIdToUserIdTable()
                + " WHERE app_id = ? AND user_id = ? FOR UPDATE";
        return execute(sqlCon, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, userId);
        }, result -> {
            if (result.next()) {
                return result.getString("recipe_id");
            }
            return null;
        });
    }

    private static List<AuthRecipeUserInfo> getPrimaryUserInfoForUserIds(Start start,
                                                                         AppIdentifier appIdentifier,
                                                                         List<String> userIds)
            throws StorageQueryException, SQLException {
        if (userIds.size() == 0) {
            return new ArrayList<>();
        }

        try (Connection con = ConnectionPool.getConnection(start)) {
            return getPrimaryUserInfoForUserIds(start, con, appIdentifier, userIds);
        }

    }

    public static Map<String, List<String>> getTenantIdsForUserIds_transaction(Start start, Connection sqlCon,
                                                                               AppIdentifier appIdentifier,
                                                                               String[] userIds)
            throws SQLException, StorageQueryException {
        if (userIds != null && userIds.length > 0) {
            StringBuilder QUERY = new StringBuilder("SELECT user_id, tenant_id "
                    + "FROM " + getConfig(start).getUsersTable());
            QUERY.append(" WHERE user_id IN (");
            for (int i = 0; i < userIds.length; i++) {

                QUERY.append("?");
                if (i != userIds.length - 1) {
                    // not the last element
                    QUERY.append(",");
                }
            }
            QUERY.append(") AND app_id = ?");

            return execute(sqlCon, QUERY.toString(), pst -> {
                for (int i = 0; i < userIds.length; i++) {
                    // i+1 cause this starts with 1 and not 0, and 1 is appId
                    pst.setString(i + 1, userIds[i]);
                }
                pst.setString(userIds.length + 1, appIdentifier.getAppId());
            }, result -> {
                Map<String, List<String>> finalResult = new HashMap<>();
                for (String userId : userIds) {
                    finalResult.put(userId, new ArrayList<>());
                }

                while (result.next()) {
                    String userId = result.getString("user_id").trim();
                    String tenantId = result.getString("tenant_id");

                    finalResult.get(userId).add(tenantId);
                }
                return finalResult;
            });
        }

        return new HashMap<>();
    }

    @TestOnly
    public static String[] getAllTablesInTheDatabase(Start start) throws StorageQueryException, SQLException {
        if (!Start.isTesting) {
            throw new UnsupportedOperationException();
        }
        String QUERY = "SELECT tablename, schemaname FROM pg_catalog.pg_tables "
                + " WHERE schemaname = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, Config.getConfig(start).getTableSchema());
        }, result -> {
            List<String> tableNames = new ArrayList<>();

            while (result.next()) {
                tableNames.add(result.getString("tablename"));
            }
            return tableNames.toArray(new String[0]);
        });
    }

    @TestOnly
    public static String[] getAllTablesInTheDatabaseThatHasDataForAppId(Start start, String appId)
            throws StorageQueryException, SQLException {
        if (!Start.isTesting) {
            throw new UnsupportedOperationException();
        }
        String[] tableNames = getAllTablesInTheDatabase(start);

        List<String> result = new ArrayList<>();
        for (String tableName : tableNames) {
            String QUERY =
                    "SELECT 1 FROM " + Config.getConfig(start).getTableSchema() + "." + tableName + " WHERE app_id = ?";

            boolean hasRows = execute(start, QUERY, pst -> {
                pst.setString(1, appId);
            }, res -> {
                return res.next();
            });
            if (hasRows) {
                result.add(tableName);
            }
        }

        return result.toArray(new String[0]);
    }

    private static class UserInfoPaginationResultHolder {
        String userId;
        String recipeId;

        UserInfoPaginationResultHolder(String userId, String recipeId) {
            this.userId = userId;
            this.recipeId = recipeId;
        }
    }

    private static class AllAuthRecipeUsersResultHolder {
        String userId;
        String tenantId;
        String primaryOrRecipeUserId;
        boolean isLinkedOrIsAPrimaryUser;
        RECIPE_ID recipeId;
        long timeJoined;

        AllAuthRecipeUsersResultHolder(String userId, String tenantId, String primaryOrRecipeUserId,
                                       boolean isLinkedOrIsAPrimaryUser, String recipeId, long timeJoined) {
            this.userId = userId;
            this.tenantId = tenantId;
            this.primaryOrRecipeUserId = primaryOrRecipeUserId;
            this.isLinkedOrIsAPrimaryUser = isLinkedOrIsAPrimaryUser;
            this.recipeId = RECIPE_ID.getEnumFromString(recipeId);
            this.timeJoined = timeJoined;
        }
    }

    private static class KeyValueInfoRowMapper implements RowMapper<KeyValueInfo, ResultSet> {
        public static final KeyValueInfoRowMapper INSTANCE = new KeyValueInfoRowMapper();

        private KeyValueInfoRowMapper() {
        }

        private static KeyValueInfoRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public KeyValueInfo map(ResultSet result) throws Exception {
            return new KeyValueInfo(result.getString("value"), result.getLong("created_at_time"));
        }
    }
}
