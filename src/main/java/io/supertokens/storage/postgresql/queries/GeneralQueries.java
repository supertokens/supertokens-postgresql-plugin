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
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.supertokens.storage.postgresql.PreparedStatementValueSetter.NO_OP_SETTER;
import static io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.CREATING_NEW_TABLE;
import static io.supertokens.storage.postgresql.ProcessState.getInstance;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;
import static io.supertokens.storage.postgresql.queries.EmailPasswordQueries.getQueryToCreatePasswordResetTokenExpiryIndex;
import static io.supertokens.storage.postgresql.queries.EmailPasswordQueries.getQueryToCreatePasswordResetTokensTable;
import static io.supertokens.storage.postgresql.queries.EmailVerificationQueries.*;
import static io.supertokens.storage.postgresql.queries.JWTSigningQueries.getQueryToCreateJWTSigningTable;
import static io.supertokens.storage.postgresql.queries.PasswordlessQueries.*;
import static io.supertokens.storage.postgresql.queries.SessionQueries.getQueryToCreateAccessTokenSigningKeysTable;
import static io.supertokens.storage.postgresql.queries.SessionQueries.getQueryToCreateSessionInfoTable;
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
                + "recipe_id VARCHAR(128) NOT NULL,"
                + "time_joined BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, usersTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, user_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, usersTable, "tenant_id", "fkey")
                + " FOREIGN KEY(app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantsTable() +  " (app_id, tenant_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, usersTable, "user_id", "fkey")
                + " FOREIGN KEY(app_id, user_id)"
                + " REFERENCES " + Config.getConfig(start).getAppIdToUserIdTable() +  " (app_id, user_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreateUserPaginationIndex(Start start) {
        return "CREATE INDEX all_auth_recipe_users_pagination_index ON " + Config.getConfig(start).getUsersTable()
                + "(time_joined DESC, user_id DESC, tenant_id DESC, app_id DESC);";
    }

    private static String getQueryToCreateAppsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String appsTable = Config.getConfig(start).getAppsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + appsTable + " ("
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "created_at_time BIGINT ,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, appsTable, null, "pkey") + " PRIMARY KEY(app_id)" +
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
                + " REFERENCES " + Config.getConfig(start).getTenantsTable() +  " (app_id, tenant_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    private static String getQueryToCreateAppIdToUserIdTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String appToUserTable = Config.getConfig(start).getAppIdToUserIdTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + appToUserTable + " ("
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "user_id CHAR(36) NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, appToUserTable, null, "pkey")
                + " PRIMARY KEY (app_id, user_id), "
                + "CONSTRAINT " + Utils.getConstraintName(schema, appToUserTable, "app_id", "fkey")
                + " FOREIGN KEY(app_id) REFERENCES " + Config.getConfig(start).getAppsTable()
                + " (app_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
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
                }

                if (!doesTableExists(start, Config.getConfig(start).getKeyValueTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateKeyValueTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getAppIdToUserIdTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateAppIdToUserIdTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getUsersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateUsersTable(start), NO_OP_SETTER);

                    // index
                    update(start, getQueryToCreateUserPaginationIndex(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getAccessTokenSigningKeysTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateAccessTokenSigningKeysTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getSessionInfoTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateSessionInfoTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getTenantConfigsTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, MultitenancyQueries.getQueryToCreateTenantConfigsTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getTenantThirdPartyProvidersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, MultitenancyQueries.getQueryToCreateTenantThirdPartyProvidersTable(start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getTenantThirdPartyProviderClientsTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, MultitenancyQueries.getQueryToCreateTenantThirdPartyProviderClientsTable(start),
                            NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getEmailPasswordUsersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, EmailPasswordQueries.getQueryToCreateUsersTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getEmailPasswordUserToTenantTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, EmailPasswordQueries.getQueryToCreateEmailPasswordUserToTenantTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getPasswordResetTokensTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreatePasswordResetTokensTable(start), NO_OP_SETTER);
                    // index
                    update(start, getQueryToCreatePasswordResetTokenExpiryIndex(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getEmailVerificationTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateEmailVerificationTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getEmailVerificationTokensTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateEmailVerificationTokensTable(start), NO_OP_SETTER);
                    // index
                    update(start, getQueryToCreateEmailVerificationTokenExpiryIndex(start), NO_OP_SETTER);
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
                }

                if (!doesTableExists(start, Config.getConfig(start).getPasswordlessUsersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, PasswordlessQueries.getQueryToCreateUsersTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getPasswordlessUserToTenantTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, PasswordlessQueries.getQueryToCreatePasswordlessUserToTenantTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getPasswordlessDevicesTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, getQueryToCreateDevicesTable(start), NO_OP_SETTER);
                    // index
                    update(start, getQueryToCreateDeviceEmailIndex(start), NO_OP_SETTER);
                    update(start, getQueryToCreateDevicePhoneNumberIndex(start), NO_OP_SETTER);
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
                }

                if (!doesTableExists(start, Config.getConfig(start).getRolesTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, UserRolesQueries.getQueryToCreateRolesTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getUserRolesPermissionsTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, UserRolesQueries.getQueryToCreateRolePermissionsTable(start), NO_OP_SETTER);
                    // index
                    update(start, UserRolesQueries.getQueryToCreateRolePermissionsPermissionIndex(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getUserRolesTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, UserRolesQueries.getQueryToCreateUserRolesTable(start), NO_OP_SETTER);
                    // index
                    update(start, UserRolesQueries.getQueryToCreateUserRolesRoleIndex(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getUserIdMappingTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, UserIdMappingQueries.getQueryToCreateUserIdMappingTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getDashboardUsersTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, DashboardQueries.getQueryToCreateDashboardUsersTable(start), NO_OP_SETTER);
                }

                if (!doesTableExists(start, Config.getConfig(start).getDashboardSessionsTable())) {
                    getInstance(start).addState(CREATING_NEW_TABLE, null);
                    update(start, DashboardQueries.getQueryToCreateDashboardUserSessionsTable(start), NO_OP_SETTER);
                    // index
                    update(start, DashboardQueries.getQueryToCreateDashboardUserSessionsExpiryIndex(start),
                            NO_OP_SETTER);
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
                    + getConfig(start).getDashboardSessionsTable();
            update(start, DROP_QUERY, NO_OP_SETTER);
        }
    }

    public static void setKeyValue_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier, String key, KeyValueInfo info)
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

    public static KeyValueInfo getKeyValue(Start start, TenantIdentifier tenantIdentifier, String key) throws SQLException, StorageQueryException {
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

    public static KeyValueInfo getKeyValue_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier, String key)
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

    public static void deleteKeyValue_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier, String key)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getKeyValueTable()
                + " WHERE app_id = ? AND tenant_id = ? AND name = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, key);
        });
    }

    public static long getUsersCount(Start start, RECIPE_ID[] includeRecipeIds)
            throws SQLException, StorageQueryException {
        StringBuilder QUERY = new StringBuilder("SELECT COUNT(*) as total FROM " + getConfig(start).getUsersTable());
        if (includeRecipeIds != null && includeRecipeIds.length > 0) {
            QUERY.append(" WHERE recipe_id IN (");
            for (int i = 0; i < includeRecipeIds.length; i++) {
                String recipeId = includeRecipeIds[i].toString();
                QUERY.append("'").append(recipeId).append("'");
                if (i != includeRecipeIds.length - 1) {
                    // not the last element
                    QUERY.append(",");
                }
            }
            QUERY.append(")");
        }

        return execute(start, QUERY.toString(), NO_OP_SETTER, result -> {
            if (result.next()) {
                return result.getLong("total");
            }
            return 0L;
        });
    }

    public static boolean doesUserIdExist(Start start, String userId) throws SQLException, StorageQueryException {

        String QUERY = "SELECT 1 FROM " + getConfig(start).getUsersTable() + " WHERE user_id = ?";
        return execute(start, QUERY, pst -> pst.setString(1, userId), ResultSet::next);

    }

    public static AuthRecipeUserInfo[] getUsers(Start start, TenantIdentifier tenantIdentifier, @NotNull Integer limit, @NotNull String timeJoinedOrder,
                                                @Nullable RECIPE_ID[] includeRecipeIds, @Nullable String userId,
                                                @Nullable Long timeJoined)
            throws SQLException, StorageQueryException {

        // This list will be used to keep track of the result's order from the db
        List<UserInfoPaginationResultHolder> usersFromQuery;

        {
            StringBuilder RECIPE_ID_CONDITION = new StringBuilder();
            if (includeRecipeIds != null && includeRecipeIds.length > 0) {
                RECIPE_ID_CONDITION.append("recipe_id IN (");
                for (int i = 0; i < includeRecipeIds.length; i++) {
                    String recipeId = includeRecipeIds[i].toString();
                    RECIPE_ID_CONDITION.append("'").append(recipeId).append("'");
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
                String QUERY = "SELECT user_id, recipe_id FROM " + getConfig(start).getUsersTable() + " WHERE "
                        + recipeIdCondition + " (time_joined " + timeJoinedOrderSymbol
                        + " ? OR (time_joined = ? AND user_id <= ?)) ORDER BY time_joined " + timeJoinedOrder
                        + ", user_id DESC LIMIT ?";
                usersFromQuery = execute(start, QUERY, pst -> {
                    pst.setLong(1, timeJoined);
                    pst.setLong(2, timeJoined);
                    pst.setString(3, userId);
                    pst.setInt(4, limit);
                }, result -> {
                    List<UserInfoPaginationResultHolder> temp = new ArrayList<>();
                    while (result.next()) {
                        temp.add(new UserInfoPaginationResultHolder(result.getString("user_id"),
                                result.getString("recipe_id")));
                    }
                    return temp;
                });
            } else {
                String recipeIdCondition = RECIPE_ID_CONDITION.toString();
                if (!recipeIdCondition.equals("")) {
                    recipeIdCondition = " WHERE " + recipeIdCondition;
                }
                String QUERY = "SELECT user_id, recipe_id FROM " + getConfig(start).getUsersTable() + recipeIdCondition
                        + " ORDER BY time_joined " + timeJoinedOrder + ", user_id DESC LIMIT ?";
                usersFromQuery = execute(start, QUERY, pst -> pst.setInt(1, limit), result -> {
                    List<UserInfoPaginationResultHolder> temp = new ArrayList<>();
                    while (result.next()) {
                        temp.add(new UserInfoPaginationResultHolder(result.getString("user_id"),
                                result.getString("recipe_id")));
                    }
                    return temp;
                });
            }
        }

        // we create a map from recipe ID -> userId[]
        Map<RECIPE_ID, List<String>> recipeIdToUserIdListMap = new HashMap<>();
        for (UserInfoPaginationResultHolder user : usersFromQuery) {
            RECIPE_ID recipeId = RECIPE_ID.getEnumFromString(user.recipeId);
            if (recipeId == null) {
                throw new SQLException("Unrecognised recipe ID in database: " + user.recipeId);
            }
            List<String> userIdList = recipeIdToUserIdListMap.get(recipeId);
            if (userIdList == null) {
                userIdList = new ArrayList<>();
            }
            userIdList.add(user.userId);
            recipeIdToUserIdListMap.put(recipeId, userIdList);
        }

        AuthRecipeUserInfo[] finalResult = new AuthRecipeUserInfo[usersFromQuery.size()];

        // we give the userId[] for each recipe to fetch all those user's details
        for (RECIPE_ID recipeId : recipeIdToUserIdListMap.keySet()) {
            List<? extends AuthRecipeUserInfo> users = getUserInfoForRecipeIdFromUserIds(start,
                    tenantIdentifier, recipeId, recipeIdToUserIdListMap.get(recipeId));

            // we fill in all the slots in finalResult based on their position in
            // usersFromQuery
            Map<String, AuthRecipeUserInfo> userIdToInfoMap = new HashMap<>();
            for (AuthRecipeUserInfo user : users) {
                userIdToInfoMap.put(user.id, user);
            }
            for (int i = 0; i < usersFromQuery.size(); i++) {
                if (finalResult[i] == null) {
                    finalResult[i] = userIdToInfoMap.get(usersFromQuery.get(i).userId);
                }
            }
        }

        return finalResult;
    }

    private static List<? extends AuthRecipeUserInfo> getUserInfoForRecipeIdFromUserIds(Start start, TenantIdentifier tenantIdentifier, RECIPE_ID recipeId,
                                                                                        List<String> userIds)
            throws StorageQueryException, SQLException {
        if (recipeId == RECIPE_ID.EMAIL_PASSWORD) {
            return EmailPasswordQueries.getUsersInfoUsingIdList(start, userIds);
        } else if (recipeId == RECIPE_ID.THIRD_PARTY) {
            return ThirdPartyQueries.getUsersInfoUsingIdList(start, userIds);
        } else if (recipeId == RECIPE_ID.PASSWORDLESS) {
            return PasswordlessQueries.getUsersByIdList(start, userIds);
        } else {
            throw new IllegalArgumentException("No implementation of get users for recipe: " + recipeId.toString());
        }
    }

    private static class UserInfoPaginationResultHolder {
        String userId;
        String recipeId;

        UserInfoPaginationResultHolder(String userId, String recipeId) {
            this.userId = userId;
            this.recipeId = recipeId;
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
