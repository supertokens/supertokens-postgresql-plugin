/*
 *    Copyright (c) 2025, VRAI Labs and/or its affiliates. All rights reserved.
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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.supertokens.pluginInterface.ACCOUNT_INFO_TYPE;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.authRecipe.exceptions.AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.sqlStorage.TransactionConnection;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import static io.supertokens.storage.postgresql.config.Config.getConfig;
import io.supertokens.storage.postgresql.utils.Utils;

public class AccountInfoQueries {
    public static void addRecipeUserAccountInfo_Transaction(Start start, Connection sqlCon,
                                                            TenantIdentifier tenantIdentifier, String userId,
                                                            String recipeId, ACCOUNT_INFO_TYPE accountInfoType,
                                                            String thirdPartyId, String thirdPartyUserId,
                                                            String accountInfoValue)
            throws SQLException {
        String QUERY = "INSERT INTO " + getConfig(start).getRecipeUserTenantsTable()
                + "(app_id, recipe_user_id, tenant_id, recipe_id, account_info_type, third_party_id, third_party_user_id, account_info_value)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

        update(sqlCon, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, userId);
            pst.setString(3, tenantIdentifier.getTenantId());
            pst.setString(4, recipeId);
            pst.setString(5, accountInfoType.toString());
            pst.setString(6, thirdPartyId);
            pst.setString(7, thirdPartyUserId);
            pst.setString(8, accountInfoValue);
        });
    }

    static String getQueryToCreateRecipeUserTenantsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getRecipeUserTenantsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "app_id VARCHAR(64) NOT NULL,"
                + "recipe_user_id CHAR(36) NOT NULL,"
                + "tenant_id VARCHAR(64) NOT NULL,"
                + "recipe_id VARCHAR(128) NOT NULL,"
                + "account_info_type VARCHAR(8) NOT NULL,"
                + "account_info_value TEXT NOT NULL,"
                + "third_party_id VARCHAR(28),"
                + "third_party_user_id VARCHAR(256),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, recipe_id, account_info_type, third_party_id, third_party_user_id, account_info_value),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "tenant_id", "fkey")
                + " FOREIGN KEY(app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantsTable() + " (app_id, tenant_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreatePrimaryUserTenantsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getPrimaryUserTenantsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "app_id VARCHAR(64) NOT NULL,"
                + "tenant_id VARCHAR(64) NOT NULL,"
                + "account_info_type VARCHAR(8) NOT NULL,"
                + "account_info_value TEXT NOT NULL,"
                + "primary_user_id CHAR(36) NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, account_info_type, account_info_value)"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreateTenantIndexForRecipeUserTenantsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_tenant ON "
                + Config.getConfig(start).getRecipeUserTenantsTable() + "(app_id, tenant_id);";
    }

    static String getQueryToCreateAccountInfoIndexForRecipeUserTenantsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_account_info ON "
                + Config.getConfig(start).getRecipeUserTenantsTable()
                + "(app_id, tenant_id, account_info_type, third_party_id, account_info_value);";
    }

    static String getQueryToCreatePrimaryUserIndexForPrimaryUserTenantsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS idx_primary_user_tenants_primary ON "
                + Config.getConfig(start).getPrimaryUserTenantsTable() + "(app_id, primary_user_id);";
    }

    public static void addPrimaryUserAccountInfo_Transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier, String userId) throws
            StorageQueryException {
        try {
            String QUERY = "INSERT INTO " + getConfig(start).getPrimaryUserTenantsTable()
                    + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id)"
                    + " SELECT app_id, tenant_id, account_info_type, account_info_value, ?"
                    + " FROM " + getConfig(start).getRecipeUserTenantsTable()
                    + " WHERE app_id = ? AND recipe_user_id = ?";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, userId); // primary_user_id
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, userId); // recipe_user_id
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void checkIfLoginMethodCanBecomePrimary_Transaction(Start start, TransactionConnection con, AppIdentifier appIdentifier, LoginMethod loginMethod)
            throws AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException, StorageQueryException, SQLException {
        Connection sqlCon = (Connection) con.getConnection();
        
        // Build the query dynamically based on which values are not null
        StringBuilder QUERY = new StringBuilder("SELECT primary_user_id, account_info_type FROM " + getConfig(start).getPrimaryUserTenantsTable());
        QUERY.append(" WHERE app_id = ? AND tenant_id IN (");
        
        // Add placeholders for tenant IDs
        List<String> tenantIds = new ArrayList<>(loginMethod.tenantIds);
        for (int i = 0; i < tenantIds.size(); i++) {
            QUERY.append("?");
            if (i != tenantIds.size() - 1) {
                QUERY.append(",");
            }
        }
        QUERY.append(") AND (");
        
        // Build OR conditions for account info types
        List<String> orConditions = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        
        // Add app_id parameter
        parameters.add(appIdentifier.getAppId());
        
        // Add tenant_id parameters
        parameters.addAll(tenantIds);
        
        // Email condition
        if (loginMethod.email != null) {
            orConditions.add("(account_info_type = ? AND account_info_value = ?)");
            parameters.add(ACCOUNT_INFO_TYPE.EMAIL.toString());
            parameters.add(loginMethod.email);
        }
        
        // Phone condition
        if (loginMethod.phoneNumber != null) {
            orConditions.add("(account_info_type = ? AND account_info_value = ?)");
            parameters.add(ACCOUNT_INFO_TYPE.PHONE_NUMBER.toString());
            parameters.add(loginMethod.phoneNumber);
        }
        
        // Third party condition
        if (loginMethod.thirdParty != null) {
            String thirdPartyAccountInfoValue = loginMethod.thirdParty.id + "::" + loginMethod.thirdParty.userId;
            orConditions.add("(account_info_type = ? AND account_info_value = ?)");
            parameters.add(ACCOUNT_INFO_TYPE.THIRD_PARTY.toString());
            parameters.add(thirdPartyAccountInfoValue);
        }
        
        // If no OR conditions, return early (nothing to check)
        if (orConditions.isEmpty()) {
            return;
        }
        
        // Join OR conditions
        for (int i = 0; i < orConditions.size(); i++) {
            QUERY.append(orConditions.get(i));
            if (i != orConditions.size() - 1) {
                QUERY.append(" OR ");
            }
        }
        
        QUERY.append(") LIMIT 1");
        
        String finalQuery = QUERY.toString();
        
        // Execute query and check for results
        String[] result = execute(sqlCon, finalQuery, pst -> {
            for (int i = 0; i < parameters.size(); i++) {
                pst.setObject(i + 1, parameters.get(i));
            }
        }, rs -> {
            if (rs.next()) {
                return new String[]{rs.getString("primary_user_id"), rs.getString("account_info_type")};
            }
            return null;
        });
        
        if (result != null) {
            String primaryUserId = result[0];
            String accountInfoType = result[1];
            
            String message;
            if (ACCOUNT_INFO_TYPE.EMAIL.toString().equals(accountInfoType)) {
                message = "This user's email is already associated with another user ID";
            } else if (ACCOUNT_INFO_TYPE.PHONE_NUMBER.toString().equals(accountInfoType)) {
                message = "This user's phone number is already associated with another user ID";
            } else if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(accountInfoType)) {
                message = "This user's third party login is already associated with another user ID";
            } else {
                message = "Account info is already associated with another primary user";
            }
            
            throw new AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException(primaryUserId, message);
        }
    }
}
