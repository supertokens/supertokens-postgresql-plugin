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
import java.util.Set;

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

    public static void checkIfLoginMethodsCanBeLinked_Transaction(Start start, TransactionConnection con, AppIdentifier appIdentifier, Set<String> tenantIds, Set<String> emails,
                                                                  Set<String> phoneNumbers, Set<LoginMethod.ThirdParty> thirdParties, String primaryUserId) throws AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException, StorageQueryException, SQLException {
        Connection sqlCon = (Connection) con.getConnection();
        
        // If no account info to check, return early
        if ((emails == null || emails.isEmpty()) && 
            (phoneNumbers == null || phoneNumbers.isEmpty()) && 
            (thirdParties == null || thirdParties.isEmpty())) {
            return;
        }
        
        // If no tenant IDs, return early
        if (tenantIds == null || tenantIds.isEmpty()) {
            return;
        }
        
        // Build OR conditions for account info types
        List<String> orConditions = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        
        // Add app_id parameter
        parameters.add(appIdentifier.getAppId());
        
        // Add tenant_id parameters
        List<String> tenantIdsList = new ArrayList<>(tenantIds);
        parameters.addAll(tenantIdsList);
        
        // Add primary_user_id parameter (to exclude)
        parameters.add(primaryUserId);
        
        // Email conditions
        if (emails != null && !emails.isEmpty()) {
            StringBuilder emailCondition = new StringBuilder("(account_info_type = ? AND account_info_value IN (");
            for (int i = 0; i < emails.size(); i++) {
                emailCondition.append("?");
                if (i != emails.size() - 1) {
                    emailCondition.append(",");
                }
            }
            emailCondition.append("))");
            orConditions.add(emailCondition.toString());
            parameters.add(ACCOUNT_INFO_TYPE.EMAIL.toString());
            parameters.addAll(emails);
        }
        
        // Phone number conditions
        if (phoneNumbers != null && !phoneNumbers.isEmpty()) {
            StringBuilder phoneCondition = new StringBuilder("(account_info_type = ? AND account_info_value IN (");
            for (int i = 0; i < phoneNumbers.size(); i++) {
                phoneCondition.append("?");
                if (i != phoneNumbers.size() - 1) {
                    phoneCondition.append(",");
                }
            }
            phoneCondition.append("))");
            orConditions.add(phoneCondition.toString());
            parameters.add(ACCOUNT_INFO_TYPE.PHONE_NUMBER.toString());
            parameters.addAll(phoneNumbers);
        }
        
        // Third party conditions
        if (thirdParties != null && !thirdParties.isEmpty()) {
            List<String> thirdPartyValues = new ArrayList<>();
            for (LoginMethod.ThirdParty tp : thirdParties) {
                thirdPartyValues.add(tp.id + "::" + tp.userId);
            }
            
            StringBuilder thirdPartyCondition = new StringBuilder("(account_info_type = ? AND account_info_value IN (");
            for (int i = 0; i < thirdPartyValues.size(); i++) {
                thirdPartyCondition.append("?");
                if (i != thirdPartyValues.size() - 1) {
                    thirdPartyCondition.append(",");
                }
            }
            thirdPartyCondition.append("))");
            orConditions.add(thirdPartyCondition.toString());
            parameters.add(ACCOUNT_INFO_TYPE.THIRD_PARTY.toString());
            parameters.addAll(thirdPartyValues);
        }
        
        // If no OR conditions, return early (shouldn't happen due to early return above)
        if (orConditions.isEmpty()) {
            return;
        }
        
        // Build the full query
        StringBuilder QUERY = new StringBuilder("SELECT primary_user_id, account_info_type, account_info_value FROM ");
        QUERY.append(getConfig(start).getPrimaryUserTenantsTable());
        QUERY.append(" WHERE app_id = ? AND tenant_id IN (");
        for (int i = 0; i < tenantIdsList.size(); i++) {
            QUERY.append("?");
            if (i != tenantIdsList.size() - 1) {
                QUERY.append(",");
            }
        }
        QUERY.append(") AND primary_user_id != ? AND (");
        
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
                return new String[]{rs.getString("primary_user_id"), rs.getString("account_info_type"), rs.getString("account_info_value")};
            }
            return null;
        });
        
        if (result != null) {
            String conflictingPrimaryUserId = result[0];
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
            
            throw new AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException(conflictingPrimaryUserId, message);
        }
    }

    public static void reserveAccountInfoForLinking_Transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier,
                                                                String recipeUserId, String primaryUserId)
            throws SQLException {
        /*
         * When linking, the primary user's tenant set becomes the union of:
         * - tenants currently associated with the primary user (via primary_user_tenants)
         * - tenants currently associated with the recipe user (via recipe_user_tenants)
         *
         * We reserve account info in primary_user_tenants for the union tenant set by doing two passes:
         * 1) recipe user's distinct account info x primary user's distinct tenants
         * 2) primary user's distinct account info x recipe user's distinct tenants
         *
         * We must not use ON CONFLICT DO NOTHING. Use INSERT ... SELECT ... WHERE NOT EXISTS.
         */

        String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
        String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();

        // 1) recipe user's account info -> all tenants of primary user
        String QUERY_1 = "INSERT INTO " + primaryUserTenantsTable
                + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id)"
                + " SELECT ?, primary_tenants.tenant_id, recipe_ai.account_info_type, recipe_ai.account_info_value, ?"
                + " FROM ("
                + "   SELECT DISTINCT tenant_id FROM " + primaryUserTenantsTable
                + "   WHERE app_id = ? AND primary_user_id = ?"
                + " ) primary_tenants,"
                + " ("
                + "   SELECT DISTINCT account_info_type, account_info_value FROM " + recipeUserTenantsTable
                + "   WHERE app_id = ? AND recipe_user_id = ?"
                + " ) recipe_ai"
                + " WHERE NOT EXISTS ("
                + "   SELECT 1 FROM " + primaryUserTenantsTable + " p"
                + "   WHERE p.app_id = ?"
                + "     AND p.tenant_id = primary_tenants.tenant_id"
                + "     AND p.account_info_type = recipe_ai.account_info_type"
                + "     AND p.account_info_value = recipe_ai.account_info_value"
                + " )";

        update(sqlCon, QUERY_1, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, primaryUserId);
            pst.setString(3, appIdentifier.getAppId());
            pst.setString(4, primaryUserId);
            pst.setString(5, appIdentifier.getAppId());
            pst.setString(6, recipeUserId);
            pst.setString(7, appIdentifier.getAppId());
        });

        // 2) primary user's account info -> all tenants of recipe user
        String QUERY_2 = "INSERT INTO " + primaryUserTenantsTable
                + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id)"
                + " SELECT ?, recipe_tenants.tenant_id, primary_ai.account_info_type, primary_ai.account_info_value, ?"
                + " FROM ("
                + "   SELECT DISTINCT tenant_id FROM " + recipeUserTenantsTable
                + "   WHERE app_id = ? AND recipe_user_id = ?"
                + " ) recipe_tenants,"
                + " ("
                + "   SELECT DISTINCT account_info_type, account_info_value FROM " + primaryUserTenantsTable
                + "   WHERE app_id = ? AND primary_user_id = ?"
                + " ) primary_ai"
                + " WHERE NOT EXISTS ("
                + "   SELECT 1 FROM " + primaryUserTenantsTable + " p"
                + "   WHERE p.app_id = ?"
                + "     AND p.tenant_id = recipe_tenants.tenant_id"
                + "     AND p.account_info_type = primary_ai.account_info_type"
                + "     AND p.account_info_value = primary_ai.account_info_value"
                + " )";

        update(sqlCon, QUERY_2, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, primaryUserId);
            pst.setString(3, appIdentifier.getAppId());
            pst.setString(4, recipeUserId);
            pst.setString(5, appIdentifier.getAppId());
            pst.setString(6, primaryUserId);
            pst.setString(7, appIdentifier.getAppId());
        });
    }

    public static void addTenantIdToRecipeUser_Transaction(Start start, Connection sqlCon, TenantIdentifier tenantIdentifier, String userId) throws StorageQueryException {
        try {
            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();

            /*
             * Duplicate all existing recipe_user_tenants rows for this recipe user into the new tenant.
             *
             * If the recipe user is already associated with this tenant (i.e. any row exists for (app_id, tenant_id, recipe_user_id)),
             * then do nothing.
             *
             * NOTE: We intentionally do NOT use "ON CONFLICT DO NOTHING" here because the table's primary key does not include
             * recipe_user_id, so ON CONFLICT could hide genuine collisions (e.g. account info already belongs to another user).
             */
            String QUERY = "INSERT INTO " + recipeUserTenantsTable
                    + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type, third_party_id, third_party_user_id, account_info_value)"
                    + " SELECT DISTINCT r.app_id, r.recipe_user_id, ?, r.recipe_id, r.account_info_type, r.third_party_id, r.third_party_user_id, r.account_info_value"
                    + " FROM " + recipeUserTenantsTable + " r"
                    + " WHERE r.app_id = ? AND r.recipe_user_id = ? AND r.tenant_id <> ?"
                    + "   AND NOT EXISTS ("
                    + "     SELECT 1 FROM " + recipeUserTenantsTable + " e"
                    + "     WHERE e.app_id = ? AND e.recipe_user_id = ? AND e.tenant_id = ?"
                    + "   )";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getTenantId());
                pst.setString(2, tenantIdentifier.getAppId());
                pst.setString(3, userId);
                pst.setString(4, tenantIdentifier.getTenantId());
                pst.setString(5, tenantIdentifier.getAppId());
                pst.setString(6, userId);
                pst.setString(7, tenantIdentifier.getTenantId());
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void addTenantIdToPrimaryUser_Transaction(Start start, TransactionConnection con, TenantIdentifier tenantIdentifier, String supertokensUserId) throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();

            /*
             * Duplicate all existing primary_user_tenants rows for this primary user into the new tenant.
             *
             * If the primary user is already associated with this tenant (i.e. any row exists for (app_id, tenant_id, primary_user_id)),
             * then do nothing.
             *
             * NOTE: We intentionally do NOT use "ON CONFLICT DO NOTHING" here because the table's primary key does not include
             * primary_user_id, so ON CONFLICT could hide genuine collisions (e.g. account info already belongs to another primary user).
             */
            String QUERY = "INSERT INTO " + primaryUserTenantsTable
                    + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id)"
                    + " SELECT DISTINCT p.app_id, ?, p.account_info_type, p.account_info_value, ?"
                    + " FROM " + primaryUserTenantsTable + " p"
                    + " WHERE p.app_id = ? AND p.primary_user_id = ? AND p.tenant_id <> ?"
                    + "   AND NOT EXISTS ("
                    + "     SELECT 1 FROM " + primaryUserTenantsTable + " e"
                    + "     WHERE e.app_id = ? AND e.primary_user_id = ? AND e.tenant_id = ?"
                    + "   )";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getTenantId());
                pst.setString(2, supertokensUserId);
                pst.setString(3, tenantIdentifier.getAppId());
                pst.setString(4, supertokensUserId);
                pst.setString(5, tenantIdentifier.getTenantId());
                pst.setString(6, tenantIdentifier.getAppId());
                pst.setString(7, supertokensUserId);
                pst.setString(8, tenantIdentifier.getTenantId());
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void removeAccountInfoForRecipeUser_Transaction(Start start, Connection sqlCon, TenantIdentifier tenantIdentifier, String userId) throws StorageQueryException {
        try {
            String QUERY = "DELETE FROM " + getConfig(start).getRecipeUserTenantsTable()
                    + " WHERE app_id = ? AND tenant_id = ? AND recipe_user_id = ?";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, userId);
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void removeAccountInfoForPrimaryUserIfNecessary_Transaction(Start start, Connection sqlCon, TenantIdentifier tenantIdentifier, String userId) throws StorageQueryException {
        try {
            // If this recipe user is not linked / not a primary user, there is no entry in primary_user_tenants to clean up.
            String appIdToUserIdTable = getConfig(start).getAppIdToUserIdTable();
            String[] linkingInfo = execute(sqlCon,
                    "SELECT primary_or_recipe_user_id, is_linked_or_is_a_primary_user FROM " + appIdToUserIdTable
                            + " WHERE app_id = ? AND user_id = ?",
                    pst -> {
                        pst.setString(1, tenantIdentifier.getAppId());
                        pst.setString(2, userId);
                    },
                    rs -> {
                        if (!rs.next()) {
                            return null;
                        }
                        return new String[]{
                                rs.getString("primary_or_recipe_user_id"),
                                String.valueOf(rs.getBoolean("is_linked_or_is_a_primary_user"))
                        };
                    });

            if (linkingInfo == null) {
                return;
            }

            String primaryUserId = linkingInfo[0];
            boolean isLinkedOrPrimary = Boolean.parseBoolean(linkingInfo[1]);
            if (!isLinkedOrPrimary) {
                return;
            }

            /*
             * Remove account info rows for this primary user in the tenant if (and only if) there is no
             * linked recipe user (including the primary user itself) that still has the same account info in
             * recipe_user_tenants for this tenant.
             */
            String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();

            String QUERY = "DELETE FROM " + primaryUserTenantsTable + " p"
                    + " WHERE p.app_id = ? AND p.tenant_id = ? AND p.primary_user_id = ?"
                    + "   AND NOT EXISTS ("
                    + "     SELECT 1"
                    + "     FROM " + recipeUserTenantsTable + " r"
                    + "     JOIN " + appIdToUserIdTable + " a"
                    + "       ON a.app_id = r.app_id AND a.user_id = r.recipe_user_id"
                    + "     WHERE r.app_id = p.app_id"
                    + "       AND r.tenant_id = p.tenant_id"
                    + "       AND r.account_info_type = p.account_info_type"
                    + "       AND r.account_info_value = p.account_info_value"
                    + "       AND a.primary_or_recipe_user_id = ?"
                    + "       AND a.is_linked_or_is_a_primary_user = true"
                    + "   )";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, primaryUserId);
                pst.setString(4, primaryUserId);
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }
}
