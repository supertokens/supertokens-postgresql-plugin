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
import java.util.Map;
import java.util.Set;

import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import io.supertokens.pluginInterface.ACCOUNT_INFO_TYPE;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.authRecipe.exceptions.AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException;
import io.supertokens.pluginInterface.authRecipe.exceptions.AnotherPrimaryUserWithEmailAlreadyExistsException;
import io.supertokens.pluginInterface.authRecipe.exceptions.AnotherPrimaryUserWithPhoneNumberAlreadyExistsException;
import io.supertokens.pluginInterface.authRecipe.exceptions.AnotherPrimaryUserWithThirdPartyInfoAlreadyExistsException;
import io.supertokens.pluginInterface.authRecipe.exceptions.EmailChangeNotAllowedException;
import io.supertokens.pluginInterface.authRecipe.exceptions.PhoneNumberChangeNotAllowedException;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicateEmailException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.passwordless.exception.DuplicatePhoneNumberException;
import io.supertokens.pluginInterface.sqlStorage.TransactionConnection;
import io.supertokens.pluginInterface.thirdparty.exception.DuplicateThirdPartyUserException;
import io.supertokens.storage.postgresql.PreparedStatementValueSetter;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.executeBatch;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import static io.supertokens.storage.postgresql.config.Config.getConfig;
import io.supertokens.storage.postgresql.utils.Utils;

public class AccountInfoQueries {
    private static boolean isPrimaryKeyError(ServerErrorMessage serverMessage, String tableName) {
        if (serverMessage == null || tableName == null) {
            return false;
        }
        String[] tableNameParts = tableName.split("\\.");
        tableName = tableNameParts[tableNameParts.length - 1];
        return "23505".equals(serverMessage.getSQLState()) && serverMessage.getConstraint() != null
                && serverMessage.getConstraint().equals(tableName + "_pkey");
    }

    private static void throwAccountInfoChangeNotAllowed(ACCOUNT_INFO_TYPE accountInfoType)
            throws EmailChangeNotAllowedException, PhoneNumberChangeNotAllowedException {
        if (ACCOUNT_INFO_TYPE.EMAIL.equals(accountInfoType)) {
            throw new EmailChangeNotAllowedException();
        }
        if (ACCOUNT_INFO_TYPE.PHONE_NUMBER.equals(accountInfoType)) {
            throw new PhoneNumberChangeNotAllowedException();
        }
        throw new IllegalArgumentException(
                "updateAccountInfo_Transaction should only be called with accountInfoType EMAIL or PHONE_NUMBER");
    }

    private static String[] getPrimaryUserTenantsConflictForAddTenant(Connection sqlCon, String primaryUserTenantsTable,
                                                                      TenantIdentifier tenantIdentifier, String supertokensUserId) throws SQLException, StorageQueryException {
        return execute(sqlCon,
                "SELECT e.primary_user_id, e.account_info_type FROM " + primaryUserTenantsTable + " e"
                        + " WHERE e.app_id = ? AND e.tenant_id = ? AND e.primary_user_id <> ?"
                        + "   AND EXISTS ("
                        + "     SELECT 1 FROM " + primaryUserTenantsTable + " p"
                        + "     WHERE p.app_id = ? AND p.primary_user_id = ? AND p.tenant_id <> ?"
                        + "       AND p.account_info_type = e.account_info_type"
                        + "       AND p.account_info_value = e.account_info_value"
                        + "   )"
                        + "   AND NOT EXISTS ("
                        + "     SELECT 1 FROM " + primaryUserTenantsTable + " already"
                        + "     WHERE already.app_id = ? AND already.primary_user_id = ? AND already.tenant_id = ?"
                        + "   )",
                pst -> {
                    pst.setString(1, tenantIdentifier.getAppId());
                    pst.setString(2, tenantIdentifier.getTenantId());
                    pst.setString(3, supertokensUserId);
                    pst.setString(4, tenantIdentifier.getAppId());
                    pst.setString(5, supertokensUserId);
                    pst.setString(6, tenantIdentifier.getTenantId());
                    pst.setString(7, tenantIdentifier.getAppId());
                    pst.setString(8, supertokensUserId);
                    pst.setString(9, tenantIdentifier.getTenantId());
                },
                rs -> {
                    String[] firstConflict = null;
                    while (rs.next()) {
                        String[] conflict = new String[]{rs.getString("primary_user_id"), rs.getString("account_info_type")};
                        if (firstConflict == null) {
                            firstConflict = conflict;
                        }
                        if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(conflict[1])) {
                            return conflict;
                        }
                    }
                    return firstConflict;
                });
    }

    private static String getRecipeUserTenantsConflictTypeForAddTenant(Connection sqlCon, String recipeUserTenantsTable,
                                                                       TenantIdentifier tenantIdentifier, String userId) throws SQLException, StorageQueryException {
        return execute(sqlCon,
                "SELECT e.account_info_type"
                        + " FROM " + recipeUserTenantsTable + " e"
                        + " WHERE e.app_id = ? AND e.tenant_id = ? AND e.recipe_user_id <> ?"
                        + "   AND EXISTS ("
                        + "     SELECT 1 FROM " + recipeUserTenantsTable + " r"
                        + "     WHERE r.app_id = ? AND r.recipe_user_id = ? AND r.tenant_id <> ?"
                        + "       AND r.recipe_id = e.recipe_id"
                        + "       AND r.account_info_type = e.account_info_type"
                        + "       AND r.account_info_value = e.account_info_value"
                        + "       AND r.third_party_id IS NOT DISTINCT FROM e.third_party_id"
                        + "       AND r.third_party_user_id IS NOT DISTINCT FROM e.third_party_user_id"
                        + "   )"
                        + "   AND NOT EXISTS ("
                        + "     SELECT 1 FROM " + recipeUserTenantsTable + " already"
                        + "     WHERE already.app_id = ? AND already.recipe_user_id = ? AND already.tenant_id = ?"
                        + "   )",
                pst -> {
                    pst.setString(1, tenantIdentifier.getAppId());
                    pst.setString(2, tenantIdentifier.getTenantId());
                    pst.setString(3, userId);
                    pst.setString(4, tenantIdentifier.getAppId());
                    pst.setString(5, userId);
                    pst.setString(6, tenantIdentifier.getTenantId());
                    pst.setString(7, tenantIdentifier.getAppId());
                    pst.setString(8, userId);
                    pst.setString(9, tenantIdentifier.getTenantId());
                },
                rs -> {
                    String firstConflictType = null;
                    while (rs.next()) {
                        String conflictType = rs.getString("account_info_type");
                        if (firstConflictType == null) {
                            firstConflictType = conflictType;
                        }
                        if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(conflictType)) {
                            return conflictType;
                        }
                    }
                    return firstConflictType;
                });
    }

    private static void throwPrimaryUserTenantsConflict(String[] conflict)
            throws AnotherPrimaryUserWithPhoneNumberAlreadyExistsException,
            AnotherPrimaryUserWithEmailAlreadyExistsException,
            AnotherPrimaryUserWithThirdPartyInfoAlreadyExistsException {
        if (conflict == null) {
            return;
        }
        String conflictingPrimaryUserId = conflict[0];
        String accountInfoType = conflict[1];

        if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(accountInfoType)) {
            throw new AnotherPrimaryUserWithThirdPartyInfoAlreadyExistsException(conflictingPrimaryUserId);
        }

        if (ACCOUNT_INFO_TYPE.EMAIL.toString().equals(accountInfoType)) {
            throw new AnotherPrimaryUserWithEmailAlreadyExistsException(conflictingPrimaryUserId);
        }

        if (ACCOUNT_INFO_TYPE.PHONE_NUMBER.toString().equals(accountInfoType)) {
            throw new AnotherPrimaryUserWithPhoneNumberAlreadyExistsException(conflictingPrimaryUserId);
        }
    }

    private static void throwRecipeUserTenantsConflict(String accountInfoType)
            throws DuplicateEmailException, DuplicatePhoneNumberException, DuplicateThirdPartyUserException {
        if (accountInfoType == null) {
            return;
        }
        if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(accountInfoType)) {
            throw new DuplicateThirdPartyUserException();
        }
        if (ACCOUNT_INFO_TYPE.EMAIL.toString().equals(accountInfoType)) {
            throw new DuplicateEmailException();
        }
        if (ACCOUNT_INFO_TYPE.PHONE_NUMBER.toString().equals(accountInfoType)) {
            throw new DuplicatePhoneNumberException();
        }
    }

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
                + " PRIMARY KEY (app_id, tenant_id, account_info_type, account_info_value),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "app_id", "fkey")
                + " FOREIGN KEY(app_id)"
                + " REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreateTenantIndexForRecipeUserTenantsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_tenant ON "
                + Config.getConfig(start).getRecipeUserTenantsTable() + "(app_id, tenant_id);";
    }

    static String getQueryToCreateRecipeUserIdIndexForRecipeUserTenantsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_recipe_user_id ON "
                + Config.getConfig(start).getRecipeUserTenantsTable() + "(recipe_user_id);";
    }

    static String getQueryToCreateAccountInfoIndexForRecipeUserTenantsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_account_info ON "
                + Config.getConfig(start).getRecipeUserTenantsTable()
                + "(app_id, tenant_id, account_info_type, third_party_id, account_info_value);";
    }

    static String getQueryToCreatePrimaryUserIndexForPrimaryUserTenantsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS idx_primary_user_tenants_primary ON "
                + Config.getConfig(start).getPrimaryUserTenantsTable() + "(primary_user_id);";
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

    public static void addPrimaryUserAccountInfoForUsers_Transaction(Start start, Connection sqlCon,
                                                                     AppIdentifier appIdentifier,
                                                                     List<String> userIds)
            throws StorageQueryException {
        if (userIds == null || userIds.isEmpty()) {
            return;
        }

        try {
            // primary_user_id == recipe_user_id when making a recipe user primary
            StringBuilder query = new StringBuilder("INSERT INTO " + getConfig(start).getPrimaryUserTenantsTable()
                    + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id)"
                    + " SELECT app_id, tenant_id, account_info_type, account_info_value, recipe_user_id"
                    + " FROM " + getConfig(start).getRecipeUserTenantsTable()
                    + " WHERE app_id = ? AND recipe_user_id IN (");

            for (int i = 0; i < userIds.size(); i++) {
                query.append("?");
                if (i != userIds.size() - 1) {
                    query.append(",");
                }
            }
            query.append(")");

            update(sqlCon, query.toString(), pst -> {
                pst.setString(1, appIdentifier.getAppId());
                for (int i = 0; i < userIds.size(); i++) {
                    pst.setString(i + 2, userIds.get(i));
                }
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
        QUERY.append(" WHERE app_id = ?");
        
        // Add placeholders for tenant IDs only if present
        List<String> tenantIds = new ArrayList<>(loginMethod.tenantIds);
        if (!tenantIds.isEmpty()) {
            QUERY.append(" AND tenant_id IN (");
            for (int i = 0; i < tenantIds.size(); i++) {
                QUERY.append("?");
                if (i != tenantIds.size() - 1) {
                    QUERY.append(",");
                }
            }
            QUERY.append(")");
        }
        QUERY.append(" AND (");
        
        // Build OR conditions for account info types
        List<String> orConditions = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        
        // Add app_id parameter
        parameters.add(appIdentifier.getAppId());
        
        // Add tenant_id parameters only if we add tenant_id filter to the query
        if (!tenantIds.isEmpty()) {
            parameters.addAll(tenantIds);
        }
        
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
        
        // Build OR conditions for account info types
        List<String> orConditions = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        
        // Add app_id parameter
        parameters.add(appIdentifier.getAppId());
        
        List<String> tenantIdsList = tenantIds == null ? new ArrayList<>() : new ArrayList<>(tenantIds);
        // Add tenant_id parameters only if we add tenant_id filter to the query
        if (!tenantIdsList.isEmpty()) {
            parameters.addAll(tenantIdsList);
        }
        
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
        QUERY.append(" WHERE app_id = ?");
        if (!tenantIdsList.isEmpty()) {
            QUERY.append(" AND tenant_id IN (");
            for (int i = 0; i < tenantIdsList.size(); i++) {
                QUERY.append("?");
                if (i != tenantIdsList.size() - 1) {
                    QUERY.append(",");
                }
            }
            QUERY.append(")");
        }
        QUERY.append(" AND primary_user_id != ? AND (");
        
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
                + "     AND p.primary_user_id = ?"
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
            pst.setString(8, primaryUserId);
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
                + "     AND p.primary_user_id = ?"
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
            pst.setString(8, primaryUserId);
        });
    }

    public static void reserveAccountInfoForLinkingMultiple_Transaction(Start start, Connection sqlCon,
                                                                        AppIdentifier appIdentifier,
                                                                        Map<String, String> recipeUserIdToPrimaryUserId)
            throws SQLException, StorageQueryException {
        if (recipeUserIdToPrimaryUserId == null || recipeUserIdToPrimaryUserId.isEmpty()) {
            return;
        }

        String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
        String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();

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
                + "     AND p.primary_user_id = ?"
                + "     AND p.tenant_id = primary_tenants.tenant_id"
                + "     AND p.account_info_type = recipe_ai.account_info_type"
                + "     AND p.account_info_value = recipe_ai.account_info_value"
                + " )";

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
                + "     AND p.primary_user_id = ?"
                + "     AND p.tenant_id = recipe_tenants.tenant_id"
                + "     AND p.account_info_type = primary_ai.account_info_type"
                + "     AND p.account_info_value = primary_ai.account_info_value"
                + " )";

        List<PreparedStatementValueSetter> query1Setters = new ArrayList<>();
        List<PreparedStatementValueSetter> query2Setters = new ArrayList<>();

        for (Map.Entry<String, String> entry : recipeUserIdToPrimaryUserId.entrySet()) {
            String recipeUserId = entry.getKey();
            String primaryUserId = entry.getValue();

            query1Setters.add(pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, primaryUserId);
                pst.setString(3, appIdentifier.getAppId());
                pst.setString(4, primaryUserId);
                pst.setString(5, appIdentifier.getAppId());
                pst.setString(6, recipeUserId);
                pst.setString(7, appIdentifier.getAppId());
                pst.setString(8, primaryUserId);
            });

            query2Setters.add(pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, primaryUserId);
                pst.setString(3, appIdentifier.getAppId());
                pst.setString(4, recipeUserId);
                pst.setString(5, appIdentifier.getAppId());
                pst.setString(6, primaryUserId);
                pst.setString(7, appIdentifier.getAppId());
                pst.setString(8, primaryUserId);
            });
        }

        executeBatch(sqlCon, QUERY_1, query1Setters);
        executeBatch(sqlCon, QUERY_2, query2Setters);
    }

    public static void addTenantIdToRecipeUser_Transaction(Start start, Connection sqlCon, TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException, DuplicateEmailException, DuplicateThirdPartyUserException, DuplicatePhoneNumberException {
        String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();

        // Pre-check conflicts before attempting the INSERT
        try {
            String accountInfoType = getRecipeUserTenantsConflictTypeForAddTenant(sqlCon, recipeUserTenantsTable, tenantIdentifier, userId);
            throwRecipeUserTenantsConflict(accountInfoType);
        } catch (SQLException lookupError) {
            throw new StorageQueryException(lookupError);
        }

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

        try {
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

    public static void addTenantIdToPrimaryUser_Transaction(Start start, TransactionConnection con, TenantIdentifier tenantIdentifier, String supertokensUserId)
            throws StorageQueryException,
            AnotherPrimaryUserWithPhoneNumberAlreadyExistsException,
            AnotherPrimaryUserWithEmailAlreadyExistsException,
            AnotherPrimaryUserWithThirdPartyInfoAlreadyExistsException {
        Connection sqlCon = (Connection) con.getConnection();
        String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();

        // Pre-check conflicts before attempting the INSERT
        try {
            String[] conflict = getPrimaryUserTenantsConflictForAddTenant(sqlCon, primaryUserTenantsTable, tenantIdentifier, supertokensUserId);
            throwPrimaryUserTenantsConflict(conflict);
        } catch (SQLException lookupError) {
            throw new StorageQueryException(lookupError);
        }

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

        try {
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

            // 1. Remove account info that is not contributed by any other linked user.
            String QUERY_1 = "DELETE FROM " + primaryUserTenantsTable + " p"
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
                    + "       AND r.recipe_user_id <> ?"
                    + "   )";

            update(sqlCon, QUERY_1, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, primaryUserId);
                pst.setString(4, primaryUserId);
                pst.setString(5, userId);
            });

            // 2. Remove tenant id that is not contributed by any other linked user.
            String QUERY_2 = "DELETE FROM " + primaryUserTenantsTable + " p"
                    + " WHERE p.app_id = ? AND p.tenant_id = ? AND p.primary_user_id = ?"
                    + "   AND NOT EXISTS ("
                    + "     SELECT 1"
                    + "     FROM " + recipeUserTenantsTable + " r"
                    + "     JOIN " + appIdToUserIdTable + " a"
                    + "       ON a.app_id = r.app_id AND a.user_id = r.recipe_user_id"
                    + "     WHERE r.app_id = p.app_id"
                    + "       AND r.tenant_id = p.tenant_id"
                    + "       AND a.primary_or_recipe_user_id = ?"
                    + "       AND a.is_linked_or_is_a_primary_user = true"
                    + "       AND r.recipe_user_id <> ?"
                    + "   )";

            update(sqlCon, QUERY_2, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, primaryUserId);
                pst.setString(4, primaryUserId);
                pst.setString(5, userId);
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void removeAccountInfoForPrimaryUserIfNecessary_Transaction(Start start, Connection sqlCon, AppIdentifier tenantIdentifier, String userId) throws StorageQueryException {
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
             * App-scoped cleanup (across all tenants):
             *
             * 1) Remove account info rows for this primary user for which there is no other linked recipe user
             *    that still has the same account info in that tenant.
             * 2) Remove tenant associations (i.e. all rows for that tenant) for which there is no other linked
             *    recipe user that has any account info in that tenant.
             */
            String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();

            // 1. Remove account info that is not contributed by any other linked user.
            String QUERY_1 = "DELETE FROM " + primaryUserTenantsTable + " p"
                    + " WHERE p.app_id = ? AND p.primary_user_id = ?"
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
                    + "       AND r.recipe_user_id <> ?"
                    + "   )";

            update(sqlCon, QUERY_1, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, primaryUserId);
                pst.setString(3, primaryUserId);
                pst.setString(4, userId);
            });

            // 2. Remove tenant id that is not contributed by any other linked user.
            String QUERY_2 = "DELETE FROM " + primaryUserTenantsTable + " p"
                    + " WHERE p.app_id = ? AND p.primary_user_id = ?"
                    + "   AND NOT EXISTS ("
                    + "     SELECT 1"
                    + "     FROM " + recipeUserTenantsTable + " r"
                    + "     JOIN " + appIdToUserIdTable + " a"
                    + "       ON a.app_id = r.app_id AND a.user_id = r.recipe_user_id"
                    + "     WHERE r.app_id = p.app_id"
                    + "       AND r.tenant_id = p.tenant_id"
                    + "       AND a.primary_or_recipe_user_id = ?"
                    + "       AND a.is_linked_or_is_a_primary_user = true"
                    + "       AND r.recipe_user_id <> ?"
                    + "   )";

            update(sqlCon, QUERY_2, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, primaryUserId);
                pst.setString(3, primaryUserId);
                pst.setString(4, userId);
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void removeAccountInfoReservations_Transaction(Start start, TransactionConnection con,
                                                                 AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();

            String appIdToUserIdTable = getConfig(start).getAppIdToUserIdTable();
            String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();

            /*
             * If this user was linked (or was itself a primary user), we may have "reserved" account info in
             * primary_user_tenants for the user's primary.
             *
             * We only remove the primary_user_tenants rows corresponding to this user's account infos (and only if no
             * other linked recipe user for the same primary still has that account info in that tenant).
             *
             * NOTE: We intentionally do NOT run a broader "orphan cleanup" for the whole primary user here.
             */
            String[] linkingInfo = execute(sqlCon,
                    "SELECT primary_or_recipe_user_id, is_linked_or_is_a_primary_user FROM " + appIdToUserIdTable
                            + " WHERE app_id = ? AND user_id = ?",
                    pst -> {
                        pst.setString(1, appIdentifier.getAppId());
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
            if (isLinkedOrPrimary) {
                /*
                 * Remove only the primary_user_tenants rows corresponding to this user's account infos.
                 *
                 * IMPORTANT: We must not delete all rows where primary_user_id = userId, since other recipe users can
                 * stay linked to the same primary user ID.
                 */
                {
                    String QUERY = "DELETE FROM " + primaryUserTenantsTable + " p"
                            + " WHERE p.app_id = ? AND p.primary_user_id = ?"
                            + "   AND EXISTS ("
                            + "     SELECT 1 FROM " + recipeUserTenantsTable + " r_me"
                            + "     WHERE r_me.app_id = p.app_id"
                            + "       AND r_me.recipe_user_id = ?"
                            + "       AND r_me.tenant_id = p.tenant_id"
                            + "       AND r_me.account_info_type = p.account_info_type"
                            + "       AND r_me.account_info_value = p.account_info_value"
                            + "   )"
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
                            + "       AND r.recipe_user_id <> ?"
                            + "   )";

                    update(sqlCon, QUERY, pst -> {
                        pst.setString(1, appIdentifier.getAppId());
                        pst.setString(2, primaryUserId);
                        pst.setString(3, userId);
                        pst.setString(4, primaryUserId);
                        pst.setString(5, userId);
                    });
                }
            }

            /*
             * Finally, delete the user's own account info rows from recipe_user_tenants at app_id scope.
             * (We do this at the end since the primary_user_tenants cleanup above consults recipe_user_tenants.)
             */
            {
                String recipeUserTenantsDelete = "DELETE FROM " + recipeUserTenantsTable
                        + " WHERE app_id = ? AND recipe_user_id = ?";
                update(sqlCon, recipeUserTenantsDelete, pst -> {
                    pst.setString(1, appIdentifier.getAppId());
                    pst.setString(2, userId);
                });
            }
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void updateAccountInfo_Transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier, String userId, ACCOUNT_INFO_TYPE accountInfoType, String accountInfoValue)
            throws
            EmailChangeNotAllowedException, PhoneNumberChangeNotAllowedException, StorageQueryException,
            DuplicateEmailException, DuplicatePhoneNumberException, DuplicateThirdPartyUserException {
        if (!ACCOUNT_INFO_TYPE.EMAIL.equals(accountInfoType) && !ACCOUNT_INFO_TYPE.PHONE_NUMBER.equals(accountInfoType)) {
            // Third party account info updates are not allowed via this function.
            throw new IllegalArgumentException(
                    "updateAccountInfo_Transaction should only be called with accountInfoType EMAIL or PHONE_NUMBER");
        }

        try {
            String appIdToUserIdTable = getConfig(start).getAppIdToUserIdTable();
            String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();

            // Find primary user ID and whether this recipe user is linked (or itself is a primary user).
            String[] linkingInfo = execute(sqlCon,
                    "SELECT primary_or_recipe_user_id, is_linked_or_is_a_primary_user FROM " + appIdToUserIdTable
                            + " WHERE app_id = ? AND user_id = ?",
                    pst -> {
                        pst.setString(1, appIdentifier.getAppId());
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

            boolean isLinkedOrPrimary = linkingInfo != null && Boolean.parseBoolean(linkingInfo[1]);
            String primaryUserId = linkingInfo != null ? linkingInfo[0] : null;

            // 1. Delete from primary_user_tenants to remove old account info if not contributed by any other linked user.
            if (isLinkedOrPrimary) {
                final String primaryUserIdFinal = primaryUserId;
                String QUERY_1 = "DELETE FROM " + primaryUserTenantsTable + " p"
                        + " WHERE p.app_id = ? AND p.primary_user_id = ?"
                        + "   AND p.account_info_type = ?"
                        + "   AND EXISTS ("
                        + "     SELECT 1 FROM " + recipeUserTenantsTable + " r_me"
                        + "     WHERE r_me.app_id = p.app_id"
                        + "       AND r_me.recipe_user_id = ?"
                        + "       AND r_me.tenant_id = p.tenant_id"
                        + "       AND r_me.account_info_type = p.account_info_type"
                        + "       AND r_me.account_info_value = p.account_info_value"
                        + "   )"
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
                        + "       AND r.recipe_user_id <> ?"
                        + "   )";

                update(sqlCon, QUERY_1, pst -> {
                    pst.setString(1, appIdentifier.getAppId());
                    pst.setString(2, primaryUserIdFinal);
                    pst.setString(3, accountInfoType.toString());
                    pst.setString(4, userId);
                    pst.setString(5, primaryUserIdFinal);
                    pst.setString(6, userId);
                });
            }

            // 2. Update account info value in recipe_user_tenants (across all tenants for this recipe user).
            // If accountInfoValue is null, delete the rows instead.
            if (accountInfoValue == null) {
                String QUERY_2_DELETE = "DELETE FROM " + recipeUserTenantsTable
                        + " WHERE app_id = ? AND recipe_user_id = ? AND account_info_type = ?";
                update(sqlCon, QUERY_2_DELETE, pst -> {
                    pst.setString(1, appIdentifier.getAppId());
                    pst.setString(2, userId);
                    pst.setString(3, accountInfoType.toString());
                });
            } else {
                String QUERY_2 = "UPDATE " + recipeUserTenantsTable
                        + " SET account_info_value = ?"
                        + " WHERE app_id = ? AND recipe_user_id = ? AND account_info_type = ?";
                update(sqlCon, QUERY_2, pst -> {
                    pst.setString(1, accountInfoValue);
                    pst.setString(2, appIdentifier.getAppId());
                    pst.setString(3, userId);
                    pst.setString(4, accountInfoType.toString());
                });
            }

            // 3. Insert into primary_user_tenants to add new account info if not already reserved by same primary.
            if (accountInfoValue != null && isLinkedOrPrimary) {
                final String primaryUserIdFinal = primaryUserId;
                String QUERY_3 = "INSERT INTO " + primaryUserTenantsTable
                        + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id)"
                        + " SELECT DISTINCT r.app_id, r.tenant_id, r.account_info_type, r.account_info_value, ?"
                        + " FROM " + recipeUserTenantsTable + " r"
                        + " WHERE r.app_id = ? AND r.recipe_user_id = ?"
                        + "   AND r.account_info_type = ? AND r.account_info_value = ?"
                        + "   AND NOT EXISTS ("
                        + "     SELECT 1 FROM " + primaryUserTenantsTable + " p"
                        + "     WHERE p.app_id = r.app_id"
                        + "       AND p.tenant_id = r.tenant_id"
                        + "       AND p.account_info_type = r.account_info_type"
                        + "       AND p.account_info_value = r.account_info_value"
                        + "       AND p.primary_user_id = ?"
                        + "   )";

                update(sqlCon, QUERY_3, pst -> {
                    pst.setString(1, primaryUserIdFinal);
                    pst.setString(2, appIdentifier.getAppId());
                    pst.setString(3, userId);
                    pst.setString(4, accountInfoType.toString());
                    pst.setString(5, accountInfoValue);
                    pst.setString(6, primaryUserIdFinal);
                });
            }
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();
                boolean isRecipeUserTenantsPk = isPrimaryKeyError(serverMessage, getConfig(start).getRecipeUserTenantsTable());
                boolean isPrimaryUserTenantsPk = isPrimaryKeyError(serverMessage, getConfig(start).getPrimaryUserTenantsTable());
                if (isPrimaryUserTenantsPk) {
                    throwAccountInfoChangeNotAllowed(accountInfoType);
                } else if (isRecipeUserTenantsPk) {
                    throwRecipeUserTenantsConflict(accountInfoType.toString());
                }
            }
            throw new StorageQueryException(e);
        }
    }
}
