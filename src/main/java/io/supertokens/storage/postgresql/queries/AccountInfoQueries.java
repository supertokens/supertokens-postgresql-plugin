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

import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import io.supertokens.pluginInterface.authRecipe.ACCOUNT_INFO_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.CanBecomePrimaryResult;
import io.supertokens.pluginInterface.authRecipe.CanLinkAccountsResult;
import io.supertokens.pluginInterface.authRecipe.exceptions.AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException;
import io.supertokens.pluginInterface.authRecipe.exceptions.AnotherPrimaryUserWithEmailAlreadyExistsException;
import io.supertokens.pluginInterface.authRecipe.exceptions.AnotherPrimaryUserWithPhoneNumberAlreadyExistsException;
import io.supertokens.pluginInterface.authRecipe.exceptions.AnotherPrimaryUserWithThirdPartyInfoAlreadyExistsException;
import io.supertokens.pluginInterface.authRecipe.exceptions.CannotBecomePrimarySinceRecipeUserIdAlreadyLinkedWithPrimaryUserIdException;
import io.supertokens.pluginInterface.authRecipe.exceptions.CannotLinkSinceRecipeUserIdAlreadyLinkedWithAnotherPrimaryUserIdException;
import io.supertokens.pluginInterface.authRecipe.exceptions.EmailChangeNotAllowedException;
import io.supertokens.pluginInterface.authRecipe.exceptions.InputUserIdIsNotAPrimaryUserException;
import io.supertokens.pluginInterface.authRecipe.exceptions.PhoneNumberChangeNotAllowedException;
import io.supertokens.pluginInterface.authRecipe.exceptions.UnknownUserIdException;
import io.supertokens.pluginInterface.bulkimport.PrimaryUser;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicateEmailException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.passwordless.exception.DuplicatePhoneNumberException;
import io.supertokens.pluginInterface.sqlStorage.TransactionConnection;
import io.supertokens.pluginInterface.thirdparty.exception.DuplicateThirdPartyUserException;
import io.supertokens.pluginInterface.useridmapping.LockedUser;
import io.supertokens.storage.postgresql.PreparedStatementValueSetter;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.executeBatch;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import static io.supertokens.storage.postgresql.config.Config.getConfig;
import io.supertokens.storage.postgresql.utils.Utils;

public class AccountInfoQueries {

    /*
     * IMPORTANT: Account Info Tables Design Note
     * ==========================================
     *
     * These tables store account identifiers that are TENANT-SCOPED:
     * - EMAIL: Scoped to (app_id, tenant_id)
     * - PHONE: Scoped to (app_id, tenant_id)
     * - THIRD_PARTY (thirdPartyId + thirdPartyUserId): Scoped to (app_id, tenant_id)
     *
     * WebAuthn CREDENTIAL IDs are intentionally NOT stored in these tables because:
     * 1. Credentials are RP-SCOPED (app_id, rp_id), not tenant-scoped
     * 2. Credentials have a direct 1:1 relationship with users via FK in webauthn_credentials table
     * 3. Credentials don't participate in account linking conflict detection
     * 4. The webauthn_credentials table already enforces uniqueness via PK (app_id, rp_id, credential_id)
     *
     * For listUsersByAccountInfo with credentialId parameter, use a union query approach:
     * - Query these tables for email/phone/thirdParty
     * - Query webauthn_credentials table separately for credentialId
     */

    static String getQueryToCreateRecipeUserAccountInfosTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getRecipeUserAccountInfosTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "app_id VARCHAR(64) NOT NULL,"
                + "recipe_user_id CHAR(36) NOT NULL,"
                + "recipe_id VARCHAR(128) NOT NULL,"
                + "account_info_type VARCHAR(8) NOT NULL,"
                + "account_info_value TEXT NOT NULL,"
                + "third_party_id VARCHAR(28),"
                + "third_party_user_id VARCHAR(256),"
                + "primary_user_id CHAR(36) NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey")
                + " PRIMARY KEY (app_id, recipe_id, recipe_user_id, account_info_type, third_party_id, third_party_user_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "tenant_id", "fkey")
                + " FOREIGN KEY(app_id)"
                + " REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
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
                + " FOREIGN KEY(app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantsTable() + " (app_id, tenant_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreateTenantIndexForRecipeUserTenantsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_tenant ON "
                + Config.getConfig(start).getRecipeUserTenantsTable() + "(app_id, tenant_id);";
    }

    static String getQueryToCreateRecipeUserIdIndexForRecipeUserTenantsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS idx_recipe_user_tenants_recipe_user_id ON "
                + Config.getConfig(start).getRecipeUserTenantsTable() + "(app_id, recipe_user_id);";
    }

    static String getQueryToCreateRecipeUserIdIndexForRecipeUserAccountInfoTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS idx_recipe_user_account_infos_app_recipe_user ON "
                + Config.getConfig(start).getRecipeUserAccountInfosTable() + "(app_id, recipe_user_id);";
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

    private static void throwRecipeUserTenantsConflict(String accountInfoType, boolean shouldThrowChangeNotAllowedExceptions)
            throws DuplicateEmailException, DuplicatePhoneNumberException, DuplicateThirdPartyUserException,
            EmailChangeNotAllowedException, PhoneNumberChangeNotAllowedException {
        if (accountInfoType == null) {
            return;
        }

        // this can never be updating
        if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(accountInfoType)) {
            throw new DuplicateThirdPartyUserException();
        }

        if (shouldThrowChangeNotAllowedExceptions) {
            if (ACCOUNT_INFO_TYPE.EMAIL.toString().equals(accountInfoType)) {
                throw new EmailChangeNotAllowedException();
            }
            if (ACCOUNT_INFO_TYPE.PHONE_NUMBER.toString().equals(accountInfoType)) {
                throw new PhoneNumberChangeNotAllowedException();
            }
        } else {
            if (ACCOUNT_INFO_TYPE.EMAIL.toString().equals(accountInfoType)) {
                throw new DuplicateEmailException();
            }
            if (ACCOUNT_INFO_TYPE.PHONE_NUMBER.toString().equals(accountInfoType)) {
                throw new DuplicatePhoneNumberException();
            }
        }
    }

    public static void addRecipeUserAccountInfo_Transaction(Start start, Connection sqlCon,
                                                            TenantIdentifier tenantIdentifier, String userId,
                                                            String recipeId, ACCOUNT_INFO_TYPE accountInfoType,
                                                            String thirdPartyId, String thirdPartyUserId,
                                                            String accountInfoValue)
            throws SQLException {
        {
            String QUERY = "INSERT INTO " + getConfig(start).getRecipeUserAccountInfosTable()
            + "(app_id, recipe_user_id, recipe_id, account_info_type, third_party_id, third_party_user_id, account_info_value, primary_user_id)"
            + " VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, userId);
                pst.setString(3, recipeId);
                pst.setString(4, accountInfoType.toString());
                pst.setString(5, thirdPartyId);
                pst.setString(6, thirdPartyUserId);
                pst.setString(7, accountInfoValue);
                pst.setObject(8, null); // primary_user_id is NULL initially
            });
        }

        {
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
    }

    /**
     * Adds account info entries to primary_user_tenants when a user becomes a primary user.
     * This overload requires a LockedUser parameter to ensure proper row-level locking has been acquired.
     *
     * @param targetUser The locked user who is becoming a primary user
     */
    public static boolean addPrimaryUserAccountInfo_Transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier,
                                                                 LockedUser targetUser)
            throws StorageQueryException, AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException,
            CannotBecomePrimarySinceRecipeUserIdAlreadyLinkedWithPrimaryUserIdException, UnknownUserIdException {

        String userId = targetUser.getRecipeUserId();

        // Validate via LockedUser state: if already linked to another primary, reject
        if (targetUser.isLinked()) {
            throw new CannotBecomePrimarySinceRecipeUserIdAlreadyLinkedWithPrimaryUserIdException(
                    targetUser.getPrimaryUserId(),
                    "This user ID is already linked to another user ID");
        }

        // If already a primary user, return false (idempotent)
        if (targetUser.isPrimary()) {
            return false;
        }

        try {
            String schema = Config.getConfig(start).getTableSchema();
            String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();
            String recipeUserAccountInfosTable = getConfig(start).getRecipeUserAccountInfosTable();

            // Note: Advisory lock is not needed since we have row-level lock via LockedUser

            // Insert with ON CONFLICT to catch primary key violations
            String QUERY = "INSERT INTO " + primaryUserTenantsTable
                    + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id)"
                    + " SELECT r.app_id, r.tenant_id, r.account_info_type, r.account_info_value, ?"
                    + " FROM " + recipeUserTenantsTable + " r"
                    + " INNER JOIN " + recipeUserAccountInfosTable + " ai"
                    + "   ON r.app_id = ai.app_id"
                    + "   AND r.recipe_user_id = ai.recipe_user_id"
                    + "   AND r.recipe_id = ai.recipe_id"
                    + "   AND r.account_info_type = ai.account_info_type"
                    + "   AND r.account_info_value = ai.account_info_value"
                    + " WHERE r.app_id = ? AND r.recipe_user_id = ? AND ai.primary_user_id IS NULL"
                    + " ON CONFLICT ON CONSTRAINT " + Utils.getConstraintName(schema, primaryUserTenantsTable, null, "pkey")
                    + " DO UPDATE SET account_info_type = EXCLUDED.account_info_type"
                    + " RETURNING primary_user_id, account_info_type";

            String[] conflict = execute(sqlCon, QUERY, pst -> {
                pst.setString(1, userId); // primary_user_id
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, userId); // recipe_user_id
            }, rs -> {
                String[] firstConflict = null;
                while (rs.next()) {
                    String returnedPrimaryUserId = rs.getString("primary_user_id");
                    String accountInfoType = rs.getString("account_info_type");

                    // Check if the returned primary_user_id is different from the userId
                    if (!userId.equals(returnedPrimaryUserId)) {
                        if (firstConflict == null) {
                            firstConflict = new String[]{returnedPrimaryUserId, accountInfoType};
                        }
                        // Prioritize THIRD_PARTY conflicts
                        if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(accountInfoType)) {
                            return new String[]{returnedPrimaryUserId, accountInfoType};
                        }
                    }
                }
                return firstConflict;
            });

            // Throw conflict if any row had a different primary_user_id
            if (conflict != null) {
                assert conflict.length == 2;
                String conflictingPrimaryUserId = conflict[0];
                String accountInfoType = conflict[1];

                String message;
                if (ACCOUNT_INFO_TYPE.EMAIL.toString().equals(accountInfoType)) {
                    message = "This user's email is already associated with another user ID";
                } else if (ACCOUNT_INFO_TYPE.PHONE_NUMBER.toString().equals(accountInfoType)) {
                    message = "This user's phone number is already associated with another user ID";
                } else if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(accountInfoType)) {
                    message = "This user's third party login is already associated with another user ID";
                } else {
                    message = "Account info is already associated with another user ID";
                }

                throw new AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException(conflictingPrimaryUserId, message);
            }

            // Update primary_user_id in recipe_user_account_infos to recipe_user_id (making it primary)
            String UPDATE_QUERY = "UPDATE " + recipeUserAccountInfosTable
                    + " SET primary_user_id = recipe_user_id"
                    + " WHERE app_id = ? AND recipe_user_id = ?";

            int rowsUpdated = update(sqlCon, UPDATE_QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, userId);
            });

            if (rowsUpdated == 0) {
                throw new UnknownUserIdException();
            }

            // all okay
            return true; // now became primary
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static CanBecomePrimaryResult checkIfLoginMethodCanBecomePrimary(Start start, AppIdentifier appIdentifier, String recipeUserId)
            throws StorageQueryException, UnknownUserIdException {
        try {
            return start.startTransaction(con -> {
                Connection sqlCon = (Connection) con.getConnection();
                
                String QUERY = "SELECT primary_user_id FROM " + getConfig(start).getRecipeUserAccountInfosTable()
                        + " WHERE app_id = ? AND recipe_user_id = ? LIMIT 1";

                String[] primaryUserId = execute(sqlCon, QUERY, pst -> {
                    pst.setString(1, appIdentifier.getAppId());
                    pst.setString(2, recipeUserId);
                }, rs -> {
                    if (rs.next()) {
                        return new String[]{rs.getString("primary_user_id")};
                    }
                    return new String[]{};
                });

                if (primaryUserId.length == 0) {
                    throw new StorageTransactionLogicException(new UnknownUserIdException());
                }

                assert primaryUserId.length == 1;

                if (primaryUserId[0] != null) {
                    if (primaryUserId[0].equals(recipeUserId)) {
                        return CanBecomePrimaryResult.wasAlreadyAPrimeryUserResult();
                    } else {
                        return CanBecomePrimaryResult.linkedWithAnotherPrimaryUserResult(primaryUserId[0]);
                    }
                }

                // now we need to check if the user can become primary by checking if there are conflicting account info
                // Get all tenant IDs and account info for this recipe user
                String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();
                String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
                
                // Query to find conflicts: check if any account info of this recipe user
                // is already associated with a different primary_user_id in primary_user_tenants
                String CONFLICT_QUERY = "SELECT p.primary_user_id, p.account_info_type"
                        + " FROM " + primaryUserTenantsTable + " p"
                        + " INNER JOIN " + recipeUserTenantsTable + " r"
                        + "   ON p.app_id = r.app_id"
                        + "   AND p.tenant_id = r.tenant_id"
                        + "   AND p.account_info_type = r.account_info_type"
                        + "   AND p.account_info_value = r.account_info_value"
                        + " WHERE r.app_id = ?"
                        + "   AND r.recipe_user_id = ?"
                        + "   AND p.primary_user_id != ?"
                        + " LIMIT 1";
                
                String[] conflict = execute(sqlCon, CONFLICT_QUERY, pst -> {
                    pst.setString(1, appIdentifier.getAppId());
                    pst.setString(2, recipeUserId);
                    pst.setString(3, recipeUserId);
                }, rs -> {
                    if (rs.next()) {
                        return new String[]{
                            rs.getString("primary_user_id"),
                            rs.getString("account_info_type")
                        };
                    }
                    return null;
                });
                
                if (conflict != null) {
                    String conflictingPrimaryUserId = conflict[0];
                    String accountInfoType = conflict[1];
                    
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
                    
                    return CanBecomePrimaryResult.conflictingAccountInfoResult(conflictingPrimaryUserId, message);
                }

                return CanBecomePrimaryResult.okResult();
            });
        } catch (StorageTransactionLogicException e) {
            Exception cause = e.actualException;
            if (cause instanceof UnknownUserIdException) {
                throw (UnknownUserIdException) cause;
            }
            throw new StorageQueryException(cause);
        }
    }

    public static CanLinkAccountsResult checkIfLoginMethodsCanBeLinked(Start start,
                                                                       AppIdentifier appIdentifier,
                                                                       String _primaryUserId,
                                                                       String recipeUserId)
            throws StorageQueryException, UnknownUserIdException {
        try {

            return start.startTransaction(con -> {

                String primaryUserId;

                Connection sqlCon = (Connection) con.getConnection();
                {
                    String QUERY = "SELECT primary_user_id FROM " + getConfig(start).getRecipeUserAccountInfosTable()
                            + " WHERE app_id = ? AND recipe_user_id = ? LIMIT 1";

                    String[] result = execute(sqlCon, QUERY, pst -> {
                        pst.setString(1, appIdentifier.getAppId());
                        pst.setString(2, _primaryUserId);
                    }, rs -> {
                        if (rs.next()) {
                            return new String[]{rs.getString("primary_user_id")};
                        }
                        return new String[]{};
                    });

                    if (result.length == 0) {
                        throw new StorageTransactionLogicException(new UnknownUserIdException());
                    }

                    assert result.length == 1;

                    if (result[0] == null) {
                        return CanLinkAccountsResult.inputUserIsNotPrimaryUserResult();
                    }

                    primaryUserId = result[0];
                }

                {
                    String QUERY = "SELECT primary_user_id FROM " + getConfig(start).getRecipeUserAccountInfosTable()
                            + " WHERE app_id = ? AND recipe_user_id = ? LIMIT 1";

                    String[] result = execute(sqlCon, QUERY, pst -> {
                        pst.setString(1, appIdentifier.getAppId());
                        pst.setString(2, recipeUserId);
                    }, rs -> {
                        if (rs.next()) {
                            return new String[]{rs.getString("primary_user_id")};
                        }
                        return new String[]{};
                    });

                    if (result.length == 0) {
                        throw new StorageTransactionLogicException(new UnknownUserIdException());
                    }

                    assert result.length == 1;

                    if (result[0] != null) {
                        if (result[0].equals(primaryUserId)) {
                            return CanLinkAccountsResult.wasAlreadyLinkedToPrimaryUserResult();
                        } else {
                            return CanLinkAccountsResult.recipeUserLinkedToAnotherPrimaryUserResult(result[0]);
                        }
                    }
                }

                String QUERY = "SELECT primary_user_id, account_info_type " +
                        "FROM " + getConfig(start).getPrimaryUserTenantsTable() + " " +
                        "WHERE app_id = ? AND ((account_info_type, account_info_value) IN (" +
                        "   (SELECT account_info_type, account_info_value " +
                        "    FROM " + getConfig(start).getPrimaryUserTenantsTable() + " " +
                        "    WHERE app_id = ? AND primary_user_id = ?) " +
                        "   UNION " +
                        "   (SELECT account_info_type, account_info_value " +
                        "    FROM " + getConfig(start).getRecipeUserAccountInfosTable() + " " +
                        "    WHERE app_id = ? AND recipe_user_id = ?)" +
                        ")) AND ((tenant_id) IN (" +
                        "   (SELECT tenant_id " +
                        "    FROM " + getConfig(start).getPrimaryUserTenantsTable() + " " +
                        "    WHERE app_id = ? AND primary_user_id = ?) " +
                        "   UNION " +
                        "   (SELECT tenant_id " +
                        "    FROM " + getConfig(start).getRecipeUserTenantsTable() + " " +
                        "    WHERE app_id = ? AND recipe_user_id = ?)" +
                        ")) AND primary_user_id != ? LIMIT 1;";

                String[] result = execute(sqlCon, QUERY, pst -> {
                    pst.setString(1, appIdentifier.getAppId()); // primary_user_tenants.app_id (main)
                    pst.setString(2, appIdentifier.getAppId()); // subquery 1: primary_user_tenants.app_id
                    pst.setString(3, primaryUserId);           // subquery 1: primary_user_tenants.primary_user_id
                    pst.setString(4, appIdentifier.getAppId()); // subquery 2: recipe_user_account_infos.app_id
                    pst.setString(5, recipeUserId);             // subquery 2: recipe_user_account_infos.recipe_user_id
                    pst.setString(6, appIdentifier.getAppId()); // tenant from primary_user_tenants
                    pst.setString(7, primaryUserId);            // tenant from primary_user_tenants.primary_user_id
                    pst.setString(8, appIdentifier.getAppId()); // tenant from recipe_user_tenants.app_id
                    pst.setString(9, recipeUserId);             // tenant from recipe_user_tenants.recipe_user_id
                    pst.setString(10, primaryUserId);           // primary user id that's not matching
                }, rs -> {
                    if (rs.next()) {
                        // Return conflicting primary_user_id and account_info_type
                        return new String[]{rs.getString("primary_user_id"), rs.getString("account_info_type")};
                    }
                    return null;
                });

                if (result != null && !result[0].equals(primaryUserId)) {
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

                    return CanLinkAccountsResult.notOkResult(conflictingPrimaryUserId, message);
                }

                return CanLinkAccountsResult.okResult();
            });
        } catch (StorageTransactionLogicException e) {
            Exception cause = e.actualException;
            if (cause instanceof UnknownUserIdException) {
                throw (UnknownUserIdException) cause;
            }
            throw new StorageQueryException(cause);
        }


    }

    /**
     * Reserves account info for linking with LockedUser enforcement.
     * This method requires LockedUser objects proving that proper row-level locks have been acquired.
     *
     * @param recipeUser The locked recipe user being linked
     * @param primaryUser The locked primary user to link to
     */
    public static boolean reserveAccountInfoForLinking_Transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier,
                                                                    LockedUser recipeUser, LockedUser primaryUser)
            throws StorageQueryException, UnknownUserIdException,
            InputUserIdIsNotAPrimaryUserException, CannotLinkSinceRecipeUserIdAlreadyLinkedWithAnotherPrimaryUserIdException,
            AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException {

        // Extract user IDs from locked users
        String recipeUserId = recipeUser.getRecipeUserId();
        // getPrimaryUserId() returns the actual primary user ID, which works whether:
        // - primaryUser is a primary user (returns its own ID)
        // - primaryUser is a linked user (returns the ID of the primary it's linked to)
        // - primaryUser is standalone (returns null)
        String primaryUserId = primaryUser.getPrimaryUserId();

        // Validate that the user passed as "primary" is actually part of a primary user group
        // (either is a primary user or is linked to one)
        if (primaryUserId == null) {
            throw new InputUserIdIsNotAPrimaryUserException(primaryUser.getRecipeUserId());
        }

        // Validate that the recipe user is not already a primary user themselves
        // A primary user cannot be linked as a recipe user to another primary
        if (recipeUser.isPrimary()) {
try {
                AuthRecipeUserInfo recipeUserInfo = GeneralQueries.getPrimaryUserInfoForUserId_Transaction(
                        start, sqlCon, appIdentifier, recipeUserId);
                if (recipeUserInfo == null) {
                    throw new UnknownUserIdException();
                }
            throw new CannotLinkSinceRecipeUserIdAlreadyLinkedWithAnotherPrimaryUserIdException(recipeUserInfo);
            } catch (SQLException e) {
                throw new StorageQueryException(e);
}
        }

        // Validate that the recipe user is not already linked to a different primary
        if (recipeUser.isLinked()) {
            String existingPrimaryId = recipeUser.getPrimaryUserId();
            if (!existingPrimaryId.equals(primaryUserId)) {
                // Recipe user is already linked to a different primary
                try {
                    AuthRecipeUserInfo recipeUserInfo = GeneralQueries.getPrimaryUserInfoForUserId_Transaction(
                            start, sqlCon, appIdentifier, recipeUserId);
                    if (recipeUserInfo == null) {
                        throw new UnknownUserIdException();
                    }
                    throw new CannotLinkSinceRecipeUserIdAlreadyLinkedWithAnotherPrimaryUserIdException(recipeUserInfo);
                } catch (SQLException e) {
                    throw new StorageQueryException(e);
                }
            } else {
                // Already linked to the same primary user
                return false;
            }
        }

        try {
            String schema = Config.getConfig(start).getTableSchema();
            String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();
            String recipeUserAccountInfosTable = getConfig(start).getRecipeUserAccountInfosTable();

            // Note: Advisory lock is not needed since we have row-level locks via LockedUser

            // Insert into primary_user_tenants - union of tenants and account info from both users
            String QUERY = "INSERT INTO " + primaryUserTenantsTable
                    + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id)"
                    + " SELECT ?, all_tenants.tenant_id, all_accounts.account_info_type, all_accounts.account_info_value, ?"
                    + " FROM ("
                    + "   SELECT tenant_id FROM " + primaryUserTenantsTable
                    + "   WHERE app_id = ? AND primary_user_id = ?"
                    + "   UNION"
                    + "   SELECT tenant_id FROM " + recipeUserTenantsTable + " WHERE app_id = ? AND recipe_user_id = ?"
                    + " ) all_tenants CROSS JOIN ("
                    + "   SELECT account_info_type, account_info_value FROM " + primaryUserTenantsTable
                    + "   WHERE app_id = ? AND primary_user_id = ?"
                    + "   UNION"
                    + "   SELECT account_info_type, account_info_value FROM " + recipeUserAccountInfosTable + " WHERE app_id = ? AND recipe_user_id = ? AND primary_user_id is NULL"
                    + " ) all_accounts"
                    + " ON CONFLICT ON CONSTRAINT " + Utils.getConstraintName(schema, primaryUserTenantsTable, null, "pkey")
                    + " DO UPDATE SET account_info_type = EXCLUDED.account_info_type"
                    + " RETURNING primary_user_id, account_info_type";

            String[] conflict = execute(sqlCon, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId()); // app_id for INSERT
                pst.setString(2, primaryUserId);            // primary_user_id for INSERT

                pst.setString(3, appIdentifier.getAppId()); // tenant subquery 1: primary_user_tenants.app_id
                pst.setString(4, primaryUserId);            // tenant subquery 1: primary_user_id
                pst.setString(5, appIdentifier.getAppId()); // tenant subquery 2: recipe_user_tenants.app_id
                pst.setString(6, recipeUserId);             // tenant subquery 2: recipe_user_tenants.recipe_user_id

                pst.setString(7, appIdentifier.getAppId()); // account subquery 1: primary_user_tenants.app_id
                pst.setString(8, primaryUserId);            // account subquery 1: primary_user_id
                pst.setString(9, appIdentifier.getAppId()); // account subquery 2: recipe_user_account_infos.app_id
                pst.setString(10, recipeUserId);            // account subquery 2: recipe_user_account_infos.recipe_user_id
            }, rs -> {
                String[] firstConflict = null;
                while (rs.next()) {
                    String returnedPrimaryUserId = rs.getString("primary_user_id");
                    String accountInfoType = rs.getString("account_info_type");

                    // Check if the returned primary_user_id is different from the expected primaryUserId
                    if (!primaryUserId.equals(returnedPrimaryUserId)) {
                        if (firstConflict == null) {
                            firstConflict = new String[]{returnedPrimaryUserId, accountInfoType};
                        }
                        // Prioritize THIRD_PARTY conflicts
                        if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(accountInfoType)) {
                            return new String[]{returnedPrimaryUserId, accountInfoType};
                        }
                    }
                }
                return firstConflict;
            });

            // Throw conflict if any row had a different primary_user_id
            if (conflict != null && conflict[0] != null) {
                String conflictingPrimaryUserId = conflict[0].trim();
                String accountInfoType = conflict[1];

                String message;
                if (ACCOUNT_INFO_TYPE.EMAIL.toString().equals(accountInfoType)) {
                    message = "This user's email is already associated with another user ID";
                } else if (ACCOUNT_INFO_TYPE.PHONE_NUMBER.toString().equals(accountInfoType)) {
                    message = "This user's phone number is already associated with another user ID";
                } else if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(accountInfoType)) {
                    message = "This user's third party login is already associated with another user ID";
                } else {
                    message = "Account info is already associated with another user ID";
                }

                throw new AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException(conflictingPrimaryUserId, message);
            }

            // Update primary_user_id in recipe_user_account_infos to link the recipe user to the primary user
            String UPDATE_QUERY = "UPDATE " + recipeUserAccountInfosTable
                    + " SET primary_user_id = ?"
                    + " WHERE app_id = ? AND recipe_user_id = ?";

            int rowsUpdated = update(sqlCon, UPDATE_QUERY, pst -> {
                pst.setString(1, primaryUserId);
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, recipeUserId);
            });

            if (rowsUpdated == 0) {
                throw new UnknownUserIdException();
            }

            // Link succeeded
            return true;
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }
    /**
     * Adds a tenant to a recipe user's tenant associations with LockedUser enforcement.
     * This method requires a LockedUser parameter to ensure proper row-level locks have been acquired,
     * preventing race conditions during concurrent tenant association and linking operations.
     *
     * @param user The locked user to associate with the tenant
     */
    public static void addTenantIdToRecipeUser_Transaction(Start start, Connection sqlCon,
                                                            TenantIdentifier tenantIdentifier, LockedUser user)
            throws StorageQueryException, DuplicateEmailException, DuplicateThirdPartyUserException, DuplicatePhoneNumberException {
        // Validate that the lock is still valid for this connection
        if (!user.isValidForConnection(sqlCon)) {
            throw new IllegalStateException("LockedUser is not valid for this connection - lock may have been released or acquired on a different connection");
        }

        AppIdentifier appIdentifier = tenantIdentifier.toAppIdentifier();
        String userId = user.getRecipeUserId();
        String schema = Config.getConfig(start).getTableSchema();
        String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();
        String recipeUserAccountInfosTable = getConfig(start).getRecipeUserAccountInfosTable();

        String QUERY = "INSERT INTO " + recipeUserTenantsTable
                + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type, third_party_id, third_party_user_id, account_info_value)"
                + " SELECT DISTINCT r.app_id, r.recipe_user_id, ?, r.recipe_id, r.account_info_type, r.third_party_id, r.third_party_user_id, r.account_info_value"
                + " FROM " + recipeUserAccountInfosTable + " r"
                + " WHERE r.app_id = ? AND r.recipe_user_id = ?"
                + " ON CONFLICT ON CONSTRAINT " + Utils.getConstraintName(schema, recipeUserTenantsTable, null, "pkey")
                + " DO UPDATE SET account_info_type = EXCLUDED.account_info_type "
                + " RETURNING recipe_user_id, account_info_type";

        try {
            String conflictAccountInfoType = execute(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getTenantId());
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, userId);
            }, rs -> {
                String firstConflictType = null;
                while (rs.next()) {
                    String returnedRecipeUserId = rs.getString("recipe_user_id");
                    String accountInfoType = rs.getString("account_info_type");

                    // Check if the returned recipe_user_id is different from the userId
                    if (!userId.equals(returnedRecipeUserId)) {
                        if (firstConflictType == null) {
                            firstConflictType = accountInfoType;
                        }
                        // Prioritize THIRD_PARTY conflicts
                        if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(accountInfoType)) {
                            return accountInfoType;
                        }
                    }
                }
                return firstConflictType;
            });

            // Throw conflict if any row had a different recipe_user_id
            throwRecipeUserTenantsConflict(conflictAccountInfoType, false);
        } catch (EmailChangeNotAllowedException | PhoneNumberChangeNotAllowedException e) {
            throw new IllegalStateException("should never happen", e);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    /**
     * Adds account info entries to primary_user_tenants when adding a tenant to a user that is part of a primary user group.
     * This method requires a LockedUser parameter to ensure proper row-level locking has been acquired.
     * The LockedUser can be either the primary user itself OR a linked recipe user - the locking mechanism
     * ensures that when we lock a linked user, the primary user is also locked.
     *
     * @param user The locked user (either primary or linked) whose primary user's account info should be reserved
     */
    public static void addTenantIdToPrimaryUser_Transaction(Start start, TransactionConnection con, TenantIdentifier tenantIdentifier, LockedUser user)
            throws StorageQueryException,
            AnotherPrimaryUserWithPhoneNumberAlreadyExistsException,
            AnotherPrimaryUserWithEmailAlreadyExistsException,
            AnotherPrimaryUserWithThirdPartyInfoAlreadyExistsException {

        // Verify the user is a primary user (either IS primary or IS linked to a primary)
        String primaryUserId = user.getPrimaryUserId();
        if (primaryUserId == null) {
            throw new IllegalStateException("User must be a primary user (either primary or linked)");
        }

        Connection sqlCon = (Connection) con.getConnection();
        String schema = Config.getConfig(start).getTableSchema();
        String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
        String recipeUserAccountInfosTable = getConfig(start).getRecipeUserAccountInfosTable();

        // Select ALL account info for the primary user (including all linked recipe users)
        // by querying on primary_user_id, not recipe_user_id
        String QUERY = "INSERT INTO " + primaryUserTenantsTable
                + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id)"
                + " SELECT DISTINCT rac.app_id, ?, rac.account_info_type, rac.account_info_value, rac.primary_user_id"
                + " FROM " + recipeUserAccountInfosTable + " rac"
                + " WHERE rac.app_id = ? AND rac.primary_user_id = ?"
                + " ON CONFLICT ON CONSTRAINT " + Utils.getConstraintName(schema, primaryUserTenantsTable, null, "pkey")
                + " DO UPDATE SET account_info_type = EXCLUDED.account_info_type "
                + " RETURNING primary_user_id, account_info_type";

        try {
            String[] conflict = execute(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getTenantId());
                pst.setString(2, tenantIdentifier.getAppId());
                pst.setString(3, primaryUserId);
            }, rs -> {
                String[] firstConflict = null;
                while (rs.next()) {
                    String returnedPrimaryUserId = rs.getString("primary_user_id");
                    String accountInfoType = rs.getString("account_info_type");

                    // Check if the returned primary_user_id is different from the primaryUserId
                    if (!primaryUserId.equals(returnedPrimaryUserId)) {
                        if (firstConflict == null) {
                            firstConflict = new String[]{returnedPrimaryUserId, accountInfoType};
                        }
                        // Prioritize THIRD_PARTY conflicts
                        if (ACCOUNT_INFO_TYPE.THIRD_PARTY.toString().equals(accountInfoType)) {
                            return new String[]{returnedPrimaryUserId, accountInfoType};
                        }
                    }
                }
                return firstConflict;
            });

            // Throw conflict if any row had a different primary_user_id
            throwPrimaryUserTenantsConflict(conflict);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void removeAccountInfoForRecipeUserWhileRemovingTenant_Transaction(Start start, Connection sqlCon, TenantIdentifier tenantIdentifier, LockedUser user) throws StorageQueryException {
        try {
            String QUERY = "DELETE FROM " + getConfig(start).getRecipeUserTenantsTable()
                    + " WHERE app_id = ? AND tenant_id = ? AND recipe_user_id = ?";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, user.getRecipeUserId());
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void removeAccountInfoReservationForPrimaryUserWhileRemovingTenant_Transaction(Start start, Connection sqlCon, TenantIdentifier tenantIdentifier, LockedUser user) throws StorageQueryException {
        String primaryUserId = user.getPrimaryUserId();
        // If the user is not linked to any primary user, there's nothing to delete
        if (primaryUserId == null) {
            return;
        }

        try {
            String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
            String recipeUserAccountInfosTable = getConfig(start).getRecipeUserAccountInfosTable();
            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();

            // This query removes rows from the primary_user_tenants table for the given primary user,
            // but only for those tenants that the user is no longer associated with after a tenant removal operation.
            // It does so by:
            //   1. Using the primary_user_id from the LockedUser (already known from the lock acquisition).
            //   2. Deleting only those primary_user_tenants rows (for this app and primary_user_id) whose tenant_id is NOT present
            //      in the list of tenants remaining for any of the primary user's linked recipe users,
            //      except for the tenant/user combination being removed (i.e., tenant_id != removed tenant).
            //   3. Effectively, this ensures that account info reservations in primary_user_tenants only remain on tenants
            //      where the primary user (or any linked user) is still active after this tenant of user is removed.
            String recipeUserId = user.getRecipeUserId();
            String QUERY = "DELETE FROM " + primaryUserTenantsTable
                    + " WHERE app_id = ? AND primary_user_id = ? AND (tenant_id) NOT IN ("
                    + "     SELECT DISTINCT tenant_id"
                    + "     FROM " + recipeUserTenantsTable
                    + "     WHERE recipe_user_id IN ("
                    + "         SELECT recipe_user_id"
                    + "         FROM " + recipeUserAccountInfosTable
                    + "         WHERE primary_user_id = ? AND ((recipe_user_id = ? AND tenant_id != ?) OR recipe_user_id != ?)"
                    + "     )"
                    + " )";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, primaryUserId);
                pst.setString(3, primaryUserId);
                pst.setString(4, recipeUserId);
                pst.setString(5, tenantIdentifier.getTenantId());
                pst.setString(6, recipeUserId);
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    // Helper that performs the actual DB work for unlinking account info reservations
    public static void doRemoveAccountInfoReservationForUnlinking(Start start, Connection sqlCon, AppIdentifier tenantIdentifier, String userId) throws StorageQueryException {
        try {
            String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
            String recipeUserAccountInfosTable = getConfig(start).getRecipeUserAccountInfosTable();
            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();

            // This query removes rows from the primary_user_tenants table for the given primary user (identified by the passed-in userId),
            // but only for those account info and tenant combinations that the user is no longer associated with after an unlinking operation.
            // It does so by:
            //   1. Identifying the primary_user_id linked to the given recipe_user (by userId).
            //   2. Deleting only those primary_user_tenants rows (for this app and primary_user_id) where:
            //      a) The (account_info_type, account_info_value) combination is NOT present in any other linked recipe user's
            //         recipe_user_tenants, OR
            //      b) The tenant_id is NOT present in any other linked recipe user's recipe_user_tenants.
            //   3. Effectively, this ensures that account info reservations in primary_user_tenants only remain where
            //      the primary user (or any other linked user) still has that account info or tenant after this user is unlinked.
            String QUERY = "DELETE FROM " + primaryUserTenantsTable
                    + " WHERE app_id = ? AND primary_user_id IN ("
                    + "     SELECT primary_user_id FROM " + recipeUserAccountInfosTable + " WHERE app_id = ? AND recipe_user_id = ? LIMIT 1"
                    + " ) AND ("
                    + "     (account_info_type, account_info_value) NOT IN ("
                    + "         SELECT DISTINCT account_info_type, account_info_value"
                    + "         FROM " + recipeUserAccountInfosTable
                    + "         WHERE app_id = ? AND primary_user_id IN ("
                    + "             SELECT primary_user_id FROM " + recipeUserAccountInfosTable
                    + "             WHERE app_id = ? AND recipe_user_id = ? LIMIT 1"
                    + "         ) AND recipe_user_id <> ?"
                    + "     )"
                    + "     OR tenant_id NOT IN ("
                    + "         SELECT DISTINCT tenant_id"
                    + "         FROM " + recipeUserTenantsTable
                    + "         WHERE app_id = ? AND recipe_user_id IN ("
                    + "             SELECT recipe_user_id"
                    + "             FROM " + recipeUserAccountInfosTable
                    + "             WHERE app_id = ? AND primary_user_id IN ("
                    + "                 SELECT primary_user_id FROM " + recipeUserAccountInfosTable
                    + "                 WHERE app_id = ? AND recipe_user_id = ? LIMIT 1"
                    + "             ) AND recipe_user_id <> ?"
                    + "         )"
                    + "     )"
                    + " )";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());  // WHERE app_id = ?
                pst.setString(2, tenantIdentifier.getAppId());  // SELECT ... WHERE app_id = ?
                pst.setString(3, userId);                       // ... AND recipe_user_id = ?
                pst.setString(4, tenantIdentifier.getAppId());  // WHERE app_id = ? (NOT IN clause)
                pst.setString(5, tenantIdentifier.getAppId());  // SELECT ... WHERE app_id = ? (nested)
                pst.setString(6, userId);                       // ... AND recipe_user_id = ? (nested)
                pst.setString(7, userId);                       // ... AND recipe_user_id <> ?
                pst.setString(8, tenantIdentifier.getAppId());  // WHERE app_id = ? (tenant_id NOT IN)
                pst.setString(9, tenantIdentifier.getAppId());  // WHERE app_id = ? (nested in tenant_id NOT IN)
                pst.setString(10, tenantIdentifier.getAppId()); // SELECT ... WHERE app_id = ? (deeply nested)
                pst.setString(11, userId);                      // ... AND recipe_user_id = ? (deeply nested)
                pst.setString(12, userId);                      // ... AND recipe_user_id <> ?
            });

            // Update primary_user_id to NULL in recipe_user_account_infos when unlinking
            String UPDATE_QUERY = "UPDATE " + recipeUserAccountInfosTable
                    + " SET primary_user_id = NULL"
                    + " WHERE app_id = ? AND recipe_user_id = ?";

            update(sqlCon, UPDATE_QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, userId);
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void removeAccountInfoReservationForPrimaryUserForUnlinking_Transaction(
            Start start, Connection sqlCon, AppIdentifier appIdentifier,
            LockedUser recipeUser) throws StorageQueryException {

        String recipeUserId = recipeUser.getRecipeUserId();

        if (!recipeUser.isLinked() && !recipeUser.isPrimary()) {
            throw new IllegalStateException("Recipe user " + recipeUserId + " is not part of any primary user group");
        }

        doRemoveAccountInfoReservationForUnlinking(start, sqlCon, appIdentifier, recipeUserId);
    }

    public static void removeAccountInfoReservationsForDeletingUser_Transaction(Start start, TransactionConnection con,
                                                                                AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();

            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();
            String recipeUserAccountInfosTable = getConfig(start).getRecipeUserAccountInfosTable();

            doRemoveAccountInfoReservationForUnlinking(start, sqlCon, appIdentifier, userId);

            {
                String recipeUserTenantsDelete = "DELETE FROM " + recipeUserTenantsTable
                        + " WHERE app_id = ? AND recipe_user_id = ?";
                update(sqlCon, recipeUserTenantsDelete, pst -> {
                    pst.setString(1, appIdentifier.getAppId());
                    pst.setString(2, userId);
                });
            }

            {
                String recipeUserTenantsDelete = "DELETE FROM " + recipeUserAccountInfosTable
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

    /**
     * Updates account info (email or phone number) for a user with LockedUser enforcement.
     * This method requires a LockedUser parameter to ensure proper locking has been acquired,
     * preventing race conditions during concurrent operations.
     *
     * @param start The Start instance
     * @param sqlCon The SQL connection
     * @param appIdentifier The app context
     * @param user The locked user whose account info is being updated
     * @param accountInfoType The type of account info to update (EMAIL or PHONE_NUMBER only)
     * @param accountInfoValue The new value for the account info (null to remove)
     */
    public static void updateAccountInfo_Transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier,
                                                     LockedUser user, ACCOUNT_INFO_TYPE accountInfoType,
                                                     String accountInfoValue)
            throws
            EmailChangeNotAllowedException, PhoneNumberChangeNotAllowedException, StorageQueryException,
            DuplicateEmailException, DuplicatePhoneNumberException, DuplicateThirdPartyUserException,
            UnknownUserIdException {
        if (!ACCOUNT_INFO_TYPE.EMAIL.equals(accountInfoType) && !ACCOUNT_INFO_TYPE.PHONE_NUMBER.equals(accountInfoType)) {
            // Third party account info updates are not allowed via this function.
            throw new IllegalArgumentException(
                    "updateAccountInfo_Transaction should only be called with accountInfoType EMAIL or PHONE_NUMBER");
        }

        // Get user ID and primary user ID from the LockedUser (already verified during lock acquisition)
        String userId = user.getRecipeUserId();
        String primaryUserId = user.getPrimaryUserId();

        try {
            String primaryUserTenantsTable = getConfig(start).getPrimaryUserTenantsTable();
            String recipeUserTenantsTable = getConfig(start).getRecipeUserTenantsTable();
            String recipeUserAccountInfosTable = getConfig(start).getRecipeUserAccountInfosTable();

            // Note: No need to query for primaryUserId - we already have it from LockedUser.
            // The lock guarantees the state hasn't changed since lock acquisition.

            // 1. Delete from primary_user_tenants to remove old account info if not contributed by any other linked user.
            if (primaryUserId != null) {
                final String primaryUserIdFinal = primaryUserId;
                String QUERY_1 = "DELETE FROM " + primaryUserTenantsTable
                        + " WHERE app_id = ? AND primary_user_id = ? AND account_info_type = ? AND account_info_value NOT IN ("
                        + "     SELECT account_info_value"
                        + "     FROM " + recipeUserTenantsTable
                        + "     WHERE recipe_user_id IN ("
                        + "         SELECT recipe_user_id"
                        + "         FROM " + recipeUserAccountInfosTable
                        + "         WHERE primary_user_id = ? AND recipe_user_id != ?"
                        + "     )"
                        + " )";

                update(sqlCon, QUERY_1, pst -> {
                    pst.setString(1, appIdentifier.getAppId());
                    pst.setString(2, primaryUserIdFinal);
                    pst.setString(3, accountInfoType.toString());
                    pst.setString(4, primaryUserIdFinal);
                    pst.setString(5, userId);
                });
            }

            // 2. Update account info value in recipe_user_tenants (across all tenants for this recipe user).
            // If accountInfoValue is null, delete the rows instead.
            if (accountInfoValue == null) {
                {
                    String QUERY_2_DELETE = "DELETE FROM " + recipeUserTenantsTable
                            + " WHERE app_id = ? AND recipe_user_id = ? AND account_info_type = ?";
                    update(sqlCon, QUERY_2_DELETE, pst -> {
                        pst.setString(1, appIdentifier.getAppId());
                        pst.setString(2, userId);
                        pst.setString(3, accountInfoType.toString());
                    });
                }
                {
                    String QUERY_2_DELETE = "DELETE FROM " + recipeUserAccountInfosTable
                            + " WHERE app_id = ? AND recipe_user_id = ? AND account_info_type = ?";
                    update(sqlCon, QUERY_2_DELETE, pst -> {
                        pst.setString(1, appIdentifier.getAppId());
                        pst.setString(2, userId);
                        pst.setString(3, accountInfoType.toString());
                    });
                }
            } else {
                {
                    // Insert accountInfoType and accountInfoValue for all tenants that match app_id and user_id
                    String QUERY_2_INSERT = "INSERT INTO " + recipeUserTenantsTable
                            + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type, third_party_id, third_party_user_id, account_info_value)"
                            + " SELECT DISTINCT r.app_id, r.recipe_user_id, r.tenant_id, r.recipe_id, ?, r.third_party_id, r.third_party_user_id, ?"
                            + " FROM " + recipeUserTenantsTable + " r"
                            + " WHERE r.app_id = ? AND r.recipe_user_id = ?";
                    update(sqlCon, QUERY_2_INSERT, pst -> {
                        pst.setString(1, accountInfoType.toString());
                        pst.setString(2, accountInfoValue);
                        pst.setString(3, appIdentifier.getAppId());
                        pst.setString(4, userId);
                    });

                    // Delete records that match app_id, user_id and account_info_type based on current account_info_value in recipe_user_account_infos
                    String QUERY_2_DELETE = "DELETE FROM " + recipeUserTenantsTable
                            + " WHERE app_id = ? AND recipe_user_id = ? AND account_info_type = ?"
                            + " AND account_info_value != ?";
                    update(sqlCon, QUERY_2_DELETE, pst -> {
                        pst.setString(1, appIdentifier.getAppId());
                        pst.setString(2, userId);
                        pst.setString(3, accountInfoType.toString());
                        pst.setString(4, accountInfoValue);
                    });
                }
                {
                    String schema = Config.getConfig(start).getTableSchema();
                    // Upsert into recipe_user_account_infos
                    String QUERY_2_UPSERT = "INSERT INTO " + recipeUserAccountInfosTable
                            + " (app_id, recipe_user_id, recipe_id, account_info_type, third_party_id, third_party_user_id, account_info_value, primary_user_id)"
                            + " SELECT ?, ?, recipe_id, ?, third_party_id, third_party_user_id, ?, primary_user_id"
                            + " FROM " + recipeUserAccountInfosTable
                            + " WHERE app_id = ? AND recipe_user_id = ? LIMIT 1"
                            + " ON CONFLICT ON CONSTRAINT " + Utils.getConstraintName(schema, recipeUserAccountInfosTable, null, "pkey")
                            + " DO UPDATE SET account_info_value = EXCLUDED.account_info_value";
                    update(sqlCon, QUERY_2_UPSERT, pst -> {
                        pst.setString(1, appIdentifier.getAppId());
                        pst.setString(2, userId);
                        pst.setString(3, accountInfoType.toString());
                        pst.setString(4, accountInfoValue);
                        pst.setString(5, appIdentifier.getAppId());
                        pst.setString(6, userId);
                    });
                }
            }

            // 3. Insert into primary_user_tenants to add new account info if not already reserved by same primary.
            if (accountInfoValue != null && primaryUserId != null) {
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
                    throwRecipeUserTenantsConflict(accountInfoType.toString(), primaryUserId != null);
                }
            }
            throw new StorageQueryException(e);
        }
    }

    public static void addRecipeUserAccountInfoToBatch(List<PreparedStatementValueSetter> recipeUserAccountInfoBatch, AppIdentifier appIdentifier, String recipeUserId, String recipeId, ACCOUNT_INFO_TYPE accountInfoType, String thirdPartyId, String thirdPartyUserId, String accountInfoValue, String primaryUserId) {
        recipeUserAccountInfoBatch.add(pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, recipeUserId);
            pst.setString(3, recipeId);
            pst.setString(4, accountInfoType.toString());
            pst.setString(5, thirdPartyId);
            pst.setString(6, thirdPartyUserId);
            pst.setString(7, accountInfoValue);
            pst.setString(8, primaryUserId);
        });
    }

    public static void addRecipeUserTenantsToBatch(List<PreparedStatementValueSetter> recipeUserAccountInfoBatch, AppIdentifier appIdentifier, String recipeUserId, String recipeId, ACCOUNT_INFO_TYPE accountInfoType, String thirdPartyId, String thirdPartyUserId, String accountInfoValue,
                                                   List<String> recipeUserTenantIds) {
        if (thirdPartyId.length() > 28) {
            System.out.println(thirdPartyId);
        }
        for (String tenantId : recipeUserTenantIds) {
            recipeUserAccountInfoBatch.add(pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, recipeUserId);
                pst.setString(3, tenantId);
                pst.setString(4, recipeId);
                pst.setString(5, accountInfoType.toString());
                pst.setString(6, thirdPartyId);
                pst.setString(7, thirdPartyUserId);
                pst.setString(8, accountInfoValue);
            });
        }
    }

    public static String getRecipeUserAccountInfoBatchQuery (Start start) {
        return "INSERT INTO " + getConfig(start).getRecipeUserAccountInfosTable()
                + "(app_id, recipe_user_id, recipe_id, account_info_type, third_party_id, third_party_user_id, account_info_value, primary_user_id)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
    }

    public static String getRecipeUserTenantBatchQuery (Start start) {
        return "INSERT INTO " + getConfig(start).getRecipeUserTenantsTable()
                + "(app_id, recipe_user_id, tenant_id, recipe_id, account_info_type, third_party_id, third_party_user_id, account_info_value)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
    }

    public static String getPrimaryUserTenantBatchQuery (Start start) {
        return "INSERT INTO " + getConfig(start).getPrimaryUserTenantsTable()
                + "(app_id, tenant_id, primary_user_id, account_info_type, account_info_value)"
                + " VALUES(?, ?, ?, ?, ?)";
    }

    public static void reservePrimaryUserAccountInfos_Transaction(Start start, TransactionConnection con, List<PrimaryUser> primaryUsers)
            throws SQLException, StorageQueryException {
        String QUERY = getPrimaryUserTenantBatchQuery(start);
        Connection sqlCon = (Connection) con.getConnection();
        List<PreparedStatementValueSetter> primaryUserTenantSetters = new ArrayList<>();

        for (var user : primaryUsers) {
            for (var accountInfo : user.accountInfos) {
                for (String tenantId : user.tenantIds) {
                    primaryUserTenantSetters.add(pst -> {
                        pst.setString(1, user.appIdentifier.getAppId());
                        pst.setString(2, tenantId);
                        pst.setString(3, user.primaryUserId);
                        pst.setString(4, accountInfo.type.toString());
                        pst.setString(5, accountInfo.value);
                    });
                }
            }
        }

        executeBatch(sqlCon, QUERY, primaryUserTenantSetters);
    }
}


