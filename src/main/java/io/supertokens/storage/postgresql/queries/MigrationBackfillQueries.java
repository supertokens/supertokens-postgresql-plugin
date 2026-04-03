/*
 *    Copyright (c) 2026, VRAI Labs and/or its affiliates. All rights reserved.
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.Start;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class MigrationBackfillQueries {

    /**
     * Returns the count of users with time_joined = 0, indicating they need backfilling.
     */
    public static int getBackfillPendingUsersCount(Start start, AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT COUNT(*) FROM " + getConfig(start).getAppIdToUserIdTable()
                + " WHERE app_id = ? AND time_joined = 0";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
        }, result -> {
            if (result.next()) {
                return result.getInt(1);
            }
            return 0;
        });
    }

    /**
     * Backfills a batch of users within a transaction.
     * Locks users with SELECT FOR UPDATE, then populates all reservation tables.
     *
     * @return Number of users processed
     */
    public static int backfillUsersBatch(Start start, Connection con, AppIdentifier appIdentifier,
                                          int batchSize) throws SQLException, StorageQueryException {
        // Step 1: Lock batch of unbackfilled users
        String lockQuery = "SELECT user_id, recipe_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user"
                + " FROM " + getConfig(start).getAppIdToUserIdTable()
                + " WHERE app_id = ? AND time_joined = 0"
                + " ORDER BY user_id"
                + " LIMIT ?"
                + " FOR UPDATE";

        List<UserToBackfill> users = execute(con, lockQuery, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setInt(2, batchSize);
        }, result -> {
            List<UserToBackfill> list = new ArrayList<>();
            while (result.next()) {
                list.add(new UserToBackfill(
                        result.getString("user_id").trim(),
                        result.getString("recipe_id"),
                        result.getString("primary_or_recipe_user_id").trim(),
                        result.getBoolean("is_linked_or_is_a_primary_user")
                ));
            }
            return list;
        });

        if (users.isEmpty()) {
            return 0;
        }

        for (UserToBackfill user : users) {
            backfillSingleUser(start, con, appIdentifier, user);
        }

        return users.size();
    }

    private static void backfillSingleUser(Start start, Connection con, AppIdentifier appIdentifier,
                                            UserToBackfill user) throws SQLException, StorageQueryException {
        String appId = appIdentifier.getAppId();

        // Step 2: Backfill time_joined from all_auth_recipe_users
        String updateTimeJoined = "UPDATE " + getConfig(start).getAppIdToUserIdTable()
                + " SET time_joined = COALESCE(("
                + "   SELECT MIN(time_joined) FROM " + getConfig(start).getUsersTable()
                + "   WHERE app_id = ? AND user_id = ?"
                + " ), 0),"
                + " primary_or_recipe_user_time_joined = COALESCE(("
                + "   SELECT MIN(primary_or_recipe_user_time_joined) FROM " + getConfig(start).getUsersTable()
                + "   WHERE app_id = ? AND user_id = ?"
                + " ), 0)"
                + " WHERE app_id = ? AND user_id = ? AND time_joined = 0";

        update(con, updateTimeJoined, pst -> {
            pst.setString(1, appId);
            pst.setString(2, user.userId);
            pst.setString(3, appId);
            pst.setString(4, user.userId);
            pst.setString(5, appId);
            pst.setString(6, user.userId);
        });

        // Step 3: Backfill recipe_user_account_infos based on recipe type
        backfillAccountInfos(start, con, appId, user);

        // Step 4: Backfill recipe_user_tenants
        backfillRecipeUserTenants(start, con, appId, user);

        // Step 5: Backfill primary_user_tenants (only for linked/primary users)
        if (user.isLinkedOrPrimary) {
            backfillPrimaryUserTenants(start, con, appId, user);
        }
    }

    private static void backfillAccountInfos(Start start, Connection con, String appId,
                                              UserToBackfill user) throws SQLException {
        String primaryUserId = user.isLinkedOrPrimary ? user.primaryOrRecipeUserId : null;
        String accountInfosTable = getConfig(start).getRecipeUserAccountInfosTable();

        switch (user.recipeId) {
            case "emailpassword": {
                String QUERY = "INSERT INTO " + accountInfosTable
                        + " (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,"
                        + "  third_party_id, third_party_user_id, primary_user_id)"
                        + " SELECT ep.app_id, ep.user_id, 'emailpassword', 'email', ep.email, '', '', ?"
                        + " FROM " + getConfig(start).getEmailPasswordUsersTable() + " ep"
                        + " WHERE ep.app_id = ? AND ep.user_id = ?"
                        + " ON CONFLICT DO NOTHING";
                update(con, QUERY, pst -> {
                    pst.setString(1, primaryUserId);
                    pst.setString(2, appId);
                    pst.setString(3, user.userId);
                });
                break;
            }
            case "passwordless": {
                // Email entry
                String emailQuery = "INSERT INTO " + accountInfosTable
                        + " (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,"
                        + "  third_party_id, third_party_user_id, primary_user_id)"
                        + " SELECT pu.app_id, pu.user_id, 'passwordless', 'email', pu.email, '', '', ?"
                        + " FROM " + getConfig(start).getPasswordlessUsersTable() + " pu"
                        + " WHERE pu.app_id = ? AND pu.user_id = ? AND pu.email IS NOT NULL"
                        + " ON CONFLICT DO NOTHING";
                update(con, emailQuery, pst -> {
                    pst.setString(1, primaryUserId);
                    pst.setString(2, appId);
                    pst.setString(3, user.userId);
                });

                // Phone entry
                String phoneQuery = "INSERT INTO " + accountInfosTable
                        + " (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,"
                        + "  third_party_id, third_party_user_id, primary_user_id)"
                        + " SELECT pu.app_id, pu.user_id, 'passwordless', 'phone', pu.phone_number, '', '', ?"
                        + " FROM " + getConfig(start).getPasswordlessUsersTable() + " pu"
                        + " WHERE pu.app_id = ? AND pu.user_id = ? AND pu.phone_number IS NOT NULL"
                        + " ON CONFLICT DO NOTHING";
                update(con, phoneQuery, pst -> {
                    pst.setString(1, primaryUserId);
                    pst.setString(2, appId);
                    pst.setString(3, user.userId);
                });
                break;
            }
            case "thirdparty": {
                // EMAIL entry (with thirdPartyId and thirdPartyUserId)
                String emailQuery = "INSERT INTO " + accountInfosTable
                        + " (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,"
                        + "  third_party_id, third_party_user_id, primary_user_id)"
                        + " SELECT tp.app_id, tp.user_id, 'thirdparty', 'email', tp.email,"
                        + "   tp.third_party_id, tp.third_party_user_id, ?"
                        + " FROM " + getConfig(start).getThirdPartyUsersTable() + " tp"
                        + " WHERE tp.app_id = ? AND tp.user_id = ?"
                        + " ON CONFLICT DO NOTHING";
                update(con, emailQuery, pst -> {
                    pst.setString(1, primaryUserId);
                    pst.setString(2, appId);
                    pst.setString(3, user.userId);
                });

                // THIRD_PARTY entry (composite value: thirdPartyId::thirdPartyUserId)
                String tpartyQuery = "INSERT INTO " + accountInfosTable
                        + " (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,"
                        + "  third_party_id, third_party_user_id, primary_user_id)"
                        + " SELECT tp.app_id, tp.user_id, 'thirdparty', 'tparty',"
                        + "   tp.third_party_id || '::' || tp.third_party_user_id,"
                        + "   '', '', ?"
                        + " FROM " + getConfig(start).getThirdPartyUsersTable() + " tp"
                        + " WHERE tp.app_id = ? AND tp.user_id = ?"
                        + " ON CONFLICT DO NOTHING";
                update(con, tpartyQuery, pst -> {
                    pst.setString(1, primaryUserId);
                    pst.setString(2, appId);
                    pst.setString(3, user.userId);
                });
                break;
            }
            case "webauthn": {
                String QUERY = "INSERT INTO " + accountInfosTable
                        + " (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,"
                        + "  third_party_id, third_party_user_id, primary_user_id)"
                        + " SELECT wu.app_id, wu.user_id, 'webauthn', 'email', wu.email, '', '', ?"
                        + " FROM " + getConfig(start).getWebAuthNUsersTable() + " wu"
                        + " WHERE wu.app_id = ? AND wu.user_id = ?"
                        + " ON CONFLICT DO NOTHING";
                update(con, QUERY, pst -> {
                    pst.setString(1, primaryUserId);
                    pst.setString(2, appId);
                    pst.setString(3, user.userId);
                });
                break;
            }
        }
    }

    private static void backfillRecipeUserTenants(Start start, Connection con, String appId,
                                                   UserToBackfill user) throws SQLException {
        String QUERY = "INSERT INTO " + getConfig(start).getRecipeUserTenantsTable()
                + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type, account_info_value,"
                + "  third_party_id, third_party_user_id)"
                + " SELECT u.app_id, u.user_id, u.tenant_id, u.recipe_id,"
                + "   rai.account_info_type, rai.account_info_value,"
                + "   rai.third_party_id, rai.third_party_user_id"
                + " FROM " + getConfig(start).getUsersTable() + " u"
                + " JOIN " + getConfig(start).getRecipeUserAccountInfosTable() + " rai"
                + "   ON u.app_id = rai.app_id AND u.user_id = rai.recipe_user_id"
                + " WHERE u.app_id = ? AND u.user_id = ?"
                + " ON CONFLICT DO NOTHING";

        update(con, QUERY, pst -> {
            pst.setString(1, appId);
            pst.setString(2, user.userId);
        });
    }

    private static void backfillPrimaryUserTenants(Start start, Connection con, String appId,
                                                    UserToBackfill user) throws SQLException {
        String QUERY = "INSERT INTO " + getConfig(start).getPrimaryUserTenantsTable()
                + " (app_id, tenant_id, primary_user_id, account_info_type, account_info_value)"
                + " SELECT DISTINCT rt.app_id, rt.tenant_id, a.primary_or_recipe_user_id,"
                + "   rt.account_info_type, rt.account_info_value"
                + " FROM " + getConfig(start).getRecipeUserTenantsTable() + " rt"
                + " JOIN " + getConfig(start).getAppIdToUserIdTable() + " a"
                + "   ON rt.app_id = a.app_id AND rt.recipe_user_id = a.user_id"
                + " WHERE a.is_linked_or_is_a_primary_user = TRUE AND a.app_id = ? AND a.user_id = ?"
                + " ON CONFLICT DO NOTHING";

        update(con, QUERY, pst -> {
            pst.setString(1, appId);
            pst.setString(2, user.userId);
        });
    }

    /**
     * Verifies backfill completeness by counting users missing from reservation tables.
     */
    public static int verifyBackfillCompleteness(Start start, AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT COUNT(*) FROM " + getConfig(start).getAppIdToUserIdTable() + " a"
                + " WHERE a.app_id = ? AND NOT EXISTS ("
                + "   SELECT 1 FROM " + getConfig(start).getRecipeUserAccountInfosTable() + " rai"
                + "   WHERE rai.app_id = a.app_id AND rai.recipe_user_id = a.user_id"
                + " )";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
        }, result -> {
            if (result.next()) {
                return result.getInt(1);
            }
            return 0;
        });
    }

    private static class UserToBackfill {
        final String userId;
        final String recipeId;
        final String primaryOrRecipeUserId;
        final boolean isLinkedOrPrimary;

        UserToBackfill(String userId, String recipeId, String primaryOrRecipeUserId, boolean isLinkedOrPrimary) {
            this.userId = userId;
            this.recipeId = recipeId;
            this.primaryOrRecipeUserId = primaryOrRecipeUserId;
            this.isLinkedOrPrimary = isLinkedOrPrimary;
        }
    }
}
