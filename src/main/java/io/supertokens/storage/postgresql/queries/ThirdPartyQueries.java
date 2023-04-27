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

import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.thirdparty.CreateUserInfo;
import io.supertokens.pluginInterface.thirdparty.UserInfo;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static io.supertokens.pluginInterface.RECIPE_ID.THIRD_PARTY;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class ThirdPartyQueries {

    static String getQueryToCreateUsersTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String thirdPartyUsersTable = Config.getConfig(start).getThirdPartyUsersTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + thirdPartyUsersTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "third_party_id VARCHAR(28) NOT NULL,"
                + "third_party_user_id VARCHAR(256) NOT NULL,"
                + "user_id CHAR(36) NOT NULL,"
                + "email VARCHAR(256) NOT NULL,"
                + "time_joined BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUsersTable, "app_id", "fkey")
                + " FOREIGN KEY(app_id, user_id)"
                + " REFERENCES " + Config.getConfig(start).getAppIdToUserIdTable() +  " (app_id, user_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUsersTable, null, "pkey")
                + " PRIMARY KEY (app_id, user_id)"
                + ");";
        // @formatter:on
    }

    public static String getQueryToThirdPartyUserEmailIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS thirdparty_users_email_index ON "
                + Config.getConfig(start).getThirdPartyUsersTable() + " (app_id, email);";
    }

    public static String getQueryToThirdPartyUserIdIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS thirdparty_users_thirdparty_user_id_index ON "
                + Config.getConfig(start).getThirdPartyUsersTable() + " (app_id, third_party_id, third_party_user_id);";
    }

    static String getQueryToCreateThirdPartyUserToTenantTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String thirdPartyUserToTenantTable = Config.getConfig(start).getThirdPartyUserToTenantTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + thirdPartyUserToTenantTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "user_id CHAR(36) NOT NULL,"
                + "third_party_id VARCHAR(28) NOT NULL,"
                + "third_party_user_id VARCHAR(256) NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUserToTenantTable, "third_party_user_id", "key")
                + " UNIQUE (app_id, tenant_id, third_party_id, third_party_user_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUserToTenantTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, user_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, thirdPartyUserToTenantTable, "user_id", "fkey")
                + " FOREIGN KEY (app_id, tenant_id, user_id)"
                + " REFERENCES " + Config.getConfig(start).getUsersTable() + "(app_id, tenant_id, user_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static void signUp(Start start, TenantIdentifier tenantIdentifier, CreateUserInfo userInfo)
            throws StorageQueryException, StorageTransactionLogicException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                { // app_id_to_user_id
                    String QUERY = "INSERT INTO " + getConfig(start).getAppIdToUserIdTable()
                            + "(app_id, user_id, recipe_id)" + " VALUES(?, ?, ?)";
                    update(sqlCon, QUERY, pst -> {
                        pst.setString(1, tenantIdentifier.getAppId());
                        pst.setString(2, userInfo.id);
                        pst.setString(3, THIRD_PARTY.toString());
                    });
                }

                { // all_auth_recipe_users
                    String QUERY = "INSERT INTO " + getConfig(start).getUsersTable()
                            + "(app_id, tenant_id, user_id, recipe_id, time_joined)" + " VALUES(?, ?, ?, ?, ?)";
                    update(sqlCon, QUERY, pst -> {
                        pst.setString(1, tenantIdentifier.getAppId());
                        pst.setString(2, tenantIdentifier.getTenantId());
                        pst.setString(3, userInfo.id);
                        pst.setString(4, THIRD_PARTY.toString());
                        pst.setLong(5, userInfo.timeJoined);
                    });
                }

                { // thirdparty_users
                    String QUERY = "INSERT INTO " + getConfig(start).getThirdPartyUsersTable()
                            + "(app_id, third_party_id, third_party_user_id, user_id, email, time_joined)"
                            + " VALUES(?, ?, ?, ?, ?, ?)";
                    update(sqlCon, QUERY, pst -> {
                        pst.setString(1, tenantIdentifier.getAppId());
                        pst.setString(2, userInfo.thirdParty.id);
                        pst.setString(3, userInfo.thirdParty.userId);
                        pst.setString(4, userInfo.id);
                        pst.setString(5, userInfo.email);
                        pst.setLong(6, userInfo.timeJoined);
                    });
                }

                { // thirdparty_user_to_tenant
                    String QUERY = "INSERT INTO " + getConfig(start).getThirdPartyUserToTenantTable()
                            + "(app_id, tenant_id, user_id, third_party_id, third_party_user_id)"
                            + " VALUES(?, ?, ?, ?, ?)";
                    update(sqlCon, QUERY, pst -> {
                        pst.setString(1, tenantIdentifier.getAppId());
                        pst.setString(2, tenantIdentifier.getTenantId());
                        pst.setString(3, userInfo.id);
                        pst.setString(4, userInfo.thirdParty.id);
                        pst.setString(5, userInfo.thirdParty.userId);
                    });
                }

                sqlCon.commit();
            } catch (SQLException throwables) {
                throw new StorageTransactionLogicException(throwables);
            }
            return null;
        });
    }

    public static void deleteUser(Start start, AppIdentifier appIdentifier, String userId)
            throws StorageQueryException, StorageTransactionLogicException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                {
                    String QUERY = "DELETE FROM " + getConfig(start).getAppIdToUserIdTable()
                            + " WHERE app_id = ? AND user_id = ?";

                    update(sqlCon, QUERY, pst -> {
                        pst.setString(1, appIdentifier.getAppId());
                        pst.setString(2, userId);
                    });
                }

                sqlCon.commit();
            } catch (SQLException throwables) {
                throw new StorageTransactionLogicException(throwables);
            }
            return null;
        });
    }

    public static UserInfo getThirdPartyUserInfoUsingId(Start start, AppIdentifier appIdentifier, String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT user_id, third_party_id, third_party_user_id, email, time_joined FROM "
                + getConfig(start).getThirdPartyUsersTable() + " WHERE app_id = ? AND user_id = ?";

        ThirdPartyUserInfo userInfo = execute(start, QUERY.toString(), pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        }, result -> {
            if (result.next()) {
                return UserInfoRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
        return userInfoWithTenantIds(start, userInfo);
    }

    public static List<UserInfo> getUsersInfoUsingIdList(Start start, List<String> ids)
            throws SQLException, StorageQueryException {
        if (ids.size() > 0) {
            // No need to filter based on tenantId because the id list is already filtered for a tenant
            StringBuilder QUERY = new StringBuilder(
                    "SELECT user_id, third_party_id, third_party_user_id, email, time_joined "
                            + "FROM " + getConfig(start).getThirdPartyUsersTable());
            QUERY.append(" WHERE user_id IN (");
            for (int i = 0; i < ids.size(); i++) {

                QUERY.append("?");
                if (i != ids.size() - 1) {
                    // not the last element
                    QUERY.append(",");
                }
            }
            QUERY.append(")");

            List<ThirdPartyUserInfo> userInfos = execute(start, QUERY.toString(), pst -> {
                for (int i = 0; i < ids.size(); i++) {
                    // i+1 cause this starts with 1 and not 0
                    pst.setString(i + 1, ids.get(i));
                }
            }, result -> {
                List<ThirdPartyUserInfo> finalResult = new ArrayList<>();
                while (result.next()) {
                    finalResult.add(UserInfoRowMapper.getInstance().mapOrThrow(result));
                }
                return finalResult;
            });
            return userInfoWithTenantIds(start, userInfos);
        }
        return Collections.emptyList();
    }

    public static UserInfo getThirdPartyUserInfoUsingId(Start start, TenantIdentifier tenantIdentifier,
                                                        String thirdPartyId, String thirdPartyUserId)
            throws SQLException, StorageQueryException {

        String QUERY = "SELECT tp_users.user_id as user_id, tp_users.third_party_id as third_party_id, "
                + "tp_users.third_party_user_id as third_party_user_id, tp_users.email as email, "
                + "tp_users.time_joined as time_joined "
                + "FROM " + getConfig(start).getThirdPartyUserToTenantTable() + " AS tp_users_to_tenant "
                + "JOIN " + getConfig(start).getThirdPartyUsersTable() + " AS tp_users "
                + "ON tp_users.app_id = tp_users_to_tenant.app_id AND tp_users.user_id = tp_users_to_tenant.user_id "
                + "WHERE tp_users_to_tenant.app_id = ? AND tp_users_to_tenant.tenant_id = ? "
                + "AND tp_users_to_tenant.third_party_id = ? AND tp_users_to_tenant.third_party_user_id = ?";

        ThirdPartyUserInfo userInfo = execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, thirdPartyId);
            pst.setString(4, thirdPartyUserId);
        }, result -> {
            if (result.next()) {
                return UserInfoRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
        return userInfoWithTenantIds(start, userInfo);
    }

    public static void updateUserEmail_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                   String thirdPartyId, String thirdPartyUserId, String newEmail)
            throws SQLException, StorageQueryException {
        String QUERY = "UPDATE " + getConfig(start).getThirdPartyUsersTable()
                + " SET email = ? WHERE app_id = ? AND third_party_id = ? AND third_party_user_id = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, newEmail);
            pst.setString(2, appIdentifier.getAppId());
            pst.setString(3, thirdPartyId);
            pst.setString(4, thirdPartyUserId);
        });
    }

    public static UserInfo getUserInfoUsingId_Transaction(Start start, Connection con,
                                                          AppIdentifier appIdentifier, String thirdPartyId,
                                                          String thirdPartyUserId)
            throws SQLException, StorageQueryException {

        String QUERY = "SELECT user_id, third_party_id, third_party_user_id, email, time_joined FROM "
                + getConfig(start).getThirdPartyUsersTable()
                + " WHERE app_id = ?  AND third_party_id = ? AND third_party_user_id = ? FOR UPDATE";
        ThirdPartyUserInfo userInfo = execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, thirdPartyId);
            pst.setString(3, thirdPartyUserId);
        }, result -> {
            if (result.next()) {
                return UserInfoRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
        return userInfoWithTenantIds(start, con, userInfo);
    }

    private static ThirdPartyUserInfo getUserInfoUsingUserId(Start start, Connection con,
                                                  AppIdentifier appIdentifier, String userId)
            throws SQLException, StorageQueryException {

        // we don't need a FOR UPDATE here because this is already part of a transaction, and locked on app_id_to_user_id table
        String QUERY = "SELECT user_id, third_party_id, third_party_user_id, email, time_joined FROM "
                + getConfig(start).getThirdPartyUsersTable()
                + " WHERE app_id = ?  AND user_id = ?";
        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        }, result -> {
            if (result.next()) {
                return UserInfoRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static UserInfo[] getThirdPartyUsersByEmail(Start start, TenantIdentifier tenantIdentifier,
                                                       @NotNull String email)
            throws SQLException, StorageQueryException {

        String QUERY = "SELECT tp_users.user_id as user_id, tp_users.third_party_id as third_party_id, "
                + "tp_users.third_party_user_id as third_party_user_id, tp_users.email as email, "
                + "tp_users.time_joined as time_joined "
                + "FROM " + getConfig(start).getThirdPartyUsersTable() + " AS tp_users "
                + "JOIN " + getConfig(start).getThirdPartyUserToTenantTable() + " AS tp_users_to_tenant "
                + "ON tp_users.app_id = tp_users_to_tenant.app_id AND tp_users.user_id = tp_users_to_tenant.user_id "
                + "WHERE tp_users_to_tenant.app_id = ? AND tp_users_to_tenant.tenant_id = ? AND tp_users.email = ? "
                + "ORDER BY time_joined";

        List<ThirdPartyUserInfo> userInfos = execute(start, QUERY.toString(), pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, email);
        }, result -> {
            List<ThirdPartyUserInfo> finalResult = new ArrayList<>();
            while (result.next()) {
                finalResult.add(UserInfoRowMapper.getInstance().mapOrThrow(result));
            }
            return finalResult;
        });
        return userInfoWithTenantIds(start, userInfos).toArray(new UserInfo[0]);
    }

    public static boolean addUserIdToTenant_Transaction(Start start, Connection sqlCon, TenantIdentifier tenantIdentifier, String userId)
            throws SQLException, StorageQueryException {
        ThirdPartyUserInfo userInfo = ThirdPartyQueries.getUserInfoUsingUserId(start, sqlCon,
                tenantIdentifier.toAppIdentifier(), userId);

        { // all_auth_recipe_users
            String QUERY = "INSERT INTO " + getConfig(start).getUsersTable()
                    + "(app_id, tenant_id, user_id, recipe_id, time_joined)"
                    + " VALUES(?, ?, ?, ?, ?)" + " ON CONFLICT DO NOTHING";
            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, userInfo.id);
                pst.setString(4, THIRD_PARTY.toString());
                pst.setLong(5, userInfo.timeJoined);
            });
        }

        { // thirdparty_user_to_tenant
            String QUERY = "INSERT INTO " + getConfig(start).getThirdPartyUserToTenantTable()
                    + "(app_id, tenant_id, user_id, third_party_id, third_party_user_id)"
                    + " VALUES(?, ?, ?, ?, ?)" + " ON CONFLICT DO NOTHING";
            int numRows = update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, userInfo.id);
                pst.setString(4, userInfo.thirdParty.id);
                pst.setString(5, userInfo.thirdParty.userId);
            });

            return numRows > 0;
        }
    }

    public static boolean removeUserIdFromTenant_Transaction(Start start, Connection sqlCon, TenantIdentifier tenantIdentifier, String userId)
            throws SQLException, StorageQueryException {
        { // all_auth_recipe_users
            String QUERY = "DELETE FROM " + getConfig(start).getUsersTable()
                    + " WHERE app_id = ? AND tenant_id = ? and user_id = ? and recipe_id = ?";
            int numRows = update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, userId);
                pst.setString(4, THIRD_PARTY.toString());
            });

            return numRows > 0;
        }

        // automatically deleted from thirdparty_user_to_tenant because of foreign key constraint
    }

    private static UserInfo userInfoWithTenantIds(Start start, ThirdPartyUserInfo userInfo)
            throws SQLException, StorageQueryException {
        if (userInfo == null) return null;
        return userInfoWithTenantIds(start, Arrays.asList(userInfo)).get(0);
    }

    private static List<UserInfo> userInfoWithTenantIds(Start start, List<ThirdPartyUserInfo> userInfos)
            throws SQLException, StorageQueryException {
        String[] userIds = new String[userInfos.size()];
        for (int i = 0; i < userInfos.size(); i++) {
            userIds[i] = userInfos.get(i).id;
        }

        Map<String, List<String>> tenantIdsForUserIds = GeneralQueries.getTenantIdsForUserIds(start, userIds);
        List<UserInfo> result = new ArrayList<>();
        for (ThirdPartyUserInfo userInfo : userInfos) {
            result.add(new UserInfo(userInfo.id, userInfo.email, userInfo.thirdParty, userInfo.timeJoined,
                    tenantIdsForUserIds.get(userInfo.id).toArray(new String[0])));
        }

        return result;
    }

    private static UserInfo userInfoWithTenantIds(Start start, Connection sqlCon, ThirdPartyUserInfo userInfo)
            throws SQLException, StorageQueryException {
        if (userInfo == null) return null;
        return userInfoWithTenantIds(start, sqlCon, Arrays.asList(userInfo)).get(0);
    }

    private static List<UserInfo> userInfoWithTenantIds(Start start, Connection sqlCon, List<ThirdPartyUserInfo> userInfos)
            throws SQLException, StorageQueryException {
        String[] userIds = new String[userInfos.size()];
        for (int i = 0; i < userInfos.size(); i++) {
            userIds[i] = userInfos.get(i).id;
        }

        Map<String, List<String>> tenantIdsForUserIds = GeneralQueries.getTenantIdsForUserIds(start, sqlCon, userIds);
        List<UserInfo> result = new ArrayList<>();
        for (ThirdPartyUserInfo userInfo : userInfos) {
            result.add(new UserInfo(userInfo.id, userInfo.email, userInfo.thirdParty, userInfo.timeJoined,
                    tenantIdsForUserIds.get(userInfo.id).toArray(new String[0])));
        }

        return result;
    }

    private static class ThirdPartyUserInfo {
        public final String id;
        public final String email;
        public final UserInfo.ThirdParty thirdParty;
        public final long timeJoined;

        public ThirdPartyUserInfo(String id, String email, UserInfo.ThirdParty thirdParty, long timeJoined) {
            this.id = id;
            this.email = email;
            this.thirdParty = thirdParty;
            this.timeJoined = timeJoined;
        }
    }

    private static class UserInfoRowMapper implements RowMapper<ThirdPartyUserInfo, ResultSet> {
        private static final UserInfoRowMapper INSTANCE = new UserInfoRowMapper();

        private UserInfoRowMapper() {
        }

        private static UserInfoRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public ThirdPartyUserInfo map(ResultSet result) throws Exception {
            return new ThirdPartyUserInfo(result.getString("user_id"), result.getString("email"),
                    new UserInfo.ThirdParty(result.getString("third_party_id"),
                            result.getString("third_party_user_id")),
                    result.getLong("time_joined"));
        }
    }
}
