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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.session.SessionInfo;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;
import static java.lang.System.currentTimeMillis;

public class SessionQueries {

    public static String getQueryToCreateSessionInfoTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String sessionInfoTable = Config.getConfig(start).getSessionInfoTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + sessionInfoTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "session_handle VARCHAR(255) NOT NULL,"
                + "user_id VARCHAR(128) NOT NULL,"
                + "refresh_token_hash_2 VARCHAR(128) NOT NULL,"
                + "session_data TEXT,"
                + "expires_at BIGINT NOT NULL,"
                + "created_at_time BIGINT NOT NULL,"
                + "jwt_user_payload TEXT,"
                + "use_static_key BOOLEAN NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, sessionInfoTable, null, "pkey")
                + " PRIMARY KEY(app_id, tenant_id, session_handle),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, sessionInfoTable, "tenant_id", "fkey")
                + " FOREIGN KEY (app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantsTable() + "(app_id, tenant_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static String getQueryToCreateTenantIdIndexForSessionInfoTable(Start start) {
        return "CREATE INDEX session_info_tenant_id_index ON "
                + Config.getConfig(start).getSessionInfoTable() + "(app_id, tenant_id);";
    }

    static String getQueryToCreateAccessTokenSigningKeysTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String accessTokenSigningKeysTable = Config.getConfig(start).getAccessTokenSigningKeysTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + accessTokenSigningKeysTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "created_at_time BIGINT NOT NULL,"
                + "value TEXT,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, accessTokenSigningKeysTable, null, "pkey")
                + " PRIMARY KEY(app_id, created_at_time),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, accessTokenSigningKeysTable, "app_id", "fkey")
                + " FOREIGN KEY (app_id)"
                + " REFERENCES " + Config.getConfig(start).getAppsTable() + "(app_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static String getQueryToCreateAppIdIndexForAccessTokenSigningKeysTable(Start start) {
        return "CREATE INDEX access_token_signing_keys_app_id_index ON "
                + Config.getConfig(start).getAccessTokenSigningKeysTable() + "(app_id);";
    }

    static String getQueryToCreateSessionExpiryIndex(Start start) {
        return "CREATE INDEX session_expiry_index ON "
                + Config.getConfig(start).getSessionInfoTable() + "(expires_at);";
    }

    static String getQueryToCreateSessionAppIdUserIdIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS session_info_user_id_app_id_index ON "
                + Config.getConfig(start).getSessionInfoTable() + "(user_id, app_id);";
    }

    public static void createNewSession(Start start, TenantIdentifier tenantIdentifier, String sessionHandle,
                                        String userId, String refreshTokenHash2,
                                        JsonObject userDataInDatabase, long expiry, JsonObject userDataInJWT,
                                        long createdAtTime, boolean useStaticKey)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getSessionInfoTable()
                + "(app_id, tenant_id, session_handle, user_id, refresh_token_hash_2, session_data, expires_at,"
                + " jwt_user_payload, created_at_time, use_static_key)" + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        update(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, sessionHandle);
            pst.setString(4, userId);
            pst.setString(5, refreshTokenHash2);
            pst.setString(6, userDataInDatabase.toString());
            pst.setLong(7, expiry);
            pst.setString(8, userDataInJWT.toString());
            pst.setLong(9, createdAtTime);
            pst.setBoolean(10, useStaticKey);
        });
    }

    public static SessionInfo getSessionInfo_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier,
                                                         String sessionHandle)
            throws SQLException, StorageQueryException {
        String QUERY =
                "SELECT session_handle, user_id, refresh_token_hash_2, session_data, " +
                        "expires_at, created_at_time, jwt_user_payload, use_static_key FROM " +
                        getConfig(start).getSessionInfoTable()
                        + " WHERE app_id = ? AND tenant_id = ? AND session_handle = ? FOR UPDATE";
        SessionInfo sessionInfo = execute(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, sessionHandle);
        }, result -> {
            if (result.next()) {
                return SessionInfoRowMapper.getInstance().mapOrThrow(result, false);
            }
            return null;
        });

        if (sessionInfo == null) {
            return null;
        }

        QUERY = "SELECT external_user_id, 0 as o " +
                "FROM " + getConfig(start).getUserIdMappingTable() + " um2 " +
                "WHERE um2.app_id = ? AND um2.supertokens_user_id IN (" +
                    "SELECT primary_or_recipe_user_id " +
                    "FROM " + getConfig(start).getUsersTable() + " " +
                    "WHERE app_id = ? AND user_id IN (" +
                        "SELECT user_id FROM (" +
                            "SELECT um1.supertokens_user_id as user_id, 0 as o1 " +
                            "FROM " + getConfig(start).getUserIdMappingTable() + " um1 " +
                            "WHERE um1.app_id = ? AND um1.external_user_id = ? " +
                            "UNION " +
                            "SELECT ?, 1 as o1 " +
                            "ORDER BY o1 ASC " +
                        ") uid1" +
                    ")" +
                ") " +
                "UNION " +
                "SELECT primary_or_recipe_user_id, 1 as o " +
                "FROM " + getConfig(start).getUsersTable() + " " +
                "WHERE app_id = ? AND user_id IN (" +
                    "SELECT user_ID FROM (" +
                        "SELECT um1.supertokens_user_id as user_id, 0 as o2 " +
                        "FROM " + getConfig(start).getUserIdMappingTable() + " um1 " +
                        "WHERE um1.app_id = ? AND um1.external_user_id = ? " +
                        "UNION " +
                        "SELECT ?, 1 as o2 " +
                        "ORDER BY o2 ASC " +
                    ") uid2 " +
                ") " +
                "ORDER BY o ASC " +
                "LIMIT 1";

        String finalUserId = execute(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getAppId());
            pst.setString(3, tenantIdentifier.getAppId());
            pst.setString(4, sessionInfo.recipeUserId);
            pst.setString(5, sessionInfo.recipeUserId);
            pst.setString(6, tenantIdentifier.getAppId());
            pst.setString(7, tenantIdentifier.getAppId());
            pst.setString(8, sessionInfo.recipeUserId);
            pst.setString(9, sessionInfo.recipeUserId);
        }, result -> {
            if (result.next()) {
                return result.getString(1);
            }
            return sessionInfo.recipeUserId;
        });

        if (finalUserId != null) {
            sessionInfo.userId = finalUserId;
        }

        return sessionInfo;
    }

    public static void updateSessionInfo_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier,
                                                     String sessionHandle,
                                                     String refreshTokenHash2, long expiry, boolean useStaticKey)
            throws SQLException, StorageQueryException {
        String QUERY = "UPDATE " + getConfig(start).getSessionInfoTable()
                + " SET refresh_token_hash_2 = ?, expires_at = ?, use_static_key = ?"
                + " WHERE app_id = ? AND tenant_id = ? AND session_handle = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, refreshTokenHash2);
            pst.setLong(2, expiry);
            pst.setBoolean(3, useStaticKey);
            pst.setString(4, tenantIdentifier.getAppId());
            pst.setString(5, tenantIdentifier.getTenantId());
            pst.setString(6, sessionHandle);
        });
    }

    public static int getNumberOfSessions(Start start, TenantIdentifier tenantIdentifier)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT count(*) as num FROM " + getConfig(start).getSessionInfoTable()
                + " WHERE app_id = ? AND tenant_id = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
        }, result -> {
            if (result.next()) {
                return result.getInt("num");
            }
            throw new SQLException("Should not have come here.");
        });
    }

    public static int deleteSession(Start start, TenantIdentifier tenantIdentifier, String[] sessionHandles)
            throws SQLException, StorageQueryException {
        if (sessionHandles.length == 0) {
            return 0;
        }
        StringBuilder QUERY = new StringBuilder(
                "DELETE FROM " + Config.getConfig(start).getSessionInfoTable()
                        + " WHERE app_id = ? AND tenant_id = ? AND session_handle IN (");
        for (int i = 0; i < sessionHandles.length; i++) {
            if (i == sessionHandles.length - 1) {
                QUERY.append("?)");
            } else {
                QUERY.append("?, ");
            }
        }

        return update(start, QUERY.toString(), pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            for (int i = 0; i < sessionHandles.length; i++) {
                pst.setString(i + 3, sessionHandles[i]);
            }
        });
    }

    public static void deleteSessionsOfUser(Start start, AppIdentifier appIdentifier, String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getSessionInfoTable()
                + " WHERE app_id = ? AND user_id = ?";

        update(start, QUERY.toString(), pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        });
    }

    public static void deleteSessionsOfUser_Transaction(Connection sqlCon, Start start, AppIdentifier appIdentifier,
                                                        String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getSessionInfoTable()
                + " WHERE app_id = ? AND user_id = ?";

        update(sqlCon, QUERY.toString(), pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        });
    }

    public static boolean deleteSessionsOfUser(Start start, TenantIdentifier tenantIdentifier, String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getSessionInfoTable()
                + " WHERE app_id = ? AND tenant_id = ? AND user_id = ?";

        int numRows = update(start, QUERY.toString(), pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, userId);
        });
        return numRows > 0;
    }

    public static String[] getAllNonExpiredSessionHandlesForUser(Start start, TenantIdentifier tenantIdentifier,
                                                                 String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT session_handle FROM " + getConfig(start).getSessionInfoTable()
                + " WHERE app_id = ? AND tenant_id = ? AND user_id = ? AND expires_at >= ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, userId);
            pst.setLong(4, currentTimeMillis());
        }, result -> {
            List<String> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(result.getString("session_handle"));
            }
            String[] finalResult = new String[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        });
    }

    public static String[] getAllNonExpiredSessionHandlesForUser(Start start, AppIdentifier appIdentifier,
                                                                 String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT session_handle FROM " + getConfig(start).getSessionInfoTable()
                + " WHERE app_id = ? AND user_id = ? AND expires_at >= ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
            pst.setLong(3, currentTimeMillis());
        }, result -> {
            List<String> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(result.getString("session_handle"));
            }
            String[] finalResult = new String[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        });
    }

    public static Map<String, List<String>> getAllNonExpiredSessionHandlesForUsers(Start start, AppIdentifier appIdentifier,
                                                                          List<String> userIds)
            throws SQLException, StorageQueryException {
        if(userIds == null || userIds.isEmpty()){
            return new HashMap<>();
        }
        String QUERY = "SELECT user_id, session_handle FROM " + getConfig(start).getSessionInfoTable()
                + " WHERE app_id = ? AND expires_at >= ? AND user_id IN ( " + Utils.generateCommaSeperatedQuestionMarks(userIds.size()) + " )";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, currentTimeMillis());
            for(int i = 0; i < userIds.size() ; i++){
                pst.setString(3 + i, userIds.get(i));
            }
        }, result -> {
            Map<String, List<String>> temp = new HashMap<>();
            while (result.next()) {
                String userId = result.getString("user_id");
                if(!temp.containsKey(userId)){
                    temp.put(userId, new ArrayList<>());
                }
                temp.get(userId).add(result.getString("session_handle"));
            }
            return temp;
        });
    }

    public static void deleteAllExpiredSessions(Start start) throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getSessionInfoTable() + " WHERE expires_at <= ?";

        update(start, QUERY, pst -> pst.setLong(1, currentTimeMillis()));
    }

    public static int updateSession(Start start, TenantIdentifier tenantIdentifier, String sessionHandle,
                                    @Nullable JsonObject sessionData,
                                    @Nullable JsonObject jwtPayload) throws SQLException, StorageQueryException {

        if (sessionData == null && jwtPayload == null) {
            throw new SQLException("sessionData and jwtPayload are null when updating session info");
        }

        String QUERY = "UPDATE " + Config.getConfig(start).getSessionInfoTable() + " SET";
        boolean somethingBefore = false;
        if (sessionData != null) {
            QUERY += " session_data = ?";
            somethingBefore = true;
        }
        if (jwtPayload != null) {
            QUERY += (somethingBefore ? "," : "") + " jwt_user_payload = ?";
        }
        QUERY += " WHERE app_id = ? AND tenant_id = ? AND session_handle = ?";

        return update(start, QUERY, pst -> {
            int currIndex = 1;
            if (sessionData != null) {
                pst.setString(currIndex, sessionData.toString());
                currIndex++;
            }
            if (jwtPayload != null) {
                pst.setString(currIndex, jwtPayload.toString());
                currIndex++;
            }
            pst.setString(currIndex++, tenantIdentifier.getAppId());
            pst.setString(currIndex++, tenantIdentifier.getTenantId());
            pst.setString(currIndex, sessionHandle);
        });
    }

    public static SessionInfo getSession(Start start, TenantIdentifier tenantIdentifier, String sessionHandle)
            throws SQLException, StorageQueryException {
        String QUERY =
                "SELECT sess.session_handle, sess.user_id, sess.refresh_token_hash_2, sess.session_data, sess" +
                        ".expires_at, "
                        +
                        "sess.created_at_time, sess.jwt_user_payload, sess.use_static_key, users" +
                        ".primary_or_recipe_user_id FROM " +
                        getConfig(start).getSessionInfoTable()
                        + " AS sess LEFT JOIN " + getConfig(start).getUsersTable() +
                        " as users ON sess.app_id = users.app_id AND sess.user_id = users.user_id WHERE sess.app_id =" +
                        " ? AND " +
                        "sess.tenant_id = ? AND sess.session_handle = ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, sessionHandle);
        }, result -> {
            if (result.next()) {
                return SessionInfoRowMapper.getInstance().mapOrThrow(result, true);
            }
            return null;
        });
    }

    public static void addAccessTokenSigningKey_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                            long createdAtTime,
                                                            String value) throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getAccessTokenSigningKeysTable()
                + "(app_id, created_at_time, value)"
                + " VALUES(?, ?, ?)";

        update(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, createdAtTime);
            pst.setString(3, value);
        });
    }

    public static KeyValueInfo[] getAccessTokenSigningKeys_Transaction(Start start, Connection con,
                                                                       AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT * FROM " + getConfig(start).getAccessTokenSigningKeysTable()
                + " WHERE app_id = ? FOR UPDATE";

        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
        }, result -> {
            List<KeyValueInfo> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(AccessTokenSigningKeyRowMapper.getInstance().mapOrThrow(result));
            }
            KeyValueInfo[] finalResult = new KeyValueInfo[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        });
    }

    public static void removeAccessTokenSigningKeysBefore(Start start, AppIdentifier appIdentifier, long time)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getAccessTokenSigningKeysTable()
                + " WHERE app_id = ? AND created_at_time < ?";

        update(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, time);
        });
    }

    static class SessionInfoRowMapper {
        public static final SessionInfoRowMapper INSTANCE = new SessionInfoRowMapper();

        private SessionInfoRowMapper() {
        }

        private static SessionInfoRowMapper getInstance() {
            return INSTANCE;
        }

        public SessionInfo mapOrThrow(ResultSet result, boolean hasPrimaryOrRecipeUserId) throws StorageQueryException {
            JsonParser jp = new JsonParser();
            // if result.getString("primary_or_recipe_user_id") is null, it will be handled by SessionInfo
            // constructor
            try {
                return new SessionInfo(result.getString("session_handle"),
                        hasPrimaryOrRecipeUserId ? result.getString("primary_or_recipe_user_id") :
                                result.getString("user_id"),
                        result.getString("user_id"),
                        result.getString("refresh_token_hash_2"),
                        jp.parse(result.getString("session_data")).getAsJsonObject(),
                        result.getLong("expires_at"),
                        jp.parse(result.getString("jwt_user_payload")).getAsJsonObject(),
                        result.getLong("created_at_time"), result.getBoolean("use_static_key"));
            } catch (Exception e) {
                throw new StorageQueryException(e);
            }
        }
    }

    private static class AccessTokenSigningKeyRowMapper implements RowMapper<KeyValueInfo, ResultSet> {
        private static final AccessTokenSigningKeyRowMapper INSTANCE = new AccessTokenSigningKeyRowMapper();

        private AccessTokenSigningKeyRowMapper() {
        }

        private static AccessTokenSigningKeyRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public KeyValueInfo map(ResultSet result) throws Exception {
            return new KeyValueInfo(result.getString("value"), result.getLong("created_at_time"));
        }
    }
}
