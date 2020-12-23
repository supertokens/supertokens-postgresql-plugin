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
import io.supertokens.pluginInterface.mapper.RowMapper;
import io.supertokens.pluginInterface.session.SessionInfo;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SessionQueries {

    public static String getQueryToCreateSessionInfoTable(Start start) {
        return "CREATE TABLE IF NOT EXISTS " + Config.getConfig(start).getSessionInfoTable() + " ("
                + "session_handle VARCHAR(255) NOT NULL," + "user_id VARCHAR(128) NOT NULL,"
                + "refresh_token_hash_2 VARCHAR(128) NOT NULL," + "session_data TEXT,"
                + "expires_at BIGINT  NOT NULL," + "created_at_time BIGINT NOT NULL," +
                "jwt_user_payload TEXT," + "PRIMARY KEY(session_handle)" + " );";

    }

    public static void createNewSession(Start start, String sessionHandle, String userId, String refreshTokenHash2,
                                        JsonObject userDataInDatabase, long expiry, JsonObject userDataInJWT,
                                        long createdAtTime)
            throws SQLException {
        String QUERY = "INSERT INTO " + Config.getConfig(start).getSessionInfoTable()
                + "(session_handle, user_id, refresh_token_hash_2, session_data, expires_at, jwt_user_payload, " +
                "created_at_time)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?)";

        try (Connection con = ConnectionPool.getConnection(start);
             PreparedStatement pst = con.prepareStatement(QUERY)) {
            pst.setString(1, sessionHandle);
            pst.setString(2, userId);
            pst.setString(3, refreshTokenHash2);
            pst.setString(4, userDataInDatabase.toString());
            pst.setLong(5, expiry);
            pst.setString(6, userDataInJWT.toString());
            pst.setLong(7, createdAtTime);
            pst.executeUpdate();
        }
    }

    static boolean isSessionBlacklisted(Start start, String sessionHandle) throws SQLException {
        String QUERY = "SELECT session_handle FROM " + Config.getConfig(start).getSessionInfoTable()
                + " WHERE session_handle = ?";

        try (Connection con = ConnectionPool.getConnection(start);
             PreparedStatement pst = con.prepareStatement(QUERY)) {
            pst.setString(1, sessionHandle);
            ResultSet result = pst.executeQuery();
            return !result.next();
        }
    }

    public static SessionInfo getSessionInfo_Transaction(Start start, Connection con, String sessionHandle)
            throws SQLException {
        String QUERY = "SELECT session_handle, user_id, refresh_token_hash_2, session_data, expires_at, " +
                "created_at_time, jwt_user_payload FROM "
                + Config.getConfig(start).getSessionInfoTable() + " WHERE session_handle = ? FOR UPDATE";
        try (PreparedStatement pst = con.prepareStatement(QUERY)) {
            pst.setString(1, sessionHandle);
            ResultSet result = pst.executeQuery();
            RowMapper<SessionInfo> mapper = RowMapper.getSessionInfoMapper();
            if (result.next()) {
                return mapper.map(result);
            }
        }
        return null;
    }

    public static void updateSessionInfo_Transaction(Start start, Connection con, String sessionHandle,
                                                     String refreshTokenHash2, long expiry) throws SQLException {
        String QUERY = "UPDATE " + Config.getConfig(start).getSessionInfoTable()
                + " SET refresh_token_hash_2 = ?, expires_at = ?"
                + " WHERE session_handle = ?";

        try (PreparedStatement pst = con.prepareStatement(QUERY)) {
            pst.setString(1, refreshTokenHash2);
            pst.setLong(2, expiry);
            pst.setString(3, sessionHandle);
            pst.executeUpdate();
        }
    }

    public static int getNumberOfSessions(Start start) throws SQLException {
        String QUERY = "SELECT count(*) as num FROM " + Config.getConfig(start).getSessionInfoTable();

        try (Connection con = ConnectionPool.getConnection(start);
             PreparedStatement pst = con.prepareStatement(QUERY)) {
            ResultSet result = pst.executeQuery();
            if (result.next()) {
                return result.getInt("num");
            }
            throw new SQLException("Should not have come here.");
        }
    }

    public static int deleteSession(Start start, String[] sessionHandles) throws SQLException {
        if (sessionHandles.length == 0) {
            return 0;
        }
        StringBuilder QUERY = new StringBuilder("DELETE FROM " + Config.getConfig(start).getSessionInfoTable() +
                " WHERE session_handle IN (");
        for (int i = 0; i < sessionHandles.length; i++) {
            if (i == sessionHandles.length - 1) {
                QUERY.append("?)");
            } else {
                QUERY.append("?, ");
            }
        }

        try (Connection con = ConnectionPool.getConnection(start);
             PreparedStatement pst = con.prepareStatement(QUERY.toString())) {
            for (int i = 0; i < sessionHandles.length; i++) {
                pst.setString(i + 1, sessionHandles[i]);
            }
            return pst.executeUpdate();
        }
    }

    public static String[] getAllSessionHandlesForUser(Start start, String userId) throws SQLException {
        String QUERY = "SELECT session_handle FROM " + Config.getConfig(start).getSessionInfoTable() +
                " WHERE user_id = ?";

        try (Connection con = ConnectionPool.getConnection(start);
             PreparedStatement pst = con.prepareStatement(QUERY)) {
            pst.setString(1, userId);
            ResultSet result = pst.executeQuery();
            List<String> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(result.getString("session_handle"));
            }
            String[] finalResult = new String[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        }
    }

    public static void deleteAllExpiredSessions(Start start) throws SQLException {
        String QUERY = "DELETE FROM " + Config.getConfig(start).getSessionInfoTable() +
                " WHERE expires_at <= ?";

        try (Connection con = ConnectionPool.getConnection(start);
             PreparedStatement pst = con.prepareStatement(QUERY)) {
            pst.setLong(1, System.currentTimeMillis());
            pst.executeUpdate();
        }
    }

    public static int updateSession(Start start, String sessionHandle, @Nullable JsonObject sessionData,
                                    @Nullable JsonObject jwtPayload) throws SQLException {

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
        QUERY += " WHERE session_handle = ?";

        int currIndex = 1;
        try (Connection con = ConnectionPool.getConnection(start);
             PreparedStatement pst = con.prepareStatement(QUERY)) {
            if (sessionData != null) {
                pst.setString(currIndex, sessionData.toString());
                currIndex++;
            }
            if (jwtPayload != null) {
                pst.setString(currIndex, jwtPayload.toString());
                currIndex++;
            }
            pst.setString(currIndex, sessionHandle);
            return pst.executeUpdate();
        }
    }

    public static SessionInfo getSession(Start start, String sessionHandle)
            throws SQLException {
        String QUERY = "SELECT session_handle, user_id, refresh_token_hash_2, session_data, expires_at, " +
                "created_at_time, jwt_user_payload FROM "
                + Config.getConfig(start).getSessionInfoTable() + " WHERE session_handle = ?";
        try (Connection con = ConnectionPool.getConnection(start);
             PreparedStatement pst = con.prepareStatement(QUERY)) {
            pst.setString(1, sessionHandle);
            ResultSet result = pst.executeQuery();
            RowMapper<SessionInfo> mapper = RowMapper.getSessionInfoMapper();
            if (result.next()) {
                return mapper.map(result);
            }
        }
        return null;
    }
}
