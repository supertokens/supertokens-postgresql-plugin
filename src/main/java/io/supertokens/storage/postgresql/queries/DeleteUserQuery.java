/*
 *    Copyright (c) 2021, VRAI Labs and/or its affiliates. All rights reserved.
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

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;

import javax.annotation.Nonnull;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DeleteUserQuery {
    private final Start start;

    public DeleteUserQuery(Start start) {
        this.start = start;
    }

    public boolean execute(@Nonnull String userId) throws StorageQueryException, StorageTransactionLogicException {
        return start.startTransaction(transaction -> {
            Connection conn = (Connection) transaction.getConnection();

            try {
                if (!doesUserExist(conn, userId)) {
                    return false;
                }

                deleteUserSessionHandles(conn, userId);
                deleteEmailVerificationData(conn, userId);
                deleteResetPasswordData(conn, userId);
                deleteUser(conn, userId);
                deleteEmailPasswordUser(conn, userId);
                deleteThirdPartyUser(conn, userId);

                conn.commit();

                return true;
            } catch (SQLException e) {
                throw new StorageQueryException(e);
            }
        });
    }

    private void deleteUser(Connection conn, @Nonnull String userId) throws SQLException {
        String query = "DELETE FROM " + Config.getConfig(start).getUsersTable() + " WHERE user_id = ?";

        try (PreparedStatement pst = conn.prepareStatement(query)) {
            pst.setString(1, userId);
            pst.executeQuery();
        }
    }

    private void deleteEmailPasswordUser(Connection conn, @Nonnull String userId) throws SQLException {
        String query = "DELETE FROM " + Config.getConfig(start).getThirdPartyUsersTable() + " WHERE user_id = ?";

        try (PreparedStatement pst = conn.prepareStatement(query)) {

            pst.setString(1, userId);
            pst.executeQuery();
        }
    }

    private void deleteThirdPartyUser(Connection conn, @Nonnull String userId) throws SQLException {
        String query = "DELETE FROM " + Config.getConfig(start).getEmailPasswordUsersTable() + " WHERE user_id = ?";

        try (PreparedStatement pst = conn.prepareStatement(query)) {
            pst.setString(1, userId);
            pst.executeQuery();
        }
    }

    private String getUserRecipeId(Connection conn, @Nonnull String userId) throws SQLException {
        String query = "SELECT recipe_id FROM " + Config.getConfig(start).getUsersTable() + " WHERE user_id = ?";

        try (PreparedStatement pst = conn.prepareStatement(query)) {
            pst.setString(1, userId);

            ResultSet result = pst.executeQuery();

            result.next();

            return result.getString("recipe_id");
        }
    }

    private boolean doesUserExist(Connection conn, @Nonnull String userId) throws SQLException {
        String query = "SELECT user_id FROM " + Config.getConfig(start).getUsersTable() + " WHERE user_id = ?";

        try (PreparedStatement pst = conn.prepareStatement(query)) {
            pst.setString(1, userId);
            ResultSet result = pst.executeQuery();

            return result.next();
        }
    }

    private String[] getUserSessionHandles(Connection conn, @Nonnull String userId) throws SQLException {
        String query = "SELECT user_id FROM " + Config.getConfig(start).getSessionInfoTable() + " WHERE user_id = ?";

        try (PreparedStatement pst = conn.prepareStatement(query)) {
            pst.setString(1, userId);
            ResultSet result = pst.executeQuery();

            List<String> sessionHandles = new ArrayList<>();

            while (result.next()) {
                sessionHandles.add(result.getString("session_handle"));
            }

            return sessionHandles.toArray(String[]::new);
        }
    }

    private void deleteUserSessionHandles(Connection conn, @Nonnull String userId) throws SQLException {
        String[] sessionHandles = getUserSessionHandles(conn, userId);

        StringBuilder query = new StringBuilder("DELETE FROM " + Config.getConfig(start).getSessionInfoTable() + "WHERE session_handle IN ");

        query.append("(");
        query.append(
                Arrays.stream(sessionHandles)
                    .map(sessionHandle -> "?")
                    .collect(Collectors.joining(", "))
        );
        query.append(")");

        try (PreparedStatement pst = conn.prepareStatement(query.toString())) {
            for (int i = 0; i < sessionHandles.length; i++) {
                // Prepared statements index from 1
                int pstIndex = i + 1;

                pst.setString(pstIndex, sessionHandles[i]);
            }

            pst.executeQuery();
        }
    }

    private void deleteEmailVerificationData(Connection conn, @Nonnull String userId) throws SQLException {
        String query = "DELETE FROM " + Config.getConfig(start).getEmailVerificationTokensTable()
                + " WHERE user_id = ?";

        try (PreparedStatement pst = conn.prepareStatement(query)) {
            pst.setString(1, userId);
            pst.executeQuery();
        }
    }

    private void deleteResetPasswordData(Connection conn, @Nonnull String userId) throws SQLException {
        String query = "DELETE FROM " + Config.getConfig(start).getPasswordResetTokensTable() + " WHERE user_id = ?";

        try (PreparedStatement pst = conn.prepareStatement(query)) {
            pst.setString(1, userId);
            pst.executeQuery();
        }
    }
}
