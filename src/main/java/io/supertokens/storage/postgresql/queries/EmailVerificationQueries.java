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

import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.emailverification.EmailVerificationTokenInfo;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.sqlStorage.TransactionConnection;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;
import static java.lang.System.currentTimeMillis;

public class EmailVerificationQueries {

    static String getQueryToCreateEmailVerificationTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String emailVerificationTable = Config.getConfig(start).getEmailVerificationTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + emailVerificationTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "user_id VARCHAR(128) NOT NULL,"
                + "email VARCHAR(256) NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, emailVerificationTable, null, "pkey")
                + " PRIMARY KEY (app_id, user_id, email),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, emailVerificationTable, "app_id", "fkey")
                + " FOREIGN KEY(app_id)"
                + " REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static String getQueryToCreateAppIdIndexForEmailVerificationTable(Start start) {
        return "CREATE INDEX emailverification_verified_emails_app_id_index ON "
                + Config.getConfig(start).getEmailVerificationTable() + "(app_id);";
    }

    static String getQueryToCreateEmailVerificationTokensTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String emailVerificationTokensTable = Config.getConfig(start).getEmailVerificationTokensTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + emailVerificationTokensTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "user_id VARCHAR(128) NOT NULL,"
                + "email VARCHAR(256) NOT NULL,"
                + "token VARCHAR(128) NOT NULL CONSTRAINT " +
                Utils.getConstraintName(schema, emailVerificationTokensTable, "token", "key") + " UNIQUE,"
                + "token_expiry BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, emailVerificationTokensTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, user_id, email, token), "
                + "CONSTRAINT " + Utils.getConstraintName(schema, emailVerificationTokensTable, "tenant_id", "fkey")
                + " FOREIGN KEY(app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantsTable() + " (app_id, tenant_id) ON DELETE CASCADE"
                + ")";
        // @formatter:on
    }

    public static String getQueryToCreateTenantIdIndexForEmailVerificationTokensTable(Start start) {
        return "CREATE INDEX emailverification_tokens_tenant_id_index ON "
                + Config.getConfig(start).getEmailVerificationTokensTable() + "(app_id, tenant_id);";
    }

    static String getQueryToCreateEmailVerificationTokenExpiryIndex(Start start) {
        return "CREATE INDEX emailverification_tokens_index ON "
                + Config.getConfig(start).getEmailVerificationTokensTable() + "(token_expiry);";
    }

    public static void deleteExpiredEmailVerificationTokens(Start start) throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getEmailVerificationTokensTable() + " WHERE token_expiry < ?";

        update(start, QUERY, pst -> pst.setLong(1, currentTimeMillis()));
    }

    public static void updateUsersIsEmailVerified_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                              String userId, String email,
                                                              boolean isEmailVerified)
            throws SQLException, StorageQueryException {

        if (isEmailVerified) {
            String QUERY = "INSERT INTO " + getConfig(start).getEmailVerificationTable()
                    + "(app_id, user_id, email) VALUES(?, ?, ?)";

            update(con, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, userId);
                pst.setString(3, email);
            });
        } else {
            String QUERY = "DELETE FROM " + getConfig(start).getEmailVerificationTable()
                    + " WHERE app_id = ? AND user_id = ? AND email = ?";

            update(con, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, userId);
                pst.setString(3, email);
            });
        }
    }

    public static void updateMultipleUsersIsEmailVerified_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                              Map<String, String> emailToUserIds,
                                                              boolean isEmailVerified)
            throws SQLException, StorageQueryException {

        if (isEmailVerified) {
            String QUERY = "INSERT INTO " + getConfig(start).getEmailVerificationTable()
                    + "(app_id, user_id, email) VALUES(?, ?, ?)";
            PreparedStatement insertQuery = con.prepareStatement(QUERY);
            int counter = 0;
            for(Map.Entry<String, String> emailToUser : emailToUserIds.entrySet()){
                insertQuery.setString(1, appIdentifier.getAppId());
                insertQuery.setString(2, emailToUser.getKey());
                insertQuery.setString(3, emailToUser.getValue());
                insertQuery.addBatch();

                counter++;
                if (counter % 100 == 0) {
                    insertQuery.executeBatch();
                }
            }
                insertQuery.executeBatch();
        } else {
            String QUERY = "DELETE FROM " + getConfig(start).getEmailVerificationTable()
                    + " WHERE app_id = ? AND user_id = ? AND email = ?";
            PreparedStatement deleteQuery = con.prepareStatement(QUERY);
            int counter = 0;
            for (Map.Entry<String, String> emailToUser : emailToUserIds.entrySet()) {
                deleteQuery.setString(1, appIdentifier.getAppId());
                deleteQuery.setString(2, emailToUser.getValue());
                deleteQuery.setString(3, emailToUser.getKey());
                deleteQuery.addBatch();

                counter++;
                if (counter % 100 == 0) {
                    deleteQuery.executeBatch();
                }
            }
            deleteQuery.executeBatch();
        }
    }

    public static void deleteAllEmailVerificationTokensForUser_Transaction(Start start, Connection con,
                                                                           TenantIdentifier tenantIdentifier,
                                                                           String userId,
                                                                           String email)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getEmailVerificationTokensTable()
                + " WHERE app_id = ? AND tenant_id = ? AND user_id = ? AND email = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, userId);
            pst.setString(4, email);
        });
    }

    public static EmailVerificationTokenInfo getEmailVerificationTokenInfo(Start start,
                                                                           TenantIdentifier tenantIdentifier,
                                                                           String token)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT user_id, token, token_expiry, email FROM "
                + getConfig(start).getEmailVerificationTokensTable()
                + " WHERE app_id = ? AND tenant_id = ? AND token = ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, token);
        }, result -> {
            if (result.next()) {
                return EmailVerificationTokenInfoRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static void addEmailVerificationToken(Start start, TenantIdentifier tenantIdentifier, String userId,
                                                 String tokenHash, long expiry,
                                                 String email) throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getEmailVerificationTokensTable()
                + "(app_id, tenant_id, user_id, token, token_expiry, email)" + " VALUES(?, ?, ?, ?, ?, ?)";

        update(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, userId);
            pst.setString(4, tokenHash);
            pst.setLong(5, expiry);
            pst.setString(6, email);
        });
    }

    public static EmailVerificationTokenInfo[] getAllEmailVerificationTokenInfoForUser_Transaction(Start start,
                                                                                                   Connection con,
                                                                                                   TenantIdentifier tenantIdentifier,
                                                                                                   String userId,
                                                                                                   String email)
            throws SQLException, StorageQueryException {

        String QUERY = "SELECT user_id, token, token_expiry, email FROM "
                + getConfig(start).getEmailVerificationTokensTable() +
                " WHERE app_id = ? AND tenant_id = ? AND user_id = ? AND email = ? FOR UPDATE";

        return execute(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, userId);
            pst.setString(4, email);
        }, result -> {
            List<EmailVerificationTokenInfo> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(EmailVerificationTokenInfoRowMapper.getInstance().mapOrThrow(result));
            }
            EmailVerificationTokenInfo[] finalResult = new EmailVerificationTokenInfo[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        });
    }

    public static EmailVerificationTokenInfo[] getAllEmailVerificationTokenInfoForUser(Start start,
                                                                                       TenantIdentifier tenantIdentifier,
                                                                                       String userId,
                                                                                       String email)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT user_id, token, token_expiry, email FROM "
                + getConfig(start).getEmailVerificationTokensTable() +
                " WHERE app_id = ? AND tenant_id = ? AND user_id = ? AND email = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, userId);
            pst.setString(4, email);
        }, result -> {
            List<EmailVerificationTokenInfo> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(EmailVerificationTokenInfoRowMapper.getInstance().mapOrThrow(result));
            }
            EmailVerificationTokenInfo[] finalResult = new EmailVerificationTokenInfo[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        });
    }

    public static boolean isEmailVerified(Start start, AppIdentifier appIdentifier, String userId, String email)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT * FROM " + getConfig(start).getEmailVerificationTable()
                + " WHERE app_id = ? AND user_id = ? AND email = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
            pst.setString(3, email);
        }, result -> result.next());
    }

    public static class UserIdAndEmail {
        public String userId;
        public String email;

        public UserIdAndEmail(String userId, String email) {
            this.userId = userId;
            this.email = email;
        }
    }

    // returns list of userIds where email is verified.
    public static List<String> isEmailVerified_transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier,
                                                           List<UserIdAndEmail> userIdAndEmail)
            throws SQLException, StorageQueryException {
        if (userIdAndEmail.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> emails = new ArrayList<>();
        List<String> supertokensUserIds = new ArrayList<>();
        for (UserIdAndEmail ue : userIdAndEmail) {
            emails.add(ue.email);
            supertokensUserIds.add(ue.userId);
        }

        // We have external user id stored in the email verification table, so we need to fetch the mapped userids for
        // calculating the verified emails

        HashMap<String, String> supertokensUserIdToExternalUserIdMap =
                UserIdMappingQueries.getUserIdMappingWithUserIds_Transaction(
                        start,
                        sqlCon, appIdentifier, supertokensUserIds);
        HashMap<String, String> externalUserIdToSupertokensUserIdMap = new HashMap<>();

        List<String> supertokensOrExternalUserIdsToQuery = new ArrayList<>();
        for (String userId : supertokensUserIds) {
            if (supertokensUserIdToExternalUserIdMap.containsKey(userId)) {
                supertokensOrExternalUserIdsToQuery.add(supertokensUserIdToExternalUserIdMap.get(userId));
                externalUserIdToSupertokensUserIdMap.put(supertokensUserIdToExternalUserIdMap.get(userId), userId);
            } else {
                supertokensOrExternalUserIdsToQuery.add(userId);
                externalUserIdToSupertokensUserIdMap.put(userId, userId);
            }
        }

        Map<String, String> supertokensOrExternalUserIdToEmailMap = new HashMap<>();
        for (UserIdAndEmail ue : userIdAndEmail) {
            String supertokensOrExternalUserId = ue.userId;
            if (supertokensUserIdToExternalUserIdMap.containsKey(supertokensOrExternalUserId)) {
                supertokensOrExternalUserId = supertokensUserIdToExternalUserIdMap.get(supertokensOrExternalUserId);
            }
            if (supertokensOrExternalUserIdToEmailMap.containsKey(supertokensOrExternalUserId)) {
                throw new RuntimeException("Found a bug!");
            }
            supertokensOrExternalUserIdToEmailMap.put(supertokensOrExternalUserId, ue.email);
        }

        String QUERY = "SELECT * FROM " + getConfig(start).getEmailVerificationTable()
                + " WHERE app_id = ? AND user_id IN (" +
                Utils.generateCommaSeperatedQuestionMarks(supertokensOrExternalUserIdsToQuery.size()) +
                ") AND email IN (" + Utils.generateCommaSeperatedQuestionMarks(emails.size()) + ")";

        return execute(sqlCon, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            int index = 2;
            for (String userId : supertokensOrExternalUserIdsToQuery) {
                pst.setString(index++, userId);
            }
            for (String email : emails) {
                pst.setString(index++, email);
            }
        }, result -> {
            List<String> res = new ArrayList<>();
            while (result.next()) {
                String supertokensOrExternalUserId = result.getString("user_id");
                String email = result.getString("email");
                if (Objects.equals(supertokensOrExternalUserIdToEmailMap.get(supertokensOrExternalUserId), email)) {
                    res.add(externalUserIdToSupertokensUserIdMap.get(supertokensOrExternalUserId));
                }
            }
            return res;
        });
    }

    public static List<String> isEmailVerified(Start start, AppIdentifier appIdentifier,
                                               List<UserIdAndEmail> userIdAndEmail)
            throws SQLException, StorageQueryException {

        if (userIdAndEmail.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> emails = new ArrayList<>();
        List<String> supertokensUserIds = new ArrayList<>();

        for (UserIdAndEmail ue : userIdAndEmail) {
            emails.add(ue.email);
            supertokensUserIds.add(ue.userId);
        }
        // We have external user id stored in the email verification table, so we need to fetch the mapped userids for
        // calculating the verified emails
        HashMap<String, String> supertokensUserIdToExternalUserIdMap = UserIdMappingQueries.getUserIdMappingWithUserIds(
                start,
                appIdentifier, supertokensUserIds);
        HashMap<String, String> externalUserIdToSupertokensUserIdMap = new HashMap<>();
        List<String> supertokensOrExternalUserIdsToQuery = new ArrayList<>();
        for (String userId : supertokensUserIds) {
            if (supertokensUserIdToExternalUserIdMap.containsKey(userId)) {
                supertokensOrExternalUserIdsToQuery.add(supertokensUserIdToExternalUserIdMap.get(userId));
                externalUserIdToSupertokensUserIdMap.put(supertokensUserIdToExternalUserIdMap.get(userId), userId);
            } else {
                supertokensOrExternalUserIdsToQuery.add(userId);
                externalUserIdToSupertokensUserIdMap.put(userId, userId);
            }
        }

        Map<String, String> supertokensOrExternalUserIdToEmailMap = new HashMap<>();
        for (UserIdAndEmail ue : userIdAndEmail) {
            String supertokensOrExternalUserId = ue.userId;
            if (supertokensUserIdToExternalUserIdMap.containsKey(supertokensOrExternalUserId)) {
                supertokensOrExternalUserId = supertokensUserIdToExternalUserIdMap.get(supertokensOrExternalUserId);
            }
            if (supertokensOrExternalUserIdToEmailMap.containsKey(supertokensOrExternalUserId)) {
                throw new RuntimeException("Found a bug!");
            }
            supertokensOrExternalUserIdToEmailMap.put(supertokensOrExternalUserId, ue.email);
        }
        String QUERY = "SELECT * FROM " + getConfig(start).getEmailVerificationTable()
                + " WHERE app_id = ? AND user_id IN (" +
                Utils.generateCommaSeperatedQuestionMarks(supertokensOrExternalUserIdsToQuery.size()) +
                ") AND email IN (" + Utils.generateCommaSeperatedQuestionMarks(emails.size()) + ")";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            int index = 2;
            for (String userId : supertokensOrExternalUserIdsToQuery) {
                pst.setString(index++, userId);
            }
            for (String email : emails) {
                pst.setString(index++, email);
            }
        }, result -> {
            List<String> res = new ArrayList<>();
            while (result.next()) {
                String supertokensOrExternalUserId = result.getString("user_id");
                String email = result.getString("email");
                if (Objects.equals(supertokensOrExternalUserIdToEmailMap.get(supertokensOrExternalUserId), email)) {
                    res.add(externalUserIdToSupertokensUserIdMap.get(supertokensOrExternalUserId));
                }
            }
            return res;
        });
    }

    public static void deleteUserInfo_Transaction(Connection sqlCon, Start start, AppIdentifier appIdentifier,
                                                  String userId)
            throws StorageQueryException, SQLException {
        {
            String QUERY = "DELETE FROM " + getConfig(start).getEmailVerificationTable()
                    + " WHERE app_id = ? AND user_id = ?";
            update(sqlCon, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, userId);
            });
        }

        {
            String QUERY = "DELETE FROM " + getConfig(start).getEmailVerificationTokensTable()
                    + " WHERE app_id = ? AND user_id = ?";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, userId);
            });
        }
    }

    public static boolean deleteUserInfo(Start start, TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException, SQLException {
        String QUERY = "DELETE FROM " + getConfig(start).getEmailVerificationTokensTable()
                + " WHERE app_id = ? AND tenant_id = ? AND user_id = ?";

        int numRows = update(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, userId);
        });
        return numRows > 0;
    }

    public static void unverifyEmail(Start start, AppIdentifier appIdentifier, String userId, String email)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getEmailVerificationTable()
                + " WHERE app_id = ? AND user_id = ? AND email = ?";

        update(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
            pst.setString(3, email);
        });
    }

    public static void revokeAllTokens(Start start, TenantIdentifier tenantIdentifier, String userId, String email)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getEmailVerificationTokensTable()
                + " WHERE app_id = ? AND tenant_id = ? AND user_id = ? AND email = ?";

        update(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, userId);
            pst.setString(4, email);
        });
    }

    public static boolean isUserIdBeingUsedForEmailVerification(Start start, AppIdentifier appIdentifier, String userId)
            throws SQLException, StorageQueryException {
        {
            String QUERY = "SELECT * FROM " + getConfig(start).getEmailVerificationTokensTable()
                    + " WHERE app_id = ? AND user_id = ?";

            boolean isUsed = execute(start, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, userId);
            }, ResultSet::next);
            if (isUsed) {
                return true;
            }
        }

        {
            String QUERY = "SELECT * FROM " + getConfig(start).getEmailVerificationTable()
                    + " WHERE app_id = ? AND user_id = ?";

            return execute(start, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, userId);
            }, ResultSet::next);
        }
    }

    public static Set<String> findUserIdsBeingUsedForEmailVerification(Start start, AppIdentifier appIdentifier, List<String> userIds)
            throws SQLException, StorageQueryException {

        Set<String> foundUserIds = new HashSet<>();

        String email_verificiation_tokens_QUERY = "SELECT * FROM " + getConfig(start).getEmailVerificationTokensTable()
                + " WHERE app_id = ? AND user_id IN (" + Utils.generateCommaSeperatedQuestionMarks(userIds.size()) +")";

        foundUserIds.addAll(execute(start, email_verificiation_tokens_QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            for (int i = 0; i < userIds.size(); i++) {
                pst.setString(2 + i, userIds.get(i));
            }
        }, result -> {
            Set<String> userIdsFound = new HashSet<>();
            while (result.next()) {
                userIdsFound.add(result.getString("user_id"));
            }
            return userIdsFound;
        }));

        String email_verification_table_QUERY = "SELECT * FROM " + getConfig(start).getEmailVerificationTable()
                + " WHERE app_id = ? AND user_id  IN (" + Utils.generateCommaSeperatedQuestionMarks(userIds.size()) +")";

        foundUserIds.addAll(execute(start, email_verification_table_QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            for (int i = 0; i < userIds.size(); i++) {
                pst.setString(2 + i, userIds.get(i));
            }
        }, result -> {
            Set<String> userIdsFound = new HashSet<>();
            while (result.next()) {
                userIdsFound.add(result.getString("user_id"));
            }
            return userIdsFound;
        }));
        return foundUserIds;
    }

    public static void updateIsEmailVerifiedToExternalUserId(Start start, AppIdentifier appIdentifier,
                                                             String supertokensUserId, String externalUserId)
            throws StorageQueryException {
        try {
            start.startTransaction((TransactionConnection con) -> {
                Connection sqlCon = (Connection) con.getConnection();
                try {
                    {
                        String QUERY = "UPDATE " + getConfig(start).getEmailVerificationTable()
                                + " SET user_id = ? WHERE app_id = ? AND user_id = ?";
                        update(sqlCon, QUERY, pst -> {
                            pst.setString(1, externalUserId);
                            pst.setString(2, appIdentifier.getAppId());
                            pst.setString(3, supertokensUserId);
                        });
                    }
                    {
                        String QUERY = "UPDATE " + getConfig(start).getEmailVerificationTokensTable()
                                + " SET user_id = ? WHERE app_id = ? AND user_id = ?";
                        update(sqlCon, QUERY, pst -> {
                            pst.setString(1, externalUserId);
                            pst.setString(2, appIdentifier.getAppId());
                            pst.setString(3, supertokensUserId);
                        });
                    }
                } catch (SQLException e) {
                    throw new StorageTransactionLogicException(e);
                }

                return null;
            });
        } catch (StorageTransactionLogicException e) {
            throw new StorageQueryException(e.actualException);
        }
    }

    public static void updateMultipleIsEmailVerifiedToExternalUserIds(Start start, AppIdentifier appIdentifier,
                                                             Map<String, String> supertokensUserIdToExternalUserId)
            throws StorageQueryException {
        try {
            start.startTransaction((TransactionConnection con) -> {
                Connection sqlCon = (Connection) con.getConnection();
                try {
                    String update_email_verification_table_query = "UPDATE " + getConfig(start).getEmailVerificationTable()
                            + " SET user_id = ? WHERE app_id = ? AND user_id = ?";
                    String update_email_verification_tokens_table_query = "UPDATE " + getConfig(start).getEmailVerificationTokensTable()
                            + " SET user_id = ? WHERE app_id = ? AND user_id = ?";
                    PreparedStatement updateEmailVerificationQuery = sqlCon.prepareStatement(update_email_verification_table_query);
                    PreparedStatement updateEmailVerificationTokensQuery = sqlCon.prepareStatement(update_email_verification_tokens_table_query);

                    int counter = 0;
                    for (String supertokensUserId : supertokensUserIdToExternalUserId.keySet()){
                        updateEmailVerificationQuery.setString(1, supertokensUserIdToExternalUserId.get(supertokensUserId));
                        updateEmailVerificationQuery.setString(2, appIdentifier.getAppId());
                        updateEmailVerificationQuery.setString(3, supertokensUserId);
                        updateEmailVerificationQuery.addBatch();

                        updateEmailVerificationTokensQuery.setString(1, supertokensUserIdToExternalUserId.get(supertokensUserId));
                        updateEmailVerificationTokensQuery.setString(2, appIdentifier.getAppId());
                        updateEmailVerificationTokensQuery.setString(3, supertokensUserId);
                        updateEmailVerificationTokensQuery.addBatch();

                        counter++;
                        if(counter % 100 == 0) {
                            updateEmailVerificationQuery.executeBatch();
                            updateEmailVerificationTokensQuery.executeBatch();
                        }
                    }
                    updateEmailVerificationQuery.executeBatch();
                    updateEmailVerificationTokensQuery.executeBatch();

                } catch (SQLException e) {
                    throw new StorageTransactionLogicException(e);
                }

                return null;
            });
        } catch (StorageTransactionLogicException e) {
            throw new StorageQueryException(e.actualException);
        }
    }

    private static class EmailVerificationTokenInfoRowMapper
            implements RowMapper<EmailVerificationTokenInfo, ResultSet> {
        private static final EmailVerificationTokenInfoRowMapper INSTANCE = new EmailVerificationTokenInfoRowMapper();

        private EmailVerificationTokenInfoRowMapper() {
        }

        private static EmailVerificationTokenInfoRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public EmailVerificationTokenInfo map(ResultSet result) throws Exception {
            return new EmailVerificationTokenInfo(result.getString("user_id"), result.getString("token"),
                    result.getLong("token_expiry"), result.getString("email"));
        }
    }
}
