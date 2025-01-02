/*
 *    Copyright (c) 2024, VRAI Labs and/or its affiliates. All rights reserved.
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
import io.supertokens.pluginInterface.bulkimport.BulkImportStorage.BULK_IMPORT_USER_STATUS;
import io.supertokens.pluginInterface.bulkimport.BulkImportUser;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.PreparedStatementValueSetter;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.*;

public class BulkImportQueries {
    static String getQueryToCreateBulkImportUsersTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getBulkImportUsersTable();
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "id CHAR(36),"
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "primary_user_id VARCHAR(36),"
                + "raw_data TEXT NOT NULL,"
                + "status VARCHAR(128) DEFAULT 'NEW',"
                + "error_msg TEXT,"
                + "created_at BIGINT NOT NULL, "
                + "updated_at BIGINT NOT NULL, "
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey")
                + " PRIMARY KEY(app_id, id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "app_id", "fkey") + " "
                + "FOREIGN KEY(app_id) "
                + "REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE"
                + " );";
    }

    public static String getQueryToCreateStatusUpdatedAtIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS bulk_import_users_status_updated_at_index ON "
                + Config.getConfig(start).getBulkImportUsersTable() + " (app_id, status, updated_at)";
    }

    public static String getQueryToCreatePaginationIndex1(Start start) {
        return "CREATE INDEX IF NOT EXISTS bulk_import_users_pagination_index1 ON "
                + Config.getConfig(start).getBulkImportUsersTable() + " (app_id, status, created_at DESC, id DESC)";
    }

    public static String getQueryToCreatePaginationIndex2(Start start) {
        return "CREATE INDEX IF NOT EXISTS bulk_import_users_pagination_index2 ON "
                + Config.getConfig(start).getBulkImportUsersTable() + " (app_id, created_at DESC, id DESC)";
    }

    public static void insertBulkImportUsers_Transaction(Start start, Connection connection, AppIdentifier appIdentifier, List<BulkImportUser> users)
            throws SQLException, StorageQueryException {
        String queryBuilder = "INSERT INTO " + Config.getConfig(start).getBulkImportUsersTable() +
                " (id, app_id, raw_data, created_at, updated_at) VALUES "
                + " (?, ?, ?, ?, ?)";

        List<PreparedStatementValueSetter> valueSetters = new ArrayList<>();
        for (BulkImportUser user : users) {
            valueSetters.add(pst -> {
                pst.setString(1, user.id);
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, user.toRawDataForDbStorage());
                pst.setLong(4, System.currentTimeMillis());
                pst.setLong(5, System.currentTimeMillis());
            });
        }

        executeBatch(connection, queryBuilder, valueSetters);
    }

    public static void updateBulkImportUserStatus_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
            @Nonnull String bulkImportUserId, @Nonnull BULK_IMPORT_USER_STATUS status, @Nullable String errorMessage)
            throws SQLException {
        String query = "UPDATE " + Config.getConfig(start).getBulkImportUsersTable()
                + " SET status = ?, error_msg = ?, updated_at = ? WHERE app_id = ? and id = ?";

        List<Object> parameters = new ArrayList<>();

        parameters.add(status.toString());
        parameters.add(errorMessage);
        parameters.add(System.currentTimeMillis());
        parameters.add(appIdentifier.getAppId());
        parameters.add(bulkImportUserId);

        update(con, query, pst -> {
            for (int i = 0; i < parameters.size(); i++) {
                pst.setObject(i + 1, parameters.get(i));
            }
        });
    }

    public static void updateMultipleBulkImportUsersStatusToError_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                                              @Nonnull Map<String,String> bulkImportUserIdToErrorMessage)
            throws SQLException, StorageQueryException {
        BULK_IMPORT_USER_STATUS errorStatus = BULK_IMPORT_USER_STATUS.FAILED;
        String query = "UPDATE " + Config.getConfig(start).getBulkImportUsersTable()
                + " SET status = ?, error_msg = ?, updated_at = ? WHERE app_id = ? and id = ?";
        List<PreparedStatementValueSetter> setters = new ArrayList<>();

        for(String bulkImportUserId : bulkImportUserIdToErrorMessage.keySet()){
            setters.add(pst -> {
                pst.setString(1, errorStatus.toString());
                pst.setString(2, bulkImportUserIdToErrorMessage.get(bulkImportUserId));
                pst.setLong(3, System.currentTimeMillis());
                pst.setString(4, appIdentifier.getAppId());
                pst.setString(5, bulkImportUserId);
            });
        }

        executeBatch(con, query, setters);
    }

    public static List<BulkImportUser> getBulkImportUsersAndChangeStatusToProcessing(Start start,
            AppIdentifier appIdentifier,
            @Nonnull Integer limit)
            throws StorageQueryException, StorageTransactionLogicException {

        return start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {

                // "FOR UPDATE" ensures that multiple cron jobs don't read the same rows simultaneously.
                // If one process locks the first 1000 rows, others will wait for the lock to be released.
                // "SKIP LOCKED" allows other processes to skip locked rows and select the next 1000 available rows.
                String selectQuery = "SELECT * FROM " + Config.getConfig(start).getBulkImportUsersTable()
                        + " WHERE app_id = ?"
                        //+ " AND (status = 'NEW' OR (status = 'PROCESSING' AND updated_at < (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000) -  10 * 60 * 1000))" /* 10 mins */
                        + " AND (status = 'NEW' OR status = 'PROCESSING' )"
                        + " LIMIT ? FOR UPDATE SKIP LOCKED";

                List<BulkImportUser> bulkImportUsers = new ArrayList<>();

                execute(sqlCon, selectQuery, pst -> {
                    pst.setString(1, appIdentifier.getAppId());
                    pst.setInt(2, limit);
                }, result -> {
                    while (result.next()) {
                        bulkImportUsers.add(BulkImportUserRowMapper.getInstance().mapOrThrow(result));
                    }
                    return null;
                });

                if (bulkImportUsers.isEmpty()) {
                    return new ArrayList<>();
                }

                String updateQuery = "UPDATE " + Config.getConfig(start).getBulkImportUsersTable()
                        + " SET status = ?, updated_at = ? WHERE app_id = ? AND id = ?";

                List<PreparedStatementValueSetter> updateSetters = new ArrayList<>();
                for(BulkImportUser user : bulkImportUsers){
                    updateSetters.add(pst -> {
                        pst.setString(1, BULK_IMPORT_USER_STATUS.PROCESSING.toString());
                        pst.setLong(2, System.currentTimeMillis());
                        pst.setString(3, appIdentifier.getAppId());
                        pst.setObject(4, user.id);
                    });
                }

                executeBatch(sqlCon, updateQuery, updateSetters);

                return bulkImportUsers;
            } catch (SQLException throwables) {
                throw new StorageTransactionLogicException(throwables);
            }
        });
    }

    public static List<BulkImportUser> getBulkImportUsers(Start start, AppIdentifier appIdentifier,
            @Nonnull Integer limit, @Nullable BULK_IMPORT_USER_STATUS status,
            @Nullable String bulkImportUserId, @Nullable Long createdAt)
            throws SQLException, StorageQueryException {

        String baseQuery = "SELECT * FROM " + Config.getConfig(start).getBulkImportUsersTable();

        StringBuilder queryBuilder = new StringBuilder(baseQuery);
        List<Object> parameters = new ArrayList<>();

        queryBuilder.append(" WHERE app_id = ?");
        parameters.add(appIdentifier.getAppId());

        if (status != null) {
            queryBuilder.append(" AND status = ?");
            parameters.add(status.toString());
        }

        if (bulkImportUserId != null && createdAt != null) {
            queryBuilder
                    .append(" AND (created_at < ? OR (created_at = ? AND id <= ?))");
            parameters.add(createdAt);
            parameters.add(createdAt);
            parameters.add(bulkImportUserId);
        }

        queryBuilder.append(" ORDER BY created_at DESC, id DESC LIMIT ?");
        parameters.add(limit);

        String query = queryBuilder.toString();

        return execute(start, query, pst -> {
            for (int i = 0; i < parameters.size(); i++) {
                pst.setObject(i + 1, parameters.get(i));
            }
        }, result -> {
            List<BulkImportUser> bulkImportUsers = new ArrayList<>();
            while (result.next()) {
                bulkImportUsers.add(BulkImportUserRowMapper.getInstance().mapOrThrow(result));
            }
            return bulkImportUsers;
        });
    }

    public static List<String> deleteBulkImportUsers(Start start, AppIdentifier appIdentifier,
            @Nonnull String[] bulkImportUserIds) throws SQLException, StorageQueryException {
        if (bulkImportUserIds.length == 0) {
            return new ArrayList<>();
        }

        String baseQuery = "DELETE FROM " + Config.getConfig(start).getBulkImportUsersTable();
        StringBuilder queryBuilder = new StringBuilder(baseQuery);

        List<Object> parameters = new ArrayList<>();

        queryBuilder.append(" WHERE app_id = ?");
        parameters.add(appIdentifier.getAppId());

        queryBuilder.append(" AND id IN (");
        for (int i = 0; i < bulkImportUserIds.length; i++) {
            if (i != 0) {
                queryBuilder.append(", ");
            }
            queryBuilder.append("?");
            parameters.add(bulkImportUserIds[i]);
        }
        queryBuilder.append(") RETURNING id");

        String query = queryBuilder.toString();

        return update(start, query, pst -> {
            for (int i = 0; i < parameters.size(); i++) {
                pst.setObject(i + 1, parameters.get(i));
            }
        }, result -> {
            List<String> deletedIds = new ArrayList<>();
            while (result.next()) {
                deletedIds.add(result.getString("id"));
            }
            return deletedIds;
        });
    }

    public static void updateBulkImportUserPrimaryUserId(Start start, AppIdentifier appIdentifier,
            @Nonnull String bulkImportUserId,
            @Nonnull String primaryUserId) throws SQLException, StorageQueryException {
        String query = "UPDATE " + Config.getConfig(start).getBulkImportUsersTable()
                + " SET primary_user_id = ?, updated_at = ? WHERE app_id = ? and id = ?";

        update(start, query, pst -> {
            pst.setString(1, primaryUserId);
            pst.setLong(2, System.currentTimeMillis());
            pst.setString(3, appIdentifier.getAppId());
            pst.setString(4, bulkImportUserId);
        });
    }

    public static long getBulkImportUsersCount(Start start, AppIdentifier appIdentifier, @Nullable BULK_IMPORT_USER_STATUS status) throws SQLException, StorageQueryException {
        String baseQuery = "SELECT COUNT(*) FROM " + Config.getConfig(start).getBulkImportUsersTable();
        StringBuilder queryBuilder = new StringBuilder(baseQuery);

        List<Object> parameters = new ArrayList<>();

        queryBuilder.append(" WHERE app_id = ?");
        parameters.add(appIdentifier.getAppId());

        if (status != null) {
            queryBuilder.append(" AND status = ?");
            parameters.add(status.toString());
        }

        String query = queryBuilder.toString();

        return execute(start, query, pst -> {
            for (int i = 0; i < parameters.size(); i++) {
                pst.setObject(i + 1, parameters.get(i));
            }
        }, result -> {
            result.next();
            return result.getLong(1);
        });
    }

    private static class BulkImportUserRowMapper implements RowMapper<BulkImportUser, ResultSet> {
        private static final BulkImportUserRowMapper INSTANCE = new BulkImportUserRowMapper();

        private BulkImportUserRowMapper() {
        }

        private static BulkImportUserRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public BulkImportUser map(ResultSet result) throws Exception {
            return BulkImportUser.fromRawDataFromDbStorage(result.getString("id"), result.getString("raw_data"),
                    BULK_IMPORT_USER_STATUS.valueOf(result.getString("status")),
                    result.getString("primary_user_id"), result.getString("error_msg"), result.getLong("created_at"),
                    result.getLong("updated_at"));
        }
    }
}
