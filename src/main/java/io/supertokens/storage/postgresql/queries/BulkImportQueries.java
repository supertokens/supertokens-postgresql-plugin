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

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.bulkimport.BulkImportStorage.BULK_IMPORT_USER_STATUS;
import io.supertokens.pluginInterface.bulkimport.BulkImportUser;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

public class BulkImportQueries {
    static String getQueryToCreateBulkImportUsersTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getBulkImportUsersTable();
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "id CHAR(36),"
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "raw_data TEXT NOT NULL,"
                + "status VARCHAR(128) DEFAULT 'NEW',"
                + "error_msg TEXT,"
                + "created_at BIGINT DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP),"
                + "updated_at BIGINT DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP),"
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

    public static String getQueryToCreateCreatedAtIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS bulk_import_users_created_at_index ON "
                + Config.getConfig(start).getBulkImportUsersTable() + " (app_id, created_at)";
    }

    public static void insertBulkImportUsers(Start start, AppIdentifier appIdentifier, List<BulkImportUser> users)
            throws SQLException, StorageQueryException {
        StringBuilder queryBuilder = new StringBuilder(
                "INSERT INTO " + Config.getConfig(start).getBulkImportUsersTable() + " (id, app_id, raw_data) VALUES ");

        int userCount = users.size();

        for (int i = 0; i < userCount; i++) {
            queryBuilder.append(" (?, ?, ?)");

            if (i < userCount - 1) {
                queryBuilder.append(",");
            }
        }

        update(start, queryBuilder.toString(), pst -> {
            int parameterIndex = 1;
            for (BulkImportUser user : users) {
                pst.setString(parameterIndex++, user.id);
                pst.setString(parameterIndex++, appIdentifier.getAppId());
                pst.setString(parameterIndex++, user.toRawDataForDbStorage());
            }
        });
    }

    public static void updateBulkImportUserStatus_Transaction(Start start, Connection con, AppIdentifier appIdentifier, @Nonnull String[] bulkImportUserIds, @Nonnull BULK_IMPORT_USER_STATUS status)
            throws SQLException, StorageQueryException {
        if (bulkImportUserIds.length == 0) {
            return;
        }

        String baseQuery = "UPDATE " + Config.getConfig(start).getBulkImportUsersTable() + " SET status = ?, updated_at = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) WHERE app_id = ?";
        StringBuilder queryBuilder = new StringBuilder(baseQuery);

        List<Object> parameters = new ArrayList<>();

        parameters.add(status.toString());
        parameters.add(appIdentifier.getAppId());

        queryBuilder.append(" AND id IN (");
        for (int i = 0; i < bulkImportUserIds.length; i++) {
            if (i != 0) {
                queryBuilder.append(", ");
            }
            queryBuilder.append("?");
            parameters.add(bulkImportUserIds[i]);
        }
        queryBuilder.append(")");

        String query = queryBuilder.toString();

        update(con, query, pst -> {
            for (int i = 0; i < parameters.size(); i++) {
                pst.setObject(i + 1, parameters.get(i));
            }
        });
    }

    public static List<BulkImportUser> getBulkImportUsers(Start start, AppIdentifier appIdentifier, @Nonnull Integer limit, @Nullable BULK_IMPORT_USER_STATUS status,
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
                .append(" AND created_at < ? OR (created_at = ? AND id <= ?)");
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

    public static List<String> deleteBulkImportUsers(Start start, AppIdentifier appIdentifier, @Nonnull String[] bulkImportUserIds) throws SQLException, StorageQueryException {
        if (bulkImportUserIds.length == 0) {
            return new ArrayList<>();
        }

        String baseQuery =  "DELETE FROM " + Config.getConfig(start).getBulkImportUsersTable();
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
                    result.getLong("created_at"), result.getLong("updated_at"));
        }
    }
}
