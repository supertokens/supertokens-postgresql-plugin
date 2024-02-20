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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.bulkimport.BulkImportStorage.BulkImportUserStatus;
import io.supertokens.pluginInterface.bulkimport.BulkImportUser;
import io.supertokens.pluginInterface.bulkimport.BulkImportUserInfo;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

public class BulkImportQueries {
    static String getQueryToCreateBulkImportUsersTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getBulkImportUsersTable();
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "id CHAR(36) PRIMARY KEY,"
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "raw_data TEXT NOT NULL,"
                + "status VARCHAR(128) DEFAULT 'NEW',"
                + "error_msg TEXT,"
                + "created_at BIGINT DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP),"
                + "updated_at BIGINT DEFAULT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "app_id", "fkey") + " "
                + "FOREIGN KEY(app_id) "
                + "REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE"
                + " );";
    }

    public static String getQueryToCreateStatusUpdatedAtIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS bulk_import_users_status_updated_at_index ON "
                + Config.getConfig(start).getBulkImportUsersTable() + " (status, updated_at)";
    }

    public static String getQueryToCreateCreatedAtIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS bulk_import_users_created_at_index ON "
                + Config.getConfig(start).getBulkImportUsersTable() + " (created_at)";
    }

    public static void insertBulkImportUsers(Start start, List<BulkImportUser> users)
            throws SQLException, StorageQueryException {
        StringBuilder queryBuilder = new StringBuilder(
                "INSERT INTO " + Config.getConfig(start).getBulkImportUsersTable() + " (id, raw_data) VALUES ");

        int userCount = users.size();

        for (int i = 0; i < userCount; i++) {
            queryBuilder.append(" (?, ?)");

            if (i < userCount - 1) {
                queryBuilder.append(",");
            }
        }

        update(start, queryBuilder.toString(), pst -> {
            int parameterIndex = 1;
            for (BulkImportUser user : users) {
                pst.setString(parameterIndex++, user.id);
                pst.setString(parameterIndex++, user.toString());
            }
        });
    }

    public static List<BulkImportUserInfo> getBulkImportUsers(Start start, @Nonnull Integer limit, @Nullable BulkImportUserStatus status,
            @Nullable String bulkImportUserId)
            throws SQLException, StorageQueryException {

        String baseQuery = "SELECT * FROM " + Config.getConfig(start).getBulkImportUsersTable();

        StringBuilder queryBuilder = new StringBuilder(baseQuery);
        List<Object> parameters = new ArrayList<>();

        if (status != null) {
            queryBuilder.append(" WHERE status = ?");
            parameters.add(status);
        }

        if (bulkImportUserId != null) {
            queryBuilder.append(status != null ? " AND" : " WHERE")
                    .append(" id <= ?");
            parameters.add(bulkImportUserId);
        }

        queryBuilder.append(" ORDER BY created_at DESC LIMIT ?");
        parameters.add(limit);

        String query = queryBuilder.toString();

        return execute(start, query, pst -> {
            for (int i = 0; i < parameters.size(); i++) {
                pst.setObject(i + 1, parameters.get(i));
            }
        }, result -> {
            List<BulkImportUserInfo> bulkImportUsers = new ArrayList<>();
            while (result.next()) {
                bulkImportUsers.add(BulkImportUserInfoRowMapper.getInstance().mapOrThrow(result));
            }
            return bulkImportUsers;
        });
    }

    private static class BulkImportUserInfoRowMapper implements RowMapper<BulkImportUserInfo, ResultSet> {
        private static final BulkImportUserInfoRowMapper INSTANCE = new BulkImportUserInfoRowMapper();

        private BulkImportUserInfoRowMapper() {
        }

        private static BulkImportUserInfoRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public BulkImportUserInfo map(ResultSet result) throws Exception {
            return new BulkImportUserInfo(result.getString("id"), result.getString("raw_data"),
                    BulkImportUserStatus.valueOf(result.getString("status")),
                    result.getLong("created_at"), result.getLong("updated_at"));
        }
    }
}
