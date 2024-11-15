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
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class UserMetadataQueries {

    public static String getQueryToCreateUserMetadataTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getUserMetadataTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "user_id VARCHAR(128) NOT NULL,"
                + "user_metadata TEXT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey")
                + " PRIMARY KEY(app_id, user_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "app_id", "fkey")
                + " FOREIGN KEY(app_id)"
                + " REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static String getQueryToCreateAppIdIndexForUserMetadataTable(Start start) {
        return "CREATE INDEX user_metadata_app_id_index ON "
                + Config.getConfig(start).getUserMetadataTable() + "(app_id);";
    }

    public static int deleteUserMetadata(Start start, AppIdentifier appIdentifier, String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getUserMetadataTable()
                + " WHERE app_id = ? AND user_id = ?";

        return update(start, QUERY.toString(), pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        });
    }

    public static int deleteUserMetadata_Transaction(Connection sqlCon, Start start, AppIdentifier appIdentifier,
                                                     String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getUserMetadataTable()
                + " WHERE app_id = ? AND user_id = ?";

        return update(sqlCon, QUERY.toString(), pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        });
    }

    public static int setUserMetadata_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                  String userId, JsonObject metadata)
            throws SQLException, StorageQueryException {

        String QUERY = "INSERT INTO " + getConfig(start).getUserMetadataTable()
                + "(app_id, user_id, user_metadata) VALUES(?, ?, ?) "
                + "ON CONFLICT(app_id, user_id) DO UPDATE SET user_metadata=excluded.user_metadata;";

        return update(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
            pst.setString(3, metadata.toString());
        });
    }

    public static void setMultipleUsersMetadatas_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                  Map<String, JsonObject> metadatasByUserId)
            throws SQLException, StorageQueryException {

        String QUERY = "INSERT INTO " + getConfig(start).getUserMetadataTable()
                + "(app_id, user_id, user_metadata) VALUES(?, ?, ?) "
                + "ON CONFLICT(app_id, user_id) DO UPDATE SET user_metadata=excluded.user_metadata;";
        PreparedStatement insertStatement = con.prepareStatement(QUERY);

        int counter = 0;
        for(Map.Entry<String, JsonObject> metadataByUserId : metadatasByUserId.entrySet()){
            insertStatement.setString(1, appIdentifier.getAppId());
            insertStatement.setString(2, metadataByUserId.getKey());
            insertStatement.setString(3, metadataByUserId.getValue().toString());
            insertStatement.addBatch();

            counter++;
            if(counter % 100 == 0) {
                insertStatement.executeBatch();
            }
        }

        insertStatement.executeBatch();
    }

    public static JsonObject getUserMetadata_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                         String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT user_metadata FROM " + getConfig(start).getUserMetadataTable()
                + " WHERE app_id = ? AND user_id = ? FOR UPDATE";
        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        }, result -> {
            if (result.next()) {
                JsonParser jp = new JsonParser();
                return jp.parse(result.getString("user_metadata")).getAsJsonObject();
            }
            return null;
        });
    }

    public static Map<String, JsonObject> getMultipleUsersMetadatas_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                                                List<String> userIds)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT user_id, user_metadata FROM " + getConfig(start).getUserMetadataTable()
                + " WHERE app_id = ? AND user_id IN (" + Utils.generateCommaSeperatedQuestionMarks(userIds.size())
                + ") FOR UPDATE";
        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            for (int i = 0; i< userIds.size(); i++){
                pst.setString(2+i, userIds.get(i));
            }
        }, result -> {
            Map<String, JsonObject>  userMetadataByUserId = new HashMap<>();
            JsonParser jp = new JsonParser();
            if (result.next()) {
                userMetadataByUserId.put(result.getString("user_id"),
                        jp.parse(result.getString("user_metadata")).getAsJsonObject());
            }
            return userMetadataByUserId;
        });
    }

    public static JsonObject getUserMetadata(Start start, AppIdentifier appIdentifier, String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT user_metadata FROM " + getConfig(start).getUserMetadataTable()
                + " WHERE app_id = ? AND user_id = ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        }, result -> {
            if (result.next()) {
                JsonParser jp = new JsonParser();
                return jp.parse(result.getString("user_metadata")).getAsJsonObject();
            }
            return null;
        });
    }
}
