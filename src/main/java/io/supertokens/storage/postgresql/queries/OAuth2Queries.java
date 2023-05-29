/*
 *    Copyright (c) 2023, VRAI Labs and/or its affiliates. All rights reserved.
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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.reflect.TypeToken;
import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.oauth2.OAuth2Client;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.utils.Utils;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class OAuth2Queries {

    static String getQueryToCreateOAuth2ClientTable(Start start) {
        String schema = getConfig(start).getTableSchema();
        String oAuth2ClientTable = getConfig(start).getOAuth2ClientTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + oAuth2ClientTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "client_id VARCHAR(128) NOT NULL,"
                + "name TEXT NOT NULL,"
                + "client_secret_hash VARCHAR(128) NOT NULL "
                + "CONSTRAINT "
                + Utils.getConstraintName(schema, oAuth2ClientTable, "client_secret_hash", "key")
                + " UNIQUE,"
                + "redirect_uris TEXT NOT NULL,"
                + "created_at_ms BIGINT NOT NULL,"
                + "updated_at_ms BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientTable, null, "pkey")
                + " PRIMARY KEY (app_id, client_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientTable, "app_id", "fkey")
                + " FOREIGN KEY(app_id) REFERENCES " + getConfig(start).getAppsTable() + "(app_id) ON DELETE CASCADE);";
        // @formatter:on
    }

    static String getQueryToCreateOAuth2ClientTableIndex(Start start) {
        String oAuth2ClientTable = getConfig(start).getOAuth2ClientTable();
        // psql does not automatically add index on foreign keys
        // hence adding index on foreign keys to support faster JOINS and DELETE ON CASCADE queries
        return "CREATE INDEX oauth2_client_app_id_index ON " + oAuth2ClientTable + "(app_id);";
    }

    static String getQueryToCreateOAuth2ScopesTable(Start start) {
        String schema = getConfig(start).getTableSchema();
        String oAuth2ScopesTable = getConfig(start).getOAuth2ScopesTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + oAuth2ScopesTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "scope TEXT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ScopesTable, null, "pkey")
                + " PRIMARY KEY (app_id, scope),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ScopesTable, "app_id", "fkey")
                + " FOREIGN KEY(app_id) REFERENCES " + getConfig(start).getAppsTable() +
                " (app_id) ON DELETE CASCADE);";
        // @formatter:on
    }

    static String getQueryToCreateOAuth2ScopesTableIndex(Start start) {
        String oAuth2ScopesTable = getConfig(start).getOAuth2ScopesTable();
        // psql does not automatically add index on foreign keys
        // hence adding index on foreign keys to support faster JOINS and DELETE ON CASCADE queries
        return "CREATE INDEX oauth2_scopes_app_id_index ON " + oAuth2ScopesTable + "(app_id);";
    }

    static String getQueryToCreateOAuth2ClientAllowedScopesTable(Start start) {
        String schema = getConfig(start).getTableSchema();
        String oAuth2ClientAllowedScopesTable = getConfig(start).getOAuth2ClientAllowedScopesTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + oAuth2ClientAllowedScopesTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "client_id VARCHAR(128) NOT NULL,"
                + "scope TEXT NOT NULL,"
                + "requires_consent BOOLEAN NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientAllowedScopesTable, null, "pkey")
                + " PRIMARY KEY(app_id, client_id, scope),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientAllowedScopesTable, "client_id", "fkey")
                + " FOREIGN KEY(app_id,client_id) REFERENCES " + getConfig(start).getOAuth2ClientTable()
                + "(app_id, client_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientAllowedScopesTable, "scope", "fkey")
                + " FOREIGN KEY(app_id, scope) REFERENCES " + getConfig(start).getOAuth2ScopesTable()
                + "(app_id, scope) ON DELETE CASCADE);";
        // @formatter:on
    }

    static String getQueryToCreateOAuth2ClientAllowedScopesTableIndex(Start start) {
        String oAuth2ClientAllowedScopesTable = getConfig(start).getOAuth2ClientAllowedScopesTable();
        // psql does not automatically add index on foreign keys
        // hence adding index on foreign keys to support faster JOINS and DELETE ON CASCADE queries
        return "CREATE INDEX oauth2_client_allowed_scopes_client_id_index ON " + oAuth2ClientAllowedScopesTable + "(app_id, client_id);"
                + "CREATE INDEX oauth2_client_allowed_scopes_scope_index ON " + oAuth2ClientAllowedScopesTable + "(app_id, scope);";
    }


    static String getQueryToCreateOAuth2AuthcodeTable(Start start) {
        String schema = getConfig(start).getTableSchema();
        String oAuth2AuthcodeTable = getConfig(start).getOAuth2AuthcodeTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + oAuth2AuthcodeTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "authorization_code_hash VARCHAR(128) NOT NULL "
                + "CONSTRAINT "
                + Utils.getConstraintName(schema, oAuth2AuthcodeTable, "authorization_code_hash", "key")
                + " UNIQUE,"
                + "session_handle VARCHAR(255) NOT NULL,"
                + "client_id VARCHAR(128) NOT NULL,"
                + "created_at_ms BIGINT NOT NULL,"
                + "expires_at_ms BIGINT NOT NULL,"
                + "scopes TEXT NOT NULL,"
                // In an ideal scenario, the 'scopes' field should be a foreign key referencing the 'scope' field in
                // the 'oauth2_scope' table,
                // containing an array of scopes. However, in this case, the scopes are not directly linked
                // to the 'oauth2_scope' table's 'scope' field.
                // This deliberate design choice ensures that any changes or deletions made to scopes in the 'oauth2_scope' table
                // do not affect existing OAuth2 codes or cause unexpected disruptions in users' sessions.
                + "redirect_uri TEXT NOT NULL,"
                + "access_type VARCHAR(10) NOT NULL,"
                + "code_challenge VARCHAR(128),"
                + "code_challenge_method VARCHAR(10),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2AuthcodeTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, authorization_code_hash),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2AuthcodeTable, "client_id", "fkey")
                + " FOREIGN KEY(app_id, client_id) REFERENCES " + getConfig(start).getOAuth2ClientTable()
                + "(app_id, client_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2AuthcodeTable, "session_handle", "fkey")
                + " FOREIGN KEY(app_id, tenant_id, session_handle) REFERENCES "
                + getConfig(start).getSessionInfoTable() + "(app_id, tenant_id, session_handle) ON DELETE CASCADE );";
        // @formatter:on
    }

    static String getQueryToCreateOAuth2AuthcodeTableIndex(Start start) {
        String oAuth2AuthcodeTable = getConfig(start).getOAuth2AuthcodeTable();
        return "CREATE INDEX oauth2_authcode_client_id_index ON " + oAuth2AuthcodeTable + "(app_id, client_id);"
                + "CREATE INDEX oauth2_authcode_session_handle_index ON " + oAuth2AuthcodeTable + "(app_id, tenant_id, session_handle);"
                + "CREATE INDEX oauth2_authcode_expires_at_ms_index ON " + oAuth2AuthcodeTable + "(expires_at_ms);";
    }

    static String getQueryToCreateOAuth2TokenTable(Start start) {
        String schema = getConfig(start).getTableSchema();
        String oAuth2TokenTable = getConfig(start).getOAuth2TokenTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + oAuth2TokenTable + " ("
                + "id CHAR(36) NOT NULL,"
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "client_id VARCHAR(128) NOT NULL,"
                + "session_handle VARCHAR(255),"
                + "scopes TEXT NOT NULL,"
                + "access_token_hash VARCHAR(128) NOT NULL "
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2TokenTable, "access_token_hash", "key")
                + " UNIQUE,"
                + "refresh_token_hash VARCHAR(128) "
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2TokenTable, "refresh_token_hash", "key")
                + " UNIQUE,"
                + "created_at_ms BIGINT NOT NULL,"
                + "last_updated_at_ms BIGINT NOT NULL,"
                + "access_token_expires_at_ms BIGINT NOT NULL,"
                + "refresh_token_expires_at_ms BIGINT,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2TokenTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2TokenTable, "client_id", "fkey")
                + " FOREIGN KEY(app_id, client_id) REFERENCES " + getConfig(start).getOAuth2ClientTable()
                + "(app_id, client_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2TokenTable, "session_handle", "fkey")
                + " FOREIGN KEY(app_id, tenant_id, session_handle) REFERENCES " + getConfig(start).getSessionInfoTable()
                + "(app_id, tenant_id, session_handle) ON DELETE CASCADE);";
        // @formatter:on
    }

    static String getQueryToCreateOAuth2TokenTableIndex(Start start) {
        String oAuth2TokenTable = getConfig(start).getOAuth2TokenTable();
        return "CREATE INDEX oauth2_access_token_client_id_index ON " + oAuth2TokenTable + "(app_id, client_id);"
                + "CREATE INDEX oauth2_access_token_session_handle_index ON " + oAuth2TokenTable + "(app_id,tenant_id,session_handle);"
                + "CREATE INDEX oauth2_token_access_token_expires_at_ms_index ON " + oAuth2TokenTable + "(access_token_expires_at_ms);"
                + "CREATE INDEX oauth2_token_refresh_token_expires_at_ms_index ON " + oAuth2TokenTable + "(refresh_token_expires_at_ms);";
    }

    public static void createOAuth2Client_Transaction(Start start, Connection con,
                                                      AppIdentifier appIdentifier, OAuth2Client oAuth2Client) throws StorageQueryException, SQLException{
        String QUERY = "INSERT INTO " + getConfig(start).getOAuth2ClientTable()
                + "(app_id , client_id, name, client_secret_hash, redirect_uris, created_at_ms, updated_at_ms)" +
                " VALUES(?, ?, ?, ?, ?, ?, ?)";

        update(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, oAuth2Client.clientId);
            pst.setString(3, oAuth2Client.name);
            pst.setString(4, oAuth2Client.clientSecretHash);
            pst.setString(5, convertListToJsonArrayString(oAuth2Client.redirectUris));
            pst.setLong(6, oAuth2Client.createdAtMs);
            pst.setLong(7, oAuth2Client.updatedAtMs);
        });
    }

    public static OAuth2Client getOAuth2ClientById(Start start, AppIdentifier appIdentifier, String clientId) throws SQLException, StorageQueryException {
        String QUERY = "SELECT * from " + getConfig(start).getOAuth2ClientTable()
                + " WHERE app_id = ? AND client_id = ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, clientId);
        }, result -> {
            if (result.next()) {
                return OAuth2Queries.OAuth2ClientRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static void createScope(Start start, AppIdentifier appIdentifier, String scope) throws StorageQueryException, SQLException {
        String QUERY = "INSERT INTO " + getConfig(start).getOAuth2ScopesTable()
                + "(app_id, scope)" +
                " VALUES(?, ?)";

        update(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, scope);
        });
    }

    public static List<String> getOAuth2Scopes(Start start, AppIdentifier appIdentifier) throws SQLException, StorageQueryException{
        String QUERY = "SELECT * from " + getConfig(start).getOAuth2ScopesTable()
                + " WHERE app_id = ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
        }, result -> {
            List<String> scopes = new ArrayList<>();
            while (result.next()) {
                scopes.add(result.getString("scope"));
            }
            return scopes;
        });
    }

    private static class OAuth2ClientRowMapper implements RowMapper<OAuth2Client, ResultSet> {
        private static final OAuth2Queries.OAuth2ClientRowMapper INSTANCE = new OAuth2Queries.OAuth2ClientRowMapper();

        private OAuth2ClientRowMapper() {
        }

        private static  OAuth2Queries.OAuth2ClientRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public OAuth2Client map(ResultSet result) throws Exception {
            return new OAuth2Client(result.getString("client_id"), result.getString("name"),
                    result.getString("client_secret_hash"),convertJSONArrayStringToList(result.getString("redirect_uris")),
                    result.getLong("created_at_ms"), result.getLong("updated_at_ms"));
        }
    }

    private  static String convertListToJsonArrayString(List<String> list) {
        // Convert List of Strings to a JSON array using Gson
        JsonArray jsonArray = new Gson().toJsonTree(list).getAsJsonArray();
        return jsonArray.toString();
    }

    private  static List<String> convertJSONArrayStringToList(String jsonArrayString) {
        Type listType = new TypeToken<List<String>>() {}.getType();
        // Convert JSON array to a List of Strings using Gson
        return new Gson().fromJson(jsonArrayString, listType);
    }

}
