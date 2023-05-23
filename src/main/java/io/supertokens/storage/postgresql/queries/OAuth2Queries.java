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

import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

public class OAuth2Queries {

    static String getQueryToCreateOAuth2ClientTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String oAuth2ClientTable = Config.getConfig(start).getOAuth2ClientTable();
        return "CREATE TABLE IF NOT EXISTS " + oAuth2ClientTable + " ("
                + "app_id VARCHAR(64) DEFAULT " + Utils.appendQuotes("public") + ","
                + "client_id VARCHAR(128) NOT NULL,"
                + "name TEXT NOT NULL,"
                + "client_secret_hash  VARCHAR(128) NOT NULL,"
                + "redirect_uris  TEXT NOT NULL,"
                + "created_at_ms  BIGINT NOT NULL,"
                + "updated_at_ms BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientTable, null, "pkey")
                + " PRIMARY KEY (app_id, client_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientTable, "app_id", "fkey")
                + "FOREIGN KEY(app_id) REFERENCES apps(app_id) ON DELETE CASCADE)";
    }

    static String getQueryToCreateOAuth2ScopesTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String oAuth2ScopesTable = Config.getConfig(start).getOAuth2ScopesTable();

        return "CREATE TABLE IF NOT EXISTS " + oAuth2ScopesTable + " ("
                + "app_id VARCHAR(64) DEFAULT " + Utils.appendQuotes("public") + ","
                + "scope TEXT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ScopesTable, null, "pkey")
                + " PRIMARY KEY (app_id, scope),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ScopesTable, "app_id", "fkey")
                + "FOREIGN KEY(app_id) REFERENCES apps(app_id) ON DELETE CASCADE)";
    }

    static String getQueryToCreateOAuth2ClientAllowedScopesTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String oAuth2ClientAllowedScopesTable = Config.getConfig(start).getOAuth2ClientAllowedScopesTable();

        return "CREATE TABLE IF NOT EXISTS " + oAuth2ClientAllowedScopesTable + " ("
                + "app_id VARCHAR(64) DEFAULT " + Utils.appendQuotes("public") + ","
                + "client_id VARCHAR(128) NOT NULL,"
                + "scope TEXT NOT NULL,"
                + "requires_consent BOOLEAN NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientAllowedScopesTable, null, "pkey")
                + " PRIMARY KEY (app_id, client_id, scope),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientAllowedScopesTable, "client_id", "fkey")
                + "FOREIGN KEY(app_id,client_id) REFERENCES apps(app_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientAllowedScopesTable, "scope", "fkey")
                +
                "FOREIGN KEY(app_id, scope) references oauth2_scopes(app_id, scope) ON DELETE CASCADE ON UPDATE " +
                "CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientAllowedScopesTable, "app_id", "fkey")
                + "FOREIGN KEY(app_id) REFERENCES apps(app_id) ON DELETE CASCADE)";
    }

    static  String getQueryToCreateOAuth2ClientAllowedScopesTableIndex(Start start){
        String oAuth2ClientAllowedScopesTable = Config.getConfig(start).getOAuth2ClientAllowedScopesTable();
        return "CREATE INDEX "+Utils.getIndexName(oAuth2ClientAllowedScopesTable,"client_id") +"ON"
                + oAuth2ClientAllowedScopesTable+"(app_id, client_id);";
    }



    static String getQueryToCreateOAuth2AuthcodelTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String oAuth2AuthcodeTable = Config.getConfig(start).getOAuth2AuthcodeTable();
        return "CREATE TABLE IF NOT EXISTS " + oAuth2AuthcodeTable + " ("
                + "app_id VARCHAR(64) DEFAULT " + Utils.appendQuotes("public") + ","
                + "tenant_id VARCHAR(64) DEFAULT " + Utils.appendQuotes("public") + ","
                + "authorization_code_hash VARCHAR(128) NOT NULL "
                + "CONSTRAINT" +
                Utils.getConstraintName(schema, oAuth2AuthcodeTable, "authorization_code_hash", "key")
                + "UNIQUE,"
                + "session_handle VARCHAR(255) NOT NULL,"
                + "client_id VARCHAR(128) NOT NULL,"
                + "created_at_ms BIGINT NOT NULL,"
                + "expires_at_ms BIGINT NOT NULL,"
                + "scope TEXT NOT NULL,"
                + "redirect_uri TEXT NOT NULL,"
                + "access_type VARCHAR(10) NOT NULL,"
                + "code_challenge VARCHAR(128),"
                + "code_challenge_method VARCHAR(10),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2AuthcodeTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, authorization_code_hash),"
                + "CONSTRAINT "+ Utils.getConstraintName(schema,oAuth2AuthcodeTable,"client_id", "fkey")
                + "FOREIGN KEY(app_id, client_id) references oauth2_client(app_id, client_id) ON DELETE CASCADE,"
                + "CONSTRAINT "+ Utils.getConstraintName(schema,oAuth2AuthcodeTable,"tenant_id", "fkey")
                + "FOREIGN KEY(app_id, tenant_id) references tenants(app_id, tenant_id) ON DELETE CASCADE)";
    }

    static  String getQueryToCreateOAuth2AuthcodelTableIndex(Start start){
        String oAuth2AuthcodeTable = Config.getConfig(start).getOAuth2AuthcodeTable();
        return "CREATE INDEX "+Utils.getIndexName(oAuth2AuthcodeTable,"client_id")+" ON" +
                oAuth2AuthcodeTable+"(app_id, client_id);"
                +  "CREATE INDEX "+Utils.getIndexName(oAuth2AuthcodeTable,"session_handle")+" ON" +
                oAuth2AuthcodeTable+"(app_id, session_handle);"
                + "CREATE INDEX "+Utils.getIndexName(oAuth2AuthcodeTable,"expires_at_ms")+" ON" +
                oAuth2AuthcodeTable+"(expires_at_ms);";
    }

    static String getQueryToCreateOAuth2TokenlTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String oAuth2TokenTable = Config.getConfig(start).getOAuth2TokenTable();
        return "CREATE TABLE IF NOT EXISTS " + oAuth2TokenTable + " ("
                + "app_id VARCHAR(64) DEFAULT " + Utils.appendQuotes("public") + ","
                + "tenant_id VARCHAR(64) DEFAULT " + Utils.appendQuotes("public") + ","
                + "client_id VARCHAR(128) NOT NULL,"
                + "session_handle VARCHAR(255),"
                + "scope TEXT NOT NULL,"
                + "access_token_hash VARCHAR(128) NOT NULL "
                + "CONSTRAINT" +
                Utils.getConstraintName(schema, oAuth2TokenTable, "access_token_hash", "key")
                + "UNIQUE,"
                + "refresh_token_hash VARCHAR(128) NOT NULL "
                + "CONSTRAINT" +
                Utils.getConstraintName(schema, oAuth2TokenTable, "refresh_token_hash", "key")
                + "UNIQUE,"
                +"created_at_ms  BIGINT NOT NULL,"
                + "last_updated_at_ms  BIGINT NOT NULL,"
                +"access_token_expires_at_ms BIGINT NOT NULL,"
                + "refresh_token_expires_at_ms BIGINT,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2TokenTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, access_token_hash),"
                + "CONSTRAINT "+ Utils.getConstraintName(schema,oAuth2TokenTable,"client_id", "fkey")
                + "FOREIGN KEY(app_id, client_id) references oauth2_client(app_id, client_id) ON DELETE CASCADE,"
                + "CONSTRAINT "+ Utils.getConstraintName(schema,oAuth2TokenTable,"tenant_id", "fkey")
                + "FOREIGN KEY(app_id, tenant_id) references tenants(app_id, tenant_id) ON DELETE CASCADE)";
    }

    static  String getQueryToCreateOAuth2TokenlTableIndex(Start start){
        String oAuth2TokenTable = Config.getConfig(start).getOAuth2AuthcodeTable();
        return "CREATE INDEX "+Utils.getIndexName(oAuth2TokenTable,"client_id")+" ON" +
                oAuth2TokenTable +"(app_id, client_id);"
                +  "CREATE INDEX "+Utils.getIndexName(oAuth2TokenTable,"session_handle")+" ON" +
                oAuth2TokenTable +"(app_id, session_handle);"
                + "CREATE INDEX "+Utils.getIndexName(oAuth2TokenTable,"access_token_expires_at_ms")+" ON" +
                oAuth2TokenTable +"(access_token_expires_at_ms);"
                + "CREATE INDEX "+Utils.getIndexName(oAuth2TokenTable,"refresh_token_expires_at_ms")+" ON" +
                oAuth2TokenTable +"(refresh_token_expires_at_ms);";
    }
}
