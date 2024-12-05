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


import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

public class WebAuthNQueries {

    static String getQueryToCreateWebAuthNUsersTable(Start start){
        String schema = Config.getConfig(start).getTableSchema();
        String webAuthNUsersTableName = Config.getConfig(start).getWebAuthNUsersTable();
        return  "CREATE TABLE IF NOT EXISTS " + webAuthNUsersTableName + "(" +
                " app_id VARCHAR(64) DEFAULT 'public' NOT NULL," +
                " user_id CHAR(36) NOT NULL," +
                " email VARCHAR(256) NOT NULL," +
                " rp_id VARCHAR(256) NOT NULL," +
                " time_joined BIGINT NOT NULL," +
                " CONSTRAINT " + Utils.getConstraintName(schema, webAuthNUsersTableName, null, "pkey") +
                " PRIMARY KEY (app_id, user_id)," +
                " CONSTRAINT " + Utils.getConstraintName(schema,webAuthNUsersTableName, "user_id", "fkey") +
                " FOREIGN KEY (app_id, user_id) REFERENCES " + Config.getConfig(start).getAppIdToUserIdTable() +
                " (app_id, user_id) ON DELETE CASCADE " +
                ");";
    }

    static String getQueryToCreateWebAuthNUsersToTenantTable(Start start){
        String schema = Config.getConfig(start).getTableSchema();
        String webAuthNUserToTenantTableName = Config.getConfig(start).getWebAuthNUserToTenantTable();
        return  "CREATE TABLE IF NOT EXISTS  " + webAuthNUserToTenantTableName +" (" +
                " app_id VARCHAR(64) DEFAULT 'public' NOT NULL," +
                " tenant_id VARCHAR(64) DEFAULT 'public' NOT NULL," +
                " user_id CHAR(36) NOT NULL," +
                " email VARCHAR(256) NOT NULL," +
                " CONSTRAINT "+ Utils.getConstraintName(schema, webAuthNUserToTenantTableName, "email", "key") +
                " UNIQUE (app_id, tenant_id, email)," +
                " CONSTRAINT "+ Utils.getConstraintName(schema, webAuthNUserToTenantTableName, null, "pkey") +
                " PRIMARY KEY (app_id, tenant_id, user_id)," +
                " CONSTRAINT "+ Utils.getConstraintName(schema, webAuthNUserToTenantTableName, "user_id", "fkey") +
                "  FOREIGN KEY (app_id, tenant_id, user_id) " +
                " REFERENCES "+ Config.getConfig(start).getUsersTable()+" (app_id, tenant_id, user_id) ON DELETE CASCADE" +
                ");";
    }

    static String getQueryToCreateWebAuthNGeneratedOptionsTable(Start start){
        String schema = Config.getConfig(start).getTableSchema();
        String webAuthNGeneratedOptionsTable = Config.getConfig(start).getWebAuthNGeneratedOptionsTable();
        return  "CREATE TABLE IF NOT EXISTS " + webAuthNGeneratedOptionsTable + "(" +
                " app_id VARCHAR(64) DEFAULT 'public' NOT NULL," +
                " tenant_id VARCHAR(64) DEFAULT 'public' NOT NULL," +
                " id CHAR(36) NOT NULL," +
                " challenge VARCHAR(256) NOT NULL," +
                " email VARCHAR(256)," +
                " rp_id VARCHAR(256) NOT NULL," +
                " origin VARCHAR(256) NOT NULL," +
                " expires_at BIGINT NOT NULL," +
                " created_at BIGINT NOT NULL," +
                " CONSTRAINT " + Utils.getConstraintName(schema, webAuthNGeneratedOptionsTable, null, "pkey") +
                "  PRIMARY KEY (app_id, tenant_id, id)," +
                " CONSTRAINT "+ Utils.getConstraintName(schema, webAuthNGeneratedOptionsTable, "tenant_id", "fkey") +
                "  FOREIGN KEY (app_id, tenant_id) " +
                "  REFERENCES " + Config.getConfig(start).getTenantsTable() + " (app_id, tenant_id) ON DELETE CASCADE" +
                ");";
    }

    static String getQueryToCreateWebAuthNChallengeExpiresIndex(Start start) {
        return  "CREATE INDEX webauthn_user_challenges_expires_at_index ON " +
                Config.getConfig(start).getWebAuthNGeneratedOptionsTable() +
                " (app_id, tenant_id, expires_at);";
    }

    static String getQueryToCreateWebAuthNCredentialsTable(Start start){
        String schema = Config.getConfig(start).getTableSchema();
        String webAuthNCredentialsTable = Config.getConfig(start).getWebAuthNCredentialsTable();
        return  "CREATE TABLE IF NOT EXISTS "+ webAuthNCredentialsTable + "(" +
                " id VARCHAR(256) NOT NULL," +
                " app_id VARCHAR(64) DEFAULT 'public'," +
                " rp_id VARCHAR(256)," +
                " user_id CHAR(36)," +
                " counter BIGINT NOT NULL," +
                " public_key BYTEA NOT NULL," + 
                " transports TEXT NOT NULL," + // planned as TEXT[], which is not supported by sqlite
                " created_at BIGINT NOT NULL," +
                " updated_at BIGINT NOT NULL," +
                " CONSTRAINT " + Utils.getConstraintName(schema, webAuthNCredentialsTable, null, "pkey") +
                "  PRIMARY KEY (app_id, rp_id, id)," +
                " CONSTRAINT "+ Utils.getConstraintName(schema, webAuthNCredentialsTable, "user_id", "fkey") +
                "  FOREIGN KEY (app_id, user_id) REFERENCES " +
                Config.getConfig(start).getWebAuthNUsersTable() + " (app_id, user_id) ON DELETE CASCADE" +
                ");";
    }

}
