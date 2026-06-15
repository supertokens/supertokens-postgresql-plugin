/*
 *    Copyright (c) 2026, VRAI Labs and/or its affiliates. All rights reserved.
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

import io.supertokens.pluginInterface.auditlog.AuditLogEvent;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;

import java.sql.SQLException;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;

/**
 * Append-only audit/activity log.
 *
 * The table is range-partitioned by {@code created_at} (monthly partitions are created by a
 * separate maintenance job; a DEFAULT partition catches anything in the meantime). No primary
 * key or foreign key — the identity sequence makes {@code id} unique by construction. The only
 * index is a BRIN on {@code created_at}: nearly free on writes for append-only data and enough
 * to prune time-range scans. Requires PostgreSQL 11+.
 */
public class ActivityLogQueries {

    static String getQueryToCreateActivityLogTable(Start start) {
        String tableName = Config.getConfig(start).getActivityLogTable();
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "id BIGINT GENERATED ALWAYS AS IDENTITY,"
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "recipe_user_id VARCHAR(128),"
                + "primary_or_recipe_user_id VARCHAR(128),"
                + "event_type VARCHAR(64) NOT NULL,"
                + "status VARCHAR(128),"
                + "auth_principal VARCHAR(256),"
                + "identifier VARCHAR(256),"
                + "created_at BIGINT NOT NULL,"
                + "payload JSONB"
                + ") PARTITION BY RANGE (created_at);";
    }

    static String getQueryToCreateActivityLogDefaultPartition(Start start) {
        String tableName = Config.getConfig(start).getActivityLogTable();
        return "CREATE TABLE IF NOT EXISTS " + tableName + "_default PARTITION OF "
                + tableName + " DEFAULT;";
    }

    static String getQueryToCreateCreatedAtBrinIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS activity_log_created_at_brin ON "
                + Config.getConfig(start).getActivityLogTable() + " USING brin (created_at);";
    }

    public static void createActivityLogEntry(Start start, TenantIdentifier tenantIdentifier, AuditLogEvent event)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + Config.getConfig(start).getActivityLogTable()
                + " (app_id, tenant_id, recipe_user_id, primary_or_recipe_user_id, event_type, status,"
                + " auth_principal, identifier, created_at, payload)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";

        update(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, event.recipeUserId);
            pst.setString(4, event.primaryOrRecipeUserId);
            pst.setString(5, event.eventType);
            pst.setString(6, event.status);
            pst.setString(7, event.authPrincipal);
            pst.setString(8, event.identifier);
            pst.setLong(9, event.createdAt);
            pst.setString(10, event.payload);
        });
    }
}
