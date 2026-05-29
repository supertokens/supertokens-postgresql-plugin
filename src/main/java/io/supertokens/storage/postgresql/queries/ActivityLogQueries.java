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

import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;

/**
 * Append-only audit/activity log.
 *
 * Shipped dormant: the table is created so that enabling the writer later does not require a
 * schema migration. It is range-partitioned by {@code created_at} (monthly partitions are
 * created by a separate job once the writer is enabled; a DEFAULT partition catches anything
 * in the meantime). No primary key (the identity sequence makes {@code id} unique by
 * construction and the log is never upserted or looked up by id) and no foreign key.
 *
 * The only index provisioned now is a BRIN on {@code created_at}: nearly free on writes for
 * append-only, physically-ordered data, and enough to prune time-range scans. The
 * {@code recipe_user_id} btree needed for per-user reads is deliberately deferred to the
 * user-facing release. Requires PostgreSQL 11+ (declarative partitioning, identity columns,
 * partitioned BRIN index).
 *
 * See PLAN-ACTIVITY-LOG-AND-MAU-ROLLUP.md for the full design.
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
}
