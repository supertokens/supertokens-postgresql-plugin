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
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;

/**
 * Append-only audit/activity log.
 *
 * The table is range-partitioned by {@code created_at} (epoch millis) into one partition per UTC
 * day. Upcoming days' partitions are pre-created — at table creation and daily by a maintenance
 * cron ({@code CleanupActivityLogPartitions}) — and partitions whose data is entirely older than
 * {@link #RETENTION_DAYS} days are dropped by that same cron. A DEFAULT partition is a backstop so
 * inserts never fail if the cron lapses beyond the pre-created window.
 *
 * No primary key or foreign key — the identity sequence makes {@code id} unique by construction.
 * The only index is a BRIN on {@code created_at} created on the parent table (Postgres propagates
 * it to every child partition automatically): nearly free on writes for append-only data and enough
 * to prune time-range scans. Requires PostgreSQL 11+.
 */
public class ActivityLogQueries {

    /** Partitions whose data is entirely older than this many days are dropped. */
    private static final int RETENTION_DAYS = 31;

    /** Number of future days (beyond today) to pre-create partitions for, so DEFAULT stays empty. */
    private static final int PREMAKE_DAYS = 2;

    private static final long MILLIS_PER_DAY = 24L * 60 * 60 * 1000;

    /** Matches the {@code _pYYYYMMDD} suffix of a daily partition; the DEFAULT partition won't match. */
    private static final Pattern PARTITION_DAY_PATTERN = Pattern.compile("_p(\\d{8})$");

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
                + "payload TEXT"
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
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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

    /**
     * DDL to pre-create the daily partitions for today and the next {@link #PREMAKE_DAYS} days.
     * Each statement is {@code CREATE TABLE IF NOT EXISTS}, so it is safe to run repeatedly.
     * Returned as strings so they can be batched alongside the table-creation DDL at startup.
     */
    public static List<String> getQueriesToCreateUpcomingDayPartitions(Start start) {
        List<String> queries = new ArrayList<>();
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        for (int i = 0; i <= PREMAKE_DAYS; i++) {
            queries.add(getQueryToCreateDailyPartition(start, today.plusDays(i)));
        }
        return queries;
    }

    private static String getQueryToCreateDailyPartition(Start start, LocalDate day) {
        String tableName = Config.getConfig(start).getActivityLogTable();
        String partitionName = tableName + "_p" + day.format(DateTimeFormatter.BASIC_ISO_DATE);
        long fromMillis = day.toEpochDay() * MILLIS_PER_DAY;
        long toMillis = fromMillis + MILLIS_PER_DAY;
        return "CREATE TABLE IF NOT EXISTS " + partitionName + " PARTITION OF " + tableName
                + " FOR VALUES FROM (" + fromMillis + ") TO (" + toMillis + ");";
    }

    /**
     * Pre-creates upcoming day partitions and drops any whose data is entirely older than
     * {@link #RETENTION_DAYS} days. Idempotent; intended to be run daily.
     */
    public static void maintainPartitions(Start start) throws SQLException, StorageQueryException {
        for (String query : getQueriesToCreateUpcomingDayPartitions(start)) {
            update(start, query, pst -> {});
        }
        dropPartitionsOlderThanRetention(start);
    }

    private static void dropPartitionsOlderThanRetention(Start start) throws SQLException, StorageQueryException {
        String tableName = Config.getConfig(start).getActivityLogTable();
        String LIST_QUERY = "SELECT n.nspname AS schema_name, c.relname AS partition_name"
                + " FROM pg_inherits i"
                + " JOIN pg_class c ON c.oid = i.inhrelid"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE i.inhparent = ?::regclass";

        LocalDate cutoff = LocalDate.now(ZoneOffset.UTC).minusDays(RETENTION_DAYS);

        List<String> partitionsToDrop = execute(start, LIST_QUERY, pst -> {
            pst.setString(1, tableName);
        }, result -> {
            List<String> toDrop = new ArrayList<>();
            while (result.next()) {
                String partitionName = result.getString("partition_name");
                Matcher matcher = PARTITION_DAY_PATTERN.matcher(partitionName);
                if (!matcher.find()) {
                    // DEFAULT partition (or anything not following the daily naming scheme) — leave it.
                    continue;
                }
                LocalDate partitionDay;
                try {
                    partitionDay = LocalDate.parse(matcher.group(1), DateTimeFormatter.BASIC_ISO_DATE);
                } catch (DateTimeParseException e) {
                    continue;
                }
                if (partitionDay.isBefore(cutoff)) {
                    toDrop.add(result.getString("schema_name") + "." + partitionName);
                }
            }
            return toDrop;
        });

        for (String partition : partitionsToDrop) {
            update(start, "DROP TABLE IF EXISTS " + partition + ";", pst -> {});
        }
    }
}
