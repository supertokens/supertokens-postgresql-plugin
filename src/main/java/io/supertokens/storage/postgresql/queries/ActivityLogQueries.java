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
import java.time.YearMonth;
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
 * calendar month. Upcoming months' partitions are pre-created — at table creation and by a daily
 * maintenance cron ({@code CleanupActivityLogPartitions}) — and a monthly partition is dropped once
 * its entire month is older than {@link #RETENTION_DAYS} days. A DEFAULT partition is a backstop so
 * inserts never fail if the cron lapses beyond the pre-created window.
 *
 * No primary key or foreign key — the identity sequence makes {@code id} unique by construction.
 * The only index is a BRIN on {@code created_at} created on the parent table (Postgres propagates
 * it to every child partition automatically): nearly free on writes for append-only data and enough
 * to prune time-range scans. Requires PostgreSQL 11+.
 */
public class ActivityLogQueries {

    /**
     * A monthly partition is dropped only once its entire month is older than this many days, so no
     * data younger than the window is ever removed (a partition is retained until its last day ages out).
     */
    private static final int RETENTION_DAYS = 31;

    /** Number of future months (beyond the current one) to pre-create partitions for, so DEFAULT stays empty. */
    private static final int PREMAKE_MONTHS = 1;

    private static final long MILLIS_PER_DAY = 24L * 60 * 60 * 1000;

    private static final DateTimeFormatter MONTH_SUFFIX_FORMAT = DateTimeFormatter.ofPattern("yyyyMM");

    /** Matches the {@code _pYYYYMM} suffix of a monthly partition; the DEFAULT partition won't match. */
    private static final Pattern PARTITION_MONTH_PATTERN = Pattern.compile("_p(\\d{6})$");

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
     * DDL to pre-create the monthly partitions for the current month and the next {@link #PREMAKE_MONTHS}
     * months. Each statement is {@code CREATE TABLE IF NOT EXISTS}, so it is safe to run repeatedly.
     * Returned as strings so they can be batched alongside the table-creation DDL at startup.
     */
    public static List<String> getQueriesToCreateUpcomingMonthPartitions(Start start) {
        List<String> queries = new ArrayList<>();
        YearMonth thisMonth = YearMonth.now(ZoneOffset.UTC);
        for (int i = 0; i <= PREMAKE_MONTHS; i++) {
            queries.add(getQueryToCreateMonthlyPartition(start, thisMonth.plusMonths(i)));
        }
        return queries;
    }

    private static String getQueryToCreateMonthlyPartition(Start start, YearMonth month) {
        String tableName = Config.getConfig(start).getActivityLogTable();
        String partitionName = tableName + "_p" + month.format(MONTH_SUFFIX_FORMAT);
        long fromMillis = month.atDay(1).toEpochDay() * MILLIS_PER_DAY;
        long toMillis = month.plusMonths(1).atDay(1).toEpochDay() * MILLIS_PER_DAY;
        return "CREATE TABLE IF NOT EXISTS " + partitionName + " PARTITION OF " + tableName
                + " FOR VALUES FROM (" + fromMillis + ") TO (" + toMillis + ");";
    }

    /**
     * Pre-creates upcoming month partitions and drops any whose entire month is older than
     * {@link #RETENTION_DAYS} days. Idempotent; intended to be run daily.
     */
    public static void maintainPartitions(Start start) throws SQLException, StorageQueryException {
        for (String query : getQueriesToCreateUpcomingMonthPartitions(start)) {
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
                Matcher matcher = PARTITION_MONTH_PATTERN.matcher(partitionName);
                if (!matcher.find()) {
                    // DEFAULT partition (or anything not following the monthly naming scheme) — leave it.
                    continue;
                }
                YearMonth partitionMonth;
                try {
                    partitionMonth = YearMonth.parse(matcher.group(1), MONTH_SUFFIX_FORMAT);
                } catch (DateTimeParseException e) {
                    continue;
                }
                // Drop only once the whole month has aged past the window — its last day is before the cutoff.
                if (partitionMonth.atEndOfMonth().isBefore(cutoff)) {
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
