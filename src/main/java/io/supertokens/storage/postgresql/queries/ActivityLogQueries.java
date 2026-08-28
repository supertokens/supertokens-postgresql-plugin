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
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.PreparedStatementValueSetter;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import io.supertokens.storage.postgresql.annotations.AtomicAutoCommitWrite;

/**
 * Append-only audit/activity log.
 *
 * The table is range-partitioned by {@code created_at} (epoch millis) into one partition per UTC
 * calendar month. Upcoming months' partitions are pre-created — at table creation and by a daily
 * maintenance cron ({@code CleanupActivityLogPartitions}) — and a monthly partition is dropped once
 * its entire month is older than the retention window the caller supplies. A DEFAULT partition is a
 * backstop so inserts never fail if the cron lapses beyond the pre-created window.
 *
 * No primary key or foreign key — the identity sequence makes {@code id} unique by construction.
 * The only index is a BRIN on {@code created_at} created on the parent table (Postgres propagates
 * it to every child partition automatically): nearly free on writes for append-only data and enough
 * to prune time-range scans. Requires PostgreSQL 11+.
 */
public class ActivityLogQueries {

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
                // JSONB (not TEXT): structured lifecycle-event payloads will start flowing into this
                // column, and JSONB rejects malformed JSON at write time. Monthly partitions are created
                // with PARTITION OF, which copies the parent's column definitions verbatim, so every new
                // partition inherits this type automatically — no per-partition DDL to keep in sync.
                + "payload JSONB"
                + ") PARTITION BY RANGE (created_at);";
    }

    /**
     * One-time, idempotent migration of a pre-existing {@code payload} column from TEXT to JSONB (the
     * column originally shipped as TEXT). Applied to the partitioned parent, so Postgres rewrites every
     * child partition with the same cast. {@code USING payload::jsonb} makes an old row holding invalid
     * JSON fail the migration loudly (invalid_text_representation) rather than being silently dropped —
     * which is why the caller runs this in the transactional DDL batch, not the best-effort index
     * backfill. The caller guards it on the column not already being JSONB, so it never re-runs.
     */
    public static String getQueryToMigratePayloadColumnToJsonb(Start start) {
        return "ALTER TABLE " + Config.getConfig(start).getActivityLogTable()
                + " ALTER COLUMN payload TYPE JSONB USING payload::jsonb;";
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

    private static String getQueryToInsertActivityLogEntry(Start start) {
        return "INSERT INTO " + Config.getConfig(start).getActivityLogTable()
                + " (app_id, tenant_id, recipe_user_id, primary_or_recipe_user_id, event_type, status,"
                + " auth_principal, identifier, created_at, payload)"
                // payload is bound as a String; ?::jsonb casts it explicitly so the JSONB column accepts
                // it (the driver sends the parameter as text, and text is not implicitly coercible to
                // JSONB). A null payload casts to SQL NULL unchanged.
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";
    }

    private static PreparedStatementValueSetter activityLogEntrySetter(TenantIdentifier tenantIdentifier,
                                                                       AuditLogEvent event) {
        return pst -> {
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
        };
    }

    @AtomicAutoCommitWrite(justification = "append-only activity-log write; the insert is the operation")
    public static void createActivityLogEntry(Start start, TenantIdentifier tenantIdentifier, AuditLogEvent event)
            throws SQLException, StorageQueryException {
        update(start, getQueryToInsertActivityLogEntry(start), activityLogEntrySetter(tenantIdentifier, event));
    }

    /**
     * Same insert as {@link #createActivityLogEntry}, but on the caller's transaction connection, so
     * the entry commits or rolls back atomically with the surrounding mutation.
     */
    public static void createActivityLogEntry_Transaction(Connection con, Start start,
                                                          TenantIdentifier tenantIdentifier, AuditLogEvent event)
            throws SQLException, StorageQueryException {
        update(con, getQueryToInsertActivityLogEntry(start), activityLogEntrySetter(tenantIdentifier, event));
    }

    /**
     * Cheap existence check for rollup-relevant activity newer than {@code sinceMillis} — the rows
     * the last-active rollup would fold ({@code user_last_active}) or reconcile ({@code account_linking}).
     * Storage-wide, no app predicate; lets the rollup cron skip work when there is nothing new.
     */
    public static boolean hasUnfoldedActivitySince(Start start, long sinceMillis)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT EXISTS (SELECT 1 FROM " + Config.getConfig(start).getActivityLogTable()
                + " WHERE event_type IN ('user_last_active', 'account_linking') AND created_at > ?) AS has_activity";
        return execute(start, QUERY, pst -> pst.setLong(1, sinceMillis), result -> {
            if (result.next()) {
                return result.getBoolean("has_activity");
            }
            return false;
        });
    }

    /**
     * App-scoped, window-bounded read of the activity log so callers can fold lifecycle events in Java
     * rather than in the database. Returns the {@code appIdentifier} events (across all its tenants, each
     * row's {@code tenant_id} preserved) whose {@code event_type} is in {@code eventTypes} and whose
     * {@code created_at} lies in the half-open interval {@code (fromExclusiveMillis, toInclusiveMillis]},
     * ordered by {@code created_at} ascending and capped at {@code limit} rows.
     *
     * The range predicate is kept literally on {@code created_at} (no expression around it) so the monthly
     * partition pruning and the BRIN index on {@code created_at} both apply. {@code LIMIT} is applied in the
     * query, never after materialising the window, so a caller can pass {@code cap + 1} to detect an
     * over-cap window without reading all of it. {@code payload} is cast to text ({@code ::text}) so the
     * JSONB column comes back as its serialised JSON string; a null payload stays null.
     */
    public static List<AuditLogEvent> getActivityLogEntriesForApp(Start start, AppIdentifier appIdentifier,
                                                                  Set<String> eventTypes, long fromExclusiveMillis,
                                                                  long toInclusiveMillis, int limit)
            throws SQLException, StorageQueryException {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be > 0");
        }
        if (eventTypes == null || eventTypes.isEmpty()) {
            throw new IllegalArgumentException("eventTypes must be non-empty");
        }
        String[] eventTypesArray = eventTypes.toArray(new String[0]);
        String QUERY = "SELECT app_id, tenant_id, recipe_user_id, primary_or_recipe_user_id, event_type, status,"
                + " auth_principal, identifier, created_at, payload::text AS payload FROM "
                + Config.getConfig(start).getActivityLogTable()
                + " WHERE app_id = ? AND event_type = ANY(?) AND created_at > ? AND created_at <= ?"
                + " ORDER BY created_at ASC LIMIT ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            Array eventTypesSqlArray = pst.getConnection().createArrayOf("VARCHAR", eventTypesArray);
            pst.setArray(2, eventTypesSqlArray);
            pst.setLong(3, fromExclusiveMillis);
            pst.setLong(4, toInclusiveMillis);
            pst.setInt(5, limit);
        }, result -> {
            List<AuditLogEvent> events = new ArrayList<>();
            while (result.next()) {
                events.add(auditLogEventFromRow(result));
            }
            return events;
        });
    }

    private static AuditLogEvent auditLogEventFromRow(ResultSet result) throws SQLException {
        return new AuditLogEvent(
                result.getString("app_id"),
                result.getString("tenant_id"),
                result.getString("recipe_user_id"),
                result.getString("primary_or_recipe_user_id"),
                result.getString("event_type"),
                result.getString("status"),
                result.getString("auth_principal"),
                result.getString("identifier"),
                result.getLong("created_at"),
                result.getString("payload"));
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
     * {@code retentionDays} days. Retention is supplied by the caller (from configuration) rather
     * than hardcoded. Idempotent; intended to be run daily.
     *
     * If rows for a month landed in the DEFAULT backstop before that month's partition existed
     * (e.g. the core was stopped or paused across a month boundary, so neither startup nor the
     * cron pre-created it), Postgres refuses to create the partition — "updated partition
     * constraint for default partition would be violated by some row". In that case the rows are
     * moved out of DEFAULT and into the new partition in a single transaction, so the cron heals
     * the backstop instead of failing on it forever.
     */
    public static void maintainPartitions(Start start, int retentionDays)
            throws SQLException, StorageQueryException {
        YearMonth thisMonth = YearMonth.now(ZoneOffset.UTC);
        for (int i = 0; i <= PREMAKE_MONTHS; i++) {
            ensureMonthlyPartition(start, thisMonth.plusMonths(i));
        }
        dropPartitionsOlderThanRetention(start, retentionDays);
        purgeExpiredRowsFromDefaultPartition(start, retentionDays);
    }

    private static void ensureMonthlyPartition(Start start, YearMonth month)
            throws SQLException, StorageQueryException {
        try {
            update(start, getQueryToCreateMonthlyPartition(start, month), pst -> {});
        } catch (SQLException e) {
            if (!isDefaultPartitionConflict(e)) {
                throw e;
            }
            createPartitionMovingRowsFromDefault(start, month);
        }
    }

    /**
     * Matches Postgres's refusal to create/attach a partition while the DEFAULT partition holds
     * rows belonging to the new partition's range (errcode 23514, check_violation).
     */
    private static boolean isDefaultPartitionConflict(SQLException e) {
        String message = e.getMessage();
        return message != null && message.contains("would be violated by some row");
    }

    /**
     * Creates the monthly partition after moving that month's rows out of the DEFAULT partition,
     * all in one transaction. The parent is locked first — the same order in which row inserts
     * acquire locks, so this cannot deadlock with them — which also stops new rows from slipping
     * into DEFAULT between the move and the partition creation.
     */
    private static void createPartitionMovingRowsFromDefault(Start start, YearMonth month)
            throws SQLException, StorageQueryException {
        String tableName = Config.getConfig(start).getActivityLogTable();
        String defaultPartition = tableName + "_default";
        long fromMillis = month.atDay(1).toEpochDay() * MILLIS_PER_DAY;
        long toMillis = month.plusMonths(1).atDay(1).toEpochDay() * MILLIS_PER_DAY;
        String rangeCondition = " WHERE created_at >= " + fromMillis + " AND created_at < " + toMillis;

        try (Connection con = ConnectionPool.getConnection(start)) {
            boolean originalAutoCommit = con.getAutoCommit();
            con.setAutoCommit(false);
            try {
                update(con, "LOCK TABLE " + tableName + " IN ACCESS EXCLUSIVE MODE", pst -> {});
                // ON COMMIT DROP also drops on rollback, so nothing leaks into the pooled session.
                update(con, "CREATE TEMP TABLE activity_log_default_moved ON COMMIT DROP AS"
                        + " SELECT * FROM " + defaultPartition + rangeCondition, pst -> {});
                update(con, "DELETE FROM " + defaultPartition + rangeCondition, pst -> {});
                update(con, getQueryToCreateMonthlyPartition(start, month), pst -> {});
                // Re-insert through the parent so the rows route into the new partition;
                // OVERRIDING SYSTEM VALUE keeps their original identity ids.
                update(con, "INSERT INTO " + tableName
                        + " (id, app_id, tenant_id, recipe_user_id, primary_or_recipe_user_id, event_type,"
                        + " status, auth_principal, identifier, created_at, payload)"
                        + " OVERRIDING SYSTEM VALUE"
                        + " SELECT id, app_id, tenant_id, recipe_user_id, primary_or_recipe_user_id, event_type,"
                        + " status, auth_principal, identifier, created_at, payload"
                        + " FROM activity_log_default_moved", pst -> {});
                con.commit();
            } catch (SQLException e) {
                con.rollback();
                throw e;
            } finally {
                con.setAutoCommit(originalAutoCommit);
            }
        }
    }

    /**
     * Rows in the DEFAULT partition are only ever for months without a partition (past months
     * whose partition was already dropped, or timestamps outside the premake window), so retention
     * is enforced on them directly — dropping monthly partitions alone would keep them forever.
     */
    @AtomicAutoCommitWrite(justification = "time-based cleanup sweep; single auto-commit DELETE with nothing to be atomic with")
    private static void purgeExpiredRowsFromDefaultPartition(Start start, int retentionDays)
            throws SQLException, StorageQueryException {
        String defaultPartition = Config.getConfig(start).getActivityLogTable() + "_default";
        long cutoffMillis = LocalDate.now(ZoneOffset.UTC).minusDays(retentionDays).toEpochDay() * MILLIS_PER_DAY;
        update(start, "DELETE FROM " + defaultPartition + " WHERE created_at < " + cutoffMillis, pst -> {});
    }

    private static void dropPartitionsOlderThanRetention(Start start, int retentionDays)
            throws SQLException, StorageQueryException {
        String tableName = Config.getConfig(start).getActivityLogTable();
        String LIST_QUERY = "SELECT n.nspname AS schema_name, c.relname AS partition_name"
                + " FROM pg_inherits i"
                + " JOIN pg_class c ON c.oid = i.inhrelid"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE i.inhparent = ?::regclass";

        LocalDate cutoff = LocalDate.now(ZoneOffset.UTC).minusDays(retentionDays);

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
