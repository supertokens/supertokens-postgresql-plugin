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
 *
 */

package io.supertokens.storage.postgresql.test;

import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.auditlog.ActivityLogStorage;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ActivityLogPartitionTest {

    private static final long MILLIS_PER_DAY = 24L * 60 * 60 * 1000;
    private static final DateTimeFormatter MONTH_SUFFIX_FORMAT = DateTimeFormatter.ofPattern("yyyyMM");

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    @AfterClass
    public static void afterTesting() {
        Utils.afterTesting();
    }

    @Before
    public void beforeEach() {
        Utils.reset();
    }

    /**
     * maintainActivityLogPartitions() must (a) pre-create the partitions for the current and next
     * months, and (b) drop a monthly partition once its entire month is older than the 31-day
     * retention window — deleting its rows along with it — while keeping months still within the
     * window (e.g. last month).
     */
    @Test
    public void oldMonthPartitionsAreDroppedAndUpcomingArePreCreated() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        String table = Config.getConfig(storage).getActivityLogTable();

        YearMonth thisMonth = YearMonth.now(ZoneOffset.UTC);
        YearMonth oldMonth = thisMonth.minusMonths(3);  // whole month outside retention -> must be dropped
        YearMonth lastMonth = thisMonth.minusMonths(1);  // its tail is still within 31 days -> must be kept

        // Set up a month partition well outside the retention window, with a row in it, plus last
        // month's partition which is still within the window.
        createMonthlyPartition(storage, table, oldMonth);
        createMonthlyPartition(storage, table, lastMonth);
        insertEventIn(storage, table, oldMonth);

        assertTrue(partitionExists(storage, table, oldMonth));
        assertEquals(1, countEventsIn(storage, table, oldMonth));

        // Run the maintenance the cron would run.
        ((ActivityLogStorage) storage).maintainActivityLogPartitions();

        // The old partition (and the row it held) is gone; last month's survives.
        assertFalse(partitionExists(storage, table, oldMonth));
        assertEquals(0, countEventsIn(storage, table, oldMonth));
        assertTrue(partitionExists(storage, table, lastMonth));

        // Upcoming months were pre-created (this month + the premake window).
        assertTrue(partitionExists(storage, table, thisMonth));
        assertTrue(partitionExists(storage, table, thisMonth.plusMonths(1)));

        // Running it again is a no-op (CREATE ... IF NOT EXISTS / nothing new to drop).
        ((ActivityLogStorage) storage).maintainActivityLogPartitions();
        assertTrue(partitionExists(storage, table, thisMonth));
        assertFalse(partitionExists(storage, table, oldMonth));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * Rows that land in the DEFAULT backstop for a month whose partition doesn't exist yet (e.g.
     * the core was stopped or paused across a month boundary) must not wedge the maintenance cron.
     * Postgres refuses to create a partition whose range is occupied by rows in DEFAULT
     * ("updated partition constraint for default partition would be violated by some row"), so the
     * cron has to move them into the new monthly partition instead of failing on them forever.
     * Rows in DEFAULT older than the retention window must be purged rather than moved.
     */
    @Test
    public void maintenanceHealsRowsStrandedInDefaultPartition() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        String table = Config.getConfig(storage).getActivityLogTable();

        YearMonth thisMonth = YearMonth.now(ZoneOffset.UTC);
        YearMonth expiredMonth = thisMonth.minusMonths(3);

        // Simulate a core that was paused across the month boundary: this month's partition does
        // not exist, and events for this month (and for a long-expired month) are stranded in the
        // DEFAULT backstop.
        dropPartition(storage, table, thisMonth);
        insertEventIn(storage, table, thisMonth);
        insertEventIn(storage, table, expiredMonth);
        assertEquals(2, countRowsInDefault(storage, table));

        // Before the fix this threw: "updated partition constraint for default partition
        // \"activity_log_default\" would be violated by some row".
        ((ActivityLogStorage) storage).maintainActivityLogPartitions();

        // This month's row was moved into the freshly created partition; the expired one was purged.
        assertTrue(partitionExists(storage, table, thisMonth));
        assertEquals(0, countRowsInDefault(storage, table));
        assertEquals(1, countEventsIn(storage, table, thisMonth));
        assertEquals(0, countEventsIn(storage, table, expiredMonth));

        // Running it again is a no-op.
        ((ActivityLogStorage) storage).maintainActivityLogPartitions();
        assertEquals(1, countEventsIn(storage, table, thisMonth));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private static String partitionFullName(String table, YearMonth month) {
        return table + "_p" + month.format(MONTH_SUFFIX_FORMAT);
    }

    private void dropPartition(Start storage, String table, YearMonth month) throws Exception {
        String query = "DROP TABLE IF EXISTS " + partitionFullName(table, month);
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.executeUpdate();
            }
            return null;
        });
    }

    private int countRowsInDefault(Start storage, String table) throws Exception {
        String query = "SELECT COUNT(*) FROM " + table + "_default";
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                try (ResultSet rs = pst.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        });
    }

    private static long monthStartMillis(YearMonth month) {
        return month.atDay(1).toEpochDay() * MILLIS_PER_DAY;
    }

    private void createMonthlyPartition(Start storage, String table, YearMonth month) throws Exception {
        long fromMillis = monthStartMillis(month);
        long toMillis = monthStartMillis(month.plusMonths(1));
        String query = "CREATE TABLE IF NOT EXISTS " + partitionFullName(table, month)
                + " PARTITION OF " + table + " FOR VALUES FROM (" + fromMillis + ") TO (" + toMillis + ")";
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.executeUpdate();
            }
            return null;
        });
    }

    private void insertEventIn(Start storage, String table, YearMonth month) throws Exception {
        long createdAt = monthStartMillis(month) + 1000;
        String query = "INSERT INTO " + table
                + " (app_id, tenant_id, event_type, created_at) VALUES (?, ?, ?, ?)";
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setString(1, "public");
                pst.setString(2, "public");
                pst.setString(3, "test_event");
                pst.setLong(4, createdAt);
                pst.executeUpdate();
            }
            return null;
        });
    }

    private int countEventsIn(Start storage, String table, YearMonth month) throws Exception {
        long fromMillis = monthStartMillis(month);
        long toMillis = monthStartMillis(month.plusMonths(1));
        String query = "SELECT COUNT(*) FROM " + table + " WHERE created_at >= ? AND created_at < ?";
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setLong(1, fromMillis);
                pst.setLong(2, toMillis);
                try (ResultSet rs = pst.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        });
    }

    private boolean partitionExists(Start storage, String table, YearMonth month) throws Exception {
        String query = "SELECT to_regclass(?) IS NOT NULL AS exists";
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setString(1, partitionFullName(table, month));
                try (ResultSet rs = pst.executeQuery()) {
                    rs.next();
                    return rs.getBoolean("exists");
                }
            }
        });
    }
}
