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
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ActivityLogPartitionTest {

    private static final long MILLIS_PER_DAY = 24L * 60 * 60 * 1000;

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
     * maintainActivityLogPartitions() must (a) pre-create the partitions for today and the next few
     * days, and (b) drop partitions whose data is entirely older than the 31-day retention window,
     * deleting their rows along with them — while keeping partitions still inside the window.
     */
    @Test
    public void oldPartitionsAreDroppedAndUpcomingArePreCreated() throws Exception {
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

        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        LocalDate oldDay = today.minusDays(40);    // outside retention -> must be dropped
        LocalDate recentDay = today.minusDays(30);  // inside retention -> must be kept

        // Set up a partition well outside the retention window, with a row in it, plus one that is
        // still inside the window.
        createDailyPartition(storage, table, oldDay);
        createDailyPartition(storage, table, recentDay);
        insertEventOn(storage, table, oldDay);

        assertTrue(partitionExists(storage, table, oldDay));
        assertEquals(1, countEventsOn(storage, table, oldDay));

        // Run the maintenance the cron would run.
        ((ActivityLogStorage) storage).maintainActivityLogPartitions();

        // The old partition (and the row it held) is gone; the recent one survives.
        assertFalse(partitionExists(storage, table, oldDay));
        assertEquals(0, countEventsOn(storage, table, oldDay));
        assertTrue(partitionExists(storage, table, recentDay));

        // Upcoming days were pre-created (today + the premake window).
        assertTrue(partitionExists(storage, table, today));
        assertTrue(partitionExists(storage, table, today.plusDays(1)));
        assertTrue(partitionExists(storage, table, today.plusDays(2)));

        // Running it again is a no-op (CREATE ... IF NOT EXISTS / nothing new to drop).
        ((ActivityLogStorage) storage).maintainActivityLogPartitions();
        assertTrue(partitionExists(storage, table, today));
        assertFalse(partitionExists(storage, table, oldDay));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private static String partitionFullName(String table, LocalDate day) {
        return table + "_p" + day.format(DateTimeFormatter.BASIC_ISO_DATE);
    }

    private void createDailyPartition(Start storage, String table, LocalDate day) throws Exception {
        long fromMillis = day.toEpochDay() * MILLIS_PER_DAY;
        long toMillis = fromMillis + MILLIS_PER_DAY;
        String query = "CREATE TABLE IF NOT EXISTS " + partitionFullName(table, day)
                + " PARTITION OF " + table + " FOR VALUES FROM (" + fromMillis + ") TO (" + toMillis + ")";
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.executeUpdate();
            }
            return null;
        });
    }

    private void insertEventOn(Start storage, String table, LocalDate day) throws Exception {
        long createdAt = day.toEpochDay() * MILLIS_PER_DAY + 1000;
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

    private int countEventsOn(Start storage, String table, LocalDate day) throws Exception {
        long fromMillis = day.toEpochDay() * MILLIS_PER_DAY;
        long toMillis = fromMillis + MILLIS_PER_DAY;
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

    private boolean partitionExists(Start storage, String table, LocalDate day) throws Exception {
        String query = "SELECT to_regclass(?) IS NOT NULL AS exists";
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setString(1, partitionFullName(table, day));
                try (ResultSet rs = pst.executeQuery()) {
                    rs.next();
                    return rs.getBoolean("exists");
                }
            }
        });
    }
}
