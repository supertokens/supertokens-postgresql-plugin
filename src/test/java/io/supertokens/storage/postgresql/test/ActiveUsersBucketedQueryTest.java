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
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ActiveUsersBucketedQueryTest {

    private static final long DAY = 24 * 3600 * 1000L;

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
     * countUsersActiveSinceGroupedByDay replaces one countUsersActiveSince call per day threshold.
     * This pins the equivalence: for every i in 0..30, the running total of buckets 0..i must equal
     * countUsersActiveSince(now - (i+1) days).
     */
    @Test
    public void bucketedCountsMatchPerThresholdCounts() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        AppIdentifier appIdentifier = new AppIdentifier(null, null);
        long now = System.currentTimeMillis();

        // users per "days ago" bucket; buckets 2..4 and 6..28 stay empty on purpose, and the
        // bucket-33 user lies beyond the 31-day window so it must be filtered out by sinceTime
        Map<Integer, Integer> seeded = new LinkedHashMap<>();
        seeded.put(0, 3);
        seeded.put(1, 2);
        seeded.put(5, 1);
        seeded.put(29, 1);
        seeded.put(30, 2);
        seeded.put(33, 1);

        int userCounter = 0;
        for (Map.Entry<Integer, Integer> entry : seeded.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                // mid-bucket timestamp, so no FLOOR boundary flakiness
                long lastActiveTime = now - entry.getKey() * DAY - DAY / 2;
                storage.updateLastActive(appIdentifier, "bucketed-user-" + (userCounter++), lastActiveTime);
            }
        }

        long sinceTime = now - 31 * DAY;
        Map<Integer, Integer> buckets = storage.countUsersActiveSinceGroupedByDay(appIdentifier, sinceTime, now);

        // the user last active 33 days ago is outside the window and must not show up at all
        assertFalse(buckets.containsKey(33));

        int runningTotal = 0;
        for (int i = 0; i <= 30; i++) {
            runningTotal += buckets.getOrDefault(i, 0);
            assertEquals("active-since count mismatch for the " + (i + 1) + "-day threshold",
                    storage.countUsersActiveSince(appIdentifier, now - (i + 1) * DAY), runningTotal);
        }

        // everything inside the window is accounted for exactly once across the buckets
        assertEquals(storage.countUsersActiveSince(appIdentifier, sinceTime),
                buckets.values().stream().mapToInt(Integer::intValue).sum());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * The composite (app_id, last_active_time) index must be created for FRESH databases by
     * createTablesIfNotExists, not only backfilled onto pre-existing ones. This pins the
     * regression where it was only emitted in the backfill path (gated on the table already
     * existing), so fresh databases never got it until a second restart.
     */
    @Test
    public void compositeIndexIsCreatedOnFreshDatabase() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());

        // getUserLastActiveTable() may be schema-qualified; pg_indexes.tablename is not
        String table = Config.getConfig(storage).getUserLastActiveTable();
        String tableName = table.contains(".") ? table.substring(table.lastIndexOf('.') + 1) : table;

        List<String> indexNames = storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            List<String> result = new ArrayList<>();
            try (PreparedStatement pst = sqlCon
                    .prepareStatement("SELECT indexname FROM pg_indexes WHERE tablename = ?")) {
                pst.setString(1, tableName);
                try (ResultSet rs = pst.executeQuery()) {
                    while (rs.next()) {
                        result.add(rs.getString("indexname"));
                    }
                }
            }
            return result;
        });

        assertTrue("missing composite index on fresh DB, found only: " + indexNames,
                indexNames.contains("user_last_active_app_id_last_active_time_index"));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
