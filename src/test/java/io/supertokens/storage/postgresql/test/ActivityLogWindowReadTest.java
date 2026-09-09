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

import com.google.gson.JsonParser;
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.auditlog.ActivityLogStorage;
import io.supertokens.pluginInterface.auditlog.AuditLogEvent;
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
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ActivityLogWindowReadTest {

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
     * Starts a core against PostgreSQL and returns its Start storage, or null when the configured
     * storage is not SQL (mirrors the guard the other activity-log tests use).
     */
    private Start startStorage(TestingProcessManager.TestingProcess process) throws Exception {
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            return null;
        }
        return (Start) StorageLayer.getStorage(process.getProcess());
    }

    private void insertEvent(Start storage, String table, String appId, String tenantId, String eventType,
                             long createdAt, String payloadJson) throws Exception {
        String query = "INSERT INTO " + table
                + " (app_id, tenant_id, recipe_user_id, primary_or_recipe_user_id, event_type, status,"
                + " auth_principal, identifier, created_at, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setString(1, appId);
                pst.setString(2, tenantId);
                pst.setString(3, null);
                pst.setString(4, null);
                pst.setString(5, eventType);
                pst.setString(6, "success");
                pst.setString(7, null);
                pst.setString(8, null);
                pst.setLong(9, createdAt);
                pst.setString(10, payloadJson);
                pst.executeUpdate();
            }
            return null;
        });
    }

    private List<Long> createdAtsOf(List<AuditLogEvent> events) {
        return events.stream().map(e -> e.createdAt).collect(Collectors.toList());
    }

    /** The window is half-open (fromExclusive, toInclusive]: a row exactly at `from` is out, at `to` is in. */
    @Test
    public void windowBoundsAreHalfOpen() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        Start storage = startStorage(process);
        if (storage == null) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }
        String table = Config.getConfig(storage).getActivityLogTable();

        long from = monthStartMillis(YearMonth.now(ZoneOffset.UTC)) + 5 * MILLIS_PER_DAY;
        long to = from + 1000;

        insertEvent(storage, table, "public", "public", "account_linking", from, null);        // == from -> excluded
        insertEvent(storage, table, "public", "public", "account_linking", from + 1, null);     // inside -> included
        insertEvent(storage, table, "public", "public", "account_linking", to, null);           // == to -> included
        insertEvent(storage, table, "public", "public", "account_linking", to + 1, null);       // > to -> excluded

        List<AuditLogEvent> events = ((ActivityLogStorage) storage).getActivityLogEntriesForApp(
                new AppIdentifier(null, null), Set.of("account_linking"), from, to, 100);

        assertEquals(List.of(from + 1, to), createdAtsOf(events));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * Only the requested event types are returned; another app in the same database is excluded; and rows
     * from every tenant of the queried app are returned, each carrying its own tenantId.
     */
    @Test
    public void filtersByEventTypeAppAndReturnsAllTenants() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        Start storage = startStorage(process);
        if (storage == null) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }
        String table = Config.getConfig(storage).getActivityLogTable();

        long base = monthStartMillis(YearMonth.now(ZoneOffset.UTC)) + 5 * MILLIS_PER_DAY;
        long from = base - 1;
        long to = base + 100;

        // Wanted: account_linking rows for app1, across its public/t1/t2 tenants.
        insertEvent(storage, table, "app1", "public", "account_linking", base, null);
        insertEvent(storage, table, "app1", "t1", "account_linking", base + 1, null);
        insertEvent(storage, table, "app1", "t2", "account_linking", base + 2, null);
        // Same window, wrong type -> excluded.
        insertEvent(storage, table, "app1", "public", "user_last_active", base + 3, null);
        // Same window, same type, different app -> excluded.
        insertEvent(storage, table, "app2", "public", "account_linking", base + 4, null);

        List<AuditLogEvent> events = ((ActivityLogStorage) storage).getActivityLogEntriesForApp(
                new AppIdentifier(null, "app1"), Set.of("account_linking"), from, to, 100);

        assertEquals(3, events.size());
        for (AuditLogEvent e : events) {
            assertEquals("app1", e.appId);
            assertEquals("account_linking", e.eventType);
        }
        assertEquals(Set.of("public", "t1", "t2"),
                events.stream().map(e -> e.tenantId).collect(Collectors.toSet()));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /** Rows come back ascending by createdAt; with more rows than limit, exactly the oldest `limit` rows. */
    @Test
    public void ordersAscendingAndLimitReturnsOldest() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        Start storage = startStorage(process);
        if (storage == null) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }
        String table = Config.getConfig(storage).getActivityLogTable();

        long base = monthStartMillis(YearMonth.now(ZoneOffset.UTC)) + 5 * MILLIS_PER_DAY;
        // Insert out of order to prove the ORDER BY, not insertion order.
        long[] times = {base + 40, base + 10, base + 30, base + 20, base + 50};
        for (long t : times) {
            insertEvent(storage, table, "public", "public", "account_linking", t, null);
        }

        List<AuditLogEvent> all = ((ActivityLogStorage) storage).getActivityLogEntriesForApp(
                new AppIdentifier(null, null), Set.of("account_linking"), base, base + 1000, 100);
        assertEquals(List.of(base + 10, base + 20, base + 30, base + 40, base + 50), createdAtsOf(all));

        // limit picks the oldest three.
        List<AuditLogEvent> limited = ((ActivityLogStorage) storage).getActivityLogEntriesForApp(
                new AppIdentifier(null, null), Set.of("account_linking"), base, base + 1000, 3);
        assertEquals(List.of(base + 10, base + 20, base + 30), createdAtsOf(limited));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /** JSONB payload round-trips as JSON-equal text (key order/whitespace normalised); null stays null. */
    @Test
    public void payloadRoundTripsAsJsonAndNullStaysNull() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        Start storage = startStorage(process);
        if (storage == null) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }
        String table = Config.getConfig(storage).getActivityLogTable();

        long base = monthStartMillis(YearMonth.now(ZoneOffset.UTC)) + 5 * MILLIS_PER_DAY;
        String payload = "{ \"z\": 1,  \"a\": { \"c\": 3, \"b\": 2 } }";
        insertEvent(storage, table, "public", "public", "account_linking", base + 1, payload);
        insertEvent(storage, table, "public", "public", "account_linking", base + 2, null);

        List<AuditLogEvent> events = ((ActivityLogStorage) storage).getActivityLogEntriesForApp(
                new AppIdentifier(null, null), Set.of("account_linking"), base, base + 1000, 100);
        assertEquals(2, events.size());

        AuditLogEvent withPayload = events.get(0);
        assertNotNull(withPayload.payload);
        JsonParser parser = new JsonParser();
        assertEquals(parser.parse(payload), parser.parse(withPayload.payload));

        assertNull(events.get(1).payload);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /** A window spanning a monthly partition boundary returns rows from both partitions. */
    @Test
    public void windowSpanningMonthlyPartitionBoundaryReturnsBothPartitions() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        Start storage = startStorage(process);
        if (storage == null) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }
        String table = Config.getConfig(storage).getActivityLogTable();

        YearMonth thisMonth = YearMonth.now(ZoneOffset.UTC);
        YearMonth nextMonth = thisMonth.plusMonths(1);  // partition pre-created at startup
        long inThisMonth = monthStartMillis(thisMonth) + 20 * MILLIS_PER_DAY;
        long inNextMonth = monthStartMillis(nextMonth) + 2 * MILLIS_PER_DAY;

        insertEvent(storage, table, "public", "public", "account_linking", inThisMonth, null);
        insertEvent(storage, table, "public", "public", "account_linking", inNextMonth, null);

        List<AuditLogEvent> events = ((ActivityLogStorage) storage).getActivityLogEntriesForApp(
                new AppIdentifier(null, null), Set.of("account_linking"),
                inThisMonth - 1, inNextMonth, 100);

        assertEquals(List.of(inThisMonth, inNextMonth), createdAtsOf(events));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /** An empty window returns an empty list, never null. */
    @Test
    public void emptyWindowReturnsEmptyListNotNull() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        Start storage = startStorage(process);
        if (storage == null) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }
        String table = Config.getConfig(storage).getActivityLogTable();

        long base = monthStartMillis(YearMonth.now(ZoneOffset.UTC)) + 5 * MILLIS_PER_DAY;
        // A row exists, but outside the queried window.
        insertEvent(storage, table, "public", "public", "account_linking", base + 5000, null);

        List<AuditLogEvent> events = ((ActivityLogStorage) storage).getActivityLogEntriesForApp(
                new AppIdentifier(null, null), Set.of("account_linking"), base, base + 100, 100);

        assertNotNull(events);
        assertTrue(events.isEmpty());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /** limit <= 0 or an empty eventTypes set is rejected before touching the database. */
    @Test
    public void rejectsNonPositiveLimitAndEmptyEventTypes() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        Start storage = startStorage(process);
        if (storage == null) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        AppIdentifier app = new AppIdentifier(null, null);
        try {
            ((ActivityLogStorage) storage).getActivityLogEntriesForApp(app, Set.of("account_linking"), 0, 100, 0);
            fail("expected IllegalArgumentException for limit <= 0");
        } catch (IllegalArgumentException expected) {
            // ok
        }
        try {
            ((ActivityLogStorage) storage).getActivityLogEntriesForApp(app, Set.of(), 0, 100, 10);
            fail("expected IllegalArgumentException for empty eventTypes");
        } catch (IllegalArgumentException expected) {
            // ok
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private static long monthStartMillis(YearMonth month) {
        return month.atDay(1).toEpochDay() * MILLIS_PER_DAY;
    }
}
