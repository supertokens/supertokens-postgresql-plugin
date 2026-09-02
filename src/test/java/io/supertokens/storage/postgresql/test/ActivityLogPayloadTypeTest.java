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
import io.supertokens.pluginInterface.auditlog.AuditLogEvent;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.queries.GeneralQueries;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The activity_log.payload column is JSONB (it originally shipped as TEXT). Covers: a fresh install
 * gets JSONB on the parent and its partitions and accepts a JSON payload through the real insert
 * path; a pre-existing TEXT column migrates in place at startup, preserving NULL and valid-JSON rows;
 * and an old row holding invalid JSON aborts that migration loudly instead of silently dropping it.
 */
public class ActivityLogPayloadTypeTest {

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
     * A freshly created activity_log table has a JSONB payload column, and so do its monthly
     * partitions (PARTITION OF copies the parent's column types). The normal insert path accepts a
     * JSON payload and it reads back as real JSONB (a JSONB-only operator succeeds on it).
     */
    @Test
    public void freshInstallHasJsonbPayloadOnParentAndPartitionsAndAcceptsJson() throws Exception {
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

        assertEquals("jsonb", payloadColumnType(storage, table));
        // The current month's partition is pre-created at install and must inherit the parent's type.
        assertEquals("jsonb", payloadColumnType(storage, partitionFullName(table, thisMonth)));

        // A JSON payload written through the production insert path must land and read back as JSONB.
        long createdAt = System.currentTimeMillis();
        storage.createActivityLogEntry(new TenantIdentifier(null, null, null), new AuditLogEvent(
                "public", "public", null, null, "test_event", "success", null, null,
                createdAt, "{\"k\":\"v\"}"));

        Connection con = ConnectionPool.getConnection(storage);
        try (PreparedStatement pst = con.prepareStatement(
                "SELECT payload->>'k' AS v FROM " + table + " WHERE created_at = ?")) {
            pst.setLong(1, createdAt);
            try (ResultSet rs = pst.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("v", rs.getString("v"));
            }
        } finally {
            con.close();
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * A deployment provisioned before the type change has a TEXT payload column. Startup migrates it
     * in place to JSONB, preserving a NULL row and a valid-JSON-string row (which becomes real JSONB).
     */
    @Test
    public void existingTextPayloadColumnMigratesPreservingNullAndValidJson() throws Exception {
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
        long nullRowAt = monthStartMillis(thisMonth) + 1000;
        long jsonRowAt = monthStartMillis(thisMonth) + 2000;

        Connection con = ConnectionPool.getConnection(storage);
        try {
            con.setAutoCommit(true);
            recreateLegacyTextTable(con, table, thisMonth);
            insertLegacyRow(con, table, nullRowAt, null);
            insertLegacyRow(con, table, jsonRowAt, "{\"a\":1}");
            assertEquals("text", payloadColumnType(storage, table));
        } finally {
            con.close();
        }

        // Re-running startup DDL detects the TEXT column and migrates it to JSONB.
        Connection migrateCon = ConnectionPool.getConnection(storage);
        try {
            GeneralQueries.createTablesIfNotExists(storage, migrateCon);
        } finally {
            migrateCon.close();
        }

        assertEquals("jsonb", payloadColumnType(storage, table));

        Connection readCon = ConnectionPool.getConnection(storage);
        try {
            // The NULL row survived as NULL.
            try (PreparedStatement pst = readCon.prepareStatement(
                    "SELECT payload IS NULL AS is_null FROM " + table + " WHERE created_at = ?")) {
                pst.setLong(1, nullRowAt);
                try (ResultSet rs = pst.executeQuery()) {
                    assertTrue(rs.next());
                    assertTrue(rs.getBoolean("is_null"));
                }
            }
            // The valid-JSON row is now real JSONB (a JSONB-only operator reads its field back).
            try (PreparedStatement pst = readCon.prepareStatement(
                    "SELECT payload->>'a' AS a FROM " + table + " WHERE created_at = ?")) {
                pst.setLong(1, jsonRowAt);
                try (ResultSet rs = pst.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("1", rs.getString("a"));
                }
            }
        } finally {
            readCon.close();
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * If an old TEXT row holds a value that is not valid JSON, the migration's payload::jsonb cast
     * must fail loudly (startup throws) rather than silently skip the row - and, the failure having
     * rolled back, the column must remain unmigrated so a fixed re-run can retry.
     */
    @Test
    public void invalidJsonInOldRowFailsMigrationLoudly() throws Exception {
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

        Connection con = ConnectionPool.getConnection(storage);
        try {
            con.setAutoCommit(true);
            recreateLegacyTextTable(con, table, thisMonth);
            insertLegacyRow(con, table, monthStartMillis(thisMonth) + 1000, "this is not json");
        } finally {
            con.close();
        }

        Connection migrateCon = ConnectionPool.getConnection(storage);
        try {
            GeneralQueries.createTablesIfNotExists(storage, migrateCon);
            fail("migration should have thrown on the invalid-JSON row");
        } catch (Exception expected) {
            // expected: the payload::jsonb cast rejects the malformed row.
        } finally {
            migrateCon.close();
        }

        // The failed migration rolled back: the column is still TEXT, not half-migrated.
        assertFalse("jsonb".equals(payloadColumnType(storage, table)));
        assertEquals("text", payloadColumnType(storage, table));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private static String partitionFullName(String table, YearMonth month) {
        return table + "_p" + month.format(MONTH_SUFFIX_FORMAT);
    }

    private static long monthStartMillis(YearMonth month) {
        return month.atDay(1).toEpochDay() * MILLIS_PER_DAY;
    }

    /** Reads the SQL type of the payload column of the given relation (parent table or a partition). */
    private String payloadColumnType(Start storage, String relName) throws Exception {
        String query = "SELECT format_type(a.atttypid, a.atttypmod) AS col_type FROM pg_attribute a"
                + " WHERE a.attrelid = ?::regclass AND a.attname = 'payload' AND NOT a.attisdropped";
        Connection con = ConnectionPool.getConnection(storage);
        try (PreparedStatement pst = con.prepareStatement(query)) {
            pst.setString(1, relName);
            try (ResultSet rs = pst.executeQuery()) {
                assertTrue("payload column not found on " + relName, rs.next());
                return rs.getString("col_type");
            }
        } finally {
            con.close();
        }
    }

    /**
     * Drops the current activity_log table and recreates it in its original shape with a TEXT payload
     * column (a DEFAULT partition plus the given month's partition so inserts route), simulating a
     * deployment provisioned before the JSONB change.
     */
    private void recreateLegacyTextTable(Connection con, String table, YearMonth month) throws Exception {
        long fromMillis = monthStartMillis(month);
        long toMillis = monthStartMillis(month.plusMonths(1));
        try (Statement st = con.createStatement()) {
            st.executeUpdate("DROP TABLE IF EXISTS " + table + " CASCADE");
            st.executeUpdate("CREATE TABLE " + table + " ("
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
                    + ") PARTITION BY RANGE (created_at)");
            st.executeUpdate("CREATE TABLE " + table + "_default PARTITION OF " + table + " DEFAULT");
            st.executeUpdate("CREATE TABLE " + partitionFullName(table, month) + " PARTITION OF " + table
                    + " FOR VALUES FROM (" + fromMillis + ") TO (" + toMillis + ")");
        }
    }

    private void insertLegacyRow(Connection con, String table, long createdAt, String payload) throws Exception {
        try (PreparedStatement pst = con.prepareStatement("INSERT INTO " + table
                + " (app_id, tenant_id, event_type, created_at, payload) VALUES (?, ?, ?, ?, ?)")) {
            pst.setString(1, "public");
            pst.setString(2, "public");
            pst.setString(3, "test_event");
            pst.setLong(4, createdAt);
            pst.setString(5, payload);
            pst.executeUpdate();
        }
    }
}
