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
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.passwordless.PasswordlessCode;
import io.supertokens.pluginInterface.sqlStorage.SQLStorage.TransactionIsolationLevel;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Pooled connections are already at READ COMMITTED (the pool's connectionInitSql), so a transaction must not
 * spend round trips reading or setting the session isolation level. A caller asking for a stronger level gets
 * it for that transaction only, via a transaction-scoped SET TRANSACTION, so the session default is never
 * changed and the next transaction on the same connection is back at READ COMMITTED.
 *
 * The SQL a transaction sends is observed through the PostgreSQL JDBC driver's own FINEST query log,
 * restricted to the test thread (startTransaction runs synchronously on its caller).
 */
public class TransactionIsolationTest {

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

    @Test
    public void defaultTransactionIssuesNoIsolationLevelRoundTrips() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());

        List<String> sent;
        try (DriverQueryLog log = DriverQueryLog.startForCurrentThread()) {
            storage.startTransaction(con -> {
                runQuery((Connection) con.getConnection(), "SELECT 4160");
                storage.commitTransaction(con);
                return null;
            });
            sent = log.queries();
        }

        // sanity: the capture works, otherwise the negative assertions below prove nothing
        assertTrue(String.valueOf(sent), sent.stream().anyMatch(q -> q.contains("SELECT 4160")));
        assertNoIsolationLevelStatements(sent);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void nonDefaultLevelIsScopedToItsTransaction() throws Exception {
        String[] args = {"../"};

        // one connection, so the follow-up transaction provably reuses the same session
        Utils.setValueInConfig("postgresql_connection_pool_size", "1");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());

        for (TransactionIsolationLevel level : new TransactionIsolationLevel[]{
                TransactionIsolationLevel.REPEATABLE_READ, TransactionIsolationLevel.SERIALIZABLE}) {
            String expected = level == TransactionIsolationLevel.REPEATABLE_READ ? "repeatable read" : "serializable";

            String[] inside = storage.startTransaction(con -> {
                Connection sqlCon = (Connection) con.getConnection();
                String[] result = new String[]{
                        runQuery(sqlCon, "SHOW transaction_isolation"),
                        runQuery(sqlCon, "SHOW default_transaction_isolation"),
                        runQuery(sqlCon, "SELECT pg_backend_pid()::text")};
                storage.commitTransaction(con);
                return result;
            }, level);

            assertEquals(expected, inside[0]);
            // the session default was never touched: the level applied to this transaction only
            assertEquals("read committed", inside[1]);

            String[] after = storage.startTransaction(con -> {
                Connection sqlCon = (Connection) con.getConnection();
                String[] result = new String[]{
                        runQuery(sqlCon, "SHOW transaction_isolation"),
                        runQuery(sqlCon, "SELECT pg_backend_pid()::text")};
                storage.commitTransaction(con);
                return result;
            });

            assertEquals(inside[2], after[1]);
            assertEquals("read committed", after[0]);
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void createDeviceWithCodeRunsAtTheSessionDefault() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());

        List<String> sent;
        try (DriverQueryLog log = DriverQueryLog.startForCurrentThread()) {
            storage.createDeviceWithCode(new TenantIdentifier(null, null, null), "test@example.com", null,
                    "linkCodeSalt", new PasswordlessCode("codeId", "deviceIdHash", "linkCodeHash",
                            System.currentTimeMillis()));
            sent = log.queries();
        }

        assertTrue(String.valueOf(sent), sent.stream().anyMatch(q -> q.contains("passwordless_devices")));
        assertNoIsolationLevelStatements(sent);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private static void assertNoIsolationLevelStatements(List<String> sent) {
        for (String q : sent) {
            String upper = q.toUpperCase();
            assertFalse("unexpected isolation-level statement: " + q, upper.contains("ISOLATION LEVEL"));
            assertFalse("unexpected session-characteristics statement: " + q,
                    upper.contains("SESSION CHARACTERISTICS"));
        }
    }

    private static String runQuery(Connection con, String sql) throws SQLException {
        try (Statement st = con.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    /** Captures the queries the PostgreSQL driver sends from the current thread, via its FINEST log. */
    private static final class DriverQueryLog extends Handler implements AutoCloseable {
        // held strongly: java.util.logging only keeps weak references to configured loggers
        private final Logger driverLogger = Logger.getLogger("org.postgresql");
        private final Level previousLevel = driverLogger.getLevel();
        private final long threadId = Thread.currentThread().getId();
        private final SimpleFormatter formatter = new SimpleFormatter();
        private final List<String> queries = Collections.synchronizedList(new ArrayList<>());

        static DriverQueryLog startForCurrentThread() {
            DriverQueryLog log = new DriverQueryLog();
            log.setLevel(Level.ALL);
            log.driverLogger.addHandler(log);
            log.driverLogger.setLevel(Level.FINEST);
            return log;
        }

        List<String> queries() {
            synchronized (queries) {
                return new ArrayList<>(queries);
            }
        }

        @Override
        public void publish(LogRecord record) {
            if (record.getLongThreadID() != threadId) {
                return;
            }
            String message = formatter.formatMessage(record);
            if (message.contains("FE=> Parse") || message.contains("FE=> SimpleQuery")) {
                queries.add(message);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            driverLogger.removeHandler(this);
            driverLogger.setLevel(previousLevel);
        }
    }
}
