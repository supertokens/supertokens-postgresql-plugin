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

package io.supertokens.storage.postgresql.test;

import com.google.gson.JsonObject;
import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.bulkimport.BulkImportStorage.BULK_IMPORT_USER_STATUS;
import io.supertokens.pluginInterface.bulkimport.BulkImportUser;
import io.supertokens.pluginInterface.bulkimport.sqlStorage.BulkImportProxySQLStorage;
import io.supertokens.pluginInterface.bulkimport.sqlStorage.BulkImportProxyStoragePool;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.BulkImportConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * The bulk import connection pool: bounded, separate from the live pool, one connection per proxy storage,
 * and savepoints that undo an import without releasing the claim on the queue rows.
 */
public class BulkImportConnectionPoolTest {
    @Rule
    public TestRule watchman = Utils.getOnFailure();

    @Rule
    public TestRule retryFlaky = Utils.retryFlakyTest();

    @AfterClass
    public static void afterTesting() {
        Utils.afterTesting();
    }

    @Before
    public void beforeEach() {
        Utils.reset();
    }

    private static final AppIdentifier APP = new AppIdentifier(null, null);

    @Test
    public void poolIsBoundedAndSharedByItsProxyStorages() throws Exception {
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(new String[]{"../"}, false);
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        Main main = process.getProcess();
        if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
            return;
        }
        Start live = (Start) StorageLayer.getStorage(main);
        int liveBackendsBefore = countBulkImportBackends(live);
        assertEquals(0, liveBackendsBefore);

        try (BulkImportProxyStoragePool pool = live.openBulkImportProxyStoragePool(2)) {
            BulkImportProxySQLStorage first = pool.createProxyStorage();
            BulkImportProxySQLStorage second = pool.createProxyStorage();
            BulkImportProxySQLStorage third = pool.createProxyStorage();

            // the proxies run against the owner's database and config ...
            assertEquals(live.getUserPoolId(), first.getUserPoolId());
            // ... but are initialised by the pool, never on their own
            assertThrows(UnsupportedOperationException.class, () -> first.initStorage(false, new ArrayList<>()));

            // two proxies, two connections: each holds its connection from first use until it is closed
            assertEquals("read committed", transactionIsolationOf(first));
            assertEquals("read committed", transactionIsolationOf(second));
            assertEquals(2, countBulkImportBackends(live));

            // a third proxy cannot get a connection while both are in use: the pool is the hard cap
            long start = System.currentTimeMillis();
            StorageQueryException e = assertThrows(StorageQueryException.class, () -> transactionIsolationOf(third));
            assertTrue(e.getCause() instanceof SQLException);
            assertTrue("should have waited for the connection timeout", System.currentTimeMillis() - start >= 4000);

            // returning one connection frees a slot for the third
            first.closeConnectionForBulkImportProxyStorage();
            assertEquals("read committed", transactionIsolationOf(third));
            assertEquals(2, countBulkImportBackends(live));
        }

        // closing the pool closes every proxy and releases every server connection
        waitForBulkImportBackends(live, 0);
        // and the live pool was never touched
        assertEquals(10, live.getDbActivityCount(DatabaseTestHelper.getCurrentTestDatabase()));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void savepointRollbackKeepsTheClaimLockedAndTheConnectionUsable() throws Exception {
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(new String[]{"../"}, false);
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        Main main = process.getProcess();
        if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
            return;
        }
        Start live = (Start) StorageLayer.getStorage(main);

        List<BulkImportUser> queued = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            queued.add(newBulkImportUser("savepoint-" + i + "@example.com"));
        }
        live.addBulkImportUsers(APP, queued);

        try (BulkImportProxyStoragePool pool = live.openBulkImportProxyStoragePool(2)) {
            BulkImportProxySQLStorage workerA = pool.createProxyStorage();
            BulkImportProxySQLStorage workerB = pool.createProxyStorage();

            // A claims one row (FOR UPDATE SKIP LOCKED) and keeps the transaction open
            AtomicReference<String> claimedByA = new AtomicReference<>();
            workerA.startTransaction(con -> {
                List<BulkImportUser> claimed = workerA.getBulkImportUsersAndChangeStatusToProcessing_Transaction(
                        APP, 1, con);
                assertEquals(1, claimed.size());
                claimedByA.set(claimed.get(0).id);
                return null;
            });

            // A's "import" fails with a real database error after a savepoint ...
            Savepoint beforeImport = workerA.createSavepointForBulkImportProxyStorage();
            assertThrows(StorageQueryException.class, () -> workerA.startTransaction(con -> {
                Connection sqlCon = (Connection) con.getConnection();
                try (PreparedStatement pst = sqlCon.prepareStatement("SELECT 1/0")) {
                    pst.executeQuery();
                }
                return null;
            }));
            // ... and is rolled back to that savepoint: the transaction is live again, the claim still held
            workerA.rollbackToSavepointForBulkImportProxyStorage(beforeImport);

            // B, concurrently, can only claim the rows A does not hold
            List<String> claimedByB = new ArrayList<>();
            workerB.startTransaction(con -> {
                for (BulkImportUser u : workerB.getBulkImportUsersAndChangeStatusToProcessing_Transaction(APP, 10,
                        con)) {
                    claimedByB.add(u.id);
                }
                return null;
            });
            assertEquals(2, claimedByB.size());
            assertFalse("row claimed by A must stay invisible to B after A's savepoint rollback",
                    claimedByB.contains(claimedByA.get()));

            // A marks its row as failed on the very same connection and commits; only then is the lock gone
            workerA.startTransaction(con -> {
                Map<String, String> errors = new HashMap<>();
                errors.put(claimedByA.get(), "import failed on purpose");
                workerA.updateMultipleBulkImportUsersStatusToError_Transaction(APP, con, errors);
                return null;
            });
            workerA.commitTransactionForBulkImportProxyStorage();
            workerB.commitTransactionForBulkImportProxyStorage();
        }

        List<BulkImportUser> failed = live.getBulkImportUsers(APP, 10, BULK_IMPORT_USER_STATUS.FAILED, null, null);
        assertEquals(1, failed.size());
        assertEquals("import failed on purpose", failed.get(0).errorMessage);
        assertEquals(2, live.getBulkImportUsers(APP, 10, BULK_IMPORT_USER_STATUS.PROCESSING, null, null).size());
        waitForBulkImportBackends(live, 0);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void closingTheProxyRollsBackAndReleasesTheClaim() throws Exception {
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(new String[]{"../"}, false);
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        Main main = process.getProcess();
        if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
            return;
        }
        Start live = (Start) StorageLayer.getStorage(main);
        live.addBulkImportUsers(APP, List.of(newBulkImportUser("abandoned@example.com")));

        try (BulkImportProxyStoragePool pool = live.openBulkImportProxyStoragePool(1)) {
            BulkImportProxySQLStorage worker = pool.createProxyStorage();
            worker.startTransaction(con -> {
                assertEquals(1, worker.getBulkImportUsersAndChangeStatusToProcessing_Transaction(APP, 1, con).size());
                return null;
            });
            // the worker dies without committing (crash, exception, ...)
            worker.closeConnectionForBulkImportProxyStorage();
        }

        // the uncommitted claim is gone: the row is NEW and unlocked again, i.e. re-claimable by anyone
        assertEquals(1, live.getBulkImportUsers(APP, 10, BULK_IMPORT_USER_STATUS.NEW, null, null).size());
        assertEquals(0, live.getBulkImportUsers(APP, 10, BULK_IMPORT_USER_STATUS.PROCESSING, null, null).size());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private static BulkImportUser newBulkImportUser(String email) {
        List<BulkImportUser.LoginMethod> loginMethods = new ArrayList<>();
        loginMethods.add(new BulkImportUser.LoginMethod(List.of(TenantIdentifier.DEFAULT_TENANT_ID), "emailpassword",
                true, true, System.currentTimeMillis(), email, "$2a", "BCRYPT", null, null, null, null,
                UUID.randomUUID().toString()));
        return new BulkImportUser(UUID.randomUUID().toString(), null, new JsonObject(), new ArrayList<>(),
                new ArrayList<>(), loginMethods);
    }

    private static String transactionIsolationOf(BulkImportProxySQLStorage proxy)
            throws StorageQueryException, StorageTransactionLogicException {
        return proxy.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement("SHOW transaction_isolation");
                 ResultSet rs = pst.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        });
    }

    private static int countBulkImportBackends(Start live) throws Exception {
        return live.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(
                    "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = ? AND datname = ?")) {
                pst.setString(1, BulkImportConnectionPool.APPLICATION_NAME);
                pst.setString(2, DatabaseTestHelper.getCurrentTestDatabase());
                try (ResultSet rs = pst.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        });
    }

    private static void waitForBulkImportBackends(Start live, int expected) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        int seen = -1;
        while (System.currentTimeMillis() < deadline) {
            seen = countBulkImportBackends(live);
            if (seen == expected) {
                return;
            }
            Thread.sleep(200);
        }
        assertEquals("bulk import backends in pg_stat_activity", expected, seen);
    }
}
