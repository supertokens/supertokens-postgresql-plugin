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

import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.totp.TOTPDevice;
import io.supertokens.pluginInterface.totp.TOTPUsedCode;
import io.supertokens.pluginInterface.totp.sqlStorage.TOTPSQLStorage;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static org.junit.Assert.*;

/**
 * The TOTP transactions must be serialised per (app, user): the storage methods take no LockedUser and TOTP user
 * ids need not exist in app_id_to_user_id, so the plugin's own row locks are what serialises them.
 *
 * These tests do not rely on sleeps: the second transaction's backend is observed in pg_stat_activity waiting on a
 * lock while the first transaction is still open, and only then is the first transaction committed.
 */
public class TOTPLockingTest {
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

    private static final AppIdentifier APP = new AppIdentifier(null, null);
    private static final TenantIdentifier TENANT = new TenantIdentifier(null, null, null);
    private static final long WAIT_TIMEOUT_MS = 30_000;

    private interface LockingCall {
        Object run(TOTPSQLStorage storage, io.supertokens.pluginInterface.sqlStorage.TransactionConnection con)
                throws Exception;
    }

    private static int backendPid(Connection sqlCon) throws Exception {
        return execute(sqlCon, "SELECT pg_backend_pid()", null, rs -> {
            rs.next();
            return rs.getInt(1);
        });
    }

    /**
     * Returns true once the backend with the given pid waits on a lock, false if `done` completes first (the
     * statement did not block) or on timeout.
     */
    private static boolean waitUntilBlockedOnLock(TOTPSQLStorage storage, int pid, CompletableFuture<?> done)
            throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (done.isDone()) {
                return false;
            }
            String waitEventType = storage.startTransaction(con -> {
                try {
                    return execute((Connection) con.getConnection(),
                            "SELECT wait_event_type FROM pg_stat_activity WHERE pid = ?",
                            pst -> pst.setInt(1, pid), rs -> rs.next() ? rs.getString(1) : null);
                } catch (Exception e) {
                    throw new StorageTransactionLogicException(e);
                }
            });
            if ("Lock".equals(waitEventType)) {
                return !done.isDone();
            }
            Thread.yield();
        }
        return false;
    }

    /**
     * Runs `first` in a transaction and keeps that transaction open, then runs `second` in another transaction
     * and asserts that `second` blocks until the first transaction commits. Returns the result of `second`.
     */
    private static Object assertSecondWaitsForFirst(TOTPSQLStorage storage, LockingCall first, LockingCall second)
            throws Exception {
        CountDownLatch firstHoldsLock = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger secondPid = new AtomicInteger(-1);
        AtomicBoolean firstCommitting = new AtomicBoolean(false);
        CountDownLatch secondPidKnown = new CountDownLatch(1);

        CompletableFuture<Object> firstDone = CompletableFuture.supplyAsync(() -> {
            try {
                return storage.startTransaction(con -> {
                    try {
                        first.run(storage, con);
                        firstHoldsLock.countDown();
                        if (!releaseFirst.await(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                            throw new IllegalStateException("first transaction was never released");
                        }
                        firstCommitting.set(true);
                        storage.commitTransaction(con);
                        return null;
                    } catch (StorageTransactionLogicException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new StorageTransactionLogicException(e);
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(firstHoldsLock.await(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));

        AtomicReference<Boolean> firstCommittingWhenSecondReturned = new AtomicReference<>(null);
        CompletableFuture<Object> secondDone = CompletableFuture.supplyAsync(() -> {
            try {
                return storage.startTransaction(con -> {
                    try {
                        // only record the pid on the first try (a retry would get a new connection)
                        if (secondPidKnown.getCount() > 0) {
                            secondPid.set(backendPid((Connection) con.getConnection()));
                            secondPidKnown.countDown();
                        }
                        Object result = second.run(storage, con);
                        firstCommittingWhenSecondReturned.compareAndSet(null, firstCommitting.get());
                        storage.commitTransaction(con);
                        return result;
                    } catch (StorageTransactionLogicException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new StorageTransactionLogicException(e);
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            assertTrue(secondPidKnown.await(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
            boolean blocked = waitUntilBlockedOnLock(storage, secondPid.get(), secondDone);
            assertTrue("the second transaction did not wait for the first one's lock", blocked);
        } finally {
            releaseFirst.countDown();
        }

        firstDone.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Object result = secondDone.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertEquals(Boolean.TRUE, firstCommittingWhenSecondReturned.get());
        return result;
    }

    @Test
    public void testIssue26_getDevicesTransactionLocks() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        TOTPSQLStorage storage = (TOTPSQLStorage) StorageLayer.getStorage(process.getProcess());
        storage.createDevice(APP, new TOTPDevice("user", "d1", "secret", 30, 1, true, System.currentTimeMillis()));

        Object devices = assertSecondWaitsForFirst(storage,
                (s, con) -> s.getDevices_Transaction(con, APP, "user"),
                (s, con) -> s.getDevices_Transaction(con, APP, "user"));
        assertEquals(1, ((TOTPDevice[]) devices).length);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testAppScopedUsedCodeReadLocksUserAndSeesCodesCommittedMeanwhile() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        TOTPSQLStorage storage = (TOTPSQLStorage) StorageLayer.getStorage(process.getProcess());
        storage.createDevice(APP, new TOTPDevice("user", "d1", "secret", 30, 1, true, System.currentTimeMillis()));
        storage.createDevice(APP, new TOTPDevice("other-user", "d1", "secret", 30, 1, true,
                System.currentTimeMillis()));

        long now = System.currentTimeMillis();
        // The user has no used codes yet: the read must still lock (there is no used-code row to lock).
        Object codes = assertSecondWaitsForFirst(storage,
                (s, con) -> {
                    TOTPUsedCode[] seen = s.getAllUsedCodesDescOrder_Transaction(con, APP, "user");
                    assertEquals(0, seen.length);
                    s.insertUsedCode_Transaction(con, TENANT,
                            new TOTPUsedCode("user", "123456", true, now + 90_000, now));
                    return null;
                },
                (s, con) -> s.getAllUsedCodesDescOrder_Transaction(con, APP, "user"));

        // The second read ran after the first transaction committed, and sees its code:
        TOTPUsedCode[] seen = (TOTPUsedCode[]) codes;
        assertEquals(1, seen.length);
        assertEquals("123456", seen[0].code);
        assertTrue(seen[0].isValid);

        // Another user of the same app is not blocked by the lock:
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch locked = new CountDownLatch(1);
        CompletableFuture<Void> holder = CompletableFuture.runAsync(() -> {
            try {
                storage.startTransaction(con -> {
                    storage.getAllUsedCodesDescOrder_Transaction(con, APP, "user");
                    locked.countDown();
                    try {
                        release.await(2 * WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new StorageTransactionLogicException(e);
                    }
                    storage.commitTransaction(con);
                    return null;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        try {
            assertTrue(locked.await(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
            CompletableFuture<TOTPUsedCode[]> otherUserRead = CompletableFuture.supplyAsync(() -> {
                try {
                    return storage.startTransaction(con -> {
                        TOTPUsedCode[] result = storage.getAllUsedCodesDescOrder_Transaction(con, APP, "other-user");
                        storage.commitTransaction(con);
                        return result;
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            // completes while the first user's lock is still held
            assertEquals(0, otherUserRead.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS).length);
        } finally {
            release.countDown();
        }
        holder.get(2 * WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
