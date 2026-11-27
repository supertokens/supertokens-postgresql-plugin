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
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * {@code deleteSession_Transaction} and {@code getAllNonExpiredSessionHandlesForUser_Transaction} must behave like
 * their auto-commit forms, but on the caller's transaction connection: a delete is visible on that connection
 * before commit and is undone by a rollback, and the read sees the caller's uncommitted writes.
 */
public class SessionTransactionTwinsTest {

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

    private static final TenantIdentifier TENANT = new TenantIdentifier(null, null, null);

    private TestingProcessManager.TestingProcess startProcess() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        return process;
    }

    private static void createSession(Start start, String sessionHandle, String userId, long expiry)
            throws Exception {
        start.createNewSession(TENANT, sessionHandle, userId, "rt-" + sessionHandle, new JsonObject(), expiry,
                new JsonObject(), System.currentTimeMillis(), false);
    }

    private static String[] sorted(String[] handles) {
        String[] copy = handles.clone();
        Arrays.sort(copy);
        return copy;
    }

    @Test
    public void deleteSessionTransactionDeletesLikeAutoCommitOnCallersConnection() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
                return;
            }
            Start start = (Start) StorageLayer.getStorage(process.getProcess());
            long expiry = System.currentTimeMillis() + 3600_000L;
            for (String handle : new String[]{"sh-1", "sh-2", "sh-3", "sh-4"}) {
                createSession(start, handle, "user-1", expiry);
            }

            // Inside a transaction that rolls back: the delete is visible on the caller's connection, and the
            // sessions are back afterwards.
            int[] deletedInTx = {-1};
            String[][] remainingInTx = {null};
            try {
                start.startTransaction(con -> {
                    deletedInTx[0] = start.deleteSession_Transaction(con, TENANT,
                            new String[]{"sh-1", "sh-2", "no-such-handle"});
                    remainingInTx[0] = start.getAllNonExpiredSessionHandlesForUser_Transaction(con, TENANT,
                            "user-1");
                    throw new StorageTransactionLogicException(new Exception("rollback"));
                });
                fail("the transaction should have rolled back");
            } catch (StorageTransactionLogicException e) {
                assertEquals("rollback", e.actualException.getMessage());
            }
            assertEquals(2, deletedInTx[0]);
            assertArrayEquals("the uncommitted delete must be visible on the caller's connection",
                    new String[]{"sh-3", "sh-4"}, sorted(remainingInTx[0]));
            assertArrayEquals("the rolled-back delete must not remove any session",
                    new String[]{"sh-1", "sh-2", "sh-3", "sh-4"},
                    sorted(start.getAllNonExpiredSessionHandlesForUser(TENANT, "user-1")));

            // Parity with the auto-commit form: same deleted-row count, including unknown handles and an empty
            // array.
            int autoCommitDeleted = start.deleteSession(TENANT, new String[]{"sh-1", "no-such-handle"});
            int[] txDeleted = {-1, -1};
            start.startTransaction(con -> {
                txDeleted[0] = start.deleteSession_Transaction(con, TENANT, new String[]{"sh-2", "no-such-handle"});
                txDeleted[1] = start.deleteSession_Transaction(con, TENANT, new String[0]);
                start.commitTransaction(con);
                return null;
            });
            assertEquals(1, autoCommitDeleted);
            assertEquals(autoCommitDeleted, txDeleted[0]);
            assertEquals(start.deleteSession(TENANT, new String[0]), txDeleted[1]);
            assertNull(start.getSession(TENANT, "sh-1"));
            assertNull(start.getSession(TENANT, "sh-2"));
            assertArrayEquals(new String[]{"sh-3", "sh-4"},
                    sorted(start.getAllNonExpiredSessionHandlesForUser(TENANT, "user-1")));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void getAllNonExpiredSessionHandlesForUserTransactionMatchesAutoCommitOnCallersConnection()
            throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
                return;
            }
            Start start = (Start) StorageLayer.getStorage(process.getProcess());
            long now = System.currentTimeMillis();
            createSession(start, "live-1", "user-1", now + 3600_000L);
            createSession(start, "live-2", "user-1", now + 3600_000L);
            createSession(start, "expired", "user-1", now - 1000L);
            createSession(start, "other-user", "user-2", now + 3600_000L);

            // Parity: same handles as the auto-commit form (expired and other users' sessions excluded).
            String[] autoCommit = start.getAllNonExpiredSessionHandlesForUser(TENANT, "user-1");
            assertArrayEquals(new String[]{"live-1", "live-2"}, sorted(autoCommit));
            String[][] inTx = {null, null};
            try {
                start.startTransaction(con -> {
                    inTx[0] = start.getAllNonExpiredSessionHandlesForUser_Transaction(con, TENANT, "user-1");
                    // Sees a session created earlier in the same, still uncommitted, transaction.
                    try {
                        start.createNewSession_Transaction(con, TENANT, "live-in-tx", "user-1", "rt-live-in-tx",
                                new JsonObject(), now + 3600_000L, new JsonObject(), now, false);
                    } catch (TenantOrAppNotFoundException e) {
                        throw new StorageTransactionLogicException(e);
                    }
                    inTx[1] = start.getAllNonExpiredSessionHandlesForUser_Transaction(con, TENANT, "user-1");
                    throw new StorageTransactionLogicException(new Exception("rollback"));
                });
                fail("the transaction should have rolled back");
            } catch (StorageTransactionLogicException e) {
                assertEquals("rollback", e.actualException.getMessage());
            }
            assertArrayEquals(sorted(autoCommit), sorted(inTx[0]));
            assertArrayEquals("the uncommitted session must be visible on the caller's connection",
                    new String[]{"live-1", "live-2", "live-in-tx"}, sorted(inTx[1]));
            assertArrayEquals(new String[]{"live-1", "live-2"},
                    sorted(start.getAllNonExpiredSessionHandlesForUser(TENANT, "user-1")));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
