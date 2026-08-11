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
import io.supertokens.storage.postgresql.queries.ActiveUsersQueries;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Regression tests for the correctness batch in issue #363:
 *   1. the pooled-connection leak in {@code createBulkUserIdMapping},
 *   2. the truncated batch read in {@code getLastActiveByMultipleUserIds},
 *   3. the unlocked {@code deleteRole}.
 */
public class QueryCorrectnessBatchTest {

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

    private Start startStorageOrNull(TestingProcessManager.TestingProcess process) throws Exception {
        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return null;
        }
        return (Start) StorageLayer.getStorage(process.getProcess());
    }

    /**
     * createBulkUserIdMapping obtained a pooled connection without try-with-resources and never
     * returned it, so every call permanently consumed one connection. Calling it many more times
     * than the pool size (default 10, connection timeout 5s) exhausts the pool and the call starts
     * throwing a pool-timeout exception. An empty map is used so no user rows are needed: the leak
     * happens on the getConnection() before any insert, and executeBatch is a no-op for no setters.
     */
    @Test
    public void bulkUserIdMappingDoesNotLeakConnections() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Start storage = startStorageOrNull(process);
        if (storage == null) {
            return;
        }

        AppIdentifier appIdentifier = new AppIdentifier(null, null);

        // Far more iterations than the default pool size of 10; on the leak this exhausts the pool
        // and throws once more than pool-size connections are held open.
        for (int i = 0; i < 30; i++) {
            storage.createBulkUserIdMapping(appIdentifier, new HashMap<>());
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * getLastActiveByMultipleUserIds used {@code if (res.next())} instead of {@code while}, so it
     * returned at most one of the requested users' last-active times; the rest read as never-active.
     */
    @Test
    public void lastActiveByMultipleUserIdsReturnsEveryUser() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Start storage = startStorageOrNull(process);
        if (storage == null) {
            return;
        }

        AppIdentifier appIdentifier = new AppIdentifier(null, null);
        long now = System.currentTimeMillis();

        Map<String, Long> expected = new HashMap<>();
        List<String> userIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String userId = "last-active-user-" + i;
            long lastActive = now - i * 1000L;
            storage.updateLastActive(appIdentifier, userId, lastActive);
            expected.put(userId, lastActive);
            userIds.add(userId);
        }

        // include an id with no last-active row: it must simply be absent, not throw
        userIds.add("never-active-user");

        Map<String, Long> actual = ActiveUsersQueries.getLastActiveByMultipleUserIds(storage, appIdentifier, userIds);

        assertEquals("every seeded user must be returned", expected.size(), actual.size());
        for (Map.Entry<String, Long> entry : expected.entrySet()) {
            assertEquals("wrong last-active time for " + entry.getKey(),
                    entry.getValue(), actual.get(entry.getKey()));
        }
        assertFalse(actual.containsKey("never-active-user"));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * deleteRole now runs inside a transaction that takes the role row lock (FOR UPDATE) before
     * deleting. This pins that the transactional path still behaves correctly: deleting an existing
     * role removes it and reports true, and deleting a missing role reports false.
     */
    @Test
    public void deleteRoleRemovesRoleThroughTransaction() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Start storage = startStorageOrNull(process);
        if (storage == null) {
            return;
        }

        AppIdentifier appIdentifier = new AppIdentifier(null, null);
        String role = "batch-test-role";

        storage.startTransaction(con -> {
            try {
                storage.createNewRoleOrDoNothingIfExists_Transaction(appIdentifier, con, role);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        assertTrue("role should exist after creation", storage.doesRoleExist(appIdentifier, role));

        assertTrue("deleting an existing role returns true", storage.deleteRole(appIdentifier, role));
        assertFalse("role should be gone after deletion", storage.doesRoleExist(appIdentifier, role));

        assertFalse("deleting a missing role returns false", storage.deleteRole(appIdentifier, role));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
