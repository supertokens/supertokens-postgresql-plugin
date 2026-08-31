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

import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.KeyValueInfo;
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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Pins the semantics of the blocking advisory lock in
 * {@code SessionQueries.getAccessTokenSigningKeys_Transaction}: a reader contending with a rotation in
 * progress must QUEUE for the holder's actual hold time - not fail the lock, roll the transaction back and
 * sleep in {@code startTransaction}'s deadlock-retry loop - and its read after the wait must see the key the
 * winner committed (which is what makes the loser skip creating a duplicate).
 *
 * <p>The "winner" is a raw JDBC transaction playing the role of another core instance holding the advisory
 * lock across key generation. With the try-variant of the lock this exact scenario fails the lock repeatedly
 * and records DEADLOCK_FOUND process states; with the blocking variant it records none.
 */
public class AccessTokenSigningKeyBlockingLockTest {

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
    public void contendedKeyReadQueuesSeesWinnersKeyAndDoesNotRetry() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Main main = process.getProcess();
        Start storage = (Start) StorageLayer.getStorage(main);
        AppIdentifier app = new AppIdentifier(null, null);
        String table = Config.getConfig(storage).getAccessTokenSigningKeysTable();
        String lockKey = app.getAppId() + "~" + table;
        String winnersKeyValue = "winners-key-" + System.currentTimeMillis();

        // "another core instance": holds the advisory lock across its key insert, the way the rotation
        // transaction holds it across RSA key generation
        try (Connection winner = DriverManager.getConnection(DatabaseTestHelper.getTestDatabaseUrl(),
                DatabaseTestHelper.getUser(), DatabaseTestHelper.getPassword())) {
            winner.setAutoCommit(false);
            try (PreparedStatement lock = winner.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?))")) {
                lock.setString(1, lockKey);
                lock.execute();
            }
            try (PreparedStatement insert = winner.prepareStatement(
                    "INSERT INTO " + table + "(app_id, created_at_time, value) VALUES(?, ?, ?)")) {
                insert.setString(1, app.getAppId());
                insert.setLong(2, System.currentTimeMillis());
                insert.setString(3, winnersKeyValue);
                insert.execute();
            }

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<KeyValueInfo[]> readerResult = executor.submit(() ->
                        storage.startTransaction(con -> {
                            KeyValueInfo[] keys = storage.getAccessTokenSigningKeys_Transaction(app, con);
                            storage.commitTransaction(con);
                            return keys;
                        }));

                // the reader must be waiting on the advisory lock, not failing it: still running while the
                // winner holds the lock, and no DEADLOCK_FOUND (= no rollback/backoff retry) recorded
                Thread.sleep(1500);
                assertFalse("reader must queue on the advisory lock while the rotation holds it",
                        readerResult.isDone());
                assertNull("waiting must not go through the deadlock-retry loop",
                        ProcessState.getInstance(main)
                                .getLastEventByName(ProcessState.PROCESS_STATE.DEADLOCK_FOUND));

                winner.commit();

                // released within the holder's actual hold time: the read completes promptly and sees the
                // winner's committed key - the property that lets a losing core skip creating a duplicate
                KeyValueInfo[] keys = readerResult.get(10, TimeUnit.SECONDS);
                assertTrue("the read after the wait must see the winner's committed key",
                        Arrays.stream(keys).anyMatch(key -> winnersKeyValue.equals(key.value)));
                assertNull("no retry must have happened at any point",
                        ProcessState.getInstance(main)
                                .getLastEventByName(ProcessState.PROCESS_STATE.DEADLOCK_FOUND));
            } finally {
                executor.shutdownNow();
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
