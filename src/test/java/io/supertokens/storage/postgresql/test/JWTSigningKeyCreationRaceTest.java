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
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.jwt.JWTAsymmetricSigningKeyInfo;
import io.supertokens.pluginInterface.jwt.JWTSigningKeyInfo;
import io.supertokens.pluginInterface.jwt.sqlstorage.JWTRecipeSQLStorage;
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
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Reproduces the duplicate JWT signing key race: the core's
 * {@code JWTSigningKey.getOrCreateAndGetKeyForAlgorithm} reads the app's keys and inserts a new one when none
 * exists for the algorithm, all under READ COMMITTED. The read used {@code SELECT ... FOR UPDATE}, which locks
 * nothing when the result set is empty, so two transactions (two cores, or two threads on one core - the core
 * does not synchronize this path) both see "no key", both generate one, and both inserts commit: the PK is
 * {@code (app_id, key_id)} and there is no unique constraint on {@code (app_id, algorithm)}. The result is
 * duplicate never-cleaned static keys and per-core cache divergence.
 *
 * <p>The two threads below drive the storage layer directly with the exact read-then-insert shape the core
 * uses, with a barrier between the read and the insert to force the racing interleaving deterministically.
 */
public class JWTSigningKeyCreationRaceTest {

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

    /** Raw SQL against the per-worker test database, independent of the core's storage layer. */
    private static void runSql(String sql) throws Exception {
        try (Connection con = DriverManager.getConnection(DatabaseTestHelper.getTestDatabaseUrl(),
                DatabaseTestHelper.getUser(), DatabaseTestHelper.getPassword());
             Statement st = con.createStatement()) {
            st.execute(sql);
        }
    }

    @Test
    public void concurrentFirstKeyCreationMustNotInsertDuplicateKeys() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        JWTRecipeSQLStorage sqlStorage = (JWTRecipeSQLStorage) storage;
        AppIdentifier app = new AppIdentifier(null, null);

        // startup already created the app's RS256 key (generateKeysForSupportedAlgos); remove it so both
        // threads race on the empty table, the way two fresh cores would
        runSql("DELETE FROM " + Config.getConfig(storage).getJWTSigningKeysTable());

        // both threads must have READ the (empty) key list before either INSERTs; the timeout lets the winner
        // proceed alone once the fix makes the loser fail the advisory lock instead of reaching the barrier
        CyclicBarrier bothHaveRead = new CyclicBarrier(2);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Runnable createKeyIfMissing = () -> {
                try {
                    sqlStorage.startTransaction(con -> {
                        // a LockFailure thrown in here is retried with backoff by startTransaction
                        List<JWTSigningKeyInfo> keys = sqlStorage.getJWTSigningKeys_Transaction(app, con);
                        boolean hasKeyForAlgorithm = keys.stream()
                                .anyMatch(key -> "RS256".equalsIgnoreCase(key.algorithm));
                        if (!hasKeyForAlgorithm) {
                            try {
                                bothHaveRead.await(3, TimeUnit.SECONDS);
                            } catch (TimeoutException e) {
                                // the other thread never read an empty list; we are creating alone
                            } catch (Exception e) {
                                throw new StorageQueryException(e);
                            }
                            try {
                                sqlStorage.setJWTSigningKey_Transaction(app, con, new JWTAsymmetricSigningKeyInfo(
                                        UUID.randomUUID().toString(), System.currentTimeMillis(), "RS256",
                                        "publicKey|privateKey"));
                            } catch (Exception e) {
                                throw new StorageQueryException(e);
                            }
                        }
                        sqlStorage.commitTransaction(con);
                        return null;
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };

            Future<?> first = executor.submit(createKeyIfMissing);
            Future<?> second = executor.submit(createKeyIfMissing);
            first.get(30, TimeUnit.SECONDS);
            second.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        List<JWTSigningKeyInfo> keysAfter = sqlStorage.startTransaction(con -> {
            List<JWTSigningKeyInfo> keys = sqlStorage.getJWTSigningKeys_Transaction(app, con);
            sqlStorage.commitTransaction(con);
            return keys;
        });
        long rs256Keys = keysAfter.stream().filter(key -> "RS256".equalsIgnoreCase(key.algorithm)).count();
        assertEquals("exactly one RS256 signing key must exist after concurrent first-key creation",
                1, rs256Keys);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
