/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
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
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.pluginInterface.sqlStorage.SQLStorage;
import io.supertokens.pluginInterface.sqlStorage.SQLStorage.TransactionIsolationLevel;
import io.supertokens.pluginInterface.totp.TOTPDevice;
import io.supertokens.pluginInterface.totp.TOTPUsedCode;
import io.supertokens.pluginInterface.totp.exception.TotpNotEnabledException;
import io.supertokens.pluginInterface.totp.exception.UsedCodeAlreadyExistsException;
import io.supertokens.pluginInterface.totp.sqlStorage.TOTPSQLStorage;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static org.junit.Assert.*;

public class DeadlockTest {
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

    @Rule
    public Retry retry = new Retry(3);

    @Test
    public void transactionDeadlockTesting()
            throws InterruptedException, StorageQueryException, StorageTransactionLogicException {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Storage storage = StorageLayer.getStorage(process.getProcess());
        SQLStorage sqlStorage = (SQLStorage) storage;
        sqlStorage.startTransaction(con -> {
            try {
                sqlStorage.setKeyValue_Transaction(new TenantIdentifier(null, null, null), con, "Key",
                        new KeyValueInfo("Value"));
                sqlStorage.setKeyValue_Transaction(new TenantIdentifier(null, null, null), con, "Key1",
                        new KeyValueInfo("Value1"));
            } catch (TenantOrAppNotFoundException e) {
                throw new IllegalStateException(e);
            }
            sqlStorage.commitTransaction(con);
            return null;
        });

        AtomicReference<String> t1State = new AtomicReference<>("init");
        AtomicReference<String> t2State = new AtomicReference<>("init");
        final Object syncObject = new Object();

        AtomicBoolean t1Failed = new AtomicBoolean(true);
        AtomicBoolean t2Failed = new AtomicBoolean(true);

        Runnable r1 = () -> {
            try {
                sqlStorage.startTransaction(con -> {

                    sqlStorage.getKeyValue_Transaction(new TenantIdentifier(null, null, null), con, "Key");

                    synchronized (syncObject) {
                        t1State.set("read");
                        syncObject.notifyAll();
                    }

                    synchronized (syncObject) {
                        while (!t2State.get().equals("read")) {
                            try {
                                syncObject.wait();
                            } catch (InterruptedException e) {
                            }
                        }
                    }

                    sqlStorage.getKeyValue_Transaction(new TenantIdentifier(null, null, null), con, "Key1");
                    t1Failed.set(false); // it should come here because we will try three times.
                    return null;
                });
            } catch (Exception ignored) {
            }
        };

        Runnable r2 = () -> {
            try {
                sqlStorage.startTransaction(con -> {

                    sqlStorage.getKeyValue_Transaction(new TenantIdentifier(null, null, null), con, "Key1");

                    synchronized (syncObject) {
                        t2State.set("read");
                        syncObject.notifyAll();
                    }

                    synchronized (syncObject) {
                        while (!t1State.get().equals("read")) {
                            try {
                                syncObject.wait();
                            } catch (InterruptedException e) {
                            }
                        }
                    }

                    sqlStorage.getKeyValue_Transaction(new TenantIdentifier(null, null, null), con, "Key");

                    t2Failed.set(false); // it should come here because we will try three times.
                    return null;
                });
            } catch (Exception ignored) {
            }
        };

        Thread t1 = new Thread(r1);
        Thread t2 = new Thread(r2);

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertTrue(!t1Failed.get() && !t2Failed.get());
        assertNotNull(process.checkOrWaitForEventInPlugin(
                io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_FOUND));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testCodeCreationRapidly() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        ExecutorService es = Executors.newFixedThreadPool(1000);

        AtomicBoolean pass = new AtomicBoolean(true);

        for (int i = 0; i < 3000; i++) {
            es.execute(() -> {
                try {
                    Passwordless.CreateCodeResponse resp = Passwordless.createCode(process.getProcess(),
                            "test@example.com", null, null, null);
                    Passwordless.ConsumeCodeResponse resp2 = Passwordless.consumeCode(process.getProcess(),
                            resp.deviceId, resp.deviceIdHash, resp.userInputCode, resp.linkCode);

                } catch (Exception e) {
                    if (e.getMessage().toLowerCase().contains("the transaction might succeed if retried")) {
                        pass.set(false);
                    }
                }
            });
        }

        es.shutdown();
        es.awaitTermination(2, TimeUnit.MINUTES);

        assert (pass.get());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testCodeCreationRapidlyWithDifferentEmails() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        ExecutorService es = Executors.newFixedThreadPool(1000);

        AtomicBoolean pass = new AtomicBoolean(true);
        AtomicLong max_duration = new AtomicLong(0);
        AtomicLong total_duration = new AtomicLong(0);
        AtomicLongArray durations = new AtomicLongArray(3000);

        for (int i = 0; i < 3000; i++) {
            final int ind = i;
            es.execute(() -> {
                try {
                    long startTime = System.currentTimeMillis();
                    Passwordless.CreateCodeResponse resp = Passwordless.createCode(process.getProcess(),
                            "test" + ind + "@example.com", null, null, null);
                    Passwordless.ConsumeCodeResponse resp2 = Passwordless.consumeCode(process.getProcess(),
                            resp.deviceId, resp.deviceIdHash, resp.userInputCode, resp.linkCode);

                    long timeElapsed = System.currentTimeMillis() - startTime;
                    total_duration.addAndGet(timeElapsed);

                    if (timeElapsed > max_duration.get()) {
                        max_duration.set(timeElapsed);
                    }
                    durations.set(ind, timeElapsed);
                } catch (Exception e) {
                    if (e.getMessage() != null
                            && e.getMessage().toLowerCase().contains("the transaction might succeed if retried")) {
                        pass.set(false);
                    }
                }
            });
        }

        es.shutdown();
        es.awaitTermination(5, TimeUnit.MINUTES);

        System.out.println("Max execution time: " + max_duration.get() + "ms");
        System.out.println("Total execution time: " + total_duration.get() + "ms");
        System.out.println("Avg execution time: " + (total_duration.get() / 3000) + "ms");
        System.out.println("Durations: " + durations.toString());

        assertNull(process
                .checkOrWaitForEventInPlugin(io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_NOT_RESOLVED));
        assertNotNull(process
                .checkOrWaitForEventInPlugin(io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_FOUND));

        assert (pass.get());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testConcurrentDeleteAndUpdate() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Storage storage = StorageLayer.getStorage(process.getProcess());
        SQLStorage sqlStorage = (SQLStorage) storage;

        // Create a device as well as a user:
        TOTPSQLStorage totpStorage = (TOTPSQLStorage) StorageLayer.getStorage(process.getProcess());
        TOTPDevice device = new TOTPDevice("user", "d1", "secret", 30, 1, false);
        totpStorage.createDevice(new AppIdentifier(null, null), device);

        long now = System.currentTimeMillis();
        long nextDay = now + 1000 * 60 * 60 * 24; // 1 day from now
        TOTPUsedCode code = new TOTPUsedCode("user", "1234", true, nextDay, now);
        totpStorage.startTransaction(con -> {
            try {
                totpStorage.insertUsedCode_Transaction(con, new TenantIdentifier(null, null, null), code);
                totpStorage.commitTransaction(con);
            } catch (TotpNotEnabledException | UsedCodeAlreadyExistsException e) {
                // This should not happen
                throw new StorageTransactionLogicException(e);
            } catch (TenantOrAppNotFoundException e) {
                throw new IllegalStateException(e);
            }
            return null;
        });

        final Object syncObject = new Object();

        AtomicReference<String> t1State = new AtomicReference<>("init");
        AtomicReference<String> t2State = new AtomicReference<>("init");

        AtomicBoolean t1Failed = new AtomicBoolean(true);
        AtomicBoolean t2Failed = new AtomicBoolean(true);

        Runnable r1 = () -> {
            try {
                sqlStorage.startTransaction(con -> {
                    // Isolation level is SERIALIZABLE
                    Connection sqlCon = (Connection) con.getConnection();

                    String QUERY = "DELETE FROM totp_users where user_id = ?";
                    try {
                        update(sqlCon, QUERY, pst -> {
                            pst.setString(1, "user");
                        });
                    } catch (SQLException e) {
                        // Something is wrong with the test
                        // This should not happen
                        throw new StorageTransactionLogicException(e);
                    }
                    // Removal of user also triggers removal of the devices because
                    // of FOREIGN KEY constraint.

                    synchronized (syncObject) {
                        // Notify t2 that that device has been deleted by t1
                        t1State.set("query");
                        syncObject.notifyAll();
                    }

                    // Wait for t2 to run the update the device query before committing
                    synchronized (syncObject) {

                        while (t2State.get().equals("init")) {
                            try {
                                syncObject.wait();
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }

                    sqlStorage.commitTransaction(con);
                    t1State.set("commit");
                    t1Failed.set(false);

                    return null;
                });
            } catch (StorageQueryException | StorageTransactionLogicException e) {
                // This is expected because of "could not serialize access"
                t1Failed.set(true);
            }
        };

        Runnable r2 = () -> {
            try {
                sqlStorage.startTransaction(con -> {
                    // Isolation level is SERIALIZABLE
                    Connection sqlCon = (Connection) con.getConnection();

                    synchronized (syncObject) {
                        // Wait for t1 to run delete device query first
                        while (t1State.get().equals("init")) {
                            try {
                                syncObject.wait();
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }

                    Runnable r2Inner = () -> {
                        // Wait for t2Inner to start running
                        try {
                            Thread.sleep(1500);
                        } catch (InterruptedException ignored) {
                        }

                        // The psql txn will wait block t2Inner thread
                        // but the t2 txn is still free and can commit.

                        synchronized (syncObject) {
                            // Notify t1 that that device has been updated by t2
                            t2State.set("query");
                            syncObject.notifyAll();
                        }
                    };

                    Thread t2Inner = new Thread(r2Inner);
                    t2Inner.start();
                    // We will not wait for t2Inner to finish

                    String QUERY = "UPDATE totp_used_codes SET is_valid=false WHERE user_id = ?";
                    try {
                        update(sqlCon, QUERY, pst -> {
                            pst.setString(1, "user");
                        });
                    } catch (SQLException e) {
                        throw new StorageTransactionLogicException(e);
                    }
                    sqlStorage.commitTransaction(con);
                    t2State.set("commit");
                    t2Failed.set(false);

                    return null;
                });
            } catch (StorageQueryException | StorageTransactionLogicException e) {
                t2Failed.set(true);
            }
        };

        Thread t1 = new Thread(r2);
        Thread t2 = new Thread(r1);

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        // t1 (delete) should succeed
        // but t2 (update) should fail because of "could not serialize access"
        assertTrue(!t1Failed.get() && !t2Failed.get());
        assert (t1State.get().equals("commit") && t2State.get().equals("commit"));
        assertNotNull(process.checkOrWaitForEventInPlugin(
                io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_FOUND));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testConcurrentDeleteAndInsert() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Storage storage = StorageLayer.getStorage(process.getProcess());
        SQLStorage sqlStorage = (SQLStorage) storage;

        // Create a device as well as a user:
        TOTPSQLStorage totpStorage = (TOTPSQLStorage) StorageLayer.getStorage(process.getProcess());
        TOTPDevice device = new TOTPDevice("user", "d1", "secret", 30, 1, false);
        totpStorage.createDevice(new AppIdentifier(null, null), device);

        long now = System.currentTimeMillis();
        long nextDay = now + 1000 * 60 * 60 * 24; // 1 day from now
        TOTPUsedCode code = new TOTPUsedCode("user", "1234", true, nextDay, now);
        totpStorage.startTransaction(con -> {
            try {
                totpStorage.insertUsedCode_Transaction(con, new TenantIdentifier(null, null, null), code);
                totpStorage.commitTransaction(con);
            } catch (TotpNotEnabledException | UsedCodeAlreadyExistsException e) {
                // This should not happen
                throw new StorageTransactionLogicException(e);
            } catch (TenantOrAppNotFoundException e) {
                throw new IllegalStateException(e);
            }
            return null;
        });

        final Object syncObject = new Object();

        AtomicReference<String> t1State = new AtomicReference<>("init");
        AtomicReference<String> t2State = new AtomicReference<>("init");

        AtomicBoolean t1Failed = new AtomicBoolean(true);
        AtomicBoolean t2Failed = new AtomicBoolean(false);

        Runnable r1 = () -> {
            try {
                sqlStorage.startTransaction(con -> {
                    // Isolation level is SERIALIZABLE
                    Connection sqlCon = (Connection) con.getConnection();

                    String QUERY = "DELETE FROM totp_users where user_id = ?";
                    try {
                        update(sqlCon, QUERY, pst -> {
                            pst.setString(1, "user");
                        });
                    } catch (SQLException e) {
                        // Something is wrong with the test
                        // This should not happen
                        throw new StorageTransactionLogicException(e);
                    }
                    // Removal of user also triggers removal of the devices because
                    // of FOREIGN KEY constraint.

                    synchronized (syncObject) {
                        // Notify t2 that that device has been deleted by t1
                        t1State.set("query");
                        syncObject.notifyAll();
                    }

                    // Wait for t2 to run the update the device query before committing
                    synchronized (syncObject) {

                        while (t2State.get().equals("init")) {
                            try {
                                syncObject.wait();
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }

                    sqlStorage.commitTransaction(con);
                    t1State.set("commit");
                    t1Failed.set(false);

                    return null;
                }, TransactionIsolationLevel.SERIALIZABLE);
            } catch (StorageQueryException | StorageTransactionLogicException e) {
                // This is expected because of "could not serialize access"
                t1Failed.set(true);
            }
        };

        Runnable r2 = () -> {
            try {
                sqlStorage.startTransaction(con -> {
                    // Isolation level is SERIALIZABLE
                    synchronized (syncObject) {
                        // Wait for t1 to run delete device query first
                        while (t1State.get().equals("init")) {
                            try {
                                syncObject.wait();
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }

                    Runnable r2Inner = () -> {
                        // Wait for t2Inner to start running
                        try {
                            Thread.sleep(1500);
                        } catch (InterruptedException ignored) {
                        }

                        // The psql txn will wait block t2Inner thread
                        // but the t2 txn is still free and can commit.

                        synchronized (syncObject) {
                            // Notify t1 that that device has been updated by t2
                            t2State.set("query");
                            syncObject.notifyAll();
                        }
                    };

                    Thread t2Inner = new Thread(r2Inner);
                    t2Inner.start();
                    // We will not wait for t2Inner to finish

                    TOTPUsedCode code2 = new TOTPUsedCode("user", "1234", false, nextDay, now + 1);
                    try {
                        totpStorage.insertUsedCode_Transaction(con, new TenantIdentifier(null, null, null), code2);
                    } catch (TotpNotEnabledException | UsedCodeAlreadyExistsException e) {
                        // This should not happen
                        throw new StorageTransactionLogicException(e);
                    } catch (TenantOrAppNotFoundException e) {
                        throw new IllegalStateException(e);
                    }
                    sqlStorage.commitTransaction(con);
                    t2State.set("commit");
                    t2Failed.set(false);

                    return null;
                });
            } catch (StorageTransactionLogicException e) {
                Exception e2 = e.actualException;

                if (e2 instanceof TotpNotEnabledException) {
                    t2Failed.set(true);
                }
            } catch (StorageQueryException e) {
                t2Failed.set(false);
            }
        };

        Thread t1 = new Thread(r2);
        Thread t2 = new Thread(r1);

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        // t1 (delete) should succeed
        // but t2 (insert) should fail because of "could not serialize access"
        assertTrue(!t1Failed.get() && t2Failed.get());
        assert (t1State.get().equals("commit") && t2State.get().equals("query"));
        assertNotNull(process
                .checkOrWaitForEventInPlugin(
                        io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_FOUND,
                        1000));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testLinkAccountsInParallel() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        ExecutorService es = Executors.newFixedThreadPool(1000);

        AtomicBoolean pass = new AtomicBoolean(true);

        AuthRecipeUserInfo user1 = EmailPassword.signUp(process.getProcess(), "test1@example.com", "password");
        AuthRecipeUserInfo user2 = EmailPassword.signUp(process.getProcess(), "test2@example.com", "password");

        AuthRecipe.createPrimaryUser(process.getProcess(), user1.getSupertokensUserId());

        for (int i = 0; i < 3000; i++) {
            es.execute(() -> {
                try {
                    AuthRecipe.linkAccounts(process.getProcess(), user2.getSupertokensUserId(), user1.getSupertokensUserId());
                    AuthRecipe.unlinkAccounts(process.getProcess(), user2.getSupertokensUserId());
                } catch (Exception e) {
                    if (e.getMessage().toLowerCase().contains("the transaction might succeed if retried")) {
                        pass.set(false);
                    }
                }
            });
        }

        es.shutdown();
        es.awaitTermination(2, TimeUnit.MINUTES);

        assert (pass.get());
        assertNull(process
                .checkOrWaitForEventInPlugin(io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_NOT_RESOLVED));
        assertNotNull(process
                .checkOrWaitForEventInPlugin(io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_FOUND));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testCreatePrimaryInParallel() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        ExecutorService es = Executors.newFixedThreadPool(1000);

        AtomicBoolean pass = new AtomicBoolean(true);

        AuthRecipeUserInfo user1 = EmailPassword.signUp(process.getProcess(), "test1@example.com", "password");

        for (int i = 0; i < 3000; i++) {
            es.execute(() -> {
                try {
                    AuthRecipe.createPrimaryUser(process.getProcess(), user1.getSupertokensUserId());
                    AuthRecipe.unlinkAccounts(process.getProcess(), user1.getSupertokensUserId());
                } catch (Exception e) {
                    if (e.getMessage().toLowerCase().contains("the transaction might succeed if retried")) {
                        pass.set(false);
                    }
                }
            });
        }

        es.shutdown();
        es.awaitTermination(2, TimeUnit.MINUTES);

        assert (pass.get());
        assertNull(process
                .checkOrWaitForEventInPlugin(io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_NOT_RESOLVED));
        assertNotNull(process
                .checkOrWaitForEventInPlugin(io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_FOUND));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}

/*
 * TODO: check later about this error
 * ------------------------
 * LATEST DETECTED DEADLOCK
 * ------------------------
 * 2020-01-15 06:07:36 0x7f546846a700
 *** (1) TRANSACTION:
 * TRANSACTION 196679, ACTIVE 0 sec starting index read
 * mysql tables in use 1, locked 1
 * LOCK WAIT 2 lock struct(s), heap size 1136, 1 row lock(s)
 * MySQL thread id 15926, OS thread handle 140000503174912, query id 335354
 * 172.31.0.253 executionMaster statistics
 * SELECT value, created_at_time FROM key_value WHERE name =
 * 'access_token_signing_key' FOR UPDATE
 *** (1) WAITING FOR THIS LOCK TO BE GRANTED:
 * RECORD LOCKS space id 61 page no 3 n bits 80 index PRIMARY of table
 * `supertokens`.`key_value` trx id 196679 lock_mode
 * X locks rec but not gap waiting
 * Record lock, heap no 8 PHYSICAL RECORD: n_fields 5; compact format; info bits
 * 0
 * 0: len 24; hex 6163636573735f746f6b656e5f7369676e696e675f6b6579; asc
 * access_token_signing_key;;
 * 1: len 6; hex 000000030003; asc ;;
 * 2: len 7; hex 39000001dd08b4; asc 9 ;;
 * 3: len 30; hex 4d494942496a414e42676b71686b6947397730424151454641414f434151;
 * asc MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ;
 * (total 2017 bytes);
 * 4: len 8; hex 0000016fa2a3229a; asc o " ;;
 ***
 * (2) TRANSACTION:
 * TRANSACTION 196680, ACTIVE 0 sec inserting
 * mysql tables in use 1, locked 1
 * 3 lock struct(s), heap size 1136, 2 row lock(s)
 * MySQL thread id 15927, OS thread handle 140000503441152, query id 335358
 * 172.31.0.253 executionMaster update
 * INSERT INTO key_value(name, value, created_at_time)
 * VALUES('access_token_signing_key',
 * 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEApm557QfYLxLc6HmqBMnd3Uz5mKyXpgZr0li1YkIZf8MfIbcVl7l7qlffZmjhgtkIGGVi1yXNFyItM
 * +2N2sOsF9c4qks3BoIkrW0ACltcmqc3wxGEQMfsPYsxRuRMlWnC0nZCzO5MEyVcV7JciSBKc00HzwNrHXsC231Qlh5cJo5
 * /Yun/
 * faW715MaHwLCrvAKXF2/yI2BFAtSBcsgVTv/ZNPuEbadPdg5utN3qSHOmK/
 * hsrQIpZYVhghNFm0q1f90D4cOtFYpJbtUAaHJ+
 * D46kh6RDk1ua6XunpUpbnGhEwtFa8BuEKq+Au5YWcxddxb/xE7h7oIzzE0SCao01ANlFwIDAQAB;
 * MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCmbnntB9gvEtzoeaoEyd3dTPmYrJemBmvSWLViQhl/
 * wx8htxWXuXuqV99maOGC2QgYZWLXJc0XIi0z7Y3aw6wX1ziqSzcGgiStbQAKW1yapzfDEYRAx+
 * w9izFG5EyVacLSdkLM7kwTJVxXslyJIEpzTQfPA2sdewLbfVCWHlwmjn9i6f99pbvXkxofAsKu8ApcXb
 * /IjYEUC1IFyyBVO/9k0+
 * 4Rtp092Dm603epIc6Yr+GytAillhWGCE0WbSrV/
 * 3QPhw60Viklu1QBocn4PjqSHpEOTW5rpe6elSlucaETC0VrwG4Qqr4C7lhZzF13Fv/
 * ETuHugjPMTRIJqjTUA2UXAgMBAAECggEBAJD8RPMcllPL1u4eruIlCUY0PGuoT
 *** (2) HOLDS THE LOCK(S):
 * RECORD LOCKS space id 61 page no 3 n bits 80 index PRIMARY of table
 * `supertokens`.`key_value` trx id 196680 lock_mode
 * X locks rec but not gap
 * Record lock, heap no 8 PHYSICAL RECORD: n_fields 5; compact format; info bits
 * 0
 * 0: len 24; hex 6163636573735f746f6b656e5f7369676e696e675f6b6579; asc
 * access_token_signing_key;;
 * 1: len 6; hex 000000030003; asc ;;
 * 2: len 7; hex 39000001dd08b4; asc 9 ;;
 * 3: len 30; hex 4d494942496a414e42676b71686b6947397730424151454641414f434151;
 * asc MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ;
 * (total 2017 bytes);
 * 4: len 8; hex 0000016fa2a3229a; asc o " ;;
 ***
 * (2) WAITING FOR THIS LOCK TO BE GRANTED:
 * RECORD LOCKS space id 61 page no 3 n bits 80 index PRIMARY of table
 * `supertokens`.`key_value` trx id 196680 lock_mode
 * X waiting
 * Record lock, heap no 8 PHYSICAL RECORD: n_fields 5; compact format; info bits
 * 0
 * 0: len 24; hex 6163636573735f746f6b656e5f7369676e696e675f6b6579; asc
 * access_token_signing_key;;
 * 1: len 6; hex 000000030003; asc ;;
 * 2: len 7; hex 39000001dd08b4; asc 9 ;;
 * 3: len 30; hex 4d494942496a414e42676b71686b6947397730424151454641414f434151;
 * asc MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ;
 * (total 2017 bytes);
 * 4: len 8; hex 0000016fa2a3229a; asc o " ;;
 ***
 * WE ROLL BACK TRANSACTION (1)
 *
 */
