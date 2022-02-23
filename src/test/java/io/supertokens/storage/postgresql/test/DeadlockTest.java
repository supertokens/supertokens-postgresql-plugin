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
import io.supertokens.passwordless.Passwordless;
import io.supertokens.passwordless.exceptions.RestartFlowException;
import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.sqlStorage.SQLStorage;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void transactionDeadlockTesting()
            throws InterruptedException, StorageQueryException, StorageTransactionLogicException {
        String[] args = { "../" };
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Storage storage = StorageLayer.getStorage(process.getProcess());
        SQLStorage sqlStorage = (SQLStorage) storage;
        sqlStorage.startTransaction(con -> {
            sqlStorage.setKeyValue_Transaction(con, "Key", new KeyValueInfo("Value"));
            sqlStorage.setKeyValue_Transaction(con, "Key1", new KeyValueInfo("Value1"));
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

                    sqlStorage.getKeyValue_Transaction(con, "Key");

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

                    sqlStorage.getKeyValue_Transaction(con, "Key1");
                    t1Failed.set(false); // it should come here because we will try three times.
                    return null;
                });
            } catch (Exception ignored) {
            }
        };

        Runnable r2 = () -> {
            try {
                sqlStorage.startTransaction(con -> {

                    sqlStorage.getKeyValue_Transaction(con, "Key1");

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

                    sqlStorage.getKeyValue_Transaction(con, "Key");

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
        String[] args = { "../" };

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        ExecutorService es = Executors.newCachedThreadPool();

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
        String[] args = { "../" };

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        ExecutorService es = Executors.newCachedThreadPool();

        AtomicBoolean pass = new AtomicBoolean(true);

        for (int i = 0; i < 3000; i++) {
            final int ind = i;
            es.execute(() -> {
                try {
                    Passwordless.CreateCodeResponse resp = Passwordless.createCode(process.getProcess(),
                            "test" + ind + "@example.com", null, null, null);
                    Passwordless.ConsumeCodeResponse resp2 = Passwordless.consumeCode(process.getProcess(),
                            resp.deviceId, resp.deviceIdHash, resp.userInputCode, resp.linkCode);

                } catch (Exception e) {
                    if (e.getMessage() != null
                            && e.getMessage().toLowerCase().contains("the transaction might succeed if retried")) {
                        pass.set(false);
                    }
                }
            });
        }

        es.shutdown();
        es.awaitTermination(2, TimeUnit.MINUTES);

        assertNull(process.checkOrWaitForEventInPlugin(
                io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_FOUND));
        assert (pass.get());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testCodeConsumeRapidlyWithoutRetries() throws Exception {
        String[] args = { "../" };

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        ExecutorService es = Executors.newCachedThreadPool();

        AtomicBoolean pass = new AtomicBoolean(true);

        final int count = 3000;
        Passwordless.CreateCodeResponse[] createResps = new Passwordless.CreateCodeResponse[count];
        for (int i = 0; i < count; i++) {
            final String email = "test" + i + "@example.com";
            createResps[i] = Passwordless.createCode(process.getProcess(), email, null, null, null);
        }

        for (int i = 0; i < count; i++) {
            final Passwordless.CreateCodeResponse resp = createResps[i];
            es.execute(() -> {
                try {
                    Passwordless.consumeCode(process.getProcess(), resp.deviceId, resp.deviceIdHash, resp.userInputCode,
                            resp.linkCode);
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
        assertNull(process.checkOrWaitForEventInPlugin(
                io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_FOUND));

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
 * MySQL thread id 15926, OS thread handle 140000503174912, query id 335354 172.31.0.253 executionMaster statistics
 * SELECT value, created_at_time FROM key_value WHERE name = 'access_token_signing_key' FOR UPDATE
 *** (1) WAITING FOR THIS LOCK TO BE GRANTED:
 * RECORD LOCKS space id 61 page no 3 n bits 80 index PRIMARY of table `supertokens`.`key_value` trx id 196679 lock_mode
 * X locks rec but not gap waiting
 * Record lock, heap no 8 PHYSICAL RECORD: n_fields 5; compact format; info bits 0
 * 0: len 24; hex 6163636573735f746f6b656e5f7369676e696e675f6b6579; asc access_token_signing_key;;
 * 1: len 6; hex 000000030003; asc ;;
 * 2: len 7; hex 39000001dd08b4; asc 9 ;;
 * 3: len 30; hex 4d494942496a414e42676b71686b6947397730424151454641414f434151; asc MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ;
 * (total 2017 bytes);
 * 4: len 8; hex 0000016fa2a3229a; asc o " ;;
 ***
 * (2) TRANSACTION:
 * TRANSACTION 196680, ACTIVE 0 sec inserting
 * mysql tables in use 1, locked 1
 * 3 lock struct(s), heap size 1136, 2 row lock(s)
 * MySQL thread id 15927, OS thread handle 140000503441152, query id 335358 172.31.0.253 executionMaster update
 * INSERT INTO key_value(name, value, created_at_time) VALUES('access_token_signing_key',
 * 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEApm557QfYLxLc6HmqBMnd3Uz5mKyXpgZr0li1YkIZf8MfIbcVl7l7qlffZmjhgtkIGGVi1yXNFyItM
 * +2N2sOsF9c4qks3BoIkrW0ACltcmqc3wxGEQMfsPYsxRuRMlWnC0nZCzO5MEyVcV7JciSBKc00HzwNrHXsC231Qlh5cJo5/Yun/
 * faW715MaHwLCrvAKXF2/yI2BFAtSBcsgVTv/ZNPuEbadPdg5utN3qSHOmK/hsrQIpZYVhghNFm0q1f90D4cOtFYpJbtUAaHJ+
 * D46kh6RDk1ua6XunpUpbnGhEwtFa8BuEKq+Au5YWcxddxb/xE7h7oIzzE0SCao01ANlFwIDAQAB;
 * MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCmbnntB9gvEtzoeaoEyd3dTPmYrJemBmvSWLViQhl/
 * wx8htxWXuXuqV99maOGC2QgYZWLXJc0XIi0z7Y3aw6wX1ziqSzcGgiStbQAKW1yapzfDEYRAx+
 * w9izFG5EyVacLSdkLM7kwTJVxXslyJIEpzTQfPA2sdewLbfVCWHlwmjn9i6f99pbvXkxofAsKu8ApcXb/IjYEUC1IFyyBVO/9k0+
 * 4Rtp092Dm603epIc6Yr+GytAillhWGCE0WbSrV/3QPhw60Viklu1QBocn4PjqSHpEOTW5rpe6elSlucaETC0VrwG4Qqr4C7lhZzF13Fv/
 * ETuHugjPMTRIJqjTUA2UXAgMBAAECggEBAJD8RPMcllPL1u4eruIlCUY0PGuoT
 *** (2) HOLDS THE LOCK(S):
 * RECORD LOCKS space id 61 page no 3 n bits 80 index PRIMARY of table `supertokens`.`key_value` trx id 196680 lock_mode
 * X locks rec but not gap
 * Record lock, heap no 8 PHYSICAL RECORD: n_fields 5; compact format; info bits 0
 * 0: len 24; hex 6163636573735f746f6b656e5f7369676e696e675f6b6579; asc access_token_signing_key;;
 * 1: len 6; hex 000000030003; asc ;;
 * 2: len 7; hex 39000001dd08b4; asc 9 ;;
 * 3: len 30; hex 4d494942496a414e42676b71686b6947397730424151454641414f434151; asc MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ;
 * (total 2017 bytes);
 * 4: len 8; hex 0000016fa2a3229a; asc o " ;;
 ***
 * (2) WAITING FOR THIS LOCK TO BE GRANTED:
 * RECORD LOCKS space id 61 page no 3 n bits 80 index PRIMARY of table `supertokens`.`key_value` trx id 196680 lock_mode
 * X waiting
 * Record lock, heap no 8 PHYSICAL RECORD: n_fields 5; compact format; info bits 0
 * 0: len 24; hex 6163636573735f746f6b656e5f7369676e696e675f6b6579; asc access_token_signing_key;;
 * 1: len 6; hex 000000030003; asc ;;
 * 2: len 7; hex 39000001dd08b4; asc 9 ;;
 * 3: len 30; hex 4d494942496a414e42676b71686b6947397730424151454641414f434151; asc MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ;
 * (total 2017 bytes);
 * 4: len 8; hex 0000016fa2a3229a; asc o " ;;
 ***
 * WE ROLL BACK TRANSACTION (1)
 *
 */