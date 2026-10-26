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

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.gson.JsonObject;

import io.supertokens.ProcessState;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.multitenancy.EmailPasswordConfig;
import io.supertokens.pluginInterface.multitenancy.PasswordlessConfig;
import io.supertokens.pluginInterface.multitenancy.TenantConfig;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.ThirdPartyConfig;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storageLayer.StorageLayer;

/**
 * Exercises ConnectionPool's test-only guard against borrowing a second connection from the same pool while a
 * startTransaction on that pool holds one. The guard throws by default, so "allowed" cases fail the test on a false
 * positive; warn-mode (opt-out) cases assert on the test thread's own warning counter, so a warning a background cron
 * raises on another thread cannot stand in for a missed one.
 */
public class NestedConnectionAcquisitionGuardTest {
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

    @After
    public void afterEach() {
        ConnectionPool.setThrowOnNestedAcquisition(true);
    }

    private static boolean causedByIllegalState(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof IllegalStateException
                    && c.getMessage() != null && c.getMessage().contains("Nested same-pool connection acquisition")) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void nestedTransactionOnSamePoolThrowsByDefaultAndWarnsWhenOptedOut() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        Start start = (Start) StorageLayer.getBaseStorage(process.getProcess());

        assertTrue(ConnectionPool.isThrowOnNestedAcquisition());
        try {
            start.startTransaction(con -> start.startTransaction(inner -> null));
            fail("nested same-pool startTransaction should throw by default");
        } catch (Exception e) {
            assertTrue("unexpected exception: " + e, causedByIllegalState(e));
        }

        ConnectionPool.setThrowOnNestedAcquisition(false);
        long before = ConnectionPool.getNestedAcquisitionWarningCountOnCurrentThread();
        start.startTransaction(con -> start.startTransaction(inner -> null));
        assertEquals(before + 1, ConnectionPool.getNestedAcquisitionWarningCountOnCurrentThread());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void plainQueryInsideTransactionThrowsByDefaultAndWarnsWhenOptedOut() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        Start start = (Start) StorageLayer.getBaseStorage(process.getProcess());

        // a plain (non-_Transaction) read borrows its own connection from the pool the transaction holds
        try {
            start.startTransaction(con -> start.getKeyValue(TenantIdentifier.BASE_TENANT, "nested-guard"));
            fail("plain same-pool read inside a transaction should throw by default");
        } catch (Exception e) {
            assertTrue("unexpected exception: " + e, causedByIllegalState(e));
        }

        ConnectionPool.setThrowOnNestedAcquisition(false);
        long before = ConnectionPool.getNestedAcquisitionWarningCountOnCurrentThread();
        start.startTransaction(con -> start.getKeyValue(TenantIdentifier.BASE_TENANT, "nested-guard"));
        assertEquals(before + 1, ConnectionPool.getNestedAcquisitionWarningCountOnCurrentThread());

        // outside any transaction the same read is fine
        start.getKeyValue(TenantIdentifier.BASE_TENANT, "nested-guard");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void warningOnAnotherThreadDoesNotCountForThisThread() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        Start start = (Start) StorageLayer.getBaseStorage(process.getProcess());

        ConnectionPool.setThrowOnNestedAcquisition(false);
        long globalBefore = ConnectionPool.getNestedAcquisitionWarningCount();
        long thisThreadBefore = ConnectionPool.getNestedAcquisitionWarningCountOnCurrentThread();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try {
                start.startTransaction(con -> start.startTransaction(inner -> null));
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        other.start();
        other.join();
        assertNull(failure.get());

        // the other thread's nested borrow is a real warning, but it is not this thread's
        assertTrue(ConnectionPool.getNestedAcquisitionWarningCount() > globalBefore);
        assertEquals(thisThreadBefore, ConnectionPool.getNestedAcquisitionWarningCountOnCurrentThread());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void borrowOnDifferentUserPoolInsideTransactionIsAllowed() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        Start baseStart = (Start) StorageLayer.getBaseStorage(process.getProcess());

        JsonObject config = new JsonObject();
        baseStart.modifyConfigToAddANewUserPoolForTesting(config, 1);
        TenantIdentifier t1 = new TenantIdentifier(null, null, "t1");
        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                t1, new EmailPasswordConfig(true), new ThirdPartyConfig(true, null), new PasswordlessConfig(true),
                null, null, config
        ), false);
        Start t1Start = (Start) StorageLayer.getStorage(t1, process.getProcess());
        assertNotEquals(baseStart.getUserPoolId(), t1Start.getUserPoolId());

        // the default throw mode is on, so a false positive fails here
        baseStart.startTransaction(con -> t1Start.getKeyValue(t1, "nested-guard"));
        baseStart.startTransaction(con -> t1Start.startTransaction(inner -> null));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void markIsClearedAfterTransactionCallbackThrows() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        Start start = (Start) StorageLayer.getBaseStorage(process.getProcess());

        try {
            start.startTransaction(con -> {
                throw new RuntimeException("callback failure");
            });
            fail("callback exception should propagate");
        } catch (Exception e) {
            assertFalse(causedByIllegalState(e));
        }

        // same thread, same pool, no open transaction: must not be flagged
        start.getKeyValue(TenantIdentifier.BASE_TENANT, "nested-guard");
        start.startTransaction(con -> null);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void warnModeOptOutIsResetWhenTheProcessStops() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        ConnectionPool.setThrowOnNestedAcquisition(false);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        assertTrue(ConnectionPool.isThrowOnNestedAcquisition());
    }
}
