/*
 *    Copyright (c) 2024, VRAI Labs and/or its affiliates. All rights reserved.
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.multitenancy.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.gson.JsonObject;

import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.authRecipe.exceptions.EmailChangeNotAllowedException;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.multitenancy.exception.BadPermissionException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;

public class DbConnectionPoolTest {
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

    /**
     * Helper method to get the tenant database name that matches what
     * Start.modifyConfigToAddANewUserPoolForTesting() creates.
     * The name is worker-specific to avoid conflicts during parallel test execution.
     */
    private static String getTenantDatabaseName(int poolNumber) {
        String workerId = System.getProperty("org.gradle.test.worker", "");
        return workerId.isEmpty() ? "st" + poolNumber : "st" + poolNumber + "_w" + workerId;
    }

    @Test
    public void testActiveConnectionsWithTenants() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Thread.sleep(2000); // let all db connections establish

        Start start = (Start) StorageLayer.getBaseStorage(process.getProcess());
        String testDbName = DatabaseTestHelper.getCurrentTestDatabase();
        assertEquals(10, start.getDbActivityCount(testDbName));

        JsonObject config = new JsonObject();
        start.modifyConfigToAddANewUserPoolForTesting(config, 1);

        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                new TenantIdentifier(null, null, "t1"),
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null, config
        ), false);

        Thread.sleep(1000); // let the new tenant be ready

        String tenantDbName = getTenantDatabaseName(1);
        assertEquals(10, start.getDbActivityCount(tenantDbName));

        // change connection pool size
        config.addProperty("postgresql_connection_pool_size", 20);

        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                new TenantIdentifier(null, null, "t1"),
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null, config
        ), false);

        Thread.sleep(2000); // let the new tenant be ready

        assertEquals(20, start.getDbActivityCount(tenantDbName));

        // delete tenant
        Multitenancy.deleteTenant(new TenantIdentifier(null, null, "t1"), process.getProcess());
        Thread.sleep(2000); // let the tenant be deleted

        assertEquals(0, start.getDbActivityCount(tenantDbName));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // Not so useful test
    // @Test
    public void testDownTimeWhenChangingConnectionPoolSize() throws Exception {
        String[] args = {"../"};

        for (int t = 0; t < 5; t++) {
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
            FeatureFlagTestContent.getInstance(process.getProcess())
                    .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
            process.startProcess();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            Thread.sleep(2000); // let all db connections establish

            Start start = (Start) StorageLayer.getBaseStorage(process.getProcess());
            String testDbName = DatabaseTestHelper.getCurrentTestDatabase();
            assertEquals(10, start.getDbActivityCount(testDbName));

            JsonObject config = new JsonObject();
            start.modifyConfigToAddANewUserPoolForTesting(config, 1);
            config.addProperty("postgresql_connection_pool_size", 300);
            AtomicLong firstErrorTime = new AtomicLong(-1);
            AtomicLong successAfterErrorTime = new AtomicLong(-1);
            AtomicInteger errorCount = new AtomicInteger(0);

            Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                    new TenantIdentifier(null, null, "t1"),
                    new EmailPasswordConfig(true),
                    new ThirdPartyConfig(true, null),
                    new PasswordlessConfig(true),
                    null, null, config
            ), false);

            Thread.sleep(15000); // let the new tenant be ready

            assertEquals(300, start.getDbActivityCount(getTenantDatabaseName(1)));

            ExecutorService es = Executors.newFixedThreadPool(100);

            for (int i = 0; i < 10000; i++) {
                int finalI = i;
                es.execute(() -> {
                    try {
                        TenantIdentifier t1 = new TenantIdentifier(null, null, "t1");
                        Storage t1Storage = (StorageLayer.getStorage(t1, process.getProcess()));
                        ThirdParty.signInUp(t1, t1Storage, process.getProcess(), "google", "googleid" + finalI, "user" +
                                finalI + "@example.com");

                        if (firstErrorTime.get() != -1 && successAfterErrorTime.get() == -1) {
                            successAfterErrorTime.set(System.currentTimeMillis());
                        }
                    } catch (StorageQueryException e) {
                        if (e.getMessage().contains("Connection is closed") ||
                                e.getMessage().contains("has been closed")) {
                            if (firstErrorTime.get() == -1) {
                                firstErrorTime.set(System.currentTimeMillis());
                            }
                        } else {
                            errorCount.incrementAndGet();
                            throw new RuntimeException(e);
                        }
                    } catch (EmailChangeNotAllowedException e) {
                        errorCount.incrementAndGet();
                        throw new RuntimeException(e);
                    } catch (TenantOrAppNotFoundException e) {
                        errorCount.incrementAndGet();
                        throw new RuntimeException(e);
                    } catch (BadPermissionException e) {
                        errorCount.incrementAndGet();
                        throw new RuntimeException(e);
                    } catch (IllegalStateException e) {
                        if (e.getMessage().contains("Please call initPool before getConnection")) {
                            if (firstErrorTime.get() == -1) {
                                firstErrorTime.set(System.currentTimeMillis());
                            }
                        } else {
                            errorCount.incrementAndGet();
                            throw e;
                        }
                    }
                });
            }

            // change connection pool size
            config.addProperty("postgresql_connection_pool_size", 200);

            Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                    new TenantIdentifier(null, null, "t1"),
                    new EmailPasswordConfig(true),
                    new ThirdPartyConfig(true, null),
                    new PasswordlessConfig(true),
                    null, null, config
            ), false);

            Thread.sleep(3000); // let the new tenant be ready

            es.shutdown();
            es.awaitTermination(2, TimeUnit.MINUTES);

            assertEquals(0, errorCount.get());

            assertEquals(200, start.getDbActivityCount(getTenantDatabaseName(1)));

            // delete tenant
            Multitenancy.deleteTenant(new TenantIdentifier(null, null, "t1"), process.getProcess());
            Thread.sleep(3000); // let the tenant be deleted

            assertEquals(0, start.getDbActivityCount(getTenantDatabaseName(1)));

            System.out.println(successAfterErrorTime.get() - firstErrorTime.get() + "ms");
            assertTrue(successAfterErrorTime.get() - firstErrorTime.get() < 250);

            if (successAfterErrorTime.get() - firstErrorTime.get() == 0) {
                process.kill();
                assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
                continue; // retry
            }

            assertTrue(successAfterErrorTime.get() - firstErrorTime.get() > 0);
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

            return;
        }

        fail(); // tried 5 times
    }


    @Test
    public void testMinimumIdleConnections() throws Exception {
        String[] args = {"../"};
        {
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
            FeatureFlagTestContent.getInstance(process.getProcess())
                    .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
            Utils.setValueInConfig("postgresql_connection_pool_size", "20");
            Utils.setValueInConfig("postgresql_minimum_idle_connections", "10");
            Utils.setValueInConfig("postgresql_idle_connection_timeout", "10000"); // HikariCP minimum is 10000ms
            System.setProperty("com.zaxxer.hikari.housekeeping.periodMs", "1000");
            process.startProcess();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            Thread.sleep(15000); // let the idle connections time out (10s idle + 1s housekeeping + buffer)

            Start start = (Start) StorageLayer.getBaseStorage(process.getProcess());
            String testDbName = DatabaseTestHelper.getCurrentTestDatabase();
            assertEquals(10, start.getDbActivityCount(testDbName));

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void testMinimumIdleConnectionForTenants() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Thread.sleep(2000); // let all db connections establish

        Start start = (Start) StorageLayer.getBaseStorage(process.getProcess());
        String testDbName = DatabaseTestHelper.getCurrentTestDatabase();
        assertEquals(10, start.getDbActivityCount(testDbName));

        JsonObject config = new JsonObject();
        start.modifyConfigToAddANewUserPoolForTesting(config, 1);

        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                new TenantIdentifier(null, null, "t1"),
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null, config
        ), false);

        Thread.sleep(1000); // let the new tenant be ready

        for (int retry = 0; retry < 5; retry++) {
            try {
                assertEquals(10, start.getDbActivityCount(getTenantDatabaseName(1)));
                break;
            } catch (AssertionError e) {
                Thread.sleep(1000);
                continue;
            }
        }

        assertEquals(10, start.getDbActivityCount(getTenantDatabaseName(1)));

        // change connection pool size
        config.addProperty("postgresql_connection_pool_size", 20);
        config.addProperty("postgresql_minimum_idle_connections", 5);

        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                new TenantIdentifier(null, null, "t1"),
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null, config
        ), false);

        Thread.sleep(2000); // let the new tenant be ready

        assertEquals(5, start.getDbActivityCount(getTenantDatabaseName(1)));

        // delete tenant
        Multitenancy.deleteTenant(new TenantIdentifier(null, null, "t1"), process.getProcess());
        Thread.sleep(2000); // let the tenant be deleted

        assertEquals(0, start.getDbActivityCount(getTenantDatabaseName(1)));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testIdleConnectionTimeout() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Thread.sleep(2000); // let all db connections establish

        Start start = (Start) StorageLayer.getBaseStorage(process.getProcess());
        String testDbName = DatabaseTestHelper.getCurrentTestDatabase();
        assertEquals(10, start.getDbActivityCount(testDbName));

        JsonObject config = new JsonObject();
        start.modifyConfigToAddANewUserPoolForTesting(config, 1);
        config.addProperty("postgresql_connection_pool_size", 300);
        config.addProperty("postgresql_minimum_idle_connections", 5);
        config.addProperty("postgresql_idle_connection_timeout", 10000); // HikariCP minimum is 10000ms
        System.setProperty("com.zaxxer.hikari.housekeeping.periodMs", "1000");

        AtomicLong errorCount = new AtomicLong(0);

        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                new TenantIdentifier(null, null, "t1"),
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null, config
        ), false);

        Thread.sleep(3000); // let the new tenant be ready

        assertTrue(10 >= start.getDbActivityCount(getTenantDatabaseName(1)));

        ExecutorService es = Executors.newFixedThreadPool(150);

        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            es.execute(() -> {
                try {
                    TenantIdentifier t1 = new TenantIdentifier(null, null, "t1");
                    Storage t1Storage = (StorageLayer.getStorage(t1, process.getProcess()));
                    ThirdParty.signInUp(t1, t1Storage, process.getProcess(), "google", "googleid" + finalI, "user" +
                            finalI + "@example.com");

                } catch (StorageQueryException e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                } catch (EmailChangeNotAllowedException e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                } catch (TenantOrAppNotFoundException e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                } catch (BadPermissionException e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
            });
        }

        es.shutdown();
        es.awaitTermination(2, TimeUnit.MINUTES);

        assertTrue(5 < start.getDbActivityCount(getTenantDatabaseName(1)));

        assertEquals(0, errorCount.get());

        Thread.sleep(15000); // let the idle connections time out (10s idle + 1s housekeeping + buffer)

        assertEquals(5, start.getDbActivityCount(getTenantDatabaseName(1)));

        // delete tenant
        Multitenancy.deleteTenant(new TenantIdentifier(null, null, "t1"), process.getProcess());
        Thread.sleep(3000); // let the tenant be deleted

        assertEquals(0, start.getDbActivityCount(getTenantDatabaseName(1)));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}