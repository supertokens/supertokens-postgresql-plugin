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

import com.google.gson.JsonObject;
import io.supertokens.ProcessState;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.multitenancy.EmailPasswordConfig;
import io.supertokens.pluginInterface.multitenancy.PasswordlessConfig;
import io.supertokens.pluginInterface.multitenancy.TenantConfig;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.ThirdPartyConfig;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

/**
 * Regression test for the production shape that hung core startup: a core with many tenant storages on
 * separate databases, booting with {@code log_level: DEBUG} so that every pool's Hikari configuration dump
 * (~40 lines each) is pushed through the shared console appender while the storages initialise in parallel.
 * Boot must finish with every storage initialised and usable.
 */
public class ParallelStorageInitDebugLoggingTest {
    private static final int TENANT_STORAGES = 12;

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

    private static TenantConfig tenantOnOwnDatabase(Start start, int number) {
        JsonObject config = new JsonObject();
        start.modifyConfigToAddANewUserPoolForTesting(config, number);
        return new TenantConfig(new TenantIdentifier(null, null, "t" + number), new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null), new PasswordlessConfig(true), null, null, config);
    }

    @Test
    public void bootWithManyTenantStoragesUnderDebugLoggingInitialisesEveryStorage() throws Exception {
        Utils.setValueInConfig("log_level", "DEBUG");
        String[] args = {"../"};

        {
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
            FeatureFlagTestContent.getInstance(process.getProcess()).setKeyValue(
                    FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
            process.startProcess();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            Start base = (Start) StorageLayer.getBaseStorage(process.getProcess());
            for (int i = 1; i <= TENANT_STORAGES; i++) {
                Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), tenantOnOwnDatabase(base, i), false);
            }

            process.kill(false); // keep the tenants: the next boot has to load all of them at once
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }

        {
            // the real thing: loadAllTenantStorage initialises base + TENANT_STORAGES storages in parallel,
            // each logging its Hikari config at DEBUG through the one shared console appender
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
            FeatureFlagTestContent.getInstance(process.getProcess()).setKeyValue(
                    FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
            process.startProcess();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.LOADING_ALL_TENANT_STORAGE));

            Storage base = StorageLayer.getBaseStorage(process.getProcess());
            Set<Storage> distinct = new HashSet<>();
            distinct.add(base);
            for (int i = 1; i <= TENANT_STORAGES; i++) {
                Storage storage = StorageLayer.getStorage(new TenantIdentifier(null, null, "t" + i),
                        process.getProcess());
                assertNotSame("tenant t" + i + " must have its own storage", base, storage);
                distinct.add(storage);
                // a query proves the pool came up (not just that the resource exists)
                assertEquals(0, ((Start) storage).getUsersCount(new TenantIdentifier(null, null, "t" + i),
                        null));
            }
            assertEquals(TENANT_STORAGES + 1, distinct.size());

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
