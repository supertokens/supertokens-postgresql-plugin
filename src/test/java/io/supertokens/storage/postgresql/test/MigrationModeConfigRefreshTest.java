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
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.migration.MigrationBackfillStorage;
import io.supertokens.pluginInterface.multitenancy.EmailPasswordConfig;
import io.supertokens.pluginInterface.multitenancy.PasswordlessConfig;
import io.supertokens.pluginInterface.multitenancy.TenantConfig;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.ThirdPartyConfig;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Regression test: updating a tenant's {@code coreConfig.migration_mode} must take effect on the
 * live storage instance without a core restart.
 *
 * <p>When a tenant's config is changed through the multitenancy CRUD path,
 * {@code MultitenancyHelper.refreshAfterKnownTenantChange} rebuilds the storage layer, reusing an
 * existing storage instance when its {@code userPoolId + connectionPoolId} is unchanged. Because
 * {@code migration_mode} is a {@code @ConnectionPoolProperty} in {@link
 * io.supertokens.storage.postgresql.config.PostgreSQLConfig}, a mode change alters the
 * connectionPoolId, so the reuse check misses and a fresh instance carrying the new mode is built
 * (the old one is closed). Before that annotation, the mode did not affect pool identity, the stale
 * instance was reused, and the persisted mode stayed dormant until the next restart.
 */
public class MigrationModeConfigRefreshTest {

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

    // A dedicated app pointed at its own database, isolated from the base tenant's storage. Only the
    // migration_mode changes between create and update below.
    private static final TenantIdentifier APP = new TenantIdentifier(null, "a1", null);
    private static final String APP_DB = "st1";

    private TenantConfig appConfig(MigrationMode mode) {
        JsonObject coreConfig = new JsonObject();
        coreConfig.addProperty("postgresql_database_name", APP_DB);
        coreConfig.addProperty("migration_mode", mode.name());
        return new TenantConfig(APP,
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null, coreConfig);
    }

    private MigrationMode liveMode(TestingProcessManager.TestingProcess process) throws Exception {
        Storage storage = StorageLayer.getStorage(APP, process.getProcess());
        return ((MigrationBackfillStorage) storage).getMigrationMode();
    }

    @Test
    public void migrationModeUpdateAppliesToLiveStorageWithoutRestart() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            return;
        }

        // Create the app already carrying migration_mode=LEGACY.
        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), appConfig(MigrationMode.LEGACY), false);
        assertEquals(MigrationMode.LEGACY, liveMode(process));

        // Update ONLY the migration_mode. The DB (userPoolId) is unchanged, but migration_mode is a
        // @ConnectionPoolProperty, so the connectionPoolId changes and a fresh storage instance is
        // built with the new mode.
        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(),
                appConfig(MigrationMode.DUAL_WRITE_READ_OLD), false);

        // The regression: before migration_mode participated in the pool identity, this still
        // returned LEGACY (stale reused instance) and only a core restart activated the new mode.
        assertEquals(MigrationMode.DUAL_WRITE_READ_OLD, liveMode(process));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}