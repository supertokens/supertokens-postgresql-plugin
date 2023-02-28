/*
 *    Copyright (c) 2023, VRAI Labs and/or its affiliates. All rights reserved.
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

package io.supertokens.storage.postgresql.test.multitenancy;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.supertokens.ProcessState;
import io.supertokens.config.Config;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.exceptions.DbInitException;
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.test.TestingProcessManager;
import io.supertokens.storage.postgresql.test.Utils;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.*;
import org.junit.rules.TestRule;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.sql.SQLTransientConnectionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class StorageLayerTest {
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
    public void normalDbConfigErrorContinuesToWork() throws InterruptedException, IOException {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_connection_pool_size", "-1");
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();

        ProcessState.EventAndException e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE);
        assertNotNull(e);
        assertEquals(e.exception.getCause().getMessage(),
                "'postgresql_connection_pool_size' in the config.yaml file must be > 0");

        assertNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.LOADING_ALL_TENANT_STORAGE, 1000));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void mergingTenantWithBaseConfigWorks()
            throws InterruptedException, IOException, InvalidConfigException, DbInitException,
            TenantOrAppNotFoundException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("postgresql_table_names_prefix", new JsonPrimitive("test"));
        tenantConfig.add("postgresql_table_schema", new JsonPrimitive("random"));

        TenantConfig[] tenants = new TenantConfig[]{
                new TenantConfig(new TenantIdentifier("abc", null, null), new EmailPasswordConfig(false),
                        new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                        new PasswordlessConfig(false),
                        tenantConfig)};

        Config.loadAllTenantConfig(process.getProcess(), tenants);

        StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);

        assertNotSame(StorageLayer.getStorage(new TenantIdentifier("abc", null, null), process.getProcess()),
                StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()));

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()))
                .getTablePrefix(), "");
        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()))
                .getTableSchema(), "public");

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("abc", null, null), process.getProcess()))
                .getTablePrefix(), "test");
        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("abc", null, null), process.getProcess()))
                .getTableSchema(), "random");

        Assert.assertEquals(
                process.getProcess().getResourceDistributor().getAllResourcesWithResourceKey(StorageLayer.RESOURCE_KEY)
                        .size(), 2);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void creatingTenantWithNoExistingDbThrowsError()
            throws InterruptedException, IOException, InvalidConfigException, DbInitException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("postgresql_table_names_prefix", new JsonPrimitive("test"));
        tenantConfig.add("postgresql_database_name", new JsonPrimitive("random"));

        try {
            TenantConfig[] tenants = new TenantConfig[]{
                    new TenantConfig(new TenantIdentifier("abc", null, null), new EmailPasswordConfig(false),
                            new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                            new PasswordlessConfig(false),
                            tenantConfig)};

            Config.loadAllTenantConfig(process.getProcess(), tenants);

            StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);
            fail();
        } catch (DbInitException e) {
            assertEquals(e.getMessage(), "com.zaxxer.hikari.pool.HikariPool$PoolInitializationException: Failed to " +
                    "initialize pool: FATAL: database \"random\" does not exist");
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void storageInstanceIsReusedAcrossTenants()
            throws InterruptedException, IOException, InvalidConfigException, DbInitException,
            TenantOrAppNotFoundException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("access_token_validity", new JsonPrimitive(3601));

        TenantConfig[] tenants = new TenantConfig[]{
                new TenantConfig(new TenantIdentifier(null, "abc", null), new EmailPasswordConfig(false),
                        new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                        new PasswordlessConfig(false),
                        tenantConfig)};

        Config.loadAllTenantConfig(process.getProcess(), tenants);

        StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);

        assertSame(StorageLayer.getStorage(new TenantIdentifier(null, "abc", null), process.getProcess()),
                StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()));

        Assert.assertEquals(
                Config.getConfig(new TenantIdentifier(null, null, null), process.getProcess()).getAccessTokenValidity(),
                (long) 3600 * 1000);

        Assert.assertEquals(Config.getConfig(new TenantIdentifier(null, "abc", null), process.getProcess())
                        .getAccessTokenValidity(),
                (long) 3601 * 1000);

        Assert.assertEquals(
                process.getProcess().getResourceDistributor().getAllResourcesWithResourceKey(StorageLayer.RESOURCE_KEY)
                        .size(), 2);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void storageInstanceIsReusedAcrossTenantsComplex()
            throws InterruptedException, IOException, InvalidConfigException, DbInitException,
            TenantOrAppNotFoundException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("access_token_validity", new JsonPrimitive(3601));

        JsonObject tenantConfig1 = new JsonObject();
        tenantConfig1.add("postgresql_connection_pool_size", new JsonPrimitive(11));

        TenantConfig[] tenants = new TenantConfig[]{
                new TenantConfig(new TenantIdentifier(null, "abc", null), new EmailPasswordConfig(false),
                        new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                        new PasswordlessConfig(false),
                        tenantConfig),
                new TenantConfig(new TenantIdentifier(null, "abc", "t1"), new EmailPasswordConfig(false),
                        new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                        new PasswordlessConfig(false),
                        tenantConfig1),
                new TenantConfig(new TenantIdentifier(null, null, "t2"), new EmailPasswordConfig(false),
                        new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                        new PasswordlessConfig(false),
                        tenantConfig1)};

        Config.loadAllTenantConfig(process.getProcess(), tenants);

        StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);

        assertSame(StorageLayer.getStorage(new TenantIdentifier(null, "abc", null), process.getProcess()),
                StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()));

        assertSame(StorageLayer.getStorage(new TenantIdentifier(null, "abc", "t1"), process.getProcess()),
                StorageLayer.getStorage(new TenantIdentifier(null, null, "t2"), process.getProcess()));

        assertNotSame(StorageLayer.getStorage(new TenantIdentifier(null, "abc", "t1"), process.getProcess()),
                StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()));

        Assert.assertEquals(
                Config.getConfig(new TenantIdentifier(null, null, null), process.getProcess()).getAccessTokenValidity(),
                (long) 3600 * 1000);

        Assert.assertEquals(Config.getConfig(new TenantIdentifier(null, "abc", null), process.getProcess())
                        .getAccessTokenValidity(),
                (long) 3601 * 1000);

        Assert.assertEquals(
                process.getProcess().getResourceDistributor().getAllResourcesWithResourceKey(StorageLayer.RESOURCE_KEY)
                        .size(), 4);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier(null, "abc", "t1"), process.getProcess()))
                .getConnectionPoolSize(), 11);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("random", null, "t2"),
                                process.getProcess()))
                .getConnectionPoolSize(), 11);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("random", null, null),
                                process.getProcess()))
                .getConnectionPoolSize(), 10);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void mergingTenantWithBaseConfigWithInvalidConfigThrowsErrorWorks()
            throws InterruptedException, IOException, DbInitException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("postgresql_connection_pool_size", new JsonPrimitive(-1));

        try {
            TenantConfig[] tenants = new TenantConfig[]{
                    new TenantConfig(new TenantIdentifier("abc", null, null), new EmailPasswordConfig(false),
                            new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                            new PasswordlessConfig(false),
                            tenantConfig)};
            Config.loadAllTenantConfig(process.getProcess(), tenants);

            StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);
            fail();
        } catch (InvalidConfigException e) {
            assert (e.getMessage()
                    .contains("'postgresql_connection_pool_size' in the config.yaml file must be > 0"));
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void mergingTenantWithBaseConfigWithConflictingConfigsThrowsError()
            throws InterruptedException, IOException, DbInitException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("postgresql_thirdparty_users_table_name", new JsonPrimitive("random"));

        try {
            TenantConfig[] tenants = new TenantConfig[]{
                    new TenantConfig(new TenantIdentifier(null, "abc", null), new EmailPasswordConfig(false),
                            new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                            new PasswordlessConfig(false),
                            tenantConfig)};
            Config.loadAllTenantConfig(process.getProcess(), tenants);

            StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);
            fail();
        } catch (InvalidConfigException e) {
            assertEquals(e.getMessage(),
                    "You cannot set different name for table random for the same user pool");
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void mergingDifferentConnectionPoolIdTenantWithBaseConfigWithConflictingConfigsShouldThrowsError()
            throws InterruptedException, IOException, DbInitException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("postgresql_table_names_prefix", new JsonPrimitive("random"));
        tenantConfig.add("postgresql_connection_pool_size", new JsonPrimitive(11));

        try {
            TenantConfig[] tenants = new TenantConfig[]{
                    new TenantConfig(new TenantIdentifier(null, "abc", null), new EmailPasswordConfig(false),
                            new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                            new PasswordlessConfig(false),
                            tenantConfig)};
            Config.loadAllTenantConfig(process.getProcess(), tenants);

            StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);
            fail();
        } catch (InvalidConfigException e) {
            assertEquals(e.getMessage(),
                    "You cannot set different name for table prefix for the same user pool");
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void mergingDifferentUserPoolIdTenantWithBaseConfigWithConflictingConfigsShouldNotThrowsError()
            throws InterruptedException, IOException, InvalidConfigException, DbInitException,
            TenantOrAppNotFoundException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("postgresql_thirdparty_users_table_name", new JsonPrimitive("random"));
        tenantConfig.add("postgresql_connection_pool_size", new JsonPrimitive(11));
        tenantConfig.add("postgresql_table_schema", new JsonPrimitive("supertokens2"));

        TenantConfig[] tenants = new TenantConfig[]{
                new TenantConfig(new TenantIdentifier("abc", null, null), new EmailPasswordConfig(false),
                        new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                        new PasswordlessConfig(false),
                        tenantConfig)};
        Config.loadAllTenantConfig(process.getProcess(), tenants);

        StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("abc", null, null), process.getProcess()))
                .getTableSchema(), "supertokens2");
        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("abc", null, null), process.getProcess()))
                .getConnectionPoolSize(), 11);
        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("abc", null, null), process.getProcess()))
                .getThirdPartyUsersTable(), "supertokens2.random");

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()))
                .getTableSchema(), "public");
        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()))
                .getConnectionPoolSize(), 10);
        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()))
                .getThirdPartyUsersTable(), "thirdparty_users");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void newStorageIsNotCreatedWhenSameTenantIsAdded()
            throws InterruptedException, IOException, InvalidConfigException, DbInitException,
            TenantOrAppNotFoundException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Storage existingStorage = StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess());

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("access_token_validity", new JsonPrimitive(3601));

        TenantConfig[] tenants = new TenantConfig[]{
                new TenantConfig(new TenantIdentifier(null, "abc", null), new EmailPasswordConfig(false),
                        new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                        new PasswordlessConfig(false),
                        tenantConfig)};

        Config.loadAllTenantConfig(process.getProcess(), tenants);

        StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);

        assertSame(StorageLayer.getStorage(new TenantIdentifier(null, "abc", null), process.getProcess()),
                existingStorage);

        Assert.assertEquals(
                Config.getConfig(new TenantIdentifier(null, null, null), process.getProcess()).getAccessTokenValidity(),
                (long) 3600 * 1000);

        Assert.assertEquals(Config.getConfig(new TenantIdentifier(null, "abc", null), process.getProcess())
                        .getAccessTokenValidity(),
                (long) 3601 * 1000);

        Assert.assertEquals(
                process.getProcess().getResourceDistributor().getAllResourcesWithResourceKey(StorageLayer.RESOURCE_KEY)
                        .size(), 2);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testDifferentWaysToGetConfigBasedOnConnectionURIAndTenantId()
            throws InterruptedException, IOException, InvalidConfigException, DbInitException,
            TenantOrAppNotFoundException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        TenantConfig[] tenants = new TenantConfig[4];

        {
            JsonObject tenantConfig = new JsonObject();
            StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess())
                    .modifyConfigToAddANewUserPoolForTesting(tenantConfig, 2);
            tenantConfig.add("postgresql_connection_pool_size", new JsonPrimitive(12));
            tenants[0] = new TenantConfig(new TenantIdentifier("c1", null, null), new EmailPasswordConfig(false),
                    new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                    new PasswordlessConfig(false),
                    tenantConfig);
        }

        {
            JsonObject tenantConfig = new JsonObject();
            StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess())
                    .modifyConfigToAddANewUserPoolForTesting(tenantConfig, 2);
            tenantConfig.add("postgresql_connection_pool_size", new JsonPrimitive(13));
            tenants[1] = new TenantConfig(new TenantIdentifier("c1", null, "t1"), new EmailPasswordConfig(false),
                    new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                    new PasswordlessConfig(false),
                    tenantConfig);
        }

        {
            JsonObject tenantConfig = new JsonObject();
            tenantConfig.add("postgresql_connection_pool_size", new JsonPrimitive(14));
            tenants[2] = new TenantConfig(new TenantIdentifier(null, null, "t2"), new EmailPasswordConfig(false),
                    new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                    new PasswordlessConfig(false),
                    tenantConfig);
        }

        {
            JsonObject tenantConfig = new JsonObject();
            tenantConfig.add("postgresql_connection_pool_size", new JsonPrimitive(15));
            tenants[3] = new TenantConfig(new TenantIdentifier(null, null, "t1"), new EmailPasswordConfig(false),
                    new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                    new PasswordlessConfig(false),
                    tenantConfig);
        }

        Config.loadAllTenantConfig(process.getProcess(), tenants);

        StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()))
                .getConnectionPoolSize(), 10);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("c1", null, null), process.getProcess()))
                .getConnectionPoolSize(), 12);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("c1", null, "t1"), process.getProcess()))
                .getConnectionPoolSize(), 13);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier(null, null, "t1"), process.getProcess()))
                .getConnectionPoolSize(), 15);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("c2", null, null), process.getProcess()))
                .getConnectionPoolSize(), 10);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("c1", null, "t1"), process.getProcess()))
                .getConnectionPoolSize(), 13);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("c3", null, "t2"), process.getProcess()))
                .getConnectionPoolSize(), 14);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier(null, null, "t2"), process.getProcess()))
                .getConnectionPoolSize(), 14);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void differentUserPoolCreatedBasedOnConnectionUri()
            throws InterruptedException, IOException, InvalidConfigException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("postgresql_connection_uri",
                new JsonPrimitive("postgresql://root:root@localhost:5432/random"));

        try {
            TenantConfig[] tenants = new TenantConfig[]{
                    new TenantConfig(new TenantIdentifier("abc", null, null), new EmailPasswordConfig(false),
                            new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                            new PasswordlessConfig(false),
                            tenantConfig)};
            Config.loadAllTenantConfig(process.getProcess(), tenants);

            StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);
            fail();
        } catch (DbInitException e) {
            assertEquals(e.getMessage(), "com.zaxxer.hikari.pool.HikariPool$PoolInitializationException: Failed to " +
                    "initialize pool: FATAL: database \"random\" does not exist");
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void differentUserPoolCreatedBasedOnSchemaInConnectionUri()
            throws InterruptedException, IOException, InvalidConfigException, DbInitException,
            TenantOrAppNotFoundException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("postgresql_connection_uri",
                new JsonPrimitive("postgresql://root:root@localhost:5432/supertokens?currentSchema=random"));

        TenantConfig[] tenants = new TenantConfig[]{
                new TenantConfig(new TenantIdentifier("abc", null, null), new EmailPasswordConfig(false),
                        new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                        new PasswordlessConfig(false),
                        tenantConfig)};
        Config.loadAllTenantConfig(process.getProcess(), tenants);

        StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier("abc", null, null), process.getProcess()))
                .getTableSchema(), "random");

        Assert.assertEquals(io.supertokens.storage.postgresql.config.Config.getConfig(
                        (Start) StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()))
                .getTableSchema(), "public");

        assertNotSame(StorageLayer.getStorage(new TenantIdentifier("abc", null, null), process.getProcess()),
                StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()));

        Assert.assertEquals(
                process.getProcess().getResourceDistributor().getAllResourcesWithResourceKey(StorageLayer.RESOURCE_KEY)
                        .size(), 2);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void multipleTenantsSameUserPoolAndConnectionPoolShouldWork()
            throws InterruptedException, IOException, InvalidConfigException, DbInitException,
            TenantOrAppNotFoundException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();

        TenantConfig[] tenants = new TenantConfig[]{
                new TenantConfig(new TenantIdentifier(null, "abc", null), new EmailPasswordConfig(false),
                        new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                        new PasswordlessConfig(false),
                        tenantConfig)};
        Config.loadAllTenantConfig(process.getProcess(), tenants);

        StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);

        assertSame(StorageLayer.getStorage(new TenantIdentifier(null, "abc", null), process.getProcess()),
                StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()));

        Assert.assertEquals(
                process.getProcess().getResourceDistributor().getAllResourcesWithResourceKey(StorageLayer.RESOURCE_KEY)
                        .size(), 2);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void multipleTenantsSameUserPoolAndDifferentConnectionPoolShouldWork()
            throws InterruptedException, IOException, InvalidConfigException, DbInitException,
            TenantOrAppNotFoundException {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject tenantConfig = new JsonObject();
        tenantConfig.add("postgresql_connection_pool_size", new JsonPrimitive(20));

        TenantConfig[] tenants = new TenantConfig[]{
                new TenantConfig(new TenantIdentifier(null, "abc", null), new EmailPasswordConfig(false),
                        new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]),
                        new PasswordlessConfig(false),
                        tenantConfig)};
        Config.loadAllTenantConfig(process.getProcess(), tenants);

        StorageLayer.loadAllTenantStorage(process.getProcess(), tenants);

        assertNotSame(StorageLayer.getStorage(new TenantIdentifier(null, "abc", null), process.getProcess()),
                StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess()));

        Assert.assertEquals(
                process.getProcess().getResourceDistributor().getAllResourcesWithResourceKey(StorageLayer.RESOURCE_KEY)
                        .size(), 2);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testOverwriteTenantForRaceConditions()
            throws StorageQueryException, InterruptedException {
        // TODO: this test is failing, need to investigate further
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.LOADING_ALL_TENANT_STORAGE));
        MultitenancyStorage mtStorage = StorageLayer.getMultitenancyStorage(process.getProcess());

        JsonObject authParams = new JsonObject();
        authParams.add("auth-param", new JsonPrimitive("auth-val"));

        JsonObject tokenParams = new JsonObject();
        tokenParams.add("token-param", new JsonPrimitive("token-val"));

        JsonObject userInfoParams = new JsonObject();
        userInfoParams.add("user-param", new JsonPrimitive("user-val"));

        JsonObject userInfoHeaders = new JsonObject();
        userInfoHeaders.add("user-header", new JsonPrimitive("user-header-val"));

        ExecutorService es = Executors.newFixedThreadPool(100);

        AtomicBoolean pass = new AtomicBoolean(true);

        for (int i = 0; i < 300; i++) {
            es.execute(() -> {
                while (true) {
                    try {
                        mtStorage.overwriteTenantConfig(new TenantConfig(
                                new TenantIdentifier(null, null, null),
                                new EmailPasswordConfig(true),
                                new ThirdPartyConfig(true, new ThirdPartyConfig.Provider[]{
                                        new ThirdPartyConfig.Provider(
                                                "google",
                                                "Google",
                                                new ThirdPartyConfig.ProviderClient[]{
                                                        new ThirdPartyConfig.ProviderClient(
                                                                "web",
                                                                "client-id",
                                                                "client-secret",
                                                                new String[]{"scope-1", "scope-2"},
                                                                true,
                                                                new JsonObject()
                                                        )
                                                },
                                                "https://auth.example.com/auth",
                                                authParams,
                                                "https://auth.example.com/token",
                                                tokenParams,
                                                "https://auth.example.com/user",
                                                userInfoParams,
                                                userInfoHeaders,
                                                "https://auth.example.com/jwks",
                                                "https://auth.example.com",
                                                true,
                                                new ThirdPartyConfig.UserInfoMap(
                                                        new ThirdPartyConfig.UserInfoMapKeyValue("id1", "email1",
                                                                "email_verified1"),
                                                        new ThirdPartyConfig.UserInfoMapKeyValue("id2", "email2",
                                                                "email_verified2")
                                                )
                                        )
                                }),
                                new PasswordlessConfig(true),
                                new JsonObject()
                        ));
                        break;
                    } catch (Exception e) {
                        if (e instanceof StorageQueryException) {
                            if (e.getMessage().contains("request timed out")) {
                                continue; // retry
                            }
                        }
                        System.out.println(e.getMessage());
                        pass.set(false);
                        break;
                    }
                }
            });
        }

        es.shutdown();
        es.awaitTermination(2, TimeUnit.MINUTES);

        assert (pass.get());

        TenantConfig[] tenantConfigs = mtStorage.getAllTenants();

        assertEquals(1, tenantConfigs.length);
        TenantConfig baseTenantConfig = tenantConfigs[0];

        assertEquals(new TenantIdentifier(null, null, null), baseTenantConfig.tenantIdentifier);
        assertTrue(baseTenantConfig.emailPasswordConfig.enabled);
        assertTrue(baseTenantConfig.passwordlessConfig.enabled);
        assertTrue(baseTenantConfig.thirdPartyConfig.enabled);
        assertNotNull(baseTenantConfig.thirdPartyConfig.providers);
        assertEquals(1, baseTenantConfig.thirdPartyConfig.providers.length);

        ThirdPartyConfig.Provider provider = baseTenantConfig.thirdPartyConfig.providers[0];

        assertEquals("google", provider.thirdPartyId);
        assertEquals("Google", provider.name);
        assertEquals("https://auth.example.com/auth", provider.authorizationEndpoint);
        assertEquals(authParams, provider.authorizationEndpointQueryParams);
        assertEquals("https://auth.example.com/token", provider.tokenEndpoint);
        assertEquals(tokenParams, provider.tokenEndpointBodyParams);
        assertEquals("https://auth.example.com/user", provider.userInfoEndpoint);
        assertEquals(userInfoParams, provider.userInfoEndpointQueryParams);
        assertEquals(userInfoHeaders, provider.userInfoEndpointHeaders);
        assertEquals("https://auth.example.com/jwks", provider.jwksURI);
        assertEquals("https://auth.example.com", provider.oidcDiscoveryEndpoint);
        assertTrue(provider.requireEmail);
        assertEquals("id1", provider.userInfoMap.fromIdTokenPayload.userId);
        assertEquals("email1", provider.userInfoMap.fromIdTokenPayload.email);
        assertEquals("email_verified1", provider.userInfoMap.fromIdTokenPayload.emailVerified);
        assertEquals("id2", provider.userInfoMap.fromUserInfoAPI.userId);
        assertEquals("email2", provider.userInfoMap.fromUserInfoAPI.email);
        assertEquals("email_verified2", provider.userInfoMap.fromUserInfoAPI.emailVerified);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

}
