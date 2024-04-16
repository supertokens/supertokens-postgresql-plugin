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

package io.supertokens.storage.postgresql.test.multitenancy;

import com.google.gson.JsonObject;
import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.featureflag.exceptions.FeatureNotEnabledException;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.multitenancy.MultitenancyHelper;
import io.supertokens.multitenancy.exception.BadPermissionException;
import io.supertokens.multitenancy.exception.CannotModifyBaseConfigException;
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.queries.MultitenancyQueries;
import io.supertokens.storage.postgresql.test.TestingProcessManager;
import io.supertokens.storage.postgresql.test.Utils;
import io.supertokens.storage.postgresql.test.httpRequest.HttpRequestForTesting;
import io.supertokens.storage.postgresql.test.httpRequest.HttpResponseException;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.InvalidProviderConfigException;
import io.supertokens.utils.SemVer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;

public class TestForNoCrashDuringStartup {
    TestingProcessManager.TestingProcess process;

    @AfterClass
    public static void afterTesting() {
        Utils.afterTesting();
    }

    @After
    public void afterEach() throws InterruptedException {
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Before
    public void beforeEach() throws InterruptedException, InvalidProviderConfigException,
            StorageQueryException, FeatureNotEnabledException, TenantOrAppNotFoundException, IOException,
            InvalidConfigException, CannotModifyBaseConfigException, BadPermissionException {
        Utils.reset();

        String[] args = {"../"};

        this.process = TestingProcessManager.start(args);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
    }

    @Test
    public void testFailureInCUDCreation() throws Exception {
        JsonObject coreConfig = new JsonObject();
        StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess())
                .modifyConfigToAddANewUserPoolForTesting(coreConfig, 1);

        TenantIdentifier tenantIdentifier = new TenantIdentifier("127.0.0.1", null, null);

        MultitenancyQueries.simulateErrorInAddingTenantIdInTargetStorage = true;
        try {
            Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                    tenantIdentifier,
                    new EmailPasswordConfig(true),
                    new ThirdPartyConfig(true, null),
                    new PasswordlessConfig(true),
                    null, null,
                    coreConfig
            ), false);
            fail();
        } catch (StorageQueryException e) {
            // ignore
            assertTrue(e.getMessage().contains("Simulated error"));
        }

        TenantConfig[] allTenants = MultitenancyHelper.getInstance(process.getProcess()).getAllTenants();
        assertEquals(2, allTenants.length); // should have the new CUD

        try {
            tpSignInUpAndGetResponse(new TenantIdentifier("127.0.0.1", null, null), "google", "googleid1", "test@example.com", process.getProcess(), SemVer.v5_0);
            fail();
        } catch (HttpResponseException e) {
            // ignore
            assertTrue(e.getMessage().contains("Internal Error")); // retried creating tenant entry
        }

        MultitenancyQueries.simulateErrorInAddingTenantIdInTargetStorage = false;

        // this should succeed now
        tpSignInUpAndGetResponse(new TenantIdentifier("127.0.0.1", null, null), "google", "googleid1", "test@example.com", process.getProcess(), SemVer.v5_0);
    }

    @Test
    public void testThatTenantComesToLifeOnceTheTargetDbIsUp() throws Exception {
        Start start = ((Start) StorageLayer.getBaseStorage(process.getProcess()));
        try {
            update(start, "DROP DATABASE st5000;", pst -> {});
        } catch (Exception e) {
            // ignore
        }

        JsonObject coreConfig = new JsonObject();
        StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess())
                .modifyConfigToAddANewUserPoolForTesting(coreConfig, 5000);

        TenantIdentifier tenantIdentifier = new TenantIdentifier("127.0.0.1", null, null);

        try {
            Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                    tenantIdentifier,
                    new EmailPasswordConfig(true),
                    new ThirdPartyConfig(true, null),
                    new PasswordlessConfig(true),
                    null, null,
                    coreConfig
            ), false);
            fail();
        } catch (StorageQueryException e) {
            // ignore
            assertEquals("java.sql.SQLException: com.zaxxer.hikari.pool.HikariPool$PoolInitializationException: Failed to initialize pool: FATAL: database \"st5000\" does not exist", e.getMessage());
        }

        TenantConfig[] allTenants = MultitenancyHelper.getInstance(process.getProcess()).getAllTenants();
        assertEquals(2, allTenants.length); // should have the new CUD

        try {
            tpSignInUpAndGetResponse(new TenantIdentifier("127.0.0.1", null, null), "google", "googleid1", "test@example.com", process.getProcess(), SemVer.v5_0);
            fail();
        } catch (HttpResponseException e) {
            // ignore
            assertTrue(e.getMessage().contains("Internal Error")); // db is still down
        }

        update(start, "CREATE DATABASE st5000;", pst -> {});

        // this should succeed now
        tpSignInUpAndGetResponse(new TenantIdentifier("127.0.0.1", null, null), "google", "googleid1", "test@example.com", process.getProcess(), SemVer.v5_0);
    }

    @Test
    public void testThatCoreDoesNotStartWithThereIsAnErrorDuringTenantCreation() throws Exception {
        JsonObject coreConfig = new JsonObject();
        StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess())
                .modifyConfigToAddANewUserPoolForTesting(coreConfig, 1);

        TenantIdentifier tenantIdentifier = new TenantIdentifier("127.0.0.1", null, null);

        MultitenancyQueries.simulateErrorInAddingTenantIdInTargetStorage = true;
        try {
            Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                    tenantIdentifier,
                    new EmailPasswordConfig(true),
                    new ThirdPartyConfig(true, null),
                    new PasswordlessConfig(true),
                    null, null,
                    coreConfig
            ), false);
            fail();
        } catch (StorageQueryException e) {
            // ignore
            assertTrue(e.getMessage().contains("Simulated error"));
        }

        TenantConfig[] allTenants = MultitenancyHelper.getInstance(process.getProcess()).getAllTenants();
        assertEquals(2, allTenants.length); // should have the new CUD

        try {
            tpSignInUpAndGetResponse(new TenantIdentifier("127.0.0.1", null, null), "google", "googleid1", "test@example.com", process.getProcess(), SemVer.v5_0);
            fail();
        } catch (HttpResponseException e) {
            // ignore
            assertTrue(e.getMessage().contains("Internal Error")); // retried creating tenant entry
        }

        MultitenancyQueries.simulateErrorInAddingTenantIdInTargetStorage = false;

        process.kill(false);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        String[] args = {"../"};

        this.process = TestingProcessManager.start(args);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        // this should succeed now
        tpSignInUpAndGetResponse(new TenantIdentifier("127.0.0.1", null, null), "google", "googleid1", "test@example.com", process.getProcess(), SemVer.v5_0);
    }

    @Test
    public void testThatTenantComesToLifeOnceTheTargetDbIsUpAfterCoreRestart() throws Exception {
        Start start = ((Start) StorageLayer.getBaseStorage(process.getProcess()));
        try {
            update(start, "DROP DATABASE st5000;", pst -> {});
        } catch (Exception e) {
            // ignore
        }

        JsonObject coreConfig = new JsonObject();
        StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess())
                .modifyConfigToAddANewUserPoolForTesting(coreConfig, 5000);

        TenantIdentifier tenantIdentifier = new TenantIdentifier("127.0.0.1", null, null);

        try {
            Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                    tenantIdentifier,
                    new EmailPasswordConfig(true),
                    new ThirdPartyConfig(true, null),
                    new PasswordlessConfig(true),
                    null, null,
                    coreConfig
            ), false);
            fail();
        } catch (StorageQueryException e) {
            // ignore
            assertEquals("java.sql.SQLException: com.zaxxer.hikari.pool.HikariPool$PoolInitializationException: Failed to initialize pool: FATAL: database \"st5000\" does not exist", e.getMessage());
        }

        TenantConfig[] allTenants = MultitenancyHelper.getInstance(process.getProcess()).getAllTenants();
        assertEquals(2, allTenants.length); // should have the new CUD

        try {
            tpSignInUpAndGetResponse(new TenantIdentifier("127.0.0.1", null, null), "google", "googleid1", "test@example.com", process.getProcess(), SemVer.v5_0);
            fail();
        } catch (HttpResponseException e) {
            // ignore
            assertTrue(e.getMessage().contains("Internal Error")); // db is still down
        }

        process.kill(false);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        String[] args = {"../"};
        this.process = TestingProcessManager.start(args);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        // the process should start successfully even though the db is down

        start = ((Start) StorageLayer.getBaseStorage(process.getProcess()));
        update(start, "CREATE DATABASE st5000;", pst -> {});

        // this should succeed now
        tpSignInUpAndGetResponse(new TenantIdentifier("127.0.0.1", null, null), "google", "googleid1", "test@example.com", process.getProcess(), SemVer.v5_0);
    }

    public static JsonObject tpSignInUpAndGetResponse(TenantIdentifier tenantIdentifier, String thirdPartyId, String thirdPartyUserId, String email, Main main, SemVer version)
            throws HttpResponseException, IOException {
        JsonObject emailObject = new JsonObject();
        emailObject.addProperty("id", email);
        emailObject.addProperty("isVerified", false);

        JsonObject signUpRequestBody = new JsonObject();
        signUpRequestBody.addProperty("thirdPartyId", thirdPartyId);
        signUpRequestBody.addProperty("thirdPartyUserId", thirdPartyUserId);
        signUpRequestBody.add("email", emailObject);

        JsonObject response = HttpRequestForTesting.sendJsonPOSTRequest(main, "",
                HttpRequestForTesting.getMultitenantUrl(tenantIdentifier, "/recipe/signinup"), signUpRequestBody,
                1000, 1000, null,
                version.get(), "thirdparty");
        return response;
    }
}
