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
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HasUserIdMappingsTest {

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
    public void reflectsMappingsOfTheGivenAppOnly() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        AppIdentifier publicApp = new AppIdentifier(null, null);
        AppIdentifier otherApp = new AppIdentifier(null, "a1");
        TenantIdentifier otherAppTenant = otherApp.getAsPublicTenantIdentifier();
        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantIdentifier(null, null, null),
                new TenantConfig(otherAppTenant, new EmailPasswordConfig(true), new ThirdPartyConfig(true, null),
                        new PasswordlessConfig(true), null, null, new JsonObject()));

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());

        AuthRecipeUserInfo publicUser = EmailPassword.signUp(process.getProcess(), "public@example.com",
                "password123");
        AuthRecipeUserInfo otherUser = EmailPassword.signUp(otherAppTenant,
                StorageLayer.getStorage(otherAppTenant, process.getProcess()), process.getProcess(),
                "other@example.com", "password123");

        // no mappings anywhere
        assertFalse(storage.hasUserIdMappings(publicApp));
        assertFalse(storage.hasUserIdMappings(otherApp));

        // a mapping in another app does not count for the public app
        storage.createUserIdMapping(otherApp, otherUser.getSupertokensUserId(), "external-other", null);
        assertTrue(storage.hasUserIdMappings(otherApp));
        assertFalse(storage.hasUserIdMappings(publicApp));

        storage.createUserIdMapping(publicApp, publicUser.getSupertokensUserId(), "external-public", null);
        assertTrue(storage.hasUserIdMappings(publicApp));

        // removing the only mapping of an app flips it back, without affecting the other app
        assertTrue(storage.deleteUserIdMapping(otherApp, otherUser.getSupertokensUserId(), true));
        assertFalse(storage.hasUserIdMappings(otherApp));
        assertTrue(storage.hasUserIdMappings(publicApp));

        assertTrue(storage.deleteUserIdMapping(publicApp, publicUser.getSupertokensUserId(), true));
        assertFalse(storage.hasUserIdMappings(publicApp));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
