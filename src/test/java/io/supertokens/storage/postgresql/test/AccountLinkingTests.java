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

package io.supertokens.storage.postgresql.test;

import com.google.gson.JsonObject;
import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.storage.postgresql.test.httpRequest.HttpRequestForTesting;
import io.supertokens.storage.postgresql.test.httpRequest.HttpResponseException;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.webserver.WebserverAPI;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class AccountLinkingTests {

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
    public void canLinkFailsIfTryingToLinkUsersAcrossDifferentStorageLayers() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            return;
        }

        if (StorageLayer.isInMemDb(process.getProcess())) {
            return;
        }


        JsonObject coreConfig = new JsonObject();
        StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess())
                .modifyConfigToAddANewUserPoolForTesting(coreConfig, 2);

        TenantIdentifier tenantIdentifier = new TenantIdentifier(null, null, "t1");
        Multitenancy.addNewOrUpdateAppOrTenant(
                process.getProcess(),
                new TenantIdentifier(null, null, null),
                new TenantConfig(
                        tenantIdentifier,
                        new EmailPasswordConfig(true),
                        new ThirdPartyConfig(true, null),
                        new PasswordlessConfig(true),
                        null, null,
                        coreConfig
                )
        );

        AuthRecipeUserInfo user1 = EmailPassword.signUp(process.getProcess(), "test@example.com", "abcd1234");

        AuthRecipe.createPrimaryUser(process.main, user1.getSupertokensUserId());

        AuthRecipeUserInfo user2 = EmailPassword.signUp(
                tenantIdentifier.withStorage(StorageLayer.getStorage(tenantIdentifier, process.main)),
                process.getProcess(), "test2@example.com", "abcd1234");

        try {
            Map<String, String> params = new HashMap<>();
            params.put("recipeUserId", user2.getSupertokensUserId());
            params.put("primaryUserId", user1.getSupertokensUserId());

            HttpRequestForTesting.sendGETRequest(process.getProcess(), "",
                    "http://localhost:3567/recipe/accountlinking/user/link/check", params, 1000, 1000, null,
                    WebserverAPI.getLatestCDIVersion().get(), "");
            assert (false);
        } catch (HttpResponseException e) {
            assert (e.statusCode == 400);
            assert (e.getMessage()
                    .equals("Http error. Status Code: 400. Message: Cannot link users that are parts of different " +
                            "databases. Different pool IDs: |localhost|5432|supertokens|public AND " +
                            "|localhost|5432|st2|public"));
        }


        coreConfig = new JsonObject();
        coreConfig.addProperty("postgresql_connection_pool_size", 11);

        tenantIdentifier = new TenantIdentifier(null, null, "t2");
        Multitenancy.addNewOrUpdateAppOrTenant(
                process.getProcess(),
                new TenantIdentifier(null, null, null),
                new TenantConfig(
                        tenantIdentifier,
                        new EmailPasswordConfig(true),
                        new ThirdPartyConfig(true, null),
                        new PasswordlessConfig(true),
                        null, null,
                        coreConfig
                )
        );

        AuthRecipeUserInfo user3 = EmailPassword.signUp(
                tenantIdentifier.withStorage(StorageLayer.getStorage(tenantIdentifier, process.main)),
                process.getProcess(), "test2@example.com", "abcd1234");

        Map<String, String> params = new HashMap<>();
        params.put("recipeUserId", user3.getSupertokensUserId());
        params.put("primaryUserId", user1.getSupertokensUserId());

        JsonObject response = HttpRequestForTesting.sendGETRequest(process.getProcess(), "",
                "http://localhost:3567/recipe/accountlinking/user/link/check", params, 1000, 1000, null,
                WebserverAPI.getLatestCDIVersion().get(), "");
        assert (response.get("status").getAsString().equals("OK"));
        assert (!response.get("accountsAlreadyLinked").getAsBoolean());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
