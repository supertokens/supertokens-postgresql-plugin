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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.test.httpRequest.HttpRequestForTesting;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;
import io.supertokens.webserver.WebserverAPI;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Reproduces the exact HTTP call sequence the node SDK performs in the failing
 * backend-sdk-testing accountlinking tests against core 12.0:
 *
 *   1. EP signup (email X)
 *   2. POST /recipe/accountlinking/user/primary  (EP user becomes primary)
 *   3. TP signinup (same email X, verified)
 *   4. GET /users/by-accountinfo?email=X&doUnionOfAccountInfo=true
 *      -> the SDK looks for a primary user here to decide link-vs-create-primary
 *   5. (what the SDK would do if step 4 finds nothing): POST user/primary for TP
 *      -> must be rejected with ACCOUNT_INFO_ALREADY_ASSOCIATED_...
 *   6. POST /recipe/accountlinking/user/link  (TP -> EP primary) -> must be OK
 *   7. POST /recipe/accountlinking/user/link again -> OK + accountsAlreadyLinked
 */
public class AccountLinkingSdkFlowReproTest {

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

    private TestingProcessManager.TestingProcess startProcess() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES,
                        new EE_FEATURES[]{EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        return process;
    }

    private JsonObject makePrimary(Main main, String recipeUserId) throws Exception {
        JsonObject params = new JsonObject();
        params.addProperty("recipeUserId", recipeUserId);
        return HttpRequestForTesting.sendJsonPOSTRequest(main, "",
                "http://localhost:3567/recipe/accountlinking/user/primary", params, 1000, 1000, null,
                WebserverAPI.getLatestCDIVersion().get(), "");
    }

    private JsonObject link(Main main, String recipeUserId, String primaryUserId) throws Exception {
        JsonObject params = new JsonObject();
        params.addProperty("recipeUserId", recipeUserId);
        params.addProperty("primaryUserId", primaryUserId);
        return HttpRequestForTesting.sendJsonPOSTRequest(main, "",
                "http://localhost:3567/recipe/accountlinking/user/link", params, 1000, 1000, null,
                WebserverAPI.getLatestCDIVersion().get(), "");
    }

    private JsonObject listByAccountInfo(Main main, String email) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("email", email);
        params.put("doUnionOfAccountInfo", "true");
        return HttpRequestForTesting.sendGETRequest(main, "",
                "http://localhost:3567/users/by-accountinfo", params, 1000, 1000, null,
                WebserverAPI.getLatestCDIVersion().get(), "");
    }

    private void runScenario(MigrationMode mode) throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            Storage storage = StorageLayer.getStorage(main);
            if (storage.getType() != STORAGE_TYPE.SQL || !(storage instanceof Start)) {
                return;
            }
            Config.getConfig((Start) storage).setMigrationModeForTesting(mode);

            String email = "sdkflow-" + mode.name().toLowerCase() + "@example.com";

            // 1. EP signup
            AuthRecipeUserInfo epUser = EmailPassword.signUp(main, email, "password123");
            String epId = epUser.getSupertokensUserId();

            // 2. make EP primary via HTTP (what AccountLinking.createPrimaryUser does)
            JsonObject makePrimaryResp = makePrimary(main, epId);
            assertEquals("[" + mode + "] make EP primary", "OK",
                    makePrimaryResp.get("status").getAsString());

            // 3. TP signinup, same email
            ThirdParty.SignInUpResponse tpResp = ThirdParty.signInUp(main, "google", "g-" + mode.name(), email);
            String tpId = tpResp.user.getSupertokensUserId();

            // 4. SDK: list users by account info, look for a primary user
            JsonObject byAccInfo = listByAccountInfo(main, email);
            assertEquals("[" + mode + "] by-accountinfo status", "OK",
                    byAccInfo.get("status").getAsString());
            JsonArray users = byAccInfo.get("users").getAsJsonArray();
            boolean foundEpAsPrimary = false;
            for (int i = 0; i < users.size(); i++) {
                JsonObject u = users.get(i).getAsJsonObject();
                if (u.get("id").getAsString().equals(epId)
                        && u.get("isPrimaryUser").getAsBoolean()) {
                    foundEpAsPrimary = true;
                }
            }
            assertTrue("[" + mode + "] by-accountinfo must return the EP user as a primary user. Got: "
                    + byAccInfo, foundEpAsPrimary);

            // 5. guard: making TP primary must be rejected (conflicting email with EP primary)
            JsonObject tpPrimaryResp = makePrimary(main, tpId);
            assertEquals("[" + mode + "] TP make-primary must conflict. Got: " + tpPrimaryResp,
                    "ACCOUNT_INFO_ALREADY_ASSOCIATED_WITH_ANOTHER_PRIMARY_USER_ID_ERROR",
                    tpPrimaryResp.get("status").getAsString());

            // 6. link TP -> EP primary
            JsonObject linkResp = link(main, tpId, epId);
            assertEquals("[" + mode + "] link TP->EP must be OK. Got: " + linkResp,
                    "OK", linkResp.get("status").getAsString());
            assertFalse("[" + mode + "] first link must not report accountsAlreadyLinked",
                    linkResp.get("accountsAlreadyLinked").getAsBoolean());

            // 7. link again -> idempotent OK (this is what the failing tests hit after auto-linking)
            JsonObject linkAgainResp = link(main, tpId, epId);
            assertEquals("[" + mode + "] repeat link TP->EP must be OK (already linked). Got: " + linkAgainResp,
                    "OK", linkAgainResp.get("status").getAsString());
            assertTrue("[" + mode + "] repeat link must report accountsAlreadyLinked",
                    linkAgainResp.get("accountsAlreadyLinked").getAsBoolean());

            // 8. link(primaryUser, primaryUser) -> must also be idempotent OK.
            // This is the exact call the failing backend-sdk tests end up making: after the SDK
            // auto-links during sign up, the test passes user.loginMethods[0].recipeUserId which
            // is the primary user's own recipe user id.
            JsonObject selfLinkResp = link(main, epId, epId);
            assertEquals("[" + mode + "] link(primary, primary) must be OK (already linked). Got: " + selfLinkResp,
                    "OK", selfLinkResp.get("status").getAsString());
            assertTrue("[" + mode + "] link(primary, primary) must report accountsAlreadyLinked",
                    selfLinkResp.get("accountsAlreadyLinked").getAsBoolean());

            // 8b. link(primary, someone-linked-to-primary) -> also already linked
            JsonObject selfLinkResp2 = link(main, epId, tpId);
            assertEquals("[" + mode + "] link(primary, linked-user) must be OK (already linked). Got: " + selfLinkResp2,
                    "OK", selfLinkResp2.get("status").getAsString());
            assertTrue("[" + mode + "] link(primary, linked-user) must report accountsAlreadyLinked",
                    selfLinkResp2.get("accountsAlreadyLinked").getAsBoolean());
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void testSdkAutoLinkFlowLegacy() throws Exception {
        runScenario(MigrationMode.LEGACY);
    }

    @Test
    public void testSdkAutoLinkFlowDualWriteReadOld() throws Exception {
        runScenario(MigrationMode.DUAL_WRITE_READ_OLD);
    }

    @Test
    public void testSdkAutoLinkFlowDualWriteReadNew() throws Exception {
        runScenario(MigrationMode.DUAL_WRITE_READ_NEW);
    }

    @Test
    public void testSdkAutoLinkFlowMigrated() throws Exception {
        runScenario(MigrationMode.MIGRATED);
    }
}
