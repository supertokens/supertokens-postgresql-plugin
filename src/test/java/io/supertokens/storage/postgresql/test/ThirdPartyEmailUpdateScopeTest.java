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
import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.emailpassword.exceptions.UnknownUserIdException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.EmailPasswordConfig;
import io.supertokens.pluginInterface.multitenancy.PasswordlessConfig;
import io.supertokens.pluginInterface.multitenancy.TenantConfig;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.ThirdPartyConfig;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.*;

/**
 * The user-scoped {@code updateUserEmail_Transaction(appIdentifier, con, thirdPartyId, userId, thirdPartyUserId,
 * newEmail)} for a third party user must only change the row of the recipe user it is called for. The same
 * (third_party_id, third_party_user_id) may belong to a different recipe user in another tenant of the app, and that
 * user must keep its email.
 */
public class ThirdPartyEmailUpdateScopeTest {

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

    private static final AppIdentifier APP = new AppIdentifier(null, null);
    private static final TenantIdentifier T1 = new TenantIdentifier(null, null, "t1");
    private static final TenantIdentifier T2 = new TenantIdentifier(null, null, "t2");

    private static void createTenant(Main main, TenantIdentifier tenant) throws Exception {
        Multitenancy.addNewOrUpdateAppOrTenant(main, new TenantConfig(tenant, new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null), new PasswordlessConfig(true), null, null, new JsonObject()), false);
    }

    private static LoginMethod loginMethod(Storage storage, String recipeUserId) throws Exception {
        AuthRecipeUserInfo user = AuthRecipe.getUserById(APP, storage, recipeUserId);
        assertNotNull(user);
        for (LoginMethod lM : user.loginMethods) {
            if (lM.getSupertokensUserId().equals(recipeUserId)) {
                return lM;
            }
        }
        fail("login method not found");
        return null;
    }

    private static void updateEmail(Start storage, String userId, String thirdPartyUserId, String email)
            throws Exception {
        storage.startTransaction(con -> {
            try {
                storage.updateUserEmail_Transaction(APP, con, "oidc", userId, thirdPartyUserId, email);
            } catch (Exception e) {
                throw new StorageTransactionLogicException(e);
            }
            storage.commitTransaction(con);
            return null;
        });
    }

    @Test
    public void testIssue106_emailUpdateOnlyChangesTheGivenRecipeUser() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES,
                        new EE_FEATURES[]{EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            // create the tenants before taking the storage handle: adding a tenant reloads the storage layer
            createTenant(main, T1);
            createTenant(main, T2);
            Start storage = (Start) StorageLayer.getStorage(main);

            // the same provider identity signs in to two tenants: one recipe user per tenant
            ThirdParty.SignInUpResponse r1 = ThirdParty.signInUp(T1, storage, main, "oidc", "sub", "user1@example.com",
                    true);
            assertTrue(r1.createdNewUser);
            String user1Id = r1.user.getSupertokensUserId();
            ThirdParty.SignInUpResponse r2 = ThirdParty.signInUp(T2, storage, main, "oidc", "sub", "user2@example.com",
                    true);
            assertTrue(r2.createdNewUser);
            String user2Id = r2.user.getSupertokensUserId();
            assertNotEquals(user1Id, user2Id);

            // an email change for t2's recipe user only reaches that user
            updateEmail(storage, user2Id, "sub", "user2-new@example.com");
            assertEquals("user2-new@example.com", loginMethod(storage, user2Id).email);
            assertEquals("user1@example.com", loginMethod(storage, user1Id).email);

            // the storage refuses an update for a recipe user that does not hold this third party identity, and
            // changes nothing
            try {
                updateEmail(storage, user2Id, "sub-not-this-user", "other@example.com");
                fail();
            } catch (StorageTransactionLogicException e) {
                assertTrue(String.valueOf(e.actualException), e.actualException instanceof UnknownUserIdException);
            }
            assertEquals("user2-new@example.com", loginMethod(storage, user2Id).email);
            assertEquals("user1@example.com", loginMethod(storage, user1Id).email);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
