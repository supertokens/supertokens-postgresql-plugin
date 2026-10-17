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
import io.supertokens.emailverification.EmailVerification;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.authRecipe.exceptions.UnknownUserIdException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.EmailPasswordConfig;
import io.supertokens.pluginInterface.multitenancy.PasswordlessConfig;
import io.supertokens.pluginInterface.multitenancy.TenantConfig;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.ThirdPartyConfig;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.*;

/**
 * {@code updateUserEmail_Transaction} for a third party user must only change the row of the recipe user it is
 * called for. The same (third_party_id, third_party_user_id) may belong to a different recipe user in another tenant
 * of the app, and that user must keep its email in every migration mode (LEGACY reads email lookups from
 * thirdparty_users.email, so an un-scoped update would also change which user an email resolves to).
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

    @Test
    public void testIssue106_emailUpdateOnlyChangesTheGivenRecipeUserInEveryMode() throws Exception {
        for (MigrationMode mode : MigrationMode.values()) {
            TestingProcessManager.TestingProcess process = startProcess();
            try {
                Main main = process.getProcess();
                if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                    return;
                }

                // create the tenants before taking the storage handle: adding a tenant reloads the storage layer
                createTenant(main, T1);
                createTenant(main, T2);
                Start storage = (Start) StorageLayer.getStorage(main);
                Config.getConfig(storage).setMigrationModeForTesting(mode);

                String tpUserId = "sub-" + mode;
                String email1 = "user1-" + mode + "@example.com";
                String email2 = "user2-" + mode + "@example.com";
                String email2New = "user2-new-" + mode + "@example.com";

                // the same provider identity signs in to two tenants: one recipe user per tenant
                ThirdParty.SignInUpResponse r1 = ThirdParty.signInUp(T1, storage, main, "oidc", tpUserId, email1,
                        true);
                assertTrue(r1.createdNewUser);
                String user1Id = r1.user.getSupertokensUserId();
                AuthRecipe.createPrimaryUser(main, APP, storage, user1Id);

                ThirdParty.SignInUpResponse r2 = ThirdParty.signInUp(T2, storage, main, "oidc", tpUserId, email2,
                        true);
                assertTrue(r2.createdNewUser);
                String user2Id = r2.user.getSupertokensUserId();
                assertNotEquals(user1Id, user2Id);

                // an email change on t2 only reaches t2's recipe user
                ThirdParty.signInUp(T2, storage, main, "oidc", tpUserId, email2New, false);
                assertEquals(mode.toString(), email2New, loginMethod(storage, user2Id).email);

                LoginMethod user1LM = loginMethod(storage, user1Id);
                assertEquals(mode.toString(), email1, user1LM.email);
                assertTrue(mode.toString(), user1LM.verified);
                AuthRecipeUserInfo[] byOldEmail = AuthRecipe.getUsersByAccountInfo(T1, storage, false, email1,
                        null, null, null, null);
                assertEquals(mode.toString(), 1, byOldEmail.length);
                assertEquals(mode.toString(), user1Id, byOldEmail[0].getSupertokensUserId());
                assertEquals(mode.toString(), 0, AuthRecipe.getUsersByAccountInfo(T1, storage, false, email2New,
                        null, null, null, null).length);

                // the storage refuses an update for a recipe user that does not hold this third party identity,
                // and changes nothing
                try {
                    storage.startTransaction(con -> {
                        try {
                            storage.updateUserEmail_Transaction(APP, con, user2Id, "oidc", "sub-not-this-user-" + mode,
                                    "other-" + mode + "@example.com");
                        } catch (Exception e) {
                            throw new StorageTransactionLogicException(e);
                        }
                        storage.commitTransaction(con);
                        return null;
                    });
                    fail(mode.toString());
                } catch (StorageTransactionLogicException e) {
                    assertTrue(mode + ": " + e.actualException, e.actualException instanceof UnknownUserIdException);
                }
                assertEquals(mode.toString(), email2New, loginMethod(storage, user2Id).email);
                assertEquals(mode.toString(), email1, loginMethod(storage, user1Id).email);
                assertFalse(mode.toString(), EmailVerification.isEmailVerified(APP, storage, user1Id, email2New));
            } finally {
                process.kill();
                assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            }
        }
    }
}
