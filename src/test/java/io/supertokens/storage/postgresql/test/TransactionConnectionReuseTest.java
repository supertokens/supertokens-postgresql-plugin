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

import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.webauthn.WebAuthNStoredCredential;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.webauthn.WebAuthN;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Storage methods that run inside a caller's transaction must read on that transaction's connection. Borrowing a
 * second connection from the same pool while the first is held is the hold-and-wait pool-exhaustion pattern. These
 * paths are postgres-only (the in-memory storage has no equivalent branch), so core's armed guard cannot catch them;
 * the nested-acquisition guard here throws by default and fails the test at the nested borrow.
 */
public class TransactionConnectionReuseTest {

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
    public void webauthnPrimaryUserLookupByCredentialStaysOnTheTransactionConnection() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        assertTrue(ConnectionPool.isThrowOnNestedAcquisition());
        Start start = (Start) StorageLayer.getBaseStorage(process.getProcess());

        String userId = io.supertokens.utils.Utils.getUUID();
        WebAuthN.saveUser(start, TenantIdentifier.BASE_TENANT, "passkey@example.com", userId, "example.com");
        WebAuthNStoredCredential credential = new WebAuthNStoredCredential();
        credential.id = "credential-" + userId;
        credential.appId = "public";
        credential.rpId = "example.com";
        credential.userId = userId;
        credential.counter = 0;
        credential.publicKey = new byte[]{1, 2, 3};
        credential.transports = "[]";
        credential.createdAt = System.currentTimeMillis();
        credential.updatedAt = credential.createdAt;
        start.saveCredentials(TenantIdentifier.BASE_TENANT, credential);

        // what WebAuthN sign-in does inside its transaction
        AuthRecipeUserInfo user = start.startTransaction(con ->
                start.getPrimaryUserByWebauthNCredentialId_Transaction(TenantIdentifier.BASE_TENANT, con,
                        credential.id));

        assertNotNull(user);
        assertEquals(userId, user.getSupertokensUserId());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void legacyModeEmailChangeOfAPrimaryUserStaysOnTheTransactionConnection() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.ACCOUNT_LINKING});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        assertTrue(ConnectionPool.isThrowOnNestedAcquisition());
        Main main = process.getProcess();
        Start start = (Start) StorageLayer.getBaseStorage(main);
        // LEGACY does not write the account-info tables, so the email change checks for conflicting primary users
        // with its own lookup in each of the user's tenants
        Config.getConfig(start).setMigrationModeForTesting(MigrationMode.LEGACY);

        AuthRecipeUserInfo signedUp = EmailPassword.signUp(main, "before@example.com", "password1234");
        AuthRecipe.createPrimaryUser(main, signedUp.getSupertokensUserId());
        AppIdentifier app = new AppIdentifier(null, null);

        start.startTransaction(con -> {
            try {
                start.updateUsersEmail_Transaction(app, con, signedUp.getSupertokensUserId(), "after@example.com");
            } catch (Exception e) {
                throw new StorageTransactionLogicException(e);
            }
            start.commitTransaction(con);
            return null;
        });

        AuthRecipeUserInfo updated = AuthRecipe.getUserById(main, signedUp.getSupertokensUserId());
        assertEquals("after@example.com", updated.loginMethods[0].email);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
