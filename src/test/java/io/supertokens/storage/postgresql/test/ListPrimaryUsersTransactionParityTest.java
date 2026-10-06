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
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * PLAN-018 Unit 3 (issue #413): {@code listPrimaryUsersByEmail_Transaction} /
 * {@code listPrimaryUsersByPhoneNumber_Transaction} must behave identically to the non-transaction
 * forms, differing only in that every read runs on the caller's connection instead of borrowing a
 * fresh pooled one. This test seeds users across recipes that share an email (and a phone), then
 * asserts the transaction reads return exactly what the non-transaction reads return, in both the
 * legacy (read-old) and new (read-new) migration-mode branches.
 */
public class ListPrimaryUsersTransactionParityTest {

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

    private static final String SHARED_EMAIL = "shared@parity.com";
    private static final String PHONE = "+15551234567";

    @Test
    public void testParityLegacyReadOld() throws Exception {
        runParityCheck(MigrationMode.LEGACY);
    }

    @Test
    public void testParityDualWriteReadNew() throws Exception {
        runParityCheck(MigrationMode.DUAL_WRITE_READ_NEW);
    }

    private void runParityCheck(MigrationMode mode) throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            return;
        }

        Main main = process.getProcess();
        Start start = (Start) StorageLayer.getStorage(main);
        // The read branch (new vs legacy) is picked from the migration mode. Writing to both table
        // sets under DUAL_WRITE keeps the new-table read path populated too.
        Config.getConfig(start).setMigrationModeForTesting(mode);

        seedUsers(main);

        TenantIdentifier tenant = TenantIdentifier.BASE_TENANT;

        // ----- email -----
        AuthRecipeUserInfo[] emailNonTx = start.listPrimaryUsersByEmail(tenant, SHARED_EMAIL);
        AuthRecipeUserInfo[] emailTx = start.startTransaction(
                con -> start.listPrimaryUsersByEmail_Transaction(tenant, con, SHARED_EMAIL));

        assertEquals("email fan-out should return all three seeded users in mode " + mode, 3, emailNonTx.length);
        assertUserIdsEqual("email lookup parity (" + mode + ")", emailNonTx, emailTx);

        // ----- phone number -----
        AuthRecipeUserInfo[] phoneNonTx = start.listPrimaryUsersByPhoneNumber(tenant, PHONE);
        AuthRecipeUserInfo[] phoneTx = start.startTransaction(
                con -> start.listPrimaryUsersByPhoneNumber_Transaction(tenant, con, PHONE));

        assertEquals("phone lookup should find the passwordless user in mode " + mode, 1, phoneNonTx.length);
        assertUserIdsEqual("phone lookup parity (" + mode + ")", phoneNonTx, phoneTx);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // Three unlinked primary users that all carry SHARED_EMAIL, across three recipes, so the email
    // fan-out (emailpassword + thirdparty + passwordless) returns more than one user and the
    // de-duplication path is exercised. The passwordless user also gets PHONE. WebAuthn is not
    // seeded: that leg reuses the pre-existing getPrimaryUserIdForTenantUsingEmail_Transaction.
    private static void seedUsers(Main main) throws Exception {
        EmailPassword.signUp(main, SHARED_EMAIL, "password123");
        ThirdParty.signInUp(main, "google", "g-parity", SHARED_EMAIL);

        Passwordless.CreateCodeResponse code = Passwordless.createCode(main, SHARED_EMAIL, null, null, null);
        Passwordless.ConsumeCodeResponse plUser = Passwordless.consumeCode(
                main, code.deviceId, code.deviceIdHash, code.userInputCode, null);
        Passwordless.updateUser(main, plUser.user.getSupertokensUserId(),
                null, new Passwordless.FieldUpdate(PHONE));
    }

    @Test
    public void testTransactionReadsReuseCallerConnectionLegacyReadOld() throws Exception {
        runConnectionReuseCheck(MigrationMode.LEGACY);
    }

    @Test
    public void testTransactionReadsReuseCallerConnectionDualWriteReadNew() throws Exception {
        runConnectionReuseCheck(MigrationMode.DUAL_WRITE_READ_NEW);
    }

    // Parity alone cannot catch a nested borrow: a _Transaction read that calls a non-tx helper
    // still returns the right rows. With a pool of one connection, the transaction holds the only
    // connection, so any second borrow from the same pool times out and the read throws instead.
    private void runConnectionReuseCheck(MigrationMode mode) throws Exception {
        String[] args = {"../"};

        // Seed with the default pool: the core-side sign-up flows are not what this test is about.
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            return;
        }

        Config.getConfig((Start) StorageLayer.getStorage(process.getProcess())).setMigrationModeForTesting(mode);
        seedUsers(process.getProcess());
        process.kill(false);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        Utils.setValueInConfig("postgresql_connection_pool_size", "1");
        process = TestingProcessManager.start(args, false);
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Start start = (Start) StorageLayer.getStorage(process.getProcess());
        Config.getConfig(start).setMigrationModeForTesting(mode);
        TenantIdentifier tenant = TenantIdentifier.BASE_TENANT;

        AuthRecipeUserInfo[] emailTx = start.startTransaction(
                con -> start.listPrimaryUsersByEmail_Transaction(tenant, con, SHARED_EMAIL));
        AuthRecipeUserInfo[] phoneTx = start.startTransaction(
                con -> start.listPrimaryUsersByPhoneNumber_Transaction(tenant, con, PHONE));

        assertEquals("email tx read on a one-connection pool (" + mode + ")", 3, emailTx.length);
        assertEquals("phone tx read on a one-connection pool (" + mode + ")", 1, phoneTx.length);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // The two reads share the same SQL and ORDER BY (time_joined), so the transaction form must
    // return the same users in the same order as the non-transaction form.
    private static void assertUserIdsEqual(String message, AuthRecipeUserInfo[] expected,
                                           AuthRecipeUserInfo[] actual) {
        List<String> expectedIds = idsOf(expected);
        List<String> actualIds = idsOf(actual);
        assertEquals(message, expectedIds, actualIds);
    }

    private static List<String> idsOf(AuthRecipeUserInfo[] users) {
        List<String> ids = new ArrayList<>();
        for (AuthRecipeUserInfo u : users) {
            ids.add(u.getSupertokensUserId());
        }
        return ids;
    }
}
