/*
 *    Copyright (c) 2025, VRAI Labs and/or its affiliates. All rights reserved.
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

import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.exceptions.AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static org.junit.Assert.*;

/**
 * Regression test for the fan-out bug in addPrimaryUserAccountInfo_Transaction.
 *
 * Root cause: the INSERT…SELECT JOIN on recipe_user_tenants × recipe_user_account_infos
 * did not include third_party_id / third_party_user_id in the JOIN condition.  When
 * recipe_user_account_infos contains two EMAIL rows for the same user with the same
 * account_info_value but different (third_party_id, third_party_user_id), a single
 * recipe_user_tenants row fans out to both, producing duplicate
 * (tenant_id, account_info_type, account_info_value) tuples in the SELECT output.
 *
 * PostgreSQL's INSERT … ON CONFLICT DO UPDATE then throws:
 *   "ON CONFLICT DO UPDATE command cannot affect row a second time" (SQLSTATE 55000)
 * because both duplicates attempt to update the same primary_user_tenants row.
 *
 * The fix is SELECT DISTINCT in the INSERT…SELECT, which collapses the duplicates
 * before they reach the ON CONFLICT clause.
 *
 * Test strategy:
 *   1. Make an EmailPassword user primary → seeds primary_user_tenants with the
 *      conflict target (public, email, test@example.com, EP_USER).
 *   2. ThirdParty sign-up with the same email → adds the normal EMAIL row:
 *      (thirdparty, email, "google", "user-google", "test@example.com").
 *   3. Directly inject a spurious second EMAIL row with empty third_party_id:
 *      (thirdparty, email, "", "", "test@example.com").
 *   4. Call createPrimaryUser on the ThirdParty user.
 *
 * Without SELECT DISTINCT: PSQLException (wrapped as StorageQueryException).
 * With    SELECT DISTINCT: AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException.
 */
public class MakePrimaryUserFanOutTest {

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
    public void createPrimaryUser_fanOutBug_spuriousEmailRowCausesConflictError() throws Exception {
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

        // Step 1 — create EP primary user, seeding primary_user_tenants with
        //           (public, email, test@example.com, EP_USER).
        AuthRecipeUserInfo epUser = EmailPassword.signUp(
                process.getProcess(), "test@example.com", "pass1234");
        AuthRecipe.createPrimaryUser(process.getProcess(), epUser.getSupertokensUserId());

        // Step 2 — ThirdParty sign-up with the same email.
        // Produces two rows in recipe_user_account_infos for TP_USER:
        //   a) recipe_id=thirdparty, account_info_type=email,   tp_id=google, tp_uid=user-google
        //   b) recipe_id=thirdparty, account_info_type=tparty,  tp_id='',     tp_uid=''
        ThirdParty.SignInUpResponse tpResponse = ThirdParty.signInUp(
                process.getProcess(), "google", "user-google", "test@example.com");
        String tpUserId = tpResponse.user.getSupertokensUserId();

        // Step 3 — inject a spurious second EMAIL row with empty third_party_id.
        //
        // PK of recipe_user_account_infos:
        //   (app_id, recipe_id, recipe_user_id, account_info_type, third_party_id, third_party_user_id)
        //
        // The existing row has (thirdparty, email, google, user-google) so this new row
        // (thirdparty, email, '', '') has a different PK and does not conflict on insert.
        //
        // Now recipe_user_account_infos has TWO rows matching
        // (recipe_user_id=TP_USER, recipe_id=thirdparty, account_info_type=email,
        //  account_info_value=test@example.com) — differing only in third_party_id.
        Start start = (Start) StorageLayer.getStorage(process.getProcess());
        String accountInfoTable = Config.getConfig(start).getRecipeUserAccountInfosTable();
        update(start,
                "INSERT INTO " + accountInfoTable
                        + " (app_id, recipe_user_id, recipe_id, account_info_type,"
                        + "  third_party_id, third_party_user_id, account_info_value)"
                        + " VALUES (?, ?, 'thirdparty', 'email', '', '', ?)",
                pst -> {
                    pst.setString(1, "public");
                    pst.setString(2, tpUserId);
                    pst.setString(3, "test@example.com");
                });

        // Step 4 — attempt to make the ThirdParty user primary.
        //
        // Inside addPrimaryUserAccountInfo_Transaction the INSERT…SELECT joins
        // recipe_user_tenants r with recipe_user_account_infos ai on
        // (recipe_user_id, recipe_id, account_info_type, account_info_value).
        // The single recipe_user_tenants row for the ThirdParty user's email matches
        // BOTH ai rows (real + spurious), so the SELECT emits two identical tuples:
        //   (public, email, test@example.com, TP_USER)
        //   (public, email, test@example.com, TP_USER)    ← duplicate
        //
        // Both duplicates try to DO UPDATE the same primary_user_tenants row
        // (public, email, test@example.com, EP_USER), triggering SQLSTATE 55000.
        //
        // With SELECT DISTINCT the duplicate is collapsed before the INSERT, so only
        // one conflict row reaches primary_user_tenants.  The conflict is detected,
        // and AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException is thrown.
        try {
            AuthRecipe.createPrimaryUser(process.getProcess(), tpUserId);
            fail("Expected AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException");
        } catch (AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException e) {
            assertEquals(epUser.getSupertokensUserId(), e.primaryUserId);
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
