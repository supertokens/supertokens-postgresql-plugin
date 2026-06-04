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
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.dashboard.DashboardSearchTags;
import io.supertokens.authRecipe.UserPaginationContainer;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests that verify reservation table integrity after key operations.
 * Uses RaceTestUtils.checkReservationConsistency() to validate all invariants (I1-I6).
 */
public class ReservationTableIntegrityTest {

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
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        return process;
    }

    private Passwordless.ConsumeCodeResponse createPasswordlessUserWithEmail(
            TestingProcessManager.TestingProcess process, String email) throws Exception {
        Passwordless.CreateCodeResponse code = Passwordless.createCode(process.getProcess(), email, null, null, null);
        return Passwordless.consumeCode(process.getProcess(), code.deviceId, code.deviceIdHash,
                code.userInputCode, null);
    }

    private Passwordless.ConsumeCodeResponse createPasswordlessUserWithPhone(
            TestingProcessManager.TestingProcess process, String phone) throws Exception {
        Passwordless.CreateCodeResponse code = Passwordless.createCode(process.getProcess(), null, phone, null, null);
        return Passwordless.consumeCode(process.getProcess(), code.deviceId, code.deviceIdHash,
                code.userInputCode, null);
    }

    // ============================================================================
    // Test 1: Bug #3 regression — passwordless user with both email AND phone
    // ============================================================================
    @Test
    public void passwordlessUserWithBothEmailAndPhone_hasCorrectReservations() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            // Create passwordless user with email
            Passwordless.ConsumeCodeResponse resp = createPasswordlessUserWithEmail(process, "pl@test.com");
            String userId = resp.user.getSupertokensUserId();

            // Update to add phone number
            Passwordless.updateUser(process.getProcess(), userId,
                    null, new Passwordless.FieldUpdate("+1234567890"));

            // Refetch and check
            AuthRecipeUserInfo user = AuthRecipe.getUserById(process.getProcess(), userId);
            assertNotNull(user);
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), user);
            assertTrue("Reservation inconsistency: " + result.issues, result.isConsistent);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ============================================================================
    // Test 2: Bug #2 regression — time_joined consistency after linking
    // ============================================================================
    @Test
    public void afterLinkAccounts_timeJoinedIsConsistent() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            // Create primary (newer) and recipe user (older)
            AuthRecipeUserInfo olderUser = EmailPassword.signUp(process.getProcess(), "older@test.com", "password123");
            Thread.sleep(50); // Ensure different timestamps
            AuthRecipeUserInfo newerUser = EmailPassword.signUp(process.getProcess(), "newer@test.com", "password123");

            // Make newer user primary, link older user
            AuthRecipe.createPrimaryUser(process.getProcess(), newerUser.getSupertokensUserId());
            AuthRecipe.linkAccounts(process.getProcess(), olderUser.getSupertokensUserId(),
                    newerUser.getSupertokensUserId());

            // Refetch and check — primary_or_recipe_user_time_joined should be MIN
            AuthRecipeUserInfo user = AuthRecipe.getUserById(process.getProcess(), newerUser.getSupertokensUserId());
            assertNotNull(user);
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), user);
            assertTrue("Reservation inconsistency: " + result.issues, result.isConsistent);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ============================================================================
    // Test 3: After unlink, primary_user_tenants cleaned up for unlinked user
    // ============================================================================
    @Test
    public void afterUnlinkAccounts_primaryUserTenantsCleanedUp() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            AuthRecipeUserInfo primary = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
            AuthRecipeUserInfo recipe = EmailPassword.signUp(process.getProcess(), "recipe@test.com", "password123");

            AuthRecipe.createPrimaryUser(process.getProcess(), primary.getSupertokensUserId());
            AuthRecipe.linkAccounts(process.getProcess(), recipe.getSupertokensUserId(),
                    primary.getSupertokensUserId());

            // Unlink
            AuthRecipe.unlinkAccounts(process.getProcess(), recipe.getSupertokensUserId());

            // Check primary user (should no longer have recipe's email in primary_user_tenants)
            AuthRecipeUserInfo primaryAfter = AuthRecipe.getUserById(process.getProcess(),
                    primary.getSupertokensUserId());
            assertNotNull(primaryAfter);
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), primaryAfter);
            assertTrue("Primary user inconsistency after unlink: " + result.issues, result.isConsistent);

            // Check unlinked user
            AuthRecipeUserInfo unlinked = AuthRecipe.getUserById(process.getProcess(),
                    recipe.getSupertokensUserId());
            assertNotNull(unlinked);
            RaceTestUtils.ConsistencyCheckResult result2 = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), unlinked);
            assertTrue("Unlinked user inconsistency: " + result2.issues, result2.isConsistent);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ============================================================================
    // Test 4: Primary with no linked users keeps own reservations
    // ============================================================================
    @Test
    public void afterUnlinkLastUser_primaryKeepsOwnReservations() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            AuthRecipeUserInfo primary = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
            AuthRecipeUserInfo recipe = EmailPassword.signUp(process.getProcess(), "recipe@test.com", "password123");

            AuthRecipe.createPrimaryUser(process.getProcess(), primary.getSupertokensUserId());
            AuthRecipe.linkAccounts(process.getProcess(), recipe.getSupertokensUserId(),
                    primary.getSupertokensUserId());

            // Unlink
            AuthRecipe.unlinkAccounts(process.getProcess(), recipe.getSupertokensUserId());

            // Primary should still be a primary user with its own email in primary_user_tenants
            AuthRecipeUserInfo primaryAfter = AuthRecipe.getUserById(process.getProcess(),
                    primary.getSupertokensUserId());
            assertNotNull(primaryAfter);
            assertTrue(primaryAfter.isPrimaryUser);
            assertEquals(1, primaryAfter.loginMethods.length);
            assertEquals("primary@test.com", primaryAfter.loginMethods[0].email);

            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), primaryAfter);
            assertTrue("Primary should keep own reservations: " + result.issues, result.isConsistent);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ============================================================================
    // Test 5: Bug #4 regression — phone+provider search returns empty
    // ============================================================================
    @Test
    public void dashboardSearchWithPhoneAndProvider_returnsEmpty() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            // Create a passwordless user with phone
            createPasswordlessUserWithPhone(process, "+1234567890");

            // Create a third-party user
            ThirdParty.signInUp(process.getProcess(), "google", "g-user-1", "tp@test.com");

            // Search with phone + provider — should return empty (no user matches both)
            DashboardSearchTags tags = new DashboardSearchTags(null, List.of("+123"), List.of("google"));
            UserPaginationContainer result = AuthRecipe.getUsers(process.getProcess(), 10, "ASC",
                    null, null, tags);
            assertEquals("phone+provider search should return 0 results", 0, result.users.length);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ============================================================================
    // Test 6: Bug #4 regression — email+phone+provider search returns empty
    // ============================================================================
    @Test
    public void dashboardSearchWithAllThreeTags_returnsEmpty() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            // Create users with various account info
            EmailPassword.signUp(process.getProcess(), "ep@test.com", "password123");
            createPasswordlessUserWithPhone(process, "+1234567890");
            ThirdParty.signInUp(process.getProcess(), "google", "g-user-1", "tp@test.com");

            // Search with all three tags — no single recipe supports all three
            DashboardSearchTags tags = new DashboardSearchTags(List.of("test"), List.of("+123"), List.of("google"));
            UserPaginationContainer result = AuthRecipe.getUsers(process.getProcess(), 10, "ASC",
                    null, null, tags);
            assertEquals("email+phone+provider search should return 0 results", 0, result.users.length);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ============================================================================
    // Test 7: Passwordless user with email only
    // ============================================================================
    @Test
    public void passwordlessUserWithEmailOnly_hasCorrectReservations() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Passwordless.ConsumeCodeResponse resp = createPasswordlessUserWithEmail(process, "emailonly@test.com");

            AuthRecipeUserInfo user = AuthRecipe.getUserById(process.getProcess(),
                    resp.user.getSupertokensUserId());
            assertNotNull(user);

            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), user);
            assertTrue("Single email PL user inconsistency: " + result.issues, result.isConsistent);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ============================================================================
    // Test 8: Third-party user has THIRD_PARTY type in reservations
    // ============================================================================
    @Test
    public void thirdPartyUser_reservationsIncludeThirdPartyType() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            ThirdParty.SignInUpResponse resp = ThirdParty.signInUp(
                    process.getProcess(), "google", "g-user-123", "tp@test.com");

            AuthRecipeUserInfo user = AuthRecipe.getUserById(process.getProcess(),
                    resp.user.getSupertokensUserId());
            assertNotNull(user);

            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), user);
            assertTrue("ThirdParty user inconsistency: " + result.issues, result.isConsistent);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ============================================================================
    // Test 9: Cross-recipe linking — EP email + PL phone both in primary_user_tenants
    // ============================================================================
    @Test
    public void linkAccountsAcrossRecipeTypes_allReservationsPresent() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            // EmailPassword user with email
            AuthRecipeUserInfo epUser = EmailPassword.signUp(process.getProcess(), "ep@test.com", "password123");
            AuthRecipe.createPrimaryUser(process.getProcess(), epUser.getSupertokensUserId());

            // Passwordless user with phone
            Passwordless.ConsumeCodeResponse plResp = createPasswordlessUserWithPhone(process, "+1234567890");
            String plUserId = plResp.user.getSupertokensUserId();

            // Link PL to EP
            AuthRecipe.linkAccounts(process.getProcess(), plUserId, epUser.getSupertokensUserId());

            // Refetch primary user
            AuthRecipeUserInfo linkedUser = AuthRecipe.getUserById(process.getProcess(),
                    epUser.getSupertokensUserId());
            assertNotNull(linkedUser);
            assertTrue(linkedUser.isPrimaryUser);
            assertEquals(2, linkedUser.loginMethods.length);

            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), linkedUser);
            assertTrue("Cross-recipe link inconsistency: " + result.issues, result.isConsistent);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ============================================================================
    // Test 10: After clearing email, phone reservation preserved
    // ============================================================================
    @Test
    public void passwordlessClearEmail_phoneReservationPreserved() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            // Create PL user with email
            Passwordless.ConsumeCodeResponse resp = createPasswordlessUserWithEmail(process, "clear@test.com");
            String userId = resp.user.getSupertokensUserId();

            // Add phone
            Passwordless.updateUser(process.getProcess(), userId,
                    null, new Passwordless.FieldUpdate("+1234567890"));

            // Clear email
            Passwordless.updateUser(process.getProcess(), userId,
                    new Passwordless.FieldUpdate(null), null);

            // Refetch
            AuthRecipeUserInfo user = AuthRecipe.getUserById(process.getProcess(), userId);
            assertNotNull(user);
            assertNull(user.loginMethods[0].email);
            assertEquals("+1234567890", user.loginMethods[0].phoneNumber);

            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), user);
            assertTrue("After clearing email, phone should remain: " + result.issues, result.isConsistent);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
