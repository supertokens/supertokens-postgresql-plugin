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

package io.supertokens.storage.postgresql.test;

import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.storageLayer.StorageLayer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * REVIEW-004: Race condition tests for deleteUser with LockedUser.
 *
 * Tests that deleteUser properly acquires LockedUser to serialize
 * with concurrent link/tenant/email-update operations.
 */
public class DeleteUserRaceTest {
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

    @Rule
    public Retry retry = new Retry(3);

    /**
     * Test 1: deleteUser + linkAccounts race
     *
     * Race scenario:
     * T1: deleteUser(R1)                    T2: linkAccounts(R1, P1)
     *     - Locks R1                            - Locks R1 (blocks on T1's lock)
     *     - Reads R1 state                      ...
     *     - Deletes R1                          ...
     *     - Commits                             - Lock acquired
     *                                           - R1 not found → fails
     * OR:
     * T2: linkAccounts(R1, P1)              T1: deleteUser(R1)
     *     - Locks R1                            - Locks R1 (blocks on T2's lock)
     *     - Links R1 to P1                      ...
     *     - Commits                             - Lock acquired
     *                                           - Reads R1 (now linked to P1)
     *                                           - Deletes R1 cleanly
     *
     * Either way, no orphaned reservations should remain.
     */
    @Test
    public void testDeleteUserDuringLinkAccounts() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            return;
        }

        Main main = process.getProcess();

        for (int iter = 0; iter < RaceTestUtils.RACE_TEST_ITERATIONS; iter++) {
            // Create primary user
            AuthRecipeUserInfo primaryUser = EmailPassword.signUp(main,
                    "primary" + iter + "@test.com", "password123");
            AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

            // Create recipe user to be linked/deleted
            AuthRecipeUserInfo recipeUser = EmailPassword.signUp(main,
                    "recipe" + iter + "@test.com", "password123");

            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch startLatch = new CountDownLatch(1);

            AtomicBoolean deleteSucceeded = new AtomicBoolean(false);
            AtomicBoolean linkSucceeded = new AtomicBoolean(false);

            // Thread 1: Delete the recipe user (only this user, not all linked accounts)
            Future<?> deleteFuture = executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.deleteUser(main, recipeUser.getSupertokensUserId(), false);
                    deleteSucceeded.set(true);
                } catch (Exception e) {
                    // Expected in race conditions
                }
            });

            // Thread 2: Link the recipe user to primary
            Future<?> linkFuture = executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.linkAccounts(main, recipeUser.getSupertokensUserId(),
                            primaryUser.getSupertokensUserId());
                    linkSucceeded.set(true);
                } catch (Exception e) {
                    // Expected in race conditions
                }
            });

            startLatch.countDown();
            deleteFuture.get(30, TimeUnit.SECONDS);
            linkFuture.get(30, TimeUnit.SECONDS);
            executor.shutdown();

            // Verify: check what state we ended up in
            AuthRecipeUserInfo finalRecipeUser = AuthRecipe.getUserById(main,
                    recipeUser.getSupertokensUserId());
            AuthRecipeUserInfo finalPrimaryUser = AuthRecipe.getUserById(main,
                    primaryUser.getSupertokensUserId());

            if (finalRecipeUser == null) {
                // User was deleted — primary user should NOT have orphaned login methods
                // referencing the deleted user
                assertNotNull("Primary user should still exist", finalPrimaryUser);
                for (var lm : finalPrimaryUser.loginMethods) {
                    assertNotEquals("Primary user should not reference deleted recipe user",
                            recipeUser.getSupertokensUserId(), lm.getSupertokensUserId());
                }
            } else {
                // User was not deleted — it may or may not be linked
                // Either way, reservation tables should be consistent
                boolean isLinked = !finalRecipeUser.getSupertokensUserId()
                        .equals(recipeUser.getSupertokensUserId());
                if (isLinked) {
                    RaceTestUtils.ConsistencyCheckResult result =
                            RaceTestUtils.checkReservationConsistency(main, finalRecipeUser);
                    assertTrue("Reservation consistency failed at iteration " + iter +
                            ": " + result.issues, result.isConsistent);
                }
            }

            // Also verify primary user's reservation consistency
            if (finalPrimaryUser != null && finalPrimaryUser.loginMethods.length > 1) {
                RaceTestUtils.ConsistencyCheckResult result =
                        RaceTestUtils.checkReservationConsistency(main, finalPrimaryUser);
                assertTrue("Primary user reservation consistency failed at iteration " + iter +
                        ": " + result.issues, result.isConsistent);
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * Test 2: deleteUser + updateEmail race
     *
     * Concurrent deleteUser and email update on the same user.
     * With LockedUser, these should be fully serialized.
     */
    @Test
    public void testDeleteUserDuringEmailUpdate() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            return;
        }

        Main main = process.getProcess();

        for (int iter = 0; iter < RaceTestUtils.RACE_TEST_ITERATIONS; iter++) {
            // Create primary user
            AuthRecipeUserInfo primaryUser = EmailPassword.signUp(main,
                    "primary-del" + iter + "@test.com", "password123");
            AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

            // Create and link a recipe user
            AuthRecipeUserInfo recipeUser = EmailPassword.signUp(main,
                    "recipe-del" + iter + "@test.com", "password123");
            AuthRecipe.linkAccounts(main, recipeUser.getSupertokensUserId(),
                    primaryUser.getSupertokensUserId());

            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch startLatch = new CountDownLatch(1);
            final int iterFinal = iter;

            // Thread 1: Delete the recipe user (not removing all linked accounts)
            Future<?> deleteFuture = executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.deleteUser(main, recipeUser.getSupertokensUserId(), false);
                } catch (Exception e) {
                    // Expected
                }
            });

            // Thread 2: Update the recipe user's email
            Future<?> updateFuture = executor.submit(() -> {
                try {
                    startLatch.await();
                    EmailPassword.updateUsersEmailOrPassword(main,
                            recipeUser.getSupertokensUserId(),
                            "updated-del" + iterFinal + "@test.com", null);
                } catch (Exception e) {
                    // Expected — user may be deleted
                }
            });

            startLatch.countDown();
            deleteFuture.get(30, TimeUnit.SECONDS);
            updateFuture.get(30, TimeUnit.SECONDS);
            executor.shutdown();

            // Verify: user is either deleted or has consistent state
            AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main,
                    recipeUser.getSupertokensUserId());

            // Primary user should always exist and have consistent reservations
            AuthRecipeUserInfo finalPrimary = AuthRecipe.getUserById(main,
                    primaryUser.getSupertokensUserId());
            assertNotNull("Primary user should still exist", finalPrimary);

            if (finalPrimary.loginMethods.length > 0) {
                RaceTestUtils.ConsistencyCheckResult result =
                        RaceTestUtils.checkReservationConsistency(main, finalPrimary);
                assertTrue("Primary reservation consistency failed at iteration " + iter +
                        ": " + result.issues, result.isConsistent);
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * Test 3: deleteAllLinkedAccounts + updateEmail race
     *
     * Delete with removeAllLinkedAccounts=true while another thread
     * updates email on one of the linked login methods.
     */
    @Test
    public void testDeleteAllLinkedAccountsDuringEmailUpdate() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            return;
        }

        Main main = process.getProcess();

        for (int iter = 0; iter < RaceTestUtils.RACE_TEST_ITERATIONS; iter++) {
            // Create primary user
            AuthRecipeUserInfo primaryUser = EmailPassword.signUp(main,
                    "primary-all" + iter + "@test.com", "password123");
            AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

            // Create and link two recipe users
            AuthRecipeUserInfo recipeUser1 = EmailPassword.signUp(main,
                    "linked1-" + iter + "@test.com", "password123");
            AuthRecipeUserInfo recipeUser2 = EmailPassword.signUp(main,
                    "linked2-" + iter + "@test.com", "password123");
            AuthRecipe.linkAccounts(main, recipeUser1.getSupertokensUserId(),
                    primaryUser.getSupertokensUserId());
            AuthRecipe.linkAccounts(main, recipeUser2.getSupertokensUserId(),
                    primaryUser.getSupertokensUserId());

            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch startLatch = new CountDownLatch(1);
            final int iterFinal = iter;

            // Thread 1: Delete ALL linked accounts via primary user
            Future<?> deleteFuture = executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.deleteUser(main, primaryUser.getSupertokensUserId(), true);
                } catch (Exception e) {
                    // Expected
                }
            });

            // Thread 2: Update email on one of the linked users
            Future<?> updateFuture = executor.submit(() -> {
                try {
                    startLatch.await();
                    EmailPassword.updateUsersEmailOrPassword(main,
                            recipeUser1.getSupertokensUserId(),
                            "updated-all" + iterFinal + "@test.com", null);
                } catch (Exception e) {
                    // Expected — user may be deleted
                }
            });

            startLatch.countDown();
            deleteFuture.get(30, TimeUnit.SECONDS);
            updateFuture.get(30, TimeUnit.SECONDS);
            executor.shutdown();

            // Verify: either all users are deleted, or state is consistent
            AuthRecipeUserInfo finalPrimary = AuthRecipe.getUserById(main,
                    primaryUser.getSupertokensUserId());
            AuthRecipeUserInfo finalRecipe1 = AuthRecipe.getUserById(main,
                    recipeUser1.getSupertokensUserId());
            AuthRecipeUserInfo finalRecipe2 = AuthRecipe.getUserById(main,
                    recipeUser2.getSupertokensUserId());

            if (finalPrimary == null) {
                // All deleted — verify all linked users are also gone
                assertNull("Recipe user 1 should be deleted when primary is deleted", finalRecipe1);
                assertNull("Recipe user 2 should be deleted when primary is deleted", finalRecipe2);
            } else {
                // Delete didn't happen (email update won the race) — check consistency
                RaceTestUtils.ConsistencyCheckResult result =
                        RaceTestUtils.checkReservationConsistency(main, finalPrimary);
                assertTrue("Reservation consistency failed at iteration " + iter +
                        ": " + result.issues, result.isConsistent);
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
