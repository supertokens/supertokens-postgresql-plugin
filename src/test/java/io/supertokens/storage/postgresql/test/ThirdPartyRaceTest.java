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

import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;

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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Race condition tests for TEST-006:
 * ThirdParty signInUp + linkAccounts concurrent race
 *
 * ThirdParty has an additional race due to pre-transaction query for email comparison.
 */
public class ThirdPartyRaceTest {
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
     * TEST-006: Test link during ThirdParty signInUp email update
     *
     * Race scenario:
     * T1: signInUp(provider, thirdPartyUserId) - provider returns new email
     *     - Pre-transaction: reads user, sees email differs
     *     - Starts transaction, reads primary_user_id = NULL
     *                                         T2: linkAccounts(user, primaryUser)
     *                                             - Links user
     *                                             - Commits
     * T1: - Updates email to provider's value
     *     - Skips primary_user_tenants (saw NULL primary)
     *     - Commits
     *
     * RESULT: User linked with new email, but primary_user_tenants has OLD email
     */
    @Test
    public void testLinkDuringThirdPartySignInUpEmailUpdate() throws Exception {
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

        // Create primary user
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        for (int iter = 0; iter < RaceTestUtils.RACE_TEST_ITERATIONS; iter++) {
            final int iterFinal = iter;
            // Create ThirdParty user with initial email
            ThirdParty.SignInUpResponse initialSignIn = ThirdParty.signInUp(
                    process.getProcess(), "google", "tp-user-" + iter, "old" + iter + "@gmail.com");
            String tpUserId = initialSignIn.user.getSupertokensUserId();

            // Concurrent operations
            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch startLatch = new CountDownLatch(1);

            // Thread 1: signInUp with NEW email from provider
            Future<?> signInFuture = executor.submit(() -> {
                try {
                    startLatch.await();
                    // Provider now returns different email
                    ThirdParty.signInUp(process.getProcess(), "google", "tp-user-" + iterFinal, "new" + iterFinal + "@gmail.com");
                } catch (Exception e) {
                    // Expected in race conditions
                }
            });

            // Thread 2: Link to primary
            Future<?> linkFuture = executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.linkAccounts(process.getProcess(), tpUserId, primaryUser.getSupertokensUserId());
                } catch (Exception e) {
                    // Expected in race conditions
                }
            });

            startLatch.countDown();
            signInFuture.get(30, TimeUnit.SECONDS);
            linkFuture.get(30, TimeUnit.SECONDS);

            // Verify
            AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), tpUserId);
            assertNotNull(finalUser);

            // Find the third party user's login method
            String currentEmail = null;
            for (var loginMethod : finalUser.loginMethods) {
                if (loginMethod.getSupertokensUserId().equals(tpUserId)) {
                    currentEmail = loginMethod.email;
                    break;
                }
            }
            assertNotNull("Third party user's login method should be found", currentEmail);

            // Check if linked (primary user ID differs from third party user ID)
            boolean isLinked = !finalUser.getSupertokensUserId().equals(tpUserId);

            if (isLinked) {
                // CRITICAL: Check reservation tables directly via SQL
                RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                        process.getProcess(), finalUser);

                if (!result.isConsistent) {
                    System.out.println("RACE CONDITION DETECTED in testLinkDuringThirdPartySignInUpEmailUpdate (iteration " + iter + "):");
                    for (String issue : result.issues) {
                        System.out.println("  " + issue);
                    }
                    RaceTestUtils.printAllReservations(process.getProcess(), finalUser);
                }

                assertTrue("Reservation consistency check failed at iteration " + iter + ": " + result.issues, result.isConsistent);
            }

            executor.shutdown();
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-006: Pre-transaction query race (multiple iterations)
     *
     * Runs many iterations to catch the race where the pre-transaction
     * email check sees stale data.
     */
    @Test
    public void testThirdPartyPreTransactionQueryRace() throws Exception {
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

        // Create primary user
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        // Create ThirdParty user
        ThirdParty.SignInUpResponse initial = ThirdParty.signInUp(
                process.getProcess(), "github", "gh-123", "dev@github.com");
        String userId = initial.user.getSupertokensUserId();

        int ITERATIONS = 50;

        for (int i = 0; i < ITERATIONS; i++) {
            // Reset: unlink if linked
            try {
                AuthRecipeUserInfo current = AuthRecipe.getUserById(process.getProcess(), userId);
                if (current != null) {
                    // Check if linked (primary user ID differs from our user ID)
                    boolean linked = !current.getSupertokensUserId().equals(userId);
                    if (linked) {
                        AuthRecipe.unlinkAccounts(process.getProcess(), userId);
                    }
                }
            } catch (Exception e) {
                // Ignore
            }

            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch startLatch = new CountDownLatch(1);

            AtomicBoolean linkSucceeded = new AtomicBoolean(false);

            final int iteration = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    ThirdParty.signInUp(process.getProcess(), "github", "gh-123",
                            "updated" + iteration + "@github.com");
                } catch (Exception e) {
                    // Expected
                }
            });

            executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.linkAccounts(process.getProcess(), userId, primaryUser.getSupertokensUserId());
                    linkSucceeded.set(true);
                } catch (Exception e) {
                    // Expected
                }
            });

            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // Check for inconsistency by querying reservation tables directly
            if (linkSucceeded.get()) {
                AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), userId);

                if (finalUser != null) {
                    // CRITICAL: Check reservation tables directly via SQL
                    RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                            process.getProcess(), finalUser);

                    if (!result.isConsistent) {
                        System.out.println("Iteration " + i + ": RACE DETECTED");
                        for (String issue : result.issues) {
                            System.out.println("  " + issue);
                        }
                        RaceTestUtils.printAllReservations(process.getProcess(), finalUser);
                        fail("Reservation consistency check failed at iteration " + i + ": " + result.issues);
                    }
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-006: ThirdParty signInUp + unlinkAccounts race
     *
     * Tests that concurrent signInUp (which may update email) and unlink
     * don't leave orphaned reservations.
     */
    @Test
    public void testThirdPartySignInUpWithUnlink() throws Exception {
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

        // Create primary user
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        for (int iter = 0; iter < RaceTestUtils.RACE_TEST_ITERATIONS; iter++) {
            final int iterFinal = iter;
            // Create and link ThirdParty user
            ThirdParty.SignInUpResponse initial = ThirdParty.signInUp(
                    process.getProcess(), "google", "g-" + iter, "user" + iter + "@gmail.com");
            String userId = initial.user.getSupertokensUserId();
            AuthRecipe.linkAccounts(process.getProcess(), userId, primaryUser.getSupertokensUserId());

            // Concurrent signInUp with email change and unlink
            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch startLatch = new CountDownLatch(1);

            executor.submit(() -> {
                try {
                    startLatch.await();
                    // Google returns updated email
                    ThirdParty.signInUp(process.getProcess(), "google", "g-" + iterFinal, "updated" + iterFinal + "@gmail.com");
                } catch (Exception e) {
                    // Expected
                }
            });

            executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.unlinkAccounts(process.getProcess(), userId);
                } catch (Exception e) {
                    // Expected
                }
            });

            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            // Verify final state is consistent
            AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), userId);
            assertNotNull("User should still exist", finalUser);

            // Check if linked (primary user ID differs from our user ID)
            boolean isLinked = !finalUser.getSupertokensUserId().equals(userId);

            if (isLinked) {
                // CRITICAL: Check reservation tables directly via SQL
                RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                        process.getProcess(), finalUser);

                if (!result.isConsistent) {
                    System.out.println("RACE CONDITION DETECTED in testThirdPartySignInUpWithUnlink (iteration " + iter + "):");
                    for (String issue : result.issues) {
                        System.out.println("  " + issue);
                    }
                    RaceTestUtils.printAllReservations(process.getProcess(), finalUser);
                }

                assertTrue("Reservation consistency check failed at iteration " + iter + ": " + result.issues, result.isConsistent);
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-006: Multiple ThirdParty providers with rapid email changes
     *
     * Stress test with rapid signInUp calls that change the email.
     */
    @Test
    public void testRapidThirdPartyEmailChanges() throws Exception {
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

        // Create primary user
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        // Create ThirdParty user
        ThirdParty.SignInUpResponse initial = ThirdParty.signInUp(
                process.getProcess(), "google", "g-stress", "initial@gmail.com");
        String userId = initial.user.getSupertokensUserId();

        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger operations = new AtomicInteger(0);

        // Link/unlink threads
        for (int t = 0; t < 2; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (running.get()) {
                        try {
                            AuthRecipe.linkAccounts(process.getProcess(), userId,
                                    primaryUser.getSupertokensUserId());
                            operations.incrementAndGet();
                        } catch (Exception e) {
                            // Expected
                        }
                        try {
                            AuthRecipe.unlinkAccounts(process.getProcess(), userId);
                            operations.incrementAndGet();
                        } catch (Exception e) {
                            // Expected
                        }
                    }
                } catch (Exception e) {
                    // Ignore
                }
            });
        }

        // SignInUp threads (email changes)
        for (int t = 0; t < 3; t++) {
            final int threadId = t;
            executor.submit(() -> {
                int i = 0;
                try {
                    startLatch.await();
                    while (running.get()) {
                        try {
                            ThirdParty.signInUp(process.getProcess(), "google", "g-stress",
                                    "email_t" + threadId + "_i" + (i++) + "@gmail.com");
                            operations.incrementAndGet();
                        } catch (Exception e) {
                            // Expected
                        }
                    }
                } catch (Exception e) {
                    // Ignore
                }
            });
        }

        startLatch.countDown();
        Thread.sleep(5000); // Run for 5 seconds
        running.set(false);
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        System.out.println("Completed " + operations.get() + " ThirdParty operations");

        // Final consistency check by querying reservation tables directly
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), userId);
        assertNotNull("User should still exist", finalUser);

        // Check if linked (primary user ID differs from our user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(userId);

        if (isLinked) {
            // CRITICAL: Check reservation tables directly via SQL
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), finalUser);

            if (!result.isConsistent) {
                System.out.println("FINAL RACE DETECTED:");
                for (String issue : result.issues) {
                    System.out.println("  " + issue);
                }
                RaceTestUtils.printAllReservations(process.getProcess(), finalUser);
                fail("Final reservation consistency check failed: " + result.issues);
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-006: ThirdParty signInUp creating new user while linking existing
     *
     * Tests race between creating a new ThirdParty user and linking
     * an existing user (different users, but can affect reservation tables).
     */
    @Test
    public void testThirdPartyNewUserCreationDuringLinking() throws Exception {
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

        // Create primary user
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        // Create recipe user to be linked
        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(process.getProcess(), "recipe@test.com", "password123");

        int ITERATIONS = 30;

        for (int i = 0; i < ITERATIONS; i++) {
            final int iteration = i;

            // Reset: unlink recipe user if linked
            try {
                AuthRecipeUserInfo current = AuthRecipe.getUserById(process.getProcess(),
                        recipeUser.getSupertokensUserId());
                if (current != null) {
                    // Check if linked (primary user ID differs from recipe user ID)
                    boolean linked = !current.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());
                    if (linked) {
                        AuthRecipe.unlinkAccounts(process.getProcess(), recipeUser.getSupertokensUserId());
                    }
                }
            } catch (Exception e) {
                // Ignore
            }

            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch startLatch = new CountDownLatch(1);

            // Thread 1: Create new ThirdParty user
            executor.submit(() -> {
                try {
                    startLatch.await();
                    ThirdParty.signInUp(process.getProcess(), "github", "new-user-" + iteration,
                            "newuser" + iteration + "@github.com");
                } catch (Exception e) {
                    // Expected
                }
            });

            // Thread 2: Link recipe user
            executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.linkAccounts(process.getProcess(), recipeUser.getSupertokensUserId(),
                            primaryUser.getSupertokensUserId());
                } catch (Exception e) {
                    // Expected
                }
            });

            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // Check consistency of recipe user if linked by querying reservation tables
            AuthRecipeUserInfo finalRecipeUser = AuthRecipe.getUserById(process.getProcess(),
                    recipeUser.getSupertokensUserId());

            if (finalRecipeUser != null) {
                // Check if linked (primary user ID differs from recipe user ID)
                boolean isLinked = !finalRecipeUser.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());

                if (isLinked) {
                    // CRITICAL: Check reservation tables directly via SQL
                    RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                            process.getProcess(), finalRecipeUser);

                    if (!result.isConsistent) {
                        System.out.println("Iteration " + i + ": RACE DETECTED");
                        for (String issue : result.issues) {
                            System.out.println("  " + issue);
                        }
                        RaceTestUtils.printAllReservations(process.getProcess(), finalRecipeUser);
                        fail("Reservation consistency check failed at iteration " + i + ": " + result.issues);
                    }
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
