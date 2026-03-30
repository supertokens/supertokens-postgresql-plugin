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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Race condition tests for TEST-001 and TEST-002:
 * - TEST-001: linkAccounts + updateEmail concurrent race
 * - TEST-002: updateEmail + linkAccounts concurrent race (reverse timing)
 *
 * These tests verify that concurrent operations on users maintain consistent state
 * across the reservation tables (recipe_user_tenants, primary_user_tenants).
 */
public class RaceConditionTest {
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
     * TEST-001: Test linkAccounts during email update
     *
     * Race scenario:
     * T1: linkAccounts(recipeUser, primaryUser)
     *     - Reads recipeUser's email = "old@test.com"
     *                                         T2: updateEmail(recipeUser, "new@test.com")
     *                                             - Updates email
     *                                             - Commits
     * T1: - Reserves "old@test.com" for primary in primary_user_tenants
     *     - Commits
     *
     * RESULT: primary_user_tenants has "old@test.com" but user has "new@test.com"
     */
    @Test
    public void testLinkAccountsDuringEmailPasswordEmailUpdate() throws Exception {
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

        // Create recipe user
        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(process.getProcess(), "recipe@test.com", "password123");

        // Run concurrent operations
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicReference<Exception> linkException = new AtomicReference<>();
        AtomicReference<Exception> updateException = new AtomicReference<>();

        // Thread 1: Link accounts
        Future<?> linkFuture = executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.linkAccounts(process.getProcess(), recipeUser.getSupertokensUserId(),
                        primaryUser.getSupertokensUserId());
            } catch (Exception e) {
                linkException.set(e);
            }
        });

        // Thread 2: Update email
        Future<?> updateFuture = executor.submit(() -> {
            try {
                startLatch.await();
                EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                        recipeUser.getSupertokensUserId(), "newemail@test.com", null);
            } catch (Exception e) {
                updateException.set(e);
            }
        });

        // Start both threads simultaneously
        startLatch.countDown();
        linkFuture.get(30, TimeUnit.SECONDS);
        updateFuture.get(30, TimeUnit.SECONDS);

        // Verify consistency by directly querying reservation tables
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), recipeUser.getSupertokensUserId());
        assertNotNull(finalUser);

        // Check reservation tables directly via SQL
        RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                process.getProcess(), finalUser);

        if (!result.isConsistent) {
            System.out.println("RACE CONDITION DETECTED in testLinkAccountsDuringEmailPasswordEmailUpdate:");
            for (String issue : result.issues) {
                System.out.println("  " + issue);
            }
            RaceTestUtils.printAllReservations(process.getProcess(), finalUser);
        }

        assertTrue("Reservation consistency check failed: " + result.issues, result.isConsistent);

        executor.shutdown();
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-001 & TEST-002: High concurrency stress test
     *
     * Creates many recipe users and concurrently:
     * - Links them to a primary user
     * - Updates their emails
     *
     * Verifies that ALL users end up in a consistent state.
     */
    @Test
    public void testLinkAccountsEmailUpdateHighConcurrency() throws Exception {
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

        // Create multiple recipe users
        int NUM_USERS = 20;
        List<AuthRecipeUserInfo> recipeUsers = new ArrayList<>();
        for (int i = 0; i < NUM_USERS; i++) {
            recipeUsers.add(EmailPassword.signUp(process.getProcess(), "user" + i + "@test.com", "password123"));
        }

        ExecutorService executor = Executors.newFixedThreadPool(50);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successfulLinks = new AtomicInteger(0);
        AtomicInteger successfulUpdates = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < NUM_USERS; i++) {
            final int idx = i;
            final AuthRecipeUserInfo user = recipeUsers.get(i);

            // Link operation
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.linkAccounts(process.getProcess(), user.getSupertokensUserId(),
                            primaryUser.getSupertokensUserId());
                    successfulLinks.incrementAndGet();
                } catch (Exception e) {
                    // Some failures are expected due to race conditions
                    failures.incrementAndGet();
                }
            }));

            // Update operation
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                            user.getSupertokensUserId(), "updated" + idx + "@test.com", null);
                    successfulUpdates.incrementAndGet();
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }));
        }

        startLatch.countDown();

        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }

        // Verify ALL users have consistent state by checking reservation tables directly
        for (AuthRecipeUserInfo user : recipeUsers) {
            AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), user.getSupertokensUserId());
            if (finalUser == null) {
                continue; // User might have been deleted
            }

            // Check reservation tables directly via SQL
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), finalUser);

            if (!result.isConsistent) {
                System.out.println("RACE DETECTED for user " + user.getSupertokensUserId() + ":");
                for (String issue : result.issues) {
                    System.out.println("  " + issue);
                }
                RaceTestUtils.printAllReservations(process.getProcess(), finalUser);
                fail("Reservation consistency check failed: " + result.issues);
            }
        }

        executor.shutdown();
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-002: Test email update during linkAccounts (reverse timing)
     *
     * Race scenario:
     * T1: updateEmail(recipeUser, "new@test.com")
     *     - Reads primary_user_id = NULL (not linked yet)
     *                                         T2: linkAccounts(recipeUser, primaryUser)
     *                                             - Links user
     *                                             - Reserves "old@test.com"
     *                                             - Commits
     * T1: - Updates email to "new@test.com"
     *     - Sees primary_user_id was NULL, skips primary_user_tenants insert
     *     - Commits
     *
     * RESULT: User linked with "new@test.com", but primary_user_tenants only has "old@test.com"
     */
    @Test
    public void testEmailUpdateDuringLinkAccounts() throws Exception {
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

        // Create recipe user
        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(process.getProcess(), "old@test.com", "password123");

        // Run concurrent operations
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Thread 1: Update email
        Future<?> updateFuture = executor.submit(() -> {
            try {
                startLatch.await();
                EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                        recipeUser.getSupertokensUserId(), "new@test.com", null);
            } catch (Exception e) {
                // Expected if race is properly handled
            }
        });

        // Thread 2: Link accounts
        Future<?> linkFuture = executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.linkAccounts(process.getProcess(), recipeUser.getSupertokensUserId(),
                        primaryUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected if race is properly handled
            }
        });

        startLatch.countDown();
        updateFuture.get(30, TimeUnit.SECONDS);
        linkFuture.get(30, TimeUnit.SECONDS);

        // Verify state by directly querying reservation tables
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), recipeUser.getSupertokensUserId());
        assertNotNull(finalUser);

        // Check reservation tables directly via SQL
        RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                process.getProcess(), finalUser);

        if (!result.isConsistent) {
            System.out.println("RACE CONDITION DETECTED in testEmailUpdateDuringLinkAccounts:");
            for (String issue : result.issues) {
                System.out.println("  " + issue);
            }
            RaceTestUtils.printAllReservations(process.getProcess(), finalUser);
        }

        assertTrue("Reservation consistency check failed: " + result.issues, result.isConsistent);

        executor.shutdown();
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-002: Repeated iterations to catch the race
     *
     * Runs many iterations of concurrent link + email update to ensure
     * no inconsistencies occur.
     */
    @Test
    public void testReservationCompletenessAfterConcurrentOps() throws Exception {
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

        // Create recipe user
        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(process.getProcess(), "original@test.com", "password123");

        // Run many iterations to catch the race
        int ITERATIONS = 50;

        for (int i = 0; i < ITERATIONS; i++) {
            final int iteration = i;
            // Reset state: unlink if linked
            try {
                AuthRecipeUserInfo currentUser = AuthRecipe.getUserById(process.getProcess(),
                        recipeUser.getSupertokensUserId());
                if (currentUser != null) {
                    // Check if user is linked (primary user ID differs from recipe user ID)
                    boolean isLinked = !currentUser.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());
                    if (isLinked) {
                        AuthRecipe.unlinkAccounts(process.getProcess(), recipeUser.getSupertokensUserId());
                    }
                }
            } catch (Exception e) {
                // Ignore errors during reset
            }

            // Reset email
            try {
                EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                        recipeUser.getSupertokensUserId(), "iter" + iteration + "@test.com", null);
            } catch (Exception e) {
                // Ignore
            }

            // Concurrent operations
            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch start = new CountDownLatch(1);

            Future<?> f1 = executor.submit(() -> {
                try {
                    start.await();
                    AuthRecipe.linkAccounts(process.getProcess(), recipeUser.getSupertokensUserId(),
                            primaryUser.getSupertokensUserId());
                } catch (Exception e) {
                    // Expected
                }
            });

            Future<?> f2 = executor.submit(() -> {
                try {
                    start.await();
                    EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                            recipeUser.getSupertokensUserId(), "updated" + iteration + "@test.com", null);
                } catch (Exception e) {
                    // Expected
                }
            });

            start.countDown();
            f1.get(10, TimeUnit.SECONDS);
            f2.get(10, TimeUnit.SECONDS);
            executor.shutdown();

            // Check consistency by querying reservation tables directly
            AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(),
                    recipeUser.getSupertokensUserId());
            if (finalUser == null) {
                continue;
            }

            // Check reservation tables directly via SQL
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

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-001/002: Rapid link/unlink with email updates
     *
     * Stress test with rapid linking, unlinking, and email updates
     * to find race conditions.
     */
    @Test
    public void testRapidLinkUnlinkWithEmailUpdates() throws Exception {
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

        // Create recipe user
        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(process.getProcess(), "recipe@test.com", "password123");

        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger iterations = new AtomicInteger(0);

        int NUM_OPS = 100;

        // Link/unlink thread
        for (int t = 0; t < 3; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < NUM_OPS / 3; i++) {
                        try {
                            AuthRecipe.linkAccounts(process.getProcess(), recipeUser.getSupertokensUserId(),
                                    primaryUser.getSupertokensUserId());
                        } catch (Exception e) {
                            // Expected
                        }
                        try {
                            AuthRecipe.unlinkAccounts(process.getProcess(), recipeUser.getSupertokensUserId());
                        } catch (Exception e) {
                            // Expected
                        }
                        iterations.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Ignore
                }
            });
        }

        // Email update threads
        for (int t = 0; t < 3; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < NUM_OPS / 3; i++) {
                        try {
                            EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                                    recipeUser.getSupertokensUserId(),
                                    "email_t" + threadId + "_i" + i + "@test.com", null);
                        } catch (Exception e) {
                            // Expected
                        }
                        iterations.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Ignore
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(120, TimeUnit.SECONDS);

        // Final consistency check by querying reservation tables directly
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), recipeUser.getSupertokensUserId());

        System.out.println("Completed " + iterations.get() + " iterations");

        if (finalUser != null) {
            // Check reservation tables directly via SQL
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
}
