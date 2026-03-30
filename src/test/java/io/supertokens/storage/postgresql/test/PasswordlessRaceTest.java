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
import io.supertokens.passwordless.Passwordless;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Race condition tests for TEST-007:
 * Passwordless updateUser + linkAccounts concurrent race
 *
 * Passwordless has an additional race due to reading user info OUTSIDE the transaction.
 */
public class PasswordlessRaceTest {
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

    private Passwordless.ConsumeCodeResponse createPasswordlessUser(
            TestingProcessManager.TestingProcess process, String email, String phone) throws Exception {
        Passwordless.CreateCodeResponse code;
        if (email != null) {
            code = Passwordless.createCode(process.getProcess(), email, null, null, null);
        } else {
            code = Passwordless.createCode(process.getProcess(), null, phone, null, null);
        }
        return Passwordless.consumeCode(process.getProcess(), code.deviceId, code.deviceIdHash,
                code.userInputCode, null);
    }

    /**
     * TEST-007: Test link during Passwordless email update
     *
     * Race scenario:
     * T1: Passwordless.updateUser(userId, newEmail, newPhone)
     *     - Reads user info OUTSIDE transaction (intentional per code comment)
     *     - Sees primary_user_id = NULL
     *     - Starts transaction
     *                                         T2: linkAccounts(user, primaryUser)
     *                                             - Links user
     *                                             - Commits
     * T1: - Updates email/phone
     *     - Skips primary_user_tenants (stale NULL check)
     *     - Commits
     *
     * RESULT: Linked user has new email, but primary_user_tenants has old email
     */
    @Test
    public void testLinkDuringPasswordlessEmailUpdate() throws Exception {
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

        // Create passwordless user with email
        Passwordless.ConsumeCodeResponse response = createPasswordlessUser(process, "old@passwordless.com", null);
        String userId = response.user.getSupertokensUserId();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Update email
        Future<?> updateFuture = executor.submit(() -> {
            try {
                startLatch.await();
                Passwordless.updateUser(process.getProcess(), userId,
                        new Passwordless.FieldUpdate("new@passwordless.com"), null);
            } catch (Exception e) {
                // Expected in race conditions
            }
        });

        // Link
        Future<?> linkFuture = executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.linkAccounts(process.getProcess(), userId, primaryUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected in race conditions
            }
        });

        startLatch.countDown();
        updateFuture.get(30, TimeUnit.SECONDS);
        linkFuture.get(30, TimeUnit.SECONDS);

        // Verify
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), userId);
        assertNotNull(finalUser);

        // Find the passwordless user's login method
        String actualEmail = null;
        for (var loginMethod : finalUser.loginMethods) {
            if (loginMethod.getSupertokensUserId().equals(userId)) {
                actualEmail = loginMethod.email;
                break;
            }
        }

        // Check if linked (primary user ID differs from our user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(userId);

        if (isLinked && actualEmail != null) {
            // CRITICAL: Check reservation tables directly via SQL
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    process.getProcess(), finalUser);

            if (!result.isConsistent) {
                System.out.println("RACE CONDITION DETECTED in testLinkDuringPasswordlessEmailUpdate:");
                for (String issue : result.issues) {
                    System.out.println("  " + issue);
                }
                RaceTestUtils.printAllReservations(process.getProcess(), finalUser);
            }

            assertTrue("Reservation consistency check failed: " + result.issues, result.isConsistent);
        }

        executor.shutdown();
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-007: Test phone update during linking
     */
    @Test
    public void testPhoneUpdateDuringLinking() throws Exception {
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

        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        // Passwordless user with phone
        Passwordless.ConsumeCodeResponse response = createPasswordlessUser(process, null, "+1111111111");
        String userId = response.user.getSupertokensUserId();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        executor.submit(() -> {
            try {
                startLatch.await();
                Passwordless.updateUser(process.getProcess(), userId, null,
                        new Passwordless.FieldUpdate("+2222222222"));
            } catch (Exception e) {
                // Expected
            }
        });

        executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.linkAccounts(process.getProcess(), userId, primaryUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Verify phone consistency
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), userId);
        assertNotNull(finalUser);

        // Find the passwordless user's login method
        String actualPhone = null;
        for (var loginMethod : finalUser.loginMethods) {
            if (loginMethod.getSupertokensUserId().equals(userId)) {
                actualPhone = loginMethod.phoneNumber;
                break;
            }
        }

        // Check if linked (primary user ID differs from our user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(userId);

        if (isLinked && actualPhone != null) {
            AuthRecipeUserInfo primaryRefetch = AuthRecipe.getUserById(process.getProcess(),
                    primaryUser.getSupertokensUserId());
            for (var lm : primaryRefetch.loginMethods) {
                if (lm.getSupertokensUserId().equals(userId)) {
                    assertEquals("Phone must be consistent", actualPhone, lm.phoneNumber);
                    break;
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-007: Read outside transaction race window
     *
     * Tests specifically targeting the wider race window from
     * Passwordless.updateUser reading user info outside the transaction.
     */
    @Test
    public void testPasswordlessReadOutsideTransactionRace() throws Exception {
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

        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        // Create passwordless user
        Passwordless.ConsumeCodeResponse response = createPasswordlessUser(process, "original@test.com", null);
        String userId = response.user.getSupertokensUserId();

        int ITERATIONS = 50;

        for (int i = 0; i < ITERATIONS; i++) {
            // Reset: unlink
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

            final int iteration = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    // The read happens BEFORE transaction starts
                    // This creates a wider race window
                    Passwordless.updateUser(process.getProcess(), userId,
                            new Passwordless.FieldUpdate("iteration" + iteration + "@test.com"), null);
                } catch (Exception e) {
                    // Expected
                }
            });

            executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.linkAccounts(process.getProcess(), userId, primaryUser.getSupertokensUserId());
                } catch (Exception e) {
                    // Expected
                }
            });

            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // Check consistency by querying reservation tables directly
            AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), userId);

            if (finalUser != null) {
                // Check if linked (primary user ID differs from our user ID)
                boolean isLinked = !finalUser.getSupertokensUserId().equals(userId);

                if (isLinked) {
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
     * TEST-007: Concurrent email AND phone update with linking
     */
    @Test
    public void testConcurrentEmailAndPhoneUpdateWithLinking() throws Exception {
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

        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        // Passwordless user with both email and phone
        Passwordless.CreateCodeResponse code = Passwordless.createCode(process.getProcess(),
                "user@test.com", "+1111111111", null, null);
        Passwordless.ConsumeCodeResponse response = Passwordless.consumeCode(process.getProcess(),
                code.deviceId, code.deviceIdHash, code.userInputCode, null);
        String userId = response.user.getSupertokensUserId();

        // Link first
        AuthRecipe.linkAccounts(process.getProcess(), userId, primaryUser.getSupertokensUserId());

        // Now concurrent email and phone updates
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Update email
        executor.submit(() -> {
            try {
                startLatch.await();
                Passwordless.updateUser(process.getProcess(), userId,
                        new Passwordless.FieldUpdate("newemail@test.com"), null);
            } catch (Exception e) {
                // Expected
            }
        });

        // Update phone
        executor.submit(() -> {
            try {
                startLatch.await();
                Passwordless.updateUser(process.getProcess(), userId, null,
                        new Passwordless.FieldUpdate("+2222222222"));
            } catch (Exception e) {
                // Expected
            }
        });

        // Another email update
        executor.submit(() -> {
            try {
                startLatch.await();
                Passwordless.updateUser(process.getProcess(), userId,
                        new Passwordless.FieldUpdate("anotheremail@test.com"), null);
            } catch (Exception e) {
                // Expected
            }
        });

        // Unlink/relink
        executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.unlinkAccounts(process.getProcess(), userId);
                AuthRecipe.linkAccounts(process.getProcess(), userId, primaryUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Verify BOTH email and phone are correctly reserved
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), userId);
        assertNotNull(finalUser);

        // Check if linked (primary user ID differs from our user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(userId);

        if (isLinked) {
            // Find the passwordless user's login method
            String actualEmail = null;
            String actualPhone = null;
            for (var loginMethod : finalUser.loginMethods) {
                if (loginMethod.getSupertokensUserId().equals(userId)) {
                    actualEmail = loginMethod.email;
                    actualPhone = loginMethod.phoneNumber;
                    break;
                }
            }

            AuthRecipeUserInfo primaryRefetch = AuthRecipe.getUserById(process.getProcess(),
                    primaryUser.getSupertokensUserId());

            for (var lm : primaryRefetch.loginMethods) {
                if (lm.getSupertokensUserId().equals(userId)) {
                    assertEquals("Email must match", actualEmail, lm.email);
                    assertEquals("Phone must match", actualPhone, lm.phoneNumber);
                    break;
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-007: Rapid Passwordless updates with linking
     */
    @Test
    public void testRapidPasswordlessUpdatesWithLinking() throws Exception {
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

        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        Passwordless.ConsumeCodeResponse response = createPasswordlessUser(process, "initial@test.com", null);
        String userId = response.user.getSupertokensUserId();

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

        // Email update threads
        for (int t = 0; t < 3; t++) {
            final int threadId = t;
            executor.submit(() -> {
                int i = 0;
                try {
                    startLatch.await();
                    while (running.get()) {
                        try {
                            Passwordless.updateUser(process.getProcess(), userId,
                                    new Passwordless.FieldUpdate("email_t" + threadId + "_i" + (i++) + "@test.com"),
                                    null);
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

        System.out.println("Completed " + operations.get() + " Passwordless operations");

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
}
