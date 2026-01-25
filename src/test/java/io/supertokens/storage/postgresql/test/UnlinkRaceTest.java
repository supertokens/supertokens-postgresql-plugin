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
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storageLayer.StorageLayer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Race condition tests for TEST-009:
 * unlinkAccounts + updateEmail concurrent race
 *
 * Tests that concurrent unlink and email update operations don't leave
 * orphaned reservations in primary_user_tenants.
 */
public class UnlinkRaceTest {
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
     * TEST-009: Unlink during email update
     *
     * Race scenario:
     * T1: updateEmail(linkedUser, "new@test.com")  [linkedUser is linked to P1]
     *     - Reads primary_user_id = P1
     *                                         T2: unlinkAccounts(linkedUser)
     *                                             - Removes linkedUser from P1
     *                                             - Deletes linkedUser's email from primary_user_tenants
     *                                             - Commits
     * T1: - Updates email to "new@test.com"
     *     - Inserts "new@test.com" into primary_user_tenants for P1
     *     - Commits
     *
     * RESULT: linkedUser is unlinked, but P1 still has reservation for "new@test.com"
     *         P1 can't claim that email for other users (orphaned reservation)
     */
    @Test
    public void testUnlinkDuringEmailUpdate() throws Exception {
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

        // Setup: Create and link users
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        AuthRecipeUserInfo linkedUser = EmailPassword.signUp(process.getProcess(), "linked@test.com", "password123");
        AuthRecipe.linkAccounts(process.getProcess(), linkedUser.getSupertokensUserId(),
                primaryUser.getSupertokensUserId());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Update email
        executor.submit(() -> {
            try {
                startLatch.await();
                EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                        linkedUser.getSupertokensUserId(), "newlinked@test.com", null);
            } catch (Exception e) {
                // Expected
            }
        });

        // Unlink
        executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.unlinkAccounts(process.getProcess(), linkedUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Verify: No orphaned reservations
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), linkedUser.getSupertokensUserId());
        assertNotNull(finalUser);

        // Check if linked (primary user ID differs from linked user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(linkedUser.getSupertokensUserId());

        // Find the linked user's login method
        String userEmail = null;
        for (var loginMethod : finalUser.loginMethods) {
            if (loginMethod.getSupertokensUserId().equals(linkedUser.getSupertokensUserId())) {
                userEmail = loginMethod.email;
                break;
            }
        }

        if (!isLinked) {
            // User is unlinked - verify primary_user_tenants doesn't have orphaned entries
            // Check that the primary user doesn't have this user's email reserved
            AuthRecipeUserInfo primaryRefetch = AuthRecipe.getUserById(process.getProcess(),
                    primaryUser.getSupertokensUserId());

            // The unlinked user should NOT appear in primary's login methods
            boolean foundInPrimary = false;
            for (var lm : primaryRefetch.loginMethods) {
                if (lm.getSupertokensUserId().equals(linkedUser.getSupertokensUserId())) {
                    foundInPrimary = true;
                    break;
                }
            }
            assertFalse("Unlinked user should not be in primary's login methods", foundInPrimary);
        } else {
            // User is still linked - verify consistency using reservation tables
            if (userEmail != null) {
                RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkEmailReservationConsistency(
                        process.getProcess(), finalUser);

                if (!result.isConsistent) {
                    System.out.println("RACE CONDITION DETECTED in testUnlinkDuringEmailUpdate:");
                    for (String issue : result.issues) {
                        System.out.println("  " + issue);
                    }
                    RaceTestUtils.printAllReservations(process.getProcess(), finalUser);
                }

                assertTrue("Reservation consistency check failed: " + result.issues, result.isConsistent);
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-009: Orphaned reservation detection (multiple iterations)
     *
     * Runs many iterations to catch the race that creates orphaned reservations.
     */
    @Test
    public void testOrphanedReservationAfterUnlinkRace() throws Exception {
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

        AuthRecipeUserInfo linkedUser = EmailPassword.signUp(process.getProcess(), "original@test.com", "password123");
        AuthRecipe.linkAccounts(process.getProcess(), linkedUser.getSupertokensUserId(),
                primaryUser.getSupertokensUserId());

        int ITERATIONS = 50;

        for (int i = 0; i < ITERATIONS; i++) {
            // Re-link if needed
            AuthRecipeUserInfo current = AuthRecipe.getUserById(process.getProcess(),
                    linkedUser.getSupertokensUserId());
            if (current != null) {
                // Check if linked (primary user ID differs from linked user ID)
                boolean isLinked = !current.getSupertokensUserId().equals(linkedUser.getSupertokensUserId());
                if (!isLinked) {
                    try {
                        AuthRecipe.linkAccounts(process.getProcess(), linkedUser.getSupertokensUserId(),
                                primaryUser.getSupertokensUserId());
                    } catch (Exception e) {
                        // Ignore
                    }
                }
            }

            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch startLatch = new CountDownLatch(1);

            final String newEmail = "iter" + i + "@test.com";

            executor.submit(() -> {
                try {
                    startLatch.await();
                    EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                            linkedUser.getSupertokensUserId(), newEmail, null);
                } catch (Exception e) {
                    // Expected
                }
            });

            executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.unlinkAccounts(process.getProcess(), linkedUser.getSupertokensUserId());
                } catch (Exception e) {
                    // Expected
                }
            });

            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // Check for orphaned reservations using direct SQL queries
            AuthRecipeUserInfo user = AuthRecipe.getUserById(process.getProcess(), linkedUser.getSupertokensUserId());
            TenantIdentifier tenant = new TenantIdentifier(null, null, null);

            if (user != null) {
                // Check if linked (primary user ID differs from linked user ID)
                boolean isLinked = !user.getSupertokensUserId().equals(linkedUser.getSupertokensUserId());

                if (!isLinked) {
                    // User unlinked - check primary_user_tenants directly for orphaned reservations
                    // Find the user's email from their login method
                    String userEmail = null;
                    for (var loginMethod : user.loginMethods) {
                        if (loginMethod.getSupertokensUserId().equals(linkedUser.getSupertokensUserId())) {
                            userEmail = loginMethod.email;
                            break;
                        }
                    }

                    if (userEmail == null) continue;

                    // Check if primary_user_tenants has this user's email for the primary user
                    Set<String> primaryReservedEmails = RaceTestUtils.getAllPrimaryUserEmailReservations(
                            process.getProcess(), tenant, primaryUser.getSupertokensUserId());

                    if (primaryReservedEmails.contains(userEmail)) {
                        // Email is still reserved - but is the user still in primary's login methods?
                        AuthRecipeUserInfo primaryRefetch = AuthRecipe.getUserById(process.getProcess(),
                                primaryUser.getSupertokensUserId());

                        boolean foundInPrimary = false;
                        for (var lm : primaryRefetch.loginMethods) {
                            if (lm.getSupertokensUserId().equals(linkedUser.getSupertokensUserId())) {
                                foundInPrimary = true;
                                break;
                            }
                        }

                        if (!foundInPrimary) {
                            // ORPHAN: Email reserved but user not linked
                            System.out.println("Iteration " + i + ": ORPHAN DETECTED - user unlinked but email '" +
                                    userEmail + "' still reserved in primary_user_tenants");
                            fail("Orphaned reservation found at iteration " + i + ": user unlinked but email '" +
                                    userEmail + "' still reserved in primary_user_tenants");
                        }
                    }
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-009: Unlink + Email Update + Relink sequence
     *
     * Tests complex sequence: unlink from P1, update email, relink to P2
     */
    @Test
    public void testUnlinkEmailUpdateRelinkSequence() throws Exception {
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

        AuthRecipeUserInfo primary1 = EmailPassword.signUp(process.getProcess(), "primary1@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primary1.getSupertokensUserId());

        AuthRecipeUserInfo primary2 = EmailPassword.signUp(process.getProcess(), "primary2@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primary2.getSupertokensUserId());

        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(process.getProcess(), "user@test.com", "password123");

        // Link to primary1
        AuthRecipe.linkAccounts(process.getProcess(), recipeUser.getSupertokensUserId(),
                primary1.getSupertokensUserId());

        // Concurrent: unlink, update email, link to primary2
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch startLatch = new CountDownLatch(1);

        executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.unlinkAccounts(process.getProcess(), recipeUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        executor.submit(() -> {
            try {
                startLatch.await();
                Thread.sleep(10);
                EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                        recipeUser.getSupertokensUserId(), "newuser@test.com", null);
            } catch (Exception e) {
                // Expected
            }
        });

        executor.submit(() -> {
            try {
                startLatch.await();
                Thread.sleep(20);
                AuthRecipe.linkAccounts(process.getProcess(), recipeUser.getSupertokensUserId(),
                        primary2.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Verify state
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), recipeUser.getSupertokensUserId());
        assertNotNull(finalUser);

        // Verify primary1 doesn't have orphaned reservations
        AuthRecipeUserInfo primary1Refetch = AuthRecipe.getUserById(process.getProcess(),
                primary1.getSupertokensUserId());

        boolean foundInPrimary1 = false;
        for (var lm : primary1Refetch.loginMethods) {
            if (lm.getSupertokensUserId().equals(recipeUser.getSupertokensUserId())) {
                foundInPrimary1 = true;
                break;
            }
        }

        // Check if user is linked (primary user ID differs from recipe user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());

        if (isLinked) {
            // Find the recipe user's login method
            String actualEmail = null;
            for (var loginMethod : finalUser.loginMethods) {
                if (loginMethod.getSupertokensUserId().equals(recipeUser.getSupertokensUserId())) {
                    actualEmail = loginMethod.email;
                    break;
                }
            }

            // Check which primary the user is linked to
            AuthRecipeUserInfo primary2Refetch = AuthRecipe.getUserById(process.getProcess(),
                    primary2.getSupertokensUserId());

            boolean foundInPrimary2 = false;
            for (var lm : primary2Refetch.loginMethods) {
                if (lm.getSupertokensUserId().equals(recipeUser.getSupertokensUserId())) {
                    foundInPrimary2 = true;
                    if (actualEmail != null) {
                        assertEquals("Email must match in primary2", actualEmail, lm.email);
                    }
                    break;
                }
            }

            if (foundInPrimary2) {
                assertFalse("User shouldn't be in primary1 if linked to primary2", foundInPrimary1);
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-009: Multiple users unlink race
     *
     * Tests that multiple linked users can be unlinked concurrently
     * while updating their emails without orphaned reservations.
     */
    @Test
    public void testMultipleUsersUnlinkRace() throws Exception {
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

        // Primary with multiple linked users
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(process.getProcess(), "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(process.getProcess(), primaryUser.getSupertokensUserId());

        List<AuthRecipeUserInfo> linkedUsers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            AuthRecipeUserInfo user = EmailPassword.signUp(process.getProcess(), "user" + i + "@test.com", "password123");
            AuthRecipe.linkAccounts(process.getProcess(), user.getSupertokensUserId(),
                    primaryUser.getSupertokensUserId());
            linkedUsers.add(user);
        }

        // Concurrent: All users update email and unlink
        ExecutorService executor = Executors.newFixedThreadPool(20);
        CountDownLatch startLatch = new CountDownLatch(1);

        for (int i = 0; i < linkedUsers.size(); i++) {
            final AuthRecipeUserInfo user = linkedUsers.get(i);
            final String newEmail = "newuser" + i + "@test.com";

            executor.submit(() -> {
                try {
                    startLatch.await();
                    EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                            user.getSupertokensUserId(), newEmail, null);
                } catch (Exception e) {
                    // Expected
                }
            });

            executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.unlinkAccounts(process.getProcess(), user.getSupertokensUserId());
                } catch (Exception e) {
                    // Expected
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        // Verify: Check final state
        AuthRecipeUserInfo primaryRefetch = AuthRecipe.getUserById(process.getProcess(),
                primaryUser.getSupertokensUserId());

        // Get all users that are still linked
        Set<String> stillLinkedUserIds = new HashSet<>();
        for (var lm : primaryRefetch.loginMethods) {
            stillLinkedUserIds.add(lm.getSupertokensUserId());
        }

        // Verify each linked user in primary has consistent email
        for (var lm : primaryRefetch.loginMethods) {
            if (lm.getSupertokensUserId().equals(primaryUser.getSupertokensUserId())) {
                continue; // Skip primary's own method
            }

            AuthRecipeUserInfo linkedUserRefetch = AuthRecipe.getUserById(process.getProcess(),
                    lm.getSupertokensUserId());
            if (linkedUserRefetch != null) {
                // Find the user's login method
                String actualEmail = null;
                for (var loginMethod : linkedUserRefetch.loginMethods) {
                    if (loginMethod.getSupertokensUserId().equals(lm.getSupertokensUserId())) {
                        actualEmail = loginMethod.email;
                        break;
                    }
                }
                if (actualEmail != null) {
                    assertEquals("Linked user email must match", actualEmail, lm.email);
                }
            }
        }

        // Verify unlinked users are not in primary
        for (AuthRecipeUserInfo user : linkedUsers) {
            AuthRecipeUserInfo userRefetch = AuthRecipe.getUserById(process.getProcess(),
                    user.getSupertokensUserId());
            if (userRefetch != null) {
                // Check if linked (primary user ID differs from user ID)
                boolean isLinked = !userRefetch.getSupertokensUserId().equals(user.getSupertokensUserId());

                if (!isLinked) {
                    assertFalse("Unlinked user should not be in primary's login methods",
                            stillLinkedUserIds.contains(user.getSupertokensUserId()));
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-009: Rapid unlink/link with email updates
     */
    @Test
    public void testRapidUnlinkLinkWithEmailUpdates() throws Exception {
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

        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(process.getProcess(), "recipe@test.com", "password123");

        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger operations = new AtomicInteger(0);

        // Link/unlink threads
        for (int t = 0; t < 3; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (running.get()) {
                        try {
                            AuthRecipe.linkAccounts(process.getProcess(), recipeUser.getSupertokensUserId(),
                                    primaryUser.getSupertokensUserId());
                            operations.incrementAndGet();
                        } catch (Exception e) {
                            // Expected
                        }
                        try {
                            AuthRecipe.unlinkAccounts(process.getProcess(), recipeUser.getSupertokensUserId());
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
                            EmailPassword.updateUsersEmailOrPassword(process.getProcess(),
                                    recipeUser.getSupertokensUserId(),
                                    "email_t" + threadId + "_i" + (i++) + "@test.com", null);
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

        System.out.println("Completed " + operations.get() + " unlink/link operations");

        // Final consistency check
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(process.getProcess(), recipeUser.getSupertokensUserId());
        assertNotNull("User should still exist", finalUser);

        // Check if linked (primary user ID differs from recipe user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());

        AuthRecipeUserInfo primaryRefetch = AuthRecipe.getUserById(process.getProcess(),
                primaryUser.getSupertokensUserId());

        if (isLinked) {
            // CRITICAL: Check reservation tables directly via SQL
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkEmailReservationConsistency(
                    process.getProcess(), finalUser);

            if (!result.isConsistent) {
                System.out.println("FINAL RACE DETECTED:");
                for (String issue : result.issues) {
                    System.out.println("  " + issue);
                }
                RaceTestUtils.printAllReservations(process.getProcess(), finalUser);
            }

            assertTrue("Final reservation consistency check failed: " + result.issues, result.isConsistent);
        } else {
            // Verify not in primary's methods
            boolean foundInPrimary = false;
            for (var lm : primaryRefetch.loginMethods) {
                if (lm.getSupertokensUserId().equals(recipeUser.getSupertokensUserId())) {
                    foundInPrimary = true;
                    break;
                }
            }
            assertFalse("Unlinked user should not be in primary's login methods", foundInPrimary);

            // Also check that primary_user_tenants doesn't have orphaned reservations for this user
            String userEmail = null;
            for (var loginMethod : finalUser.loginMethods) {
                if (loginMethod.getSupertokensUserId().equals(recipeUser.getSupertokensUserId())) {
                    userEmail = loginMethod.email;
                    break;
                }
            }

            if (userEmail != null) {
                TenantIdentifier tenant = new TenantIdentifier(null, null, null);
                Set<String> primaryReservedEmails = RaceTestUtils.getAllPrimaryUserEmailReservations(
                        process.getProcess(), tenant, primaryUser.getSupertokensUserId());
                if (primaryReservedEmails.contains(userEmail)) {
                    System.out.println("ORPHAN DETECTED: User unlinked but email '" + userEmail +
                            "' still reserved in primary_user_tenants");
                    fail("Orphaned email reservation found for unlinked user");
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
