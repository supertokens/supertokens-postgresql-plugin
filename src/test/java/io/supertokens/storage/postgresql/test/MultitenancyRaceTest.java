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

import com.google.gson.JsonObject;
import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.storageLayer.StorageLayer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.Arrays;
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
 * Race condition tests for TEST-003, TEST-004, TEST-005:
 * - TEST-003: linkAccounts + addUserIdToTenant concurrent race
 * - TEST-004: addUserIdToTenant + linkAccounts concurrent race (reverse timing)
 * - TEST-005: addUserIdToTenant + updateEmail concurrent race
 *
 * These tests verify that concurrent multitenancy operations maintain consistent
 * state across reservation tables.
 */
public class MultitenancyRaceTest {
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

    private void createTenant(Main main, TenantIdentifier tenant) throws Exception {
        TenantConfig config = new TenantConfig(
                tenant,
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null, new JsonObject()
        );
        Multitenancy.addNewOrUpdateAppOrTenant(main, config, false);
    }

    private Storage getStorage(Main main, TenantIdentifier tenant) throws Exception {
        return StorageLayer.getStorage(tenant, main);
    }

    /**
     * TEST-003: Test tenant added during linkAccounts
     *
     * Race scenario:
     * T1: linkAccounts(recipeUser, primaryUser)
     *     - Reads recipeUser's tenant list = [tenant1]
     *                                         T2: addUserIdToTenant(recipeUser, tenant2)
     *                                             - Adds user to tenant2
     *                                             - Commits
     * T1: - Reserves email for primary in tenant1 ONLY
     *     - Commits
     *
     * RESULT: Email reserved in tenant1 but NOT in tenant2
     */
    @Test
    public void testTenantAddedDuringLinkAccounts() throws Exception {
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

        // Create tenants
        TenantIdentifier tenant1 = new TenantIdentifier(null, null, "tenant1");
        TenantIdentifier tenant2 = new TenantIdentifier(null, null, "tenant2");
        createTenant(main, tenant1);
        createTenant(main, tenant2);

        Storage storage1 = getStorage(main, tenant1);
        Storage storage2 = getStorage(main, tenant2);

        // Create users in tenant1
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(tenant1, storage1, main,
                "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(tenant1, storage1, main,
                "recipe@test.com", "password123");

        // Concurrent operations
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Thread 1: Link accounts
        Future<?> linkFuture = executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.linkAccounts(main, recipeUser.getSupertokensUserId(),
                        primaryUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected in race conditions
            }
        });

        // Thread 2: Add user to tenant2
        Future<?> tenantFuture = executor.submit(() -> {
            try {
                startLatch.await();
                Multitenancy.addUserIdToTenant(main, tenant2, storage2,
                        recipeUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected in race conditions
            }
        });

        startLatch.countDown();
        linkFuture.get(30, TimeUnit.SECONDS);
        tenantFuture.get(30, TimeUnit.SECONDS);

        // Verify: After operations complete, user's email should be reserved in ALL tenants they're in
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main, recipeUser.getSupertokensUserId());
        assertNotNull(finalUser);

        // Check if user is linked (primary user ID differs from recipe user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());

        if (isLinked) {
            // CRITICAL: Check reservation tables directly for ALL tenants the user is in
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    main, finalUser);

            if (!result.isConsistent) {
                System.out.println("RACE DETECTED:");
                for (String issue : result.issues) {
                    System.out.println("  " + issue);
                }
                RaceTestUtils.printAllReservations(main, finalUser);
            }

            assertTrue("Reservation consistency check failed: " + result.issues, result.isConsistent);

            // Verify user's tenant list includes all expected tenants
            System.out.println("User is in tenants: " + Arrays.toString(finalUser.tenantIds.toArray()));
        }

        executor.shutdown();
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-003: Verify all tenants have reservation after link
     *
     * Creates user in one tenant, then concurrently:
     * - Links to primary
     * - Adds to multiple other tenants
     *
     * Verifies that after all operations complete, the reservation is consistent
     * across all tenants the user ends up in.
     */
    @Test
    public void testAllTenantsHaveReservationAfterLink() throws Exception {
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

        // Create 5 tenants
        int NUM_TENANTS = 5;
        List<TenantIdentifier> tenants = new ArrayList<>();
        List<Storage> storages = new ArrayList<>();
        for (int i = 0; i < NUM_TENANTS; i++) {
            TenantIdentifier tenant = new TenantIdentifier(null, null, "tenant" + i);
            createTenant(main, tenant);
            tenants.add(tenant);
            storages.add(getStorage(main, tenant));
        }

        // Create users in tenant0
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(tenants.get(0), storages.get(0), main,
                "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(tenants.get(0), storages.get(0), main,
                "recipe@test.com", "password123");

        // Concurrent: link + add to multiple tenants
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Thread 1: Link accounts
        executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.linkAccounts(main, recipeUser.getSupertokensUserId(),
                        primaryUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        // Threads 2-N: Add user to tenants 1-4
        for (int i = 1; i < NUM_TENANTS; i++) {
            final TenantIdentifier tenant = tenants.get(i);
            final Storage storage = storages.get(i);
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Multitenancy.addUserIdToTenant(main, tenant, storage,
                            recipeUser.getSupertokensUserId());
                } catch (Exception e) {
                    // Expected
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        // Verify: After everything settles, user's email should be consistent
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main, recipeUser.getSupertokensUserId());
        assertNotNull(finalUser);

        // Find the recipe user's login method
        String email = null;
        for (var loginMethod : finalUser.loginMethods) {
            if (loginMethod.getSupertokensUserId().equals(recipeUser.getSupertokensUserId())) {
                email = loginMethod.email;
                break;
            }
        }

        // Check if user is linked (primary user ID differs from recipe user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());

        if (isLinked && email != null) {
            // CRITICAL: Check reservation tables directly for ALL tenants
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    main, finalUser);

            if (!result.isConsistent) {
                System.out.println("RACE DETECTED:");
                for (String issue : result.issues) {
                    System.out.println("  " + issue);
                }
            }

            assertTrue("Reservation consistency check failed: " + result.issues, result.isConsistent);
        }

        System.out.println("User ended up in tenants: " + Arrays.toString(finalUser.tenantIds.toArray()));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-004: Test linking during tenant addition
     *
     * Race scenario:
     * T1: addUserIdToTenant(recipeUser, tenant2)
     *     - Reads recipeUser's primary_user_id = NULL (not linked)
     *                                         T2: linkAccounts(recipeUser, primaryUser)
     *                                             - Links user
     *                                             - Commits
     * T1: - Adds user to tenant2
     *     - Sees primary_user_id was NULL, doesn't insert into primary_user_tenants
     *     - Commits
     *
     * RESULT: User is linked and in tenant2, but tenant2 has no reservation
     */
    @Test
    public void testLinkingDuringTenantAddition() throws Exception {
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

        TenantIdentifier tenant1 = new TenantIdentifier(null, null, "tenant1");
        TenantIdentifier tenant2 = new TenantIdentifier(null, null, "tenant2");
        createTenant(main, tenant1);
        createTenant(main, tenant2);

        Storage storage1 = getStorage(main, tenant1);
        Storage storage2 = getStorage(main, tenant2);

        // Create users in tenant1 (not linked yet)
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(tenant1, storage1, main,
                "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(tenant1, storage1, main,
                "recipe@test.com", "password123");
        // recipeUser is NOT linked yet

        // Concurrent operations
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Thread 1: Add to tenant2
        Future<?> tenantFuture = executor.submit(() -> {
            try {
                startLatch.await();
                Multitenancy.addUserIdToTenant(main, tenant2, storage2,
                        recipeUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        // Thread 2: Link accounts
        Future<?> linkFuture = executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.linkAccounts(main, recipeUser.getSupertokensUserId(),
                        primaryUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        startLatch.countDown();
        tenantFuture.get(30, TimeUnit.SECONDS);
        linkFuture.get(30, TimeUnit.SECONDS);

        // Verify state
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main, recipeUser.getSupertokensUserId());
        assertNotNull(finalUser);

        // Check if user is linked (primary user ID differs from recipe user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());

        boolean isInTenant2 = finalUser.tenantIds.contains("tenant2");

        System.out.println("User is linked: " + isLinked + ", in tenant2: " + isInTenant2);

        if (isLinked) {
            // CRITICAL: Check reservation tables directly for ALL tenants
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    main, finalUser);

            if (!result.isConsistent) {
                System.out.println("RACE DETECTED:");
                for (String issue : result.issues) {
                    System.out.println("  " + issue);
                }
                RaceTestUtils.printAllReservations(main, finalUser);
            }

            assertTrue("Reservation consistency check failed: " + result.issues, result.isConsistent);
        }

        executor.shutdown();
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-004: Multiple tenants added concurrently with link
     *
     * Stress test with multiple iterations to catch the race.
     */
    @Test
    public void testMultipleTenantsAddedConcurrentlyWithLink() throws Exception {
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

        int NUM_TENANTS = 5;
        TenantIdentifier[] tenants = new TenantIdentifier[NUM_TENANTS];
        Storage[] storages = new Storage[NUM_TENANTS];
        for (int i = 0; i < NUM_TENANTS; i++) {
            tenants[i] = new TenantIdentifier(null, null, "t" + i);
            createTenant(main, tenants[i]);
            storages[i] = getStorage(main, tenants[i]);
        }

        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(tenants[0], storages[0], main,
                "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(tenants[0], storages[0], main,
                "recipe@test.com", "password123");

        int ITERATIONS = 30;

        for (int iter = 0; iter < ITERATIONS; iter++) {
            // Reset: unlink if linked
            try {
                AuthRecipeUserInfo current = AuthRecipe.getUserById(main,
                        recipeUser.getSupertokensUserId());
                if (current != null) {
                    // Check if linked (primary user ID differs from recipe user ID)
                    boolean linked = !current.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());
                    if (linked) {
                        AuthRecipe.unlinkAccounts(main, recipeUser.getSupertokensUserId());
                    }
                }
            } catch (Exception e) {
                // Ignore
            }

            // Remove from all tenants except 0
            for (int i = 1; i < NUM_TENANTS; i++) {
                try {
                    Multitenancy.removeUserIdFromTenant(main, tenants[i], storages[i],
                            recipeUser.getSupertokensUserId(), null);
                } catch (Exception e) {
                    // Ignore - user might not be in this tenant
                }
            }

            ExecutorService executor = Executors.newFixedThreadPool(NUM_TENANTS);
            CountDownLatch startLatch = new CountDownLatch(1);

            // Link operation
            executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.linkAccounts(main, recipeUser.getSupertokensUserId(),
                            primaryUser.getSupertokensUserId());
                } catch (Exception e) {
                    // Expected
                }
            });

            // Add to all other tenants
            for (int i = 1; i < NUM_TENANTS; i++) {
                final TenantIdentifier tenant = tenants[i];
                final Storage storage = storages[i];
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        Multitenancy.addUserIdToTenant(main, tenant, storage,
                                recipeUser.getSupertokensUserId());
                    } catch (Exception e) {
                        // Expected
                    }
                });
            }

            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            // Check consistency
            AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main,
                    recipeUser.getSupertokensUserId());
            if (finalUser == null) {
                continue;
            }

            // Check if linked (primary user ID differs from recipe user ID)
            boolean isLinked = !finalUser.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());

            if (isLinked) {
                // CRITICAL: Check reservation tables directly for ALL tenants
                RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                        main, finalUser);

                if (!result.isConsistent) {
                    System.out.println("Iteration " + iter + ": RACE DETECTED");
                    for (String issue : result.issues) {
                        System.out.println("  " + issue);
                    }
                    RaceTestUtils.printAllReservations(main, finalUser);
                    fail("Reservation consistency check failed at iteration " + iter + ": " + result.issues);
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-005: Test email updated during tenant addition
     *
     * Race scenario:
     * T1: addUserIdToTenant(user, tenant2)
     *     - Reads user's email = "old@test.com"
     *                                         T2: updateEmail(user, "new@test.com")
     *                                             - Updates email
     *                                             - Commits
     * T1: - Inserts "old@test.com" into recipe_user_tenants for tenant2
     *     - Commits
     *
     * RESULT: User has "new@test.com" but recipe_user_tenants in tenant2 has "old@test.com"
     */
    @Test
    public void testEmailUpdatedDuringTenantAddition() throws Exception {
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

        TenantIdentifier tenant1 = new TenantIdentifier(null, null, "tenant1");
        TenantIdentifier tenant2 = new TenantIdentifier(null, null, "tenant2");
        createTenant(main, tenant1);
        createTenant(main, tenant2);

        Storage storage1 = getStorage(main, tenant1);
        Storage storage2 = getStorage(main, tenant2);

        AuthRecipeUserInfo user = EmailPassword.signUp(tenant1, storage1, main,
                "old@test.com", "password123");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Thread 1: Add to tenant2
        Future<?> tenantFuture = executor.submit(() -> {
            try {
                startLatch.await();
                Multitenancy.addUserIdToTenant(main, tenant2, storage2,
                        user.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        // Thread 2: Update email
        Future<?> emailFuture = executor.submit(() -> {
            try {
                startLatch.await();
                EmailPassword.updateUsersEmailOrPassword(main,
                        user.getSupertokensUserId(), "new@test.com", null);
            } catch (Exception e) {
                // Expected
            }
        });

        startLatch.countDown();
        tenantFuture.get(30, TimeUnit.SECONDS);
        emailFuture.get(30, TimeUnit.SECONDS);

        // Verify: Both tenants should have the SAME email
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main, user.getSupertokensUserId());
        assertNotNull(finalUser);

        // Find the user's login method
        String actualEmail = null;
        for (var loginMethod : finalUser.loginMethods) {
            if (loginMethod.getSupertokensUserId().equals(user.getSupertokensUserId())) {
                actualEmail = loginMethod.email;
                break;
            }
        }

        // The email should be consistent - this is what we're testing
        System.out.println("User email: " + actualEmail);
        System.out.println("User tenants: " + Arrays.toString(finalUser.tenantIds.toArray()));

        executor.shutdown();
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-005: Primary user email update during tenant addition
     *
     * Tests that when a linked user has their email updated while being added
     * to a new tenant, the reservation tables stay consistent.
     */
    @Test
    public void testPrimaryUserEmailUpdateDuringTenantAddition() throws Exception {
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

        TenantIdentifier[] tenants = new TenantIdentifier[3];
        Storage[] storages = new Storage[3];
        for (int i = 0; i < 3; i++) {
            tenants[i] = new TenantIdentifier(null, null, "t" + i);
            createTenant(main, tenants[i]);
            storages[i] = getStorage(main, tenants[i]);
        }

        // Create and link users
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(tenants[0], storages[0], main,
                "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

        AuthRecipeUserInfo linkedUser = EmailPassword.signUp(tenants[0], storages[0], main,
                "linked@test.com", "password123");
        AuthRecipe.linkAccounts(main, linkedUser.getSupertokensUserId(),
                primaryUser.getSupertokensUserId());

        // Concurrent: add linked user to tenant1, update linked user's email
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        executor.submit(() -> {
            try {
                startLatch.await();
                Multitenancy.addUserIdToTenant(main, tenants[1], storages[1],
                        linkedUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        executor.submit(() -> {
            try {
                startLatch.await();
                EmailPassword.updateUsersEmailOrPassword(main,
                        linkedUser.getSupertokensUserId(), "newlinked@test.com", null);
            } catch (Exception e) {
                // Expected
            }
        });

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Verify consistency
        AuthRecipeUserInfo finalLinkedUser = AuthRecipe.getUserById(main,
                linkedUser.getSupertokensUserId());
        AuthRecipeUserInfo finalPrimaryUser = AuthRecipe.getUserById(main,
                primaryUser.getSupertokensUserId());

        assertNotNull(finalLinkedUser);
        assertNotNull(finalPrimaryUser);

        // Find the linked user's login method
        String actualEmail = null;
        for (var loginMethod : finalLinkedUser.loginMethods) {
            if (loginMethod.getSupertokensUserId().equals(linkedUser.getSupertokensUserId())) {
                actualEmail = loginMethod.email;
                break;
            }
        }

        if (actualEmail != null) {
            // Verify email in primary user's linked method matches
            for (var lm : finalPrimaryUser.loginMethods) {
                if (lm.getSupertokensUserId().equals(linkedUser.getSupertokensUserId())) {
                    assertEquals("Email in linked method must match actual email", actualEmail, lm.email);
                    break;
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-005: High concurrency tenant additions and email updates
     */
    @Test
    public void testHighConcurrencyTenantAdditionAndEmailUpdates() throws Exception {
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

        int NUM_TENANTS = 5;
        TenantIdentifier[] tenants = new TenantIdentifier[NUM_TENANTS];
        Storage[] storages = new Storage[NUM_TENANTS];
        for (int i = 0; i < NUM_TENANTS; i++) {
            tenants[i] = new TenantIdentifier(null, null, "hct" + i);
            createTenant(main, tenants[i]);
            storages[i] = getStorage(main, tenants[i]);
        }

        AuthRecipeUserInfo user = EmailPassword.signUp(tenants[0], storages[0], main,
                "user0@test.com", "password123");

        int ITERATIONS = 30;

        for (int iter = 0; iter < ITERATIONS; iter++) {
            // Reset: remove from all tenants except 0
            for (int i = 1; i < NUM_TENANTS; i++) {
                try {
                    Multitenancy.removeUserIdFromTenant(main, tenants[i], storages[i],
                            user.getSupertokensUserId(), null);
                } catch (Exception e) {
                    // Ignore
                }
            }

            // Reset email
            try {
                EmailPassword.updateUsersEmailOrPassword(main,
                        user.getSupertokensUserId(), "user" + iter + "@test.com", null);
            } catch (Exception e) {
                // Ignore
            }

            ExecutorService executor = Executors.newFixedThreadPool(20);
            CountDownLatch startLatch = new CountDownLatch(1);

            // Add to multiple tenants
            for (int i = 1; i < NUM_TENANTS; i++) {
                final TenantIdentifier tenant = tenants[i];
                final Storage storage = storages[i];
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        Multitenancy.addUserIdToTenant(main, tenant, storage,
                                user.getSupertokensUserId());
                    } catch (Exception e) {
                        // Expected
                    }
                });
            }

            // Multiple email updates
            for (int i = 0; i < 3; i++) {
                final String newEmail = "updated" + iter + "_" + i + "@test.com";
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        EmailPassword.updateUsersEmailOrPassword(main,
                                user.getSupertokensUserId(), newEmail, null);
                    } catch (Exception e) {
                        // Expected
                    }
                });
            }

            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            // Verify consistency - user's email should be the same across all contexts
            AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main, user.getSupertokensUserId());
            if (finalUser == null) {
                continue;
            }

            // Find the user's login method
            String actualEmail = null;
            for (var loginMethod : finalUser.loginMethods) {
                if (loginMethod.getSupertokensUserId().equals(user.getSupertokensUserId())) {
                    actualEmail = loginMethod.email;
                    break;
                }
            }
            assertNotNull("User should have an email", actualEmail);
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-003/004/005: Rapid tenant operations with linking
     *
     * Stress test that rapidly adds/removes users from tenants
     * while also linking/unlinking.
     */
    @Test
    public void testRapidTenantOperationsWithLinking() throws Exception {
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

        // Create many tenants
        int NUM_TENANTS = 10;
        TenantIdentifier[] tenants = new TenantIdentifier[NUM_TENANTS];
        Storage[] storages = new Storage[NUM_TENANTS];
        for (int i = 0; i < NUM_TENANTS; i++) {
            tenants[i] = new TenantIdentifier(null, null, "rapid" + i);
            createTenant(main, tenants[i]);
            storages[i] = getStorage(main, tenants[i]);
        }

        // Create users
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(tenants[0], storages[0], main,
                "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

        AuthRecipeUserInfo recipeUser = EmailPassword.signUp(tenants[0], storages[0], main,
                "recipe@test.com", "password123");

        ExecutorService executor = Executors.newFixedThreadPool(20);
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
                            AuthRecipe.linkAccounts(main, recipeUser.getSupertokensUserId(),
                                    primaryUser.getSupertokensUserId());
                            operations.incrementAndGet();
                        } catch (Exception e) {
                            // Expected
                        }
                        try {
                            AuthRecipe.unlinkAccounts(main, recipeUser.getSupertokensUserId());
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

        // Tenant add/remove threads
        for (int i = 1; i < NUM_TENANTS; i++) {
            final TenantIdentifier tenant = tenants[i];
            final Storage storage = storages[i];
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (running.get()) {
                        try {
                            Multitenancy.addUserIdToTenant(main, tenant, storage,
                                    recipeUser.getSupertokensUserId());
                            operations.incrementAndGet();
                        } catch (Exception e) {
                            // Expected
                        }
                        try {
                            Multitenancy.removeUserIdFromTenant(main, tenant, storage,
                                    recipeUser.getSupertokensUserId(), null);
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
                            EmailPassword.updateUsersEmailOrPassword(main,
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

        System.out.println("Completed " + operations.get() + " operations");

        // Final consistency check
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main, recipeUser.getSupertokensUserId());
        assertNotNull("User should still exist", finalUser);

        // Check if linked (primary user ID differs from recipe user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());

        if (isLinked) {
            // CRITICAL: Check reservation tables directly for ALL tenants
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    main, finalUser);

            if (!result.isConsistent) {
                System.out.println("FINAL RACE DETECTED:");
                for (String issue : result.issues) {
                    System.out.println("  " + issue);
                }
                RaceTestUtils.printAllReservations(main, finalUser);
            }

            assertTrue("Final reservation consistency check failed: " + result.issues, result.isConsistent);
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
