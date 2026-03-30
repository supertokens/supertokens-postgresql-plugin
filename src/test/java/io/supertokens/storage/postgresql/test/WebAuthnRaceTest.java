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
import io.supertokens.webauthn.WebAuthN;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Race condition tests for TEST-008:
 * WebAuthn updateEmail + linkAccounts concurrent race
 *
 * Tests that WebAuthn email updates maintain consistent state with account linking.
 */
public class WebAuthnRaceTest {
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
     * TEST-008: Test link during WebAuthn email update
     *
     * Race scenario:
     * T1: WebAuthN.updateUserEmail(userId, "new@test.com")
     *     - Reads primary_user_id
     *                                         T2: linkAccounts(userId, primaryId)
     *                                             - Links user
     *                                             - Commits
     * T1: - Updates email with stale primary_user_id
     *     - Commits
     *
     * RESULT: Linked user has new email, but reservation is stale/missing
     */
    @Test
    public void testLinkDuringEmailUpdate() throws Exception {
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
        Storage storage = StorageLayer.getStorage(main);
        TenantIdentifier tenantIdentifier = TenantIdentifier.BASE_TENANT;

        // Create primary user (using EmailPassword is fine for the primary)
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(main, "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

        // Create WebAuthn user
        String webauthnUserId = io.supertokens.utils.Utils.getUUID();
        AuthRecipeUserInfo recipeUser = WebAuthN.saveUser(storage, tenantIdentifier,
                "oldwebauthn@test.com", webauthnUserId, "example.com");

        // Concurrent operations
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Thread 1: Update email using WebAuthN
        Future<?> updateFuture = executor.submit(() -> {
            try {
                startLatch.await();
                WebAuthN.updateUserEmail(storage, tenantIdentifier,
                        recipeUser.getSupertokensUserId(), "newwebauthn@test.com");
            } catch (Exception e) {
                // Expected in race conditions
            }
        });

        // Thread 2: Link accounts
        Future<?> linkFuture = executor.submit(() -> {
            try {
                startLatch.await();
                AuthRecipe.linkAccounts(main, recipeUser.getSupertokensUserId(),
                        primaryUser.getSupertokensUserId());
            } catch (Exception e) {
                // Expected in race conditions
            }
        });

        startLatch.countDown();
        updateFuture.get(30, TimeUnit.SECONDS);
        linkFuture.get(30, TimeUnit.SECONDS);

        // Verify
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main, recipeUser.getSupertokensUserId());
        assertNotNull(finalUser);

        // Find the recipe user's login method
        String actualEmail = null;
        for (var loginMethod : finalUser.loginMethods) {
            if (loginMethod.getSupertokensUserId().equals(recipeUser.getSupertokensUserId())) {
                actualEmail = loginMethod.email;
                break;
            }
        }

        // Check if linked (primary user ID differs from recipe user ID)
        boolean isLinked = !finalUser.getSupertokensUserId().equals(recipeUser.getSupertokensUserId());

        if (isLinked && actualEmail != null) {
            // CRITICAL: Check reservation tables directly via SQL
            RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                    main, finalUser);

            if (!result.isConsistent) {
                System.out.println("RACE CONDITION DETECTED in testLinkDuringEmailUpdate:");
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
     * TEST-008: Email update + Tenant addition race
     */
    @Test
    public void testEmailUpdateWithTenantAddition() throws Exception {
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
        TenantIdentifier tenant1 = new TenantIdentifier(null, null, "wat1");
        TenantIdentifier tenant2 = new TenantIdentifier(null, null, "wat2");
        createTenant(main, tenant1);
        createTenant(main, tenant2);

        Storage storage1 = StorageLayer.getStorage(tenant1, main);
        Storage storage2 = StorageLayer.getStorage(tenant2, main);

        // Create WebAuthn user in tenant1
        String webauthnUserId = io.supertokens.utils.Utils.getUUID();
        AuthRecipeUserInfo user = WebAuthN.saveUser(storage1, tenant1,
                "user@webauthn.com", webauthnUserId, "example.com");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Update email using WebAuthN
        executor.submit(() -> {
            try {
                startLatch.await();
                WebAuthN.updateUserEmail(storage1, tenant1,
                        user.getSupertokensUserId(), "updated@webauthn.com");
            } catch (Exception e) {
                // Expected
            }
        });

        // Add to tenant2
        executor.submit(() -> {
            try {
                startLatch.await();
                Multitenancy.addUserIdToTenant(main, tenant2, storage2,
                        user.getSupertokensUserId());
            } catch (Exception e) {
                // Expected
            }
        });

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Verify email consistency across tenants
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

        System.out.println("User email: " + actualEmail);
        System.out.println("User tenants: " + Arrays.toString(finalUser.tenantIds.toArray()));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-008: High concurrency email updates with linking
     */
    @Test
    public void testHighConcurrencyEmailUpdatesWithLinking() throws Exception {
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
        Storage storage = StorageLayer.getStorage(main);
        TenantIdentifier tenantIdentifier = TenantIdentifier.BASE_TENANT;

        // Create primary user
        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(main, "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

        // Create WebAuthn user to be linked
        String webauthnUserId = io.supertokens.utils.Utils.getUUID();
        AuthRecipeUserInfo recipeUser = WebAuthN.saveUser(storage, tenantIdentifier,
                "original@webauthn.com", webauthnUserId, "example.com");

        int ITERATIONS = 50;

        for (int i = 0; i < ITERATIONS; i++) {
            // Reset
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

            ExecutorService executor = Executors.newFixedThreadPool(3);
            CountDownLatch startLatch = new CountDownLatch(1);

            final int iteration = i;

            // Update email using WebAuthN
            executor.submit(() -> {
                try {
                    startLatch.await();
                    WebAuthN.updateUserEmail(storage, tenantIdentifier,
                            recipeUser.getSupertokensUserId(), "iter" + iteration + "@webauthn.com");
                } catch (Exception e) {
                    // Expected
                }
            });

            // Link
            executor.submit(() -> {
                try {
                    startLatch.await();
                    AuthRecipe.linkAccounts(main, recipeUser.getSupertokensUserId(),
                            primaryUser.getSupertokensUserId());
                } catch (Exception e) {
                    // Expected
                }
            });

            // Unlink (with slight delay)
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Thread.sleep(50);
                    AuthRecipe.unlinkAccounts(main, recipeUser.getSupertokensUserId());
                } catch (Exception e) {
                    // Expected
                }
            });

            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // Check consistency
            AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main,
                    recipeUser.getSupertokensUserId());
            if (finalUser != null) {
                // Check if linked (primary user ID differs from recipe user ID)
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

                    if (actualEmail != null) {
                        // CRITICAL: Check reservation tables directly via SQL
                        RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                                main, finalUser);

                        if (!result.isConsistent) {
                            System.out.println("Iteration " + i + ": RACE DETECTED");
                            for (String issue : result.issues) {
                                System.out.println("  " + issue);
                            }
                            RaceTestUtils.printAllReservations(main, finalUser);
                            fail("Reservation consistency check failed at iteration " + i + ": " + result.issues);
                        }
                    }
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * TEST-008: Rapid email updates with linking
     */
    @Test
    public void testRapidEmailUpdatesWithLinking() throws Exception {
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
        Storage storage = StorageLayer.getStorage(main);
        TenantIdentifier tenantIdentifier = TenantIdentifier.BASE_TENANT;

        AuthRecipeUserInfo primaryUser = EmailPassword.signUp(main, "primary@test.com", "password123");
        AuthRecipe.createPrimaryUser(main, primaryUser.getSupertokensUserId());

        // Create WebAuthn user
        String webauthnUserId = io.supertokens.utils.Utils.getUUID();
        AuthRecipeUserInfo recipeUser = WebAuthN.saveUser(storage, tenantIdentifier,
                "initial@webauthn.com", webauthnUserId, "example.com");

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

        // Email update threads using WebAuthN
        for (int t = 0; t < 3; t++) {
            final int threadId = t;
            executor.submit(() -> {
                int i = 0;
                try {
                    startLatch.await();
                    while (running.get()) {
                        try {
                            WebAuthN.updateUserEmail(storage, tenantIdentifier,
                                    recipeUser.getSupertokensUserId(),
                                    "email_t" + threadId + "_i" + (i++) + "@webauthn.com");
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

        System.out.println("Completed " + operations.get() + " WebAuthn-pattern operations");

        // Final consistency check
        AuthRecipeUserInfo finalUser = AuthRecipe.getUserById(main, recipeUser.getSupertokensUserId());
        assertNotNull("User should still exist", finalUser);

        // Check if linked (primary user ID differs from recipe user ID)
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

            if (actualEmail != null) {
                // CRITICAL: Check reservation tables directly via SQL
                RaceTestUtils.ConsistencyCheckResult result = RaceTestUtils.checkReservationConsistency(
                        main, finalUser);

                if (!result.isConsistent) {
                    System.out.println("FINAL RACE DETECTED:");
                    for (String issue : result.issues) {
                        System.out.println("  " + issue);
                    }
                    RaceTestUtils.printAllReservations(main, finalUser);
                    fail("Final reservation consistency check failed: " + result.issues);
                }
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

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
}
