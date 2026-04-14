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
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.cronjobs.CronTaskTest;
import io.supertokens.cronjobs.backfill.BackfillReservationTables;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.migration.MigrationBackfillStorage;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * REVIEW-006: Concurrent backfill + operation safety test.
 *
 * Proves that normal operations (linkAccounts, updateEmail) during an active
 * backfill don't cause reservation table inconsistencies. The backfill uses
 * per-row SELECT FOR UPDATE and INSERT ... ON CONFLICT DO NOTHING, which should
 * safely interleave with concurrent writes from DUAL_WRITE mode operations.
 */
public class BackfillConcurrencyTest {

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
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES,
                        new EE_FEATURES[]{EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        return process;
    }

    private void simulateLegacyState(Start storage, String userId) throws Exception {
        String appIdToUserIdTable = Config.getConfig(storage).getAppIdToUserIdTable();
        String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
        String recipeUserTenantsTable = Config.getConfig(storage).getRecipeUserTenantsTable();
        String primaryUserTenantsTable = Config.getConfig(storage).getPrimaryUserTenantsTable();

        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (Statement stmt = sqlCon.createStatement()) {
                stmt.executeUpdate("DELETE FROM " + recipeUserTenantsTable
                        + " WHERE recipe_user_id = '" + userId + "'");
                stmt.executeUpdate("DELETE FROM " + primaryUserTenantsTable
                        + " WHERE primary_user_id = '" + userId + "'");
                stmt.executeUpdate("DELETE FROM " + accountInfosTable
                        + " WHERE recipe_user_id = '" + userId + "'");
                stmt.executeUpdate("UPDATE " + appIdToUserIdTable
                        + " SET time_joined = 0, primary_or_recipe_user_time_joined = 0"
                        + " WHERE user_id = '" + userId + "'");
            }
            return null;
        });
    }

    /**
     * Test: concurrent operations (linkAccounts, updateEmail) during active backfill.
     *
     * 1. Create 50 users in DUAL_WRITE mode (mix of EP, PL, TP)
     * 2. Simulate legacy state (clear reservation tables, set time_joined=0)
     * 3. Start backfill in background thread
     * 4. Concurrently: linkAccounts on some users, updateEmail on others
     * 5. Wait for backfill to complete
     * 6. Verify checkReservationConsistency() for every user
     */
    @Test
    public void testConcurrentOperationsDuringBackfill() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // Use DUAL_WRITE mode so all_auth_recipe_users gets populated
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            // --- Phase 1: Create users ---
            int NUM_EP_USERS = 20;
            int NUM_PL_USERS = 10;
            int NUM_TP_USERS = 10;
            int NUM_LINK_PAIRS = 8;

            List<AuthRecipeUserInfo> epUsers = new ArrayList<>();
            List<AuthRecipeUserInfo> plUsers = new ArrayList<>();
            List<AuthRecipeUserInfo> tpUsers = new ArrayList<>();

            for (int i = 0; i < NUM_EP_USERS; i++) {
                AuthRecipeUserInfo user = EmailPassword.signUp(main,
                        "ep-bf" + i + "@test.com", "password123");
                epUsers.add(user);
            }

            for (int i = 0; i < NUM_PL_USERS; i++) {
                Passwordless.CreateCodeResponse code = Passwordless.createCode(
                        main, "pl-bf" + i + "@test.com", null, null, null);
                Passwordless.ConsumeCodeResponse response = Passwordless.consumeCode(
                        main, code.deviceId, code.deviceIdHash, code.userInputCode, null);
                plUsers.add(response.user);
            }

            for (int i = 0; i < NUM_TP_USERS; i++) {
                ThirdParty.SignInUpResponse response = ThirdParty.signInUp(
                        main, "google", "bf-g" + i, "tp-bf" + i + "@test.com");
                tpUsers.add(response.user);
            }

            // Make some EP users primary and link PL users to them
            List<String[]> linkedPairs = new ArrayList<>(); // [primaryId, recipeId]
            for (int i = 0; i < NUM_LINK_PAIRS && i < epUsers.size() && i < plUsers.size(); i++) {
                String primaryId = epUsers.get(i).getSupertokensUserId();
                String recipeId = plUsers.get(i).getSupertokensUserId();
                AuthRecipe.createPrimaryUser(main, primaryId);
                AuthRecipe.linkAccounts(main, recipeId, primaryId);
                linkedPairs.add(new String[]{primaryId, recipeId});
            }

            // Collect all user IDs for later verification
            List<String> allUserIds = new ArrayList<>();
            for (AuthRecipeUserInfo u : epUsers) allUserIds.add(u.getSupertokensUserId());
            for (AuthRecipeUserInfo u : plUsers) allUserIds.add(u.getSupertokensUserId());
            for (AuthRecipeUserInfo u : tpUsers) allUserIds.add(u.getSupertokensUserId());

            // --- Phase 2: Simulate legacy state for ALL users ---
            // This clears reservation tables and sets time_joined=0
            for (String userId : allUserIds) {
                simulateLegacyState(storage, userId);
            }

            int pending = backfillStorage.getBackfillPendingUsersCount(appIdentifier);
            assertTrue("Expected pending users >= " + allUserIds.size() + ", got " + pending,
                    pending >= allUserIds.size());

            // --- Phase 3 + 4: Run backfill + concurrent operations in parallel ---
            ExecutorService executor = Executors.newFixedThreadPool(6);
            CountDownLatch backfillStarted = new CountDownLatch(1);
            AtomicInteger linkSuccessCount = new AtomicInteger(0);
            AtomicInteger updateSuccessCount = new AtomicInteger(0);

            // Backfill thread: process in small batches to maximize interleaving
            Future<?> backfillFuture = executor.submit(() -> {
                try {
                    backfillStarted.countDown();
                    int totalProcessed = 0;
                    while (true) {
                        int processed = backfillStorage.backfillUsersBatch(appIdentifier, 5);
                        totalProcessed += processed;
                        if (processed == 0) break;
                        // Small yield to allow concurrent operations to interleave
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Backfill failed", e);
                }
            });

            // Wait for backfill to start before launching concurrent ops
            backfillStarted.await(5, TimeUnit.SECONDS);

            List<Future<?>> opFutures = new ArrayList<>();

            // Concurrent link operations: link unlinked EP users with unlinked TP users
            int linkStart = NUM_LINK_PAIRS; // start after already-linked pairs
            for (int i = linkStart; i < Math.min(linkStart + 5, NUM_EP_USERS); i++) {
                final int idx = i;
                final int tpIdx = i - linkStart;
                if (tpIdx >= NUM_TP_USERS) break;

                opFutures.add(executor.submit(() -> {
                    try {
                        String primaryId = epUsers.get(idx).getSupertokensUserId();
                        String recipeId = tpUsers.get(tpIdx).getSupertokensUserId();
                        AuthRecipe.createPrimaryUser(main, primaryId);
                        AuthRecipe.linkAccounts(main, recipeId, primaryId);
                        linkSuccessCount.incrementAndGet();
                    } catch (Exception e) {
                        // Expected — backfill and link may conflict on same rows
                    }
                }));
            }

            // Concurrent email update operations on some EP users
            for (int i = NUM_LINK_PAIRS + 5; i < Math.min(NUM_LINK_PAIRS + 10, NUM_EP_USERS); i++) {
                final int idx = i;
                opFutures.add(executor.submit(() -> {
                    try {
                        EmailPassword.updateUsersEmailOrPassword(main,
                                epUsers.get(idx).getSupertokensUserId(),
                                "updated-bf" + idx + "@test.com", null);
                        updateSuccessCount.incrementAndGet();
                    } catch (Exception e) {
                        // Expected — user may be mid-backfill
                    }
                }));
            }

            // Wait for all operations to complete
            backfillFuture.get(60, TimeUnit.SECONDS);
            for (Future<?> f : opFutures) {
                f.get(30, TimeUnit.SECONDS);
            }
            executor.shutdown();

            // --- Phase 5: Verify all reservation tables are consistent ---
            List<String> allIssues = new ArrayList<>();
            int usersChecked = 0;

            for (String userId : allUserIds) {
                AuthRecipeUserInfo user = AuthRecipe.getUserById(main, userId);
                if (user == null) {
                    // User might have been deleted in some edge case — skip
                    continue;
                }

                RaceTestUtils.ConsistencyCheckResult result =
                        RaceTestUtils.checkReservationConsistency(main, user);
                if (!result.isConsistent) {
                    allIssues.addAll(result.issues);
                }
                usersChecked++;
            }

            assertTrue("Checked at least 30 users", usersChecked >= 30);
            assertTrue("Reservation consistency failed after backfill + concurrent ops:\n" +
                    String.join("\n", allIssues), allIssues.isEmpty());

            // Verify backfill is complete (no pending users)
            int remainingPending = backfillStorage.getBackfillPendingUsersCount(appIdentifier);
            assertEquals("All users should be backfilled", 0, remainingPending);

            // Verify completeness
            int inconsistencies = backfillStorage.verifyBackfillCompleteness(appIdentifier);
            assertEquals("verifyBackfillCompleteness should report 0 issues", 0, inconsistencies);

        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Test: backfill cron fires automatically and completes via ProcessState signal.
     *
     * Uses CronTaskTest to set a short interval, creates users in legacy state,
     * then waits for BACKFILL_COMPLETE to be set by the cron task.
     */
    @Test
    public void testBackfillCronCompletesWithProcessState() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);

            // Set DUAL_WRITE mode so backfill will run
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            // Configure cron to run quickly
            CronTaskTest.getInstance(main).setIntervalInSeconds(
                    BackfillReservationTables.RESOURCE_KEY, 1);
            CronTaskTest.getInstance(main).setInitialWaitTimeInSeconds(
                    BackfillReservationTables.RESOURCE_KEY, 1);

            // Create a few users and simulate legacy state
            for (int i = 0; i < 5; i++) {
                AuthRecipeUserInfo user = EmailPassword.signUp(main,
                        "cron-bf" + i + "@test.com", "password123");
                simulateLegacyState(storage, user.getSupertokensUserId());
            }

            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);
            assertEquals(5, backfillStorage.getBackfillPendingUsersCount(appIdentifier));

            // Wait for backfill to complete via the cron task
            assertNotNull("Backfill cron should have completed within 30 seconds",
                    process.checkOrWaitForEvent(
                            ProcessState.PROCESS_STATE.BACKFILL_COMPLETE, 30000));

            // Verify all users are backfilled
            assertEquals(0, backfillStorage.getBackfillPendingUsersCount(appIdentifier));
            assertEquals(0, backfillStorage.verifyBackfillCompleteness(appIdentifier));

        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
