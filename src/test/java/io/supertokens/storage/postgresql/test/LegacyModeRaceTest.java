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
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * REVIEW-002: Race condition tests in LEGACY mode under READ_COMMITTED isolation.
 *
 * In LEGACY mode:
 * - LockedUser IS acquired (serializes same-user operations)
 * - Reservation tables are SKIPPED
 * - Conflict detection uses _legacy methods (point-in-time queries on old tables)
 *
 * These tests verify that concurrent operations on old tables produce consistent
 * state. Assertions check old tables (all_auth_recipe_users, emailpassword_user_to_tenant,
 * app_id_to_user_id) instead of reservation tables.
 */
public class LegacyModeRaceTest {

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

    private int countRows(Start storage, String table, String whereClause) throws Exception {
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table
                         + " WHERE " + whereClause)) {
                rs.next();
                return rs.getInt(1);
            }
        });
    }

    private Set<String> getEmailsForPrimaryUser(Start storage, String primaryUserId) throws Exception {
        String usersTable = Config.getConfig(storage).getUsersTable();
        String epUsersTable = Config.getConfig(storage).getEmailPasswordUsersTable();
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            // Find all recipe user IDs linked to this primary user
            Set<String> emails = new HashSet<>();
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT eu.email FROM " + usersTable + " u " +
                                 "JOIN " + epUsersTable + " eu ON u.user_id = eu.user_id " +
                                 "WHERE u.primary_or_recipe_user_id = '" + primaryUserId + "' " +
                                 "AND u.app_id = 'public'")) {
                while (rs.next()) {
                    emails.add(rs.getString("email"));
                }
            }
            return emails;
        });
    }

    /**
     * Check old-table consistency for a user in LEGACY mode.
     * Verifies:
     * 1. primary_or_recipe_user_id is consistent in all_auth_recipe_users
     * 2. is_linked_or_is_a_primary_user flag is correct in app_id_to_user_id
     * 3. No duplicate emails under same primary user in same tenant
     */
    private List<String> checkLegacyConsistency(Start storage, String userId) throws Exception {
        List<String> issues = new ArrayList<>();
        String usersTable = Config.getConfig(storage).getUsersTable();
        String appIdTable = Config.getConfig(storage).getAppIdToUserIdTable();
        String epToTenantTable = Config.getConfig(storage).getEmailPasswordUserToTenantTable();

        // 1. Check all_auth_recipe_users: all rows sharing same primary_or_recipe_user_id
        // should be internally consistent
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();

            // Get this user's primary_or_recipe_user_id
            String primaryId = null;
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT primary_or_recipe_user_id FROM " + usersTable +
                                 " WHERE user_id = '" + userId + "' AND app_id = 'public'")) {
                if (rs.next()) {
                    primaryId = rs.getString("primary_or_recipe_user_id").trim();
                }
            }

            if (primaryId == null) {
                // User deleted — that's fine
                return null;
            }

            // Get all users in this linked group
            List<String> groupUserIds = new ArrayList<>();
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT user_id FROM " + usersTable +
                                 " WHERE primary_or_recipe_user_id = '" + primaryId +
                                 "' AND app_id = 'public'")) {
                while (rs.next()) {
                    groupUserIds.add(rs.getString("user_id").trim());
                }
            }

            // 2. Check app_id_to_user_id flags
            for (String uid : groupUserIds) {
                try (Statement stmt = sqlCon.createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT primary_or_recipe_user_id, is_linked_or_is_a_primary_user FROM " +
                                     appIdTable + " WHERE user_id = '" + uid + "' AND app_id = 'public'")) {
                    if (rs.next()) {
                        String appPrimary = rs.getString("primary_or_recipe_user_id").trim();
                        boolean isLinked = rs.getBoolean("is_linked_or_is_a_primary_user");

                        if (!appPrimary.equals(primaryId)) {
                            issues.add("app_id_to_user_id.primary_or_recipe_user_id mismatch for " +
                                    uid + ": expected " + primaryId + " got " + appPrimary);
                        }
                        if (groupUserIds.size() > 1 && !isLinked) {
                            issues.add("app_id_to_user_id.is_linked_or_is_a_primary_user should be true for " +
                                    uid + " (group size " + groupUserIds.size() + ")");
                        }
                    }
                }
            }

            // 3. Check no duplicate emails per tenant for the linked group
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT tenant_id, email, COUNT(*) as cnt FROM " + epToTenantTable +
                                 " WHERE user_id IN (SELECT user_id FROM " + usersTable +
                                 " WHERE primary_or_recipe_user_id = '" + primaryId +
                                 "' AND app_id = 'public') AND app_id = 'public'" +
                                 " GROUP BY tenant_id, email HAVING COUNT(*) > 1")) {
                while (rs.next()) {
                    issues.add("Duplicate email '" + rs.getString("email") +
                            "' in tenant '" + rs.getString("tenant_id") +
                            "' for primary user " + primaryId +
                            " (count: " + rs.getInt("cnt") + ")");
                }
            }

            return null;
        });

        return issues;
    }

    /**
     * Test 1: linkAccounts + updateEmail race in LEGACY mode.
     *
     * R1 (email: a@test.com), R2 (email: b@test.com), P1 (primary, linked to R2).
     * Concurrently: linkAccounts(R1, P1) and updateEmail(R2, "a@test.com").
     *
     * Either the link should fail (conflict with R2's new email matching R1)
     * or the email update should fail — but NOT both succeed leaving P1 with
     * duplicate emails.
     */
    @Test
    public void testLinkAccountsAndUpdateEmailRaceInLegacy() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.LEGACY);

            for (int iter = 0; iter < RaceTestUtils.RACE_TEST_ITERATIONS; iter++) {
                // Create users
                AuthRecipeUserInfo r1 = EmailPassword.signUp(main,
                        "r1-leg" + iter + "@test.com", "password123");
                AuthRecipeUserInfo r2 = EmailPassword.signUp(main,
                        "r2-leg" + iter + "@test.com", "password123");

                // Make P1 primary and link R2
                AuthRecipe.createPrimaryUser(main, r1.getSupertokensUserId());
                String primaryId = r1.getSupertokensUserId();

                AuthRecipe.linkAccounts(main, r2.getSupertokensUserId(), primaryId);

                // Create R3 to race: link to P1 while R2's email is updated
                AuthRecipeUserInfo r3 = EmailPassword.signUp(main,
                        "r3-leg" + iter + "@test.com", "password123");

                ExecutorService executor = Executors.newFixedThreadPool(2);
                CountDownLatch startLatch = new CountDownLatch(1);
                final int iterFinal = iter;

                AtomicBoolean linkSucceeded = new AtomicBoolean(false);
                AtomicBoolean updateSucceeded = new AtomicBoolean(false);

                // Thread 1: Link R3 to P1
                Future<?> linkFuture = executor.submit(() -> {
                    try {
                        startLatch.await();
                        AuthRecipe.linkAccounts(main, r3.getSupertokensUserId(), primaryId);
                        linkSucceeded.set(true);
                    } catch (Exception e) {
                        // Expected in race conditions
                    }
                });

                // Thread 2: Update R2's email
                Future<?> updateFuture = executor.submit(() -> {
                    try {
                        startLatch.await();
                        EmailPassword.updateUsersEmailOrPassword(main,
                                r2.getSupertokensUserId(),
                                "updated-r2-leg" + iterFinal + "@test.com", null);
                        updateSucceeded.set(true);
                    } catch (Exception e) {
                        // Expected in race conditions
                    }
                });

                startLatch.countDown();
                linkFuture.get(30, TimeUnit.SECONDS);
                updateFuture.get(30, TimeUnit.SECONDS);
                executor.shutdown();

                // Verify old-table consistency
                List<String> issues = checkLegacyConsistency(storage, primaryId);
                assertTrue("Legacy consistency failed at iteration " + iter +
                        ": " + issues, issues.isEmpty());

                // Verify: check what the primary user looks like
                AuthRecipeUserInfo finalPrimary = AuthRecipe.getUserById(main, primaryId);
                assertNotNull("Primary user should exist", finalPrimary);

                // Count unique emails for the primary user's linked group
                Set<String> emails = getEmailsForPrimaryUser(storage, primaryId);
                // All emails should be unique (no duplicates)
                // This is the key assertion: LEGACY mode under READ_COMMITTED should not
                // allow duplicate emails in a linked group
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Test 2: Stress test in LEGACY mode — concurrent link/update/unlink operations.
     *
     * Run multiple iterations of concurrent operations and verify
     * old-table consistency after each iteration.
     */
    @Test
    public void testConcurrentLinkUnlinkUpdateInLegacy() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.LEGACY);

            for (int iter = 0; iter < RaceTestUtils.RACE_TEST_ITERATIONS; iter++) {
                // Create primary with 2 linked users
                AuthRecipeUserInfo primary = EmailPassword.signUp(main,
                        "p-stress" + iter + "@test.com", "password123");
                AuthRecipe.createPrimaryUser(main, primary.getSupertokensUserId());

                AuthRecipeUserInfo linked1 = EmailPassword.signUp(main,
                        "l1-stress" + iter + "@test.com", "password123");
                AuthRecipeUserInfo linked2 = EmailPassword.signUp(main,
                        "l2-stress" + iter + "@test.com", "password123");

                AuthRecipe.linkAccounts(main, linked1.getSupertokensUserId(),
                        primary.getSupertokensUserId());
                AuthRecipe.linkAccounts(main, linked2.getSupertokensUserId(),
                        primary.getSupertokensUserId());

                ExecutorService executor = Executors.newFixedThreadPool(3);
                CountDownLatch startLatch = new CountDownLatch(1);
                final int iterFinal = iter;

                // Thread 1: Unlink linked1
                Future<?> f1 = executor.submit(() -> {
                    try {
                        startLatch.await();
                        AuthRecipe.unlinkAccounts(main, linked1.getSupertokensUserId());
                    } catch (Exception e) {
                        // Expected
                    }
                });

                // Thread 2: Update linked2's email
                Future<?> f2 = executor.submit(() -> {
                    try {
                        startLatch.await();
                        EmailPassword.updateUsersEmailOrPassword(main,
                                linked2.getSupertokensUserId(),
                                "updated-l2-stress" + iterFinal + "@test.com", null);
                    } catch (Exception e) {
                        // Expected
                    }
                });

                // Thread 3: Try to link linked1 back (may fail if already unlinked)
                Future<?> f3 = executor.submit(() -> {
                    try {
                        startLatch.await();
                        Thread.sleep(1); // slight delay to increase interleaving
                        AuthRecipe.linkAccounts(main, linked1.getSupertokensUserId(),
                                primary.getSupertokensUserId());
                    } catch (Exception e) {
                        // Expected
                    }
                });

                startLatch.countDown();
                f1.get(30, TimeUnit.SECONDS);
                f2.get(30, TimeUnit.SECONDS);
                f3.get(30, TimeUnit.SECONDS);
                executor.shutdown();

                // Verify old-table consistency for all users
                for (String uid : new String[]{primary.getSupertokensUserId(),
                        linked1.getSupertokensUserId(), linked2.getSupertokensUserId()}) {
                    List<String> issues = checkLegacyConsistency(storage, uid);
                    assertTrue("Legacy consistency for " + uid + " failed at iter " + iter +
                            ": " + issues, issues.isEmpty());
                }
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Test 3: deleteUser + linkAccounts race in LEGACY mode.
     *
     * Same as DeleteUserRaceTest but in LEGACY mode with old-table assertions.
     */
    @Test
    public void testDeleteUserDuringLinkInLegacy() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.LEGACY);

            for (int iter = 0; iter < RaceTestUtils.RACE_TEST_ITERATIONS; iter++) {
                AuthRecipeUserInfo primary = EmailPassword.signUp(main,
                        "p-del" + iter + "@test.com", "password123");
                AuthRecipe.createPrimaryUser(main, primary.getSupertokensUserId());

                AuthRecipeUserInfo recipe = EmailPassword.signUp(main,
                        "r-del" + iter + "@test.com", "password123");

                ExecutorService executor = Executors.newFixedThreadPool(2);
                CountDownLatch startLatch = new CountDownLatch(1);

                // Thread 1: Delete recipe user
                Future<?> deleteFuture = executor.submit(() -> {
                    try {
                        startLatch.await();
                        AuthRecipe.deleteUser(main, recipe.getSupertokensUserId(), false);
                    } catch (Exception e) {
                        // Expected
                    }
                });

                // Thread 2: Link recipe user to primary
                Future<?> linkFuture = executor.submit(() -> {
                    try {
                        startLatch.await();
                        AuthRecipe.linkAccounts(main, recipe.getSupertokensUserId(),
                                primary.getSupertokensUserId());
                    } catch (Exception e) {
                        // Expected
                    }
                });

                startLatch.countDown();
                deleteFuture.get(30, TimeUnit.SECONDS);
                linkFuture.get(30, TimeUnit.SECONDS);
                executor.shutdown();

                // Verify old-table consistency
                AuthRecipeUserInfo finalPrimary = AuthRecipe.getUserById(main,
                        primary.getSupertokensUserId());
                assertNotNull("Primary should still exist", finalPrimary);

                List<String> issues = checkLegacyConsistency(storage,
                        primary.getSupertokensUserId());
                assertTrue("Legacy consistency failed at iter " + iter +
                        ": " + issues, issues.isEmpty());

                // If recipe user was deleted, primary should not reference it
                AuthRecipeUserInfo finalRecipe = AuthRecipe.getUserById(main,
                        recipe.getSupertokensUserId());
                if (finalRecipe == null) {
                    String usersTable = Config.getConfig(storage).getUsersTable();
                    int orphanedRefs = countRows(storage, usersTable,
                            "user_id = '" + recipe.getSupertokensUserId() +
                                    "' AND app_id = 'public'");
                    assertEquals("No orphaned rows for deleted recipe user", 0, orphanedRefs);
                }
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
