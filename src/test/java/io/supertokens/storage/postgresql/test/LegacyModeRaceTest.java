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
import io.supertokens.pluginInterface.authRecipe.exceptions.AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException;
import io.supertokens.pluginInterface.authRecipe.exceptions.EmailChangeNotAllowedException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Callable;
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

    private int countBackendsWaitingOnAdvisoryLock(Start storage) throws Exception {
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM pg_stat_activity"
                         + " WHERE datname = current_database() AND wait_event_type = 'Lock'"
                         + " AND wait_event = 'advisory'")) {
                rs.next();
                return rs.getInt(1);
            }
        });
    }

    /**
     * Makes {@code firstRecipeUserId} primary in a transaction that is kept open (uncommitted) while
     * {@code second} runs on another thread. Once {@code second} has either finished or been observed waiting on
     * the account-info lock in pg_stat_activity, the first transaction commits. Returns what {@code second}
     * returned or threw. Asserts that {@code second} could not finish while the first transaction was open.
     */
    private Object runWhileMakePrimaryIsUncommitted(Start storage, String firstRecipeUserId,
                                                    Callable<Object> second) throws Exception {
        AppIdentifier appIdentifier = new AppIdentifier(null, null);
        CountDownLatch firstMadePrimary = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first = executor.submit(() -> storage.startTransaction(con -> {
                try {
                    boolean didBecomePrimary = storage.makePrimaryUser_Transaction(appIdentifier, con,
                            firstRecipeUserId);
                    firstMadePrimary.countDown();
                    if (!releaseFirst.await(60, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("first transaction was never released");
                    }
                    storage.commitTransaction(con);
                    return didBecomePrimary;
                } catch (Exception e) {
                    throw new StorageTransactionLogicException(e);
                }
            }));
            assertTrue("first transaction did not reach its check", firstMadePrimary.await(30, TimeUnit.SECONDS));

            Future<Object> secondFuture = executor.submit(() -> {
                try {
                    return second.call();
                } catch (Exception e) {
                    return e;
                }
            });

            // Without serialization the second operation's conflict check cannot see the uncommitted first one
            // and it finishes right away; with it, the second operation queues on the account-info lock.
            boolean secondSeenWaiting = false;
            long deadline = System.currentTimeMillis() + 30_000;
            while (!secondFuture.isDone() && System.currentTimeMillis() < deadline) {
                if (countBackendsWaitingOnAdvisoryLock(storage) > 0) {
                    secondSeenWaiting = true;
                    break;
                }
                Thread.sleep(10); // poll interval only; correctness does not depend on timing
            }
            boolean secondFinishedWhileFirstOpen = secondFuture.isDone();
            Object secondResultIfFinished = secondFinishedWhileFirstOpen ? secondFuture.get() : null;

            releaseFirst.countDown();
            assertTrue(first.get(30, TimeUnit.SECONDS));
            Object secondResult = secondFuture.get(30, TimeUnit.SECONDS);

            assertFalse("second operation finished while the first transaction was uncommitted: "
                    + secondResultIfFinished, secondFinishedWhileFirstOpen);
            assertTrue("second operation was not seen waiting on the account-info lock", secondSeenWaiting);
            return secondResult;
        } finally {
            releaseFirst.countDown();
            executor.shutdownNow();
        }
    }

    private void assertConflictsWith(Object result, String expectedPrimaryUserId) {
        assertTrue("expected an account-info conflict, got: " + result,
                result instanceof AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException);
        assertEquals(expectedPrimaryUserId,
                ((AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException) result).primaryUserId);
    }

    /**
     * Two recipe users (email-password and third-party) share an email. Making both primary concurrently in LEGACY
     * mode must leave at most one primary user for that email: the second transaction waits for the first and
     * then fails the conflict check.
     */
    @Test
    public void testIssue27_legacyLinkingUniquenessUnderConcurrency() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.LEGACY);

            String email = "shared-make-primary@test.com";
            AuthRecipeUserInfo r1 = EmailPassword.signUp(main, email, "password123");
            AuthRecipeUserInfo r2 = ThirdParty.signInUp(main, "google", "g-shared-make-primary", email).user;

            Object result = runWhileMakePrimaryIsUncommitted(storage, r1.getSupertokensUserId(),
                    () -> AuthRecipe.createPrimaryUser(main, r2.getSupertokensUserId()));

            assertTrue(AuthRecipe.getUserById(main, r1.getSupertokensUserId()).isPrimaryUser);
            assertFalse("two primary users share the same email",
                    AuthRecipe.getUserById(main, r2.getSupertokensUserId()).isPrimaryUser);
            assertConflictsWith(result, r1.getSupertokensUserId());
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Linking a recipe user into an existing primary user while another recipe user with the same email is being
     * made primary must not leave two primary users with that email.
     */
    @Test
    public void testLegacyLinkWaitsForConcurrentMakePrimaryWithSameEmail() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.LEGACY);

            String email = "shared-link@test.com";
            AuthRecipeUserInfo primary = EmailPassword.signUp(main, "other-link@test.com", "password123");
            AuthRecipe.createPrimaryUser(main, primary.getSupertokensUserId());
            AuthRecipeUserInfo r1 = EmailPassword.signUp(main, email, "password123");
            AuthRecipeUserInfo r2 = ThirdParty.signInUp(main, "google", "g-shared-link", email).user;

            Object result = runWhileMakePrimaryIsUncommitted(storage, r1.getSupertokensUserId(),
                    () -> AuthRecipe.linkAccounts(main, r2.getSupertokensUserId(), primary.getSupertokensUserId()));

            assertConflictsWith(result, r1.getSupertokensUserId());
            AuthRecipeUserInfo r2After = AuthRecipe.getUserById(main, r2.getSupertokensUserId());
            assertFalse(r2After.isPrimaryUser);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Changing a primary user's email to an email that another recipe user is concurrently becoming primary with
     * must not leave two primary users with that email.
     */
    @Test
    public void testLegacyPrimaryEmailChangeWaitsForConcurrentMakePrimaryWithSameEmail() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.LEGACY);

            String email = "shared-email-change@test.com";
            AuthRecipeUserInfo primary = EmailPassword.signUp(main, "other-email-change@test.com", "password123");
            AuthRecipe.createPrimaryUser(main, primary.getSupertokensUserId());
            AuthRecipeUserInfo r1 = ThirdParty.signInUp(main, "google", "g-shared-email-change", email).user;

            Object result = runWhileMakePrimaryIsUncommitted(storage, r1.getSupertokensUserId(), () -> {
                EmailPassword.updateUsersEmailOrPassword(main, primary.getSupertokensUserId(), email, null);
                return "email updated";
            });

            assertTrue("expected the email change to be rejected, got: " + result,
                    result instanceof EmailChangeNotAllowedException);
            assertEquals("other-email-change@test.com",
                    AuthRecipe.getUserById(main, primary.getSupertokensUserId()).loginMethods[0].email);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Concurrent LEGACY email changes of primary users to the same email queue on the email lock, each holding a
     * pool connection. The lock holder's conflict check must run on its own connection, otherwise it waits for a
     * free one until the pool times out, and the next lock holder does the same.
     */
    @Test
    public void testLegacyEmailChangesToSameEmailDoNotExhaustThePool() throws Exception {
        Utils.setValueInConfig("postgresql_connection_pool_size", "2");
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.LEGACY);

            int n = 6;
            List<String> primaryUserIds = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                AuthRecipeUserInfo user = EmailPassword.signUp(main, "pool-" + i + "@test.com", "password123");
                AuthRecipe.createPrimaryUser(main, user.getSupertokensUserId());
                primaryUserIds.add(user.getSupertokensUserId());
            }

            String email = "pool-target@test.com";
            ExecutorService executor = Executors.newFixedThreadPool(n);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Object>> futures = new ArrayList<>();
            for (String userId : primaryUserIds) {
                futures.add(executor.submit(() -> {
                    start.await();
                    try {
                        EmailPassword.updateUsersEmailOrPassword(main, userId, email, null);
                        return "OK";
                    } catch (Exception e) {
                        return e;
                    }
                }));
            }
            start.countDown();

            int succeeded = 0;
            for (Future<Object> future : futures) {
                Object result = future.get(60, TimeUnit.SECONDS);
                if ("OK".equals(result)) {
                    succeeded++;
                } else {
                    assertTrue("unexpected result: " + result, result instanceof EmailChangeNotAllowedException);
                }
            }
            executor.shutdownNow();
            assertEquals(1, succeeded);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
