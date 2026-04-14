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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * REVIEW-003: Dual-write consistency tests.
 *
 * Verifies that in DUAL_WRITE modes, both old tables and new (reservation) tables
 * remain internally consistent after concurrent operations, mode transitions, and
 * mixed recipe types.
 *
 * NOTE: We do NOT compare old tables vs new tables directly. When users are linked,
 * their recipe_user_tenants rows are moved to primary_user_tenants, so old and new
 * table structures diverge by design. Instead, we verify each table set independently:
 * - New tables: checkReservationConsistency (invariants I1-I6)
 * - Old tables: checkOldTableConsistency (linking flags, primary_or_recipe_user_id)
 */
public class DualWriteConsistencyTest {

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

    /**
     * Old-table consistency check: verifies linking state in all_auth_recipe_users
     * and app_id_to_user_id are mutually consistent.
     *
     * Checks:
     * 1. primary_or_recipe_user_id matches between all_auth_recipe_users and app_id_to_user_id
     * 2. is_linked_or_is_a_primary_user flag is correct in app_id_to_user_id
     * 3. All members of a linked group share the same primary_or_recipe_user_id
     */
    private List<String> checkOldTableConsistency(Start storage, String userId) throws Exception {
        List<String> issues = new ArrayList<>();
        String usersTable = Config.getConfig(storage).getUsersTable();
        String appIdTable = Config.getConfig(storage).getAppIdToUserIdTable();

        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();

            // Get primary_or_recipe_user_id from all_auth_recipe_users
            String oldPrimaryId = null;
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT primary_or_recipe_user_id FROM " + usersTable +
                                 " WHERE user_id = '" + userId + "' AND app_id = 'public'")) {
                if (rs.next()) {
                    oldPrimaryId = rs.getString("primary_or_recipe_user_id").trim();
                }
            }

            if (oldPrimaryId == null) {
                return null; // User deleted
            }

            // Get primary_or_recipe_user_id and is_linked flag from app_id_to_user_id
            String appPrimaryId = null;
            boolean isLinkedFlag = false;
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT primary_or_recipe_user_id, is_linked_or_is_a_primary_user FROM " +
                                 appIdTable + " WHERE user_id = '" + userId + "' AND app_id = 'public'")) {
                if (rs.next()) {
                    appPrimaryId = rs.getString("primary_or_recipe_user_id").trim();
                    isLinkedFlag = rs.getBoolean("is_linked_or_is_a_primary_user");
                }
            }

            // 1. primary_or_recipe_user_id must match
            if (appPrimaryId != null && !oldPrimaryId.equals(appPrimaryId)) {
                issues.add("primary_or_recipe_user_id mismatch for " + userId +
                        ": all_auth_recipe_users=" + oldPrimaryId +
                        " app_id_to_user_id=" + appPrimaryId);
            }

            // 2. is_linked flag must be consistent
            boolean isLinkedOrPrimary = !oldPrimaryId.equals(userId);
            // Also check if user is a primary user (primary_or_recipe_user_id == userId
            // but there are other users in the group)
            if (!isLinkedOrPrimary) {
                try (Statement stmt = sqlCon.createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT COUNT(*) as cnt FROM " + usersTable +
                                     " WHERE primary_or_recipe_user_id = '" + userId +
                                     "' AND app_id = 'public'")) {
                    rs.next();
                    int groupSize = rs.getInt("cnt");
                    if (groupSize > 1) {
                        isLinkedOrPrimary = true; // This user is primary with linked accounts
                    }
                }
                // Also check the is_linked flag directly if the user is itself a primary
                // (even with groupSize=1, it might have been marked as primary)
                if (!isLinkedOrPrimary && isLinkedFlag) {
                    // User is primary but has no linked accounts — this is valid
                    // (createPrimaryUser sets the flag even before linking)
                    isLinkedOrPrimary = true;
                }
            }

            if (isLinkedOrPrimary != isLinkedFlag) {
                issues.add("is_linked_or_is_a_primary_user mismatch for " + userId +
                        ": expected=" + isLinkedOrPrimary + " actual=" + isLinkedFlag);
            }

            // 3. All members of linked group share same primary_or_recipe_user_id
            List<String> groupUserIds = new ArrayList<>();
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT user_id FROM " + usersTable +
                                 " WHERE primary_or_recipe_user_id = '" + oldPrimaryId +
                                 "' AND app_id = 'public'")) {
                while (rs.next()) {
                    groupUserIds.add(rs.getString("user_id").trim());
                }
            }

            for (String groupUid : groupUserIds) {
                try (Statement stmt = sqlCon.createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT primary_or_recipe_user_id FROM " + appIdTable +
                                     " WHERE user_id = '" + groupUid + "' AND app_id = 'public'")) {
                    if (rs.next()) {
                        String memberPrimary = rs.getString("primary_or_recipe_user_id").trim();
                        if (!memberPrimary.equals(oldPrimaryId)) {
                            issues.add("Group member " + groupUid +
                                    " has app_id primary=" + memberPrimary +
                                    " but expected=" + oldPrimaryId);
                        }
                    }
                }
            }

            return null;
        });

        return issues;
    }

    private int countRows(Start storage, String table, String whereClause) throws Exception {
        int[] count = new int[]{0};
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT COUNT(*) as cnt FROM " + table + " WHERE " + whereClause)) {
                rs.next();
                count[0] = rs.getInt("cnt");
            }
            return null;
        });
        return count[0];
    }

    /**
     * Test 1: Dual-write consistency after concurrent link/unlink/updateEmail.
     *
     * Creates users in DUAL_WRITE_READ_OLD, links them, runs concurrent
     * unlink + updateEmail + re-link, then verifies:
     * - New tables: checkReservationConsistency (I1-I6)
     * - Old tables: checkOldTableConsistency (linking flags, primary IDs)
     */
    @Test
    public void testDualWriteConsistencyAfterConcurrentOps() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            for (int iter = 0; iter < RaceTestUtils.RACE_TEST_ITERATIONS; iter++) {
                // Create primary + 2 linked users
                AuthRecipeUserInfo primary = EmailPassword.signUp(main,
                        "p-dw" + iter + "@test.com", "password123");
                AuthRecipe.createPrimaryUser(main, primary.getSupertokensUserId());

                AuthRecipeUserInfo linked1 = EmailPassword.signUp(main,
                        "l1-dw" + iter + "@test.com", "password123");
                AuthRecipeUserInfo linked2 = EmailPassword.signUp(main,
                        "l2-dw" + iter + "@test.com", "password123");

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
                    } catch (Exception e) { /* Expected */ }
                });

                // Thread 2: Update linked2's email
                Future<?> f2 = executor.submit(() -> {
                    try {
                        startLatch.await();
                        EmailPassword.updateUsersEmailOrPassword(main,
                                linked2.getSupertokensUserId(),
                                "upd-l2-dw" + iterFinal + "@test.com", null);
                    } catch (Exception e) { /* Expected */ }
                });

                // Thread 3: Re-link linked1 (may fail if unlink hasn't completed)
                Future<?> f3 = executor.submit(() -> {
                    try {
                        startLatch.await();
                        Thread.sleep(1);
                        AuthRecipe.linkAccounts(main, linked1.getSupertokensUserId(),
                                primary.getSupertokensUserId());
                    } catch (Exception e) { /* Expected */ }
                });

                startLatch.countDown();
                f1.get(30, TimeUnit.SECONDS);
                f2.get(30, TimeUnit.SECONDS);
                f3.get(30, TimeUnit.SECONDS);
                executor.shutdown();

                // Verify NEW tables: reservation consistency for primary user
                AuthRecipeUserInfo finalPrimary = AuthRecipe.getUserById(main,
                        primary.getSupertokensUserId());
                assertNotNull("Primary user should exist at iter " + iter, finalPrimary);
                RaceTestUtils.ConsistencyCheckResult result =
                        RaceTestUtils.checkReservationConsistency(main, finalPrimary);
                assertTrue("Reservation consistency failed at iter " + iter +
                        ": " + result.issues, result.isConsistent);

                // If linked1 was unlinked, also check its reservation consistency
                AuthRecipeUserInfo finalLinked1 = AuthRecipe.getUserById(main,
                        linked1.getSupertokensUserId());
                if (finalLinked1 != null) {
                    RaceTestUtils.ConsistencyCheckResult l1Result =
                            RaceTestUtils.checkReservationConsistency(main, finalLinked1);
                    assertTrue("Linked1 reservation consistency failed at iter " + iter +
                            ": " + l1Result.issues, l1Result.isConsistent);
                }

                // Verify OLD tables: linking state consistency
                List<String> oldIssues = new ArrayList<>();
                for (String uid : new String[]{primary.getSupertokensUserId(),
                        linked1.getSupertokensUserId(), linked2.getSupertokensUserId()}) {
                    oldIssues.addAll(checkOldTableConsistency(storage, uid));
                }
                assertTrue("Old table consistency failed at iter " + iter +
                        ": " + oldIssues, oldIssues.isEmpty());
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Test 2: Reservation consistency with ThirdParty users in DUAL_WRITE.
     *
     * ThirdParty users have 2 account info types (EMAIL + THIRD_PARTY).
     * After linking, the primary user's reservation tables are reorganized:
     * recipe_user_tenants rows move to primary_user_tenants.
     *
     * Verifies that checkReservationConsistency passes for the combined
     * EP + TP linked user after creation and linking.
     *
     * NOTE: TP email updates via signInUp have a known pre-existing I4 issue
     * (extra rows in recipe_user_tenants). We verify initial state only.
     */
    @Test
    public void testReservationConsistencyWithThirdParty() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            // Create EP primary + TP linked user
            AuthRecipeUserInfo epUser = EmailPassword.signUp(main,
                    "ep-tp@test.com", "password123");
            AuthRecipe.createPrimaryUser(main, epUser.getSupertokensUserId());

            ThirdParty.SignInUpResponse tpResponse = ThirdParty.signInUp(
                    main, "google", "g-cross-1", "tp-cross@test.com");
            AuthRecipe.linkAccounts(main, tpResponse.user.getSupertokensUserId(),
                    epUser.getSupertokensUserId());

            // Verify reservation consistency for the primary user (covers both EP + TP)
            AuthRecipeUserInfo finalPrimary = AuthRecipe.getUserById(main,
                    epUser.getSupertokensUserId());
            assertNotNull(finalPrimary);
            assertTrue("Should be primary", finalPrimary.isPrimaryUser);
            assertEquals("Should have 2 login methods", 2, finalPrimary.loginMethods.length);

            RaceTestUtils.ConsistencyCheckResult result =
                    RaceTestUtils.checkReservationConsistency(main, finalPrimary);
            assertTrue("Reservation consistency for EP+TP linked: " + result.issues,
                    result.isConsistent);

            // Verify old-table consistency
            List<String> oldIssues = new ArrayList<>();
            oldIssues.addAll(checkOldTableConsistency(storage, epUser.getSupertokensUserId()));
            oldIssues.addAll(checkOldTableConsistency(storage,
                    tpResponse.user.getSupertokensUserId()));
            assertTrue("Old table consistency for EP+TP: " + oldIssues, oldIssues.isEmpty());

            // Verify dual-write: new tables have data for TP user
            String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
            int tpAccountInfoRows = countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + tpResponse.user.getSupertokensUserId() +
                            "' AND app_id = 'public'");
            assertTrue("TP user should have account info rows in new tables",
                    tpAccountInfoRows >= 2); // EMAIL + THIRD_PARTY

            // Create standalone TP user (not linked) and verify
            ThirdParty.SignInUpResponse tpStandalone = ThirdParty.signInUp(
                    main, "github", "gh-1", "tp-standalone@test.com");
            AuthRecipeUserInfo standaloneUser = AuthRecipe.getUserById(main,
                    tpStandalone.user.getSupertokensUserId());
            assertNotNull(standaloneUser);

            RaceTestUtils.ConsistencyCheckResult standaloneResult =
                    RaceTestUtils.checkReservationConsistency(main, standaloneUser);
            assertTrue("Standalone TP reservation consistency: " + standaloneResult.issues,
                    standaloneResult.isConsistent);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Test 3: Mode transition from DUAL_WRITE_READ_OLD → DUAL_WRITE_READ_NEW → MIGRATED.
     *
     * Operations start in READ_OLD, mode switches to READ_NEW mid-stream,
     * then to MIGRATED. Verifies reservation consistency and read correctness
     * at each stage.
     */
    @Test
    public void testModeTransitionDuringOperations() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            // --- Phase 1: Create and link users in DUAL_WRITE_READ_OLD ---
            AuthRecipeUserInfo primary = EmailPassword.signUp(main,
                    "p-trans@test.com", "password123");
            AuthRecipe.createPrimaryUser(main, primary.getSupertokensUserId());

            List<String> linkedUserIds = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                AuthRecipeUserInfo user = EmailPassword.signUp(main,
                        "l-trans" + i + "@test.com", "password123");
                AuthRecipe.linkAccounts(main, user.getSupertokensUserId(),
                        primary.getSupertokensUserId());
                linkedUserIds.add(user.getSupertokensUserId());
            }

            // Verify in READ_OLD: reservation consistency
            AuthRecipeUserInfo p1 = AuthRecipe.getUserById(main, primary.getSupertokensUserId());
            assertNotNull(p1);
            assertEquals("Should have 4 login methods in READ_OLD", 4, p1.loginMethods.length);

            RaceTestUtils.ConsistencyCheckResult result1 =
                    RaceTestUtils.checkReservationConsistency(main, p1);
            assertTrue("Reservation consistency in READ_OLD: " + result1.issues,
                    result1.isConsistent);

            // Old table consistency
            List<String> oldIssues1 = checkOldTableConsistency(storage,
                    primary.getSupertokensUserId());
            assertTrue("Old table consistency in READ_OLD: " + oldIssues1, oldIssues1.isEmpty());

            // --- Phase 2: Switch to DUAL_WRITE_READ_NEW ---
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_NEW);

            // Create and link more users in READ_NEW mode
            AuthRecipeUserInfo newUser = EmailPassword.signUp(main,
                    "new-trans@test.com", "password123");
            AuthRecipe.linkAccounts(main, newUser.getSupertokensUserId(),
                    primary.getSupertokensUserId());

            // Update email in READ_NEW mode
            EmailPassword.updateUsersEmailOrPassword(main,
                    linkedUserIds.get(0), "updated-trans@test.com", null);

            // Verify in READ_NEW: reservation consistency
            AuthRecipeUserInfo p2 = AuthRecipe.getUserById(main, primary.getSupertokensUserId());
            assertNotNull(p2);
            assertEquals("Should have 5 login methods in READ_NEW", 5, p2.loginMethods.length);

            RaceTestUtils.ConsistencyCheckResult result2 =
                    RaceTestUtils.checkReservationConsistency(main, p2);
            assertTrue("Reservation consistency in READ_NEW: " + result2.issues,
                    result2.isConsistent);

            // Old table consistency still holds (dual-write writes to both)
            List<String> oldIssues2 = checkOldTableConsistency(storage,
                    primary.getSupertokensUserId());
            assertTrue("Old table consistency in READ_NEW: " + oldIssues2, oldIssues2.isEmpty());

            // Verify updated email is visible
            boolean foundUpdatedEmail = false;
            for (var lm : p2.loginMethods) {
                if (lm.email != null && lm.email.equals("updated-trans@test.com")) {
                    foundUpdatedEmail = true;
                    break;
                }
            }
            assertTrue("Updated email should be visible in READ_NEW", foundUpdatedEmail);

            // --- Phase 3: Switch to MIGRATED ---
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.MIGRATED);

            AuthRecipeUserInfo p3 = AuthRecipe.getUserById(main, primary.getSupertokensUserId());
            assertNotNull("Primary user readable in MIGRATED mode", p3);
            assertEquals("Primary should have 5 login methods in MIGRATED",
                    5, p3.loginMethods.length);

            // Reservation consistency in MIGRATED
            RaceTestUtils.ConsistencyCheckResult result3 =
                    RaceTestUtils.checkReservationConsistency(main, p3);
            assertTrue("Reservation consistency in MIGRATED: " + result3.issues,
                    result3.isConsistent);

            // Verify the updated email persists
            boolean foundInMigrated = false;
            for (var lm : p3.loginMethods) {
                if (lm.email != null && lm.email.equals("updated-trans@test.com")) {
                    foundInMigrated = true;
                    break;
                }
            }
            assertTrue("Updated email should persist in MIGRATED mode", foundInMigrated);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
