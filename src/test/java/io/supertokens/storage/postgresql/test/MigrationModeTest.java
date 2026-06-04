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
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;

import com.google.gson.JsonObject;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.*;

/**
 * REVIEW-008 Parts 1-3: Migration mode write-path verification, dual-write
 * edge cases, and blue-green deployment simulation.
 *
 * Verifies that each migration mode writes to exactly the expected tables,
 * and that dual-write mode keeps old and new tables in sync.
 */
public class MigrationModeTest {

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

    // ======================== Part 1: Write-Path Verification ========================

    /**
     * LEGACY mode: writes to old tables only, skips new (reservation) tables.
     */
    @Test
    public void testLegacyModeSkipsNewTableWrites() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.LEGACY);

            AuthRecipeUserInfo user = EmailPassword.signUp(main, "legacy@test.com", "password123");
            String uid = user.getSupertokensUserId();

            // Old tables SHOULD have rows
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getUsersTable(),
                    "user_id = '" + uid + "'"));
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getEmailPasswordUserToTenantTable(),
                    "user_id = '" + uid + "'"));
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getAppIdToUserIdTable(),
                    "user_id = '" + uid + "'"));

            // New (reservation) tables should have NO rows
            assertEquals(0, countRows(storage,
                    Config.getConfig(storage).getRecipeUserTenantsTable(),
                    "recipe_user_id = '" + uid + "'"));
            assertEquals(0, countRows(storage,
                    Config.getConfig(storage).getRecipeUserAccountInfosTable(),
                    "recipe_user_id = '" + uid + "'"));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * MIGRATED mode: writes to new (reservation) tables only, skips old tables.
     */
    @Test
    public void testMigratedModeSkipsOldTableWrites() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.MIGRATED);

            AuthRecipeUserInfo user = EmailPassword.signUp(main, "migrated@test.com", "password123");
            String uid = user.getSupertokensUserId();

            // app_id_to_user_id is ALWAYS written
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getAppIdToUserIdTable(),
                    "user_id = '" + uid + "'"));

            // Old tables should have NO rows
            assertEquals(0, countRows(storage,
                    Config.getConfig(storage).getUsersTable(),
                    "user_id = '" + uid + "'"));
            assertEquals(0, countRows(storage,
                    Config.getConfig(storage).getEmailPasswordUserToTenantTable(),
                    "user_id = '" + uid + "'"));

            // New (reservation) tables SHOULD have rows
            assertTrue(countRows(storage,
                    Config.getConfig(storage).getRecipeUserTenantsTable(),
                    "recipe_user_id = '" + uid + "'") >= 1);
            assertTrue(countRows(storage,
                    Config.getConfig(storage).getRecipeUserAccountInfosTable(),
                    "recipe_user_id = '" + uid + "'") >= 1);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * DUAL_WRITE_READ_OLD mode: writes to BOTH old and new tables.
     */
    @Test
    public void testDualWriteCreatesRowsInBothTableSets() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            AuthRecipeUserInfo user = EmailPassword.signUp(main, "dual@test.com", "password123");
            String uid = user.getSupertokensUserId();

            // app_id_to_user_id always written
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getAppIdToUserIdTable(),
                    "user_id = '" + uid + "'"));

            // Old tables SHOULD have rows
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getUsersTable(),
                    "user_id = '" + uid + "'"));
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getEmailPasswordUserToTenantTable(),
                    "user_id = '" + uid + "'"));

            // New tables SHOULD also have rows
            assertTrue(countRows(storage,
                    Config.getConfig(storage).getRecipeUserTenantsTable(),
                    "recipe_user_id = '" + uid + "'") >= 1);
            assertTrue(countRows(storage,
                    Config.getConfig(storage).getRecipeUserAccountInfosTable(),
                    "recipe_user_id = '" + uid + "'") >= 1);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * DUAL_WRITE: linking updates both old and new tables consistently.
     */
    @Test
    public void testDualWriteLinkingUpdatesOldAndNew() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            // Create primary + recipe user
            AuthRecipeUserInfo primary = EmailPassword.signUp(main, "primary@test.com", "password123");
            AuthRecipeUserInfo recipe = EmailPassword.signUp(main, "recipe@test.com", "password123");

            AuthRecipe.createPrimaryUser(main, primary.getSupertokensUserId());
            AuthRecipe.linkAccounts(main, recipe.getSupertokensUserId(),
                    primary.getSupertokensUserId());

            String primaryId = primary.getSupertokensUserId();
            String recipeId = recipe.getSupertokensUserId();

            // OLD table: all_auth_recipe_users should reflect linking
            int linkedInOld = countRows(storage,
                    Config.getConfig(storage).getUsersTable(),
                    "primary_or_recipe_user_id = '" + primaryId + "'");
            assertEquals("Old table should show both users linked", 2, linkedInOld);

            // OLD table: app_id_to_user_id should reflect linking
            int linkedInAppId = countRows(storage,
                    Config.getConfig(storage).getAppIdToUserIdTable(),
                    "primary_or_recipe_user_id = '" + primaryId + "'");
            assertEquals("app_id_to_user_id should show both linked", 2, linkedInAppId);

            // NEW table: primary_user_tenants should have reservations
            int primaryReservations = countRows(storage,
                    Config.getConfig(storage).getPrimaryUserTenantsTable(),
                    "primary_user_id = '" + primaryId + "'");
            // Primary has own email + linked recipe's email = at least 2
            assertTrue("primary_user_tenants should have at least 2 reservations, got " +
                    primaryReservations, primaryReservations >= 2);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ======================== Part 2: Dual-Write Edge Cases ========================

    /**
     * Delete user in DUAL_WRITE mode removes from both old and new tables.
     */
    @Test
    public void testDeleteUserInDualWriteMode() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            AuthRecipeUserInfo user = EmailPassword.signUp(main, "delete@test.com", "password123");
            String uid = user.getSupertokensUserId();

            // Verify rows exist in both before delete
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getUsersTable(),
                    "user_id = '" + uid + "'"));
            assertTrue(countRows(storage,
                    Config.getConfig(storage).getRecipeUserTenantsTable(),
                    "recipe_user_id = '" + uid + "'") >= 1);

            // Delete the user
            AuthRecipe.deleteUser(main, uid);

            // Both old and new tables should be empty
            assertEquals(0, countRows(storage,
                    Config.getConfig(storage).getUsersTable(),
                    "user_id = '" + uid + "'"));
            assertEquals(0, countRows(storage,
                    Config.getConfig(storage).getEmailPasswordUserToTenantTable(),
                    "user_id = '" + uid + "'"));
            assertEquals(0, countRows(storage,
                    Config.getConfig(storage).getRecipeUserTenantsTable(),
                    "recipe_user_id = '" + uid + "'"));
            assertEquals(0, countRows(storage,
                    Config.getConfig(storage).getRecipeUserAccountInfosTable(),
                    "recipe_user_id = '" + uid + "'"));
            assertEquals(0, countRows(storage,
                    Config.getConfig(storage).getAppIdToUserIdTable(),
                    "user_id = '" + uid + "'"));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Update email in DUAL_WRITE mode updates both old and new tables.
     */
    @Test
    public void testUpdateEmailInDualWriteMode() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            AuthRecipeUserInfo user = EmailPassword.signUp(main, "before@test.com", "password123");
            String uid = user.getSupertokensUserId();

            // Update email
            EmailPassword.updateUsersEmailOrPassword(main, uid, "after@test.com", null);

            // Old table: emailpassword_user_to_tenant should have new email
            String epToTenantTable = Config.getConfig(storage).getEmailPasswordUserToTenantTable();
            assertEquals("Old email should be gone", 0,
                    countRows(storage, epToTenantTable,
                            "user_id = '" + uid + "' AND email = 'before@test.com'"));
            assertEquals("New email should exist", 1,
                    countRows(storage, epToTenantTable,
                            "user_id = '" + uid + "' AND email = 'after@test.com'"));

            // New table: recipe_user_account_infos should have new email
            String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
            assertEquals("Old email should be gone from account_infos", 0,
                    countRows(storage, accountInfosTable,
                            "recipe_user_id = '" + uid + "' AND account_info_value = 'before@test.com'"));
            assertEquals("New email should exist in account_infos", 1,
                    countRows(storage, accountInfosTable,
                            "recipe_user_id = '" + uid + "' AND account_info_value = 'after@test.com'"));

            // recipe_user_tenants should also reflect the new email
            String recipeTenantsTable = Config.getConfig(storage).getRecipeUserTenantsTable();
            assertEquals("Old email in recipe_user_tenants should be gone", 0,
                    countRows(storage, recipeTenantsTable,
                            "recipe_user_id = '" + uid + "' AND account_info_value = 'before@test.com'"));
            assertEquals("New email in recipe_user_tenants should exist", 1,
                    countRows(storage, recipeTenantsTable,
                            "recipe_user_id = '" + uid + "' AND account_info_value = 'after@test.com'"));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Add/remove tenant in DUAL_WRITE mode updates both old and new tables.
     */
    @Test
    public void testAddRemoveTenantInDualWriteMode() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            // Create a second tenant
            TenantIdentifier t1 = new TenantIdentifier(null, null, "t1");
            Multitenancy.addNewOrUpdateAppOrTenant(main, new TenantConfig(
                    t1,
                    new EmailPasswordConfig(true),
                    new ThirdPartyConfig(true, null),
                    new PasswordlessConfig(true),
                    null, null, new JsonObject()
            ), false);

            Storage t1Storage = StorageLayer.getStorage(t1, main);

            AuthRecipeUserInfo user = EmailPassword.signUp(main, "tenant@test.com", "password123");
            String uid = user.getSupertokensUserId();

            // Add user to second tenant
            Multitenancy.addUserIdToTenant(main, t1, t1Storage, uid);

            // Old table: emailpassword_user_to_tenant should have 2 rows
            assertEquals(2, countRows(storage,
                    Config.getConfig(storage).getEmailPasswordUserToTenantTable(),
                    "user_id = '" + uid + "'"));

            // New table: recipe_user_tenants should have 2 rows (1 per tenant)
            assertEquals(2, countRows(storage,
                    Config.getConfig(storage).getRecipeUserTenantsTable(),
                    "recipe_user_id = '" + uid + "'"));

            // Remove user from second tenant
            Multitenancy.removeUserIdFromTenant(main, t1, t1Storage, uid, null);

            // Both should be back to 1 row
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getEmailPasswordUserToTenantTable(),
                    "user_id = '" + uid + "'"));
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getRecipeUserTenantsTable(),
                    "recipe_user_id = '" + uid + "'"));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ======================== Part 3: Blue-Green Deployment Simulation ========================

    /**
     * Simulates blue-green deployment by switching mode mid-test:
     *
     * 1. LEGACY mode: create user (old tables only)
     * 2. Switch to DUAL_WRITE_READ_OLD: create another user (both tables)
     * 3. Reads via old tables should see BOTH users
     * 4. Link DUAL_WRITE users (not legacy ones — legacy users need backfill first)
     * 5. Switch to DUAL_WRITE_READ_NEW: verify reads work for dual-write users
     *
     * Note: Operations that write to new tables (createPrimaryUser, linkAccounts)
     * will fail for legacy-only users because their data isn't in the new tables yet.
     * In a real deployment, backfill runs before such operations. This test verifies
     * the read path coexistence, not linking of legacy users.
     */
    @Test
    public void testBlueGreenDeploymentSimulation() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);

            // --- Phase 1: LEGACY mode (old deployment) ---
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.LEGACY);

            AuthRecipeUserInfo legacyUser = EmailPassword.signUp(main,
                    "legacy-bg@test.com", "password123");
            String legacyId = legacyUser.getSupertokensUserId();

            // Legacy user: in old tables, NOT in new tables
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getUsersTable(),
                    "user_id = '" + legacyId + "'"));
            assertEquals(0, countRows(storage,
                    Config.getConfig(storage).getRecipeUserTenantsTable(),
                    "recipe_user_id = '" + legacyId + "'"));

            // --- Phase 2: Switch to DUAL_WRITE_READ_OLD (new deployment rolling out) ---
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            AuthRecipeUserInfo dualUser1 = EmailPassword.signUp(main,
                    "dual-bg1@test.com", "password123");
            AuthRecipeUserInfo dualUser2 = EmailPassword.signUp(main,
                    "dual-bg2@test.com", "password123");
            String dualId1 = dualUser1.getSupertokensUserId();
            String dualId2 = dualUser2.getSupertokensUserId();

            // Dual users: in BOTH table sets
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getUsersTable(),
                    "user_id = '" + dualId1 + "'"));
            assertTrue(countRows(storage,
                    Config.getConfig(storage).getRecipeUserTenantsTable(),
                    "recipe_user_id = '" + dualId1 + "'") >= 1);

            // Reading via old tables (DUAL_WRITE_READ_OLD) sees ALL users
            assertNotNull("Legacy user readable in DUAL_WRITE_READ_OLD",
                    AuthRecipe.getUserById(main, legacyId));
            assertNotNull("Dual user 1 readable in DUAL_WRITE_READ_OLD",
                    AuthRecipe.getUserById(main, dualId1));
            assertNotNull("Dual user 2 readable in DUAL_WRITE_READ_OLD",
                    AuthRecipe.getUserById(main, dualId2));

            // --- Phase 3: Link DUAL_WRITE users (these have data in new tables) ---
            AuthRecipe.createPrimaryUser(main, dualId1);
            AuthRecipe.linkAccounts(main, dualId2, dualId1);

            // Both old and new tables should reflect the link
            int linkedOld = countRows(storage,
                    Config.getConfig(storage).getUsersTable(),
                    "primary_or_recipe_user_id = '" + dualId1 + "'");
            assertEquals("Old table should show 2 linked users", 2, linkedOld);

            int primaryReservations = countRows(storage,
                    Config.getConfig(storage).getPrimaryUserTenantsTable(),
                    "primary_user_id = '" + dualId1 + "'");
            assertTrue("primary_user_tenants should have reservations for linked users",
                    primaryReservations >= 2);

            // --- Phase 4: Switch to DUAL_WRITE_READ_NEW ---
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_NEW);

            // Read via new tables — dual-write users should be found
            AuthRecipeUserInfo readNewDual1 = AuthRecipe.getUserById(main, dualId1);
            assertNotNull("Dual user 1 readable in DUAL_WRITE_READ_NEW", readNewDual1);
            assertTrue("Dual user 1 should be primary", readNewDual1.isPrimaryUser);
            assertEquals("Dual user 1 should have 2 login methods",
                    2, readNewDual1.loginMethods.length);

            AuthRecipeUserInfo readNewDual2 = AuthRecipe.getUserById(main, dualId2);
            assertNotNull("Dual user 2 readable in DUAL_WRITE_READ_NEW", readNewDual2);

            // Legacy user: NOT in new tables, so getUserById via new path may return null.
            // This is expected — backfill must run before switching to READ_NEW.
            // The important thing is no crash/exception.
            AuthRecipe.getUserById(main, legacyId); // no crash
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * DUAL_WRITE_READ_NEW mode: verify ThirdParty user writes to both table sets.
     * Third-party users have 2 account info types (EMAIL + THIRD_PARTY).
     */
    @Test
    public void testDualWriteThirdPartyUser() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);

            ThirdParty.SignInUpResponse response = ThirdParty.signInUp(
                    main, "google", "g-123", "tp-user@test.com");
            String uid = response.user.getSupertokensUserId();

            // Old tables
            assertEquals(1, countRows(storage,
                    Config.getConfig(storage).getUsersTable(),
                    "user_id = '" + uid + "'"));

            // New tables: should have 2 account infos (EMAIL + THIRD_PARTY)
            assertEquals(2, countRows(storage,
                    Config.getConfig(storage).getRecipeUserAccountInfosTable(),
                    "recipe_user_id = '" + uid + "'"));

            // recipe_user_tenants should also have 2 rows (email + thirdparty per tenant)
            assertEquals(2, countRows(storage,
                    Config.getConfig(storage).getRecipeUserTenantsTable(),
                    "recipe_user_id = '" + uid + "'"));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
