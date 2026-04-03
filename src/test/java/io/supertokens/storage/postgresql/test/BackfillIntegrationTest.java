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
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.migration.MigrationBackfillStorage;
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

public class BackfillIntegrationTest {

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

    @Test
    public void fullBackfillFlow_allRecipeTypes() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // Create users of each type
            AuthRecipeUserInfo epUser = EmailPassword.signUp(main, "ep@test.com", "password123");

            Passwordless.CreateCodeResponse code = Passwordless.createCode(
                    main, "pl@test.com", null, null, null);
            Passwordless.ConsumeCodeResponse plResponse = Passwordless.consumeCode(
                    main, code.deviceId, code.deviceIdHash, code.userInputCode, null);
            // Add phone to passwordless user
            Passwordless.updateUser(main, plResponse.user.getSupertokensUserId(),
                    null, new Passwordless.FieldUpdate("+11234567890"));

            ThirdParty.SignInUpResponse tpResponse = ThirdParty.signInUp(
                    main, "google", "g1", "tp@test.com");

            // Link EP + PL under EP as primary
            AuthRecipe.createPrimaryUser(main, epUser.getSupertokensUserId());
            AuthRecipe.linkAccounts(main, plResponse.user.getSupertokensUserId(),
                    epUser.getSupertokensUserId());

            // Simulate legacy state for all users
            simulateLegacyState(storage, epUser.getSupertokensUserId());
            simulateLegacyState(storage, plResponse.user.getSupertokensUserId());
            simulateLegacyState(storage, tpResponse.user.getSupertokensUserId());

            // Verify all need backfill
            assertEquals(3, backfillStorage.getBackfillPendingUsersCount(appIdentifier));
            assertTrue(backfillStorage.verifyBackfillCompleteness(appIdentifier) > 0);

            // Run backfill
            int processed = backfillStorage.backfillUsersBatch(appIdentifier, 1000);
            assertEquals(3, processed);

            // Verify completion
            assertEquals(0, backfillStorage.getBackfillPendingUsersCount(appIdentifier));
            assertEquals(0, backfillStorage.verifyBackfillCompleteness(appIdentifier));

            // Verify EP user: 1 account info (email)
            String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
            assertEquals(1, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + epUser.getSupertokensUserId() + "'"));

            // Verify PL user: 2 account infos (email + phone)
            assertEquals(2, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + plResponse.user.getSupertokensUserId() + "'"));

            // Verify TP user: 2 account infos (email + tparty)
            assertEquals(2, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + tpResponse.user.getSupertokensUserId() + "'"));

            // Verify primary_user_tenants has reservations for linked users
            String primaryUserTenantsTable = Config.getConfig(storage).getPrimaryUserTenantsTable();
            int primaryReservations = countRows(storage, primaryUserTenantsTable,
                    "primary_user_id = '" + epUser.getSupertokensUserId() + "'");
            // EP email + PL email + PL phone = 3 reservations
            assertTrue("Expected at least 3 primary_user_tenants rows, got " + primaryReservations,
                    primaryReservations >= 3);

            // Verify recipe_user_tenants
            String recipeUserTenantsTable = Config.getConfig(storage).getRecipeUserTenantsTable();
            assertTrue(countRows(storage, recipeUserTenantsTable,
                    "recipe_user_id = '" + epUser.getSupertokensUserId() + "'") >= 1);
            assertTrue(countRows(storage, recipeUserTenantsTable,
                    "recipe_user_id = '" + plResponse.user.getSupertokensUserId() + "'") >= 2);
            assertTrue(countRows(storage, recipeUserTenantsTable,
                    "recipe_user_id = '" + tpResponse.user.getSupertokensUserId() + "'") >= 2);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillCronSkipsInLegacyMode() throws Exception {
        // Default mode is LEGACY, so the cron should skip
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;

            // Verify mode is LEGACY
            assertEquals(io.supertokens.pluginInterface.MigrationMode.LEGACY,
                    backfillStorage.getMigrationMode());

            // Create a user and simulate legacy state
            AuthRecipeUserInfo user = EmailPassword.signUp(main, "test@test.com", "password123");
            simulateLegacyState(storage, user.getSupertokensUserId());

            // The BackfillReservationTables cron is registered but should skip in LEGACY mode
            // Wait a bit to give cron a chance to run
            Thread.sleep(2000);

            // User should still need backfill (cron didn't process it)
            AppIdentifier appIdentifier = new AppIdentifier(null, null);
            assertEquals(1, backfillStorage.getBackfillPendingUsersCount(appIdentifier));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillWithMultiTenantUsers() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

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
            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // Create user in default tenant
            AuthRecipeUserInfo user = EmailPassword.signUp(main, "multi@test.com", "password123");

            // Add user to second tenant
            Multitenancy.addUserIdToTenant(main, t1, t1Storage,
                    user.getSupertokensUserId());

            // Simulate legacy state
            simulateLegacyState(storage, user.getSupertokensUserId());

            // Run backfill
            backfillStorage.backfillUsersBatch(appIdentifier, 100);

            // Verify recipe_user_tenants has entries for both tenants
            String recipeUserTenantsTable = Config.getConfig(storage).getRecipeUserTenantsTable();
            int tenantCount = countRows(storage, recipeUserTenantsTable,
                    "recipe_user_id = '" + user.getSupertokensUserId() + "'");
            // Should have 2 rows (1 per tenant × 1 account info type)
            assertEquals(2, tenantCount);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillWithLinkedAccountsAcrossRecipes() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // Create EP user (primary) + TP user (linked)
            AuthRecipeUserInfo epUser = EmailPassword.signUp(main, "primary@test.com", "password123");
            ThirdParty.SignInUpResponse tpResponse = ThirdParty.signInUp(
                    main, "github", "gh1", "linked@test.com");

            AuthRecipe.createPrimaryUser(main, epUser.getSupertokensUserId());
            AuthRecipe.linkAccounts(main, tpResponse.user.getSupertokensUserId(),
                    epUser.getSupertokensUserId());

            // Simulate legacy for both
            simulateLegacyState(storage, epUser.getSupertokensUserId());
            simulateLegacyState(storage, tpResponse.user.getSupertokensUserId());

            // Backfill
            backfillStorage.backfillUsersBatch(appIdentifier, 100);

            // Verify primary_user_tenants has both EP email and TP email
            String primaryUserTenantsTable = Config.getConfig(storage).getPrimaryUserTenantsTable();
            int reservations = countRows(storage, primaryUserTenantsTable,
                    "primary_user_id = '" + epUser.getSupertokensUserId() + "'");
            // EP email + TP email + TP tparty = at least 3
            assertTrue("Expected at least 3 primary_user_tenants rows, got " + reservations,
                    reservations >= 3);

            // Verify completeness
            assertEquals(0, backfillStorage.getBackfillPendingUsersCount(appIdentifier));
            assertEquals(0, backfillStorage.verifyBackfillCompleteness(appIdentifier));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillBatchSizeIsRespected() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // Create 5 users
            for (int i = 0; i < 5; i++) {
                AuthRecipeUserInfo user = EmailPassword.signUp(main,
                        "batch" + i + "@test.com", "password123");
                simulateLegacyState(storage, user.getSupertokensUserId());
            }

            assertEquals(5, backfillStorage.getBackfillPendingUsersCount(appIdentifier));

            // Process with batch size 2
            int batch1 = backfillStorage.backfillUsersBatch(appIdentifier, 2);
            assertEquals(2, batch1);
            assertEquals(3, backfillStorage.getBackfillPendingUsersCount(appIdentifier));

            int batch2 = backfillStorage.backfillUsersBatch(appIdentifier, 2);
            assertEquals(2, batch2);
            assertEquals(1, backfillStorage.getBackfillPendingUsersCount(appIdentifier));

            int batch3 = backfillStorage.backfillUsersBatch(appIdentifier, 2);
            assertEquals(1, batch3);
            assertEquals(0, backfillStorage.getBackfillPendingUsersCount(appIdentifier));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
