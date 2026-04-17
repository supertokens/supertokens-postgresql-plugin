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
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.migration.MigrationBackfillStorage;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.*;

public class BackfillTest {

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

        // Backfill sources from the legacy all_auth_recipe_users table. Switch to
        // DUAL_WRITE_READ_OLD so new signups populate both the legacy table (for
        // backfill to read) and the reservation tables (for simulateLegacyState
        // to clear).
        Storage storage = StorageLayer.getStorage(process.getProcess());
        if (storage.getType() == STORAGE_TYPE.SQL) {
            Config.getConfig((Start) storage).setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_OLD);
        }
        return process;
    }

    /**
     * Simulates legacy state for a user by clearing their reservation table data
     * and resetting time_joined to 0 on app_id_to_user_id.
     */
    private void simulateLegacyState(Start storage, String userId) throws Exception {
        String appIdToUserIdTable = Config.getConfig(storage).getAppIdToUserIdTable();
        String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
        String recipeUserTenantsTable = Config.getConfig(storage).getRecipeUserTenantsTable();
        String primaryUserTenantsTable = Config.getConfig(storage).getPrimaryUserTenantsTable();

        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (Statement stmt = sqlCon.createStatement()) {
                // Clear reservation tables for this user
                stmt.executeUpdate("DELETE FROM " + recipeUserTenantsTable
                        + " WHERE recipe_user_id = '" + userId + "'");
                stmt.executeUpdate("DELETE FROM " + primaryUserTenantsTable
                        + " WHERE primary_user_id = '" + userId + "'");
                stmt.executeUpdate("DELETE FROM " + accountInfosTable
                        + " WHERE recipe_user_id = '" + userId + "'");

                // Reset time_joined to simulate pre-migration state
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
    public void backfillPopulatesTimeJoined() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // Create a user
            AuthRecipeUserInfo user = EmailPassword.signUp(main, "test@example.com", "password123");

            // Simulate legacy state
            simulateLegacyState(storage, user.getSupertokensUserId());

            // Verify time_joined is 0
            assertEquals(1, backfillStorage.getBackfillPendingUsersCount(appIdentifier));

            // Run backfill
            int processed = backfillStorage.backfillUsersBatch(appIdentifier, 100);
            assertEquals(1, processed);

            // Verify time_joined is no longer 0
            assertEquals(0, backfillStorage.getBackfillPendingUsersCount(appIdentifier));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillPopulatesEmailPasswordAccountInfo() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            AuthRecipeUserInfo user = EmailPassword.signUp(main, "ep@example.com", "password123");
            simulateLegacyState(storage, user.getSupertokensUserId());

            // Verify account infos cleared
            String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
            assertEquals(0, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + user.getSupertokensUserId() + "'"));

            // Run backfill
            backfillStorage.backfillUsersBatch(appIdentifier, 100);

            // Verify account info restored: 1 row (email)
            assertEquals(1, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + user.getSupertokensUserId() + "'"));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillPopulatesPasswordlessAccountInfo() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // Create passwordless user with email
            Passwordless.CreateCodeResponse code = Passwordless.createCode(
                    main, "pl@example.com", null, null, null);
            Passwordless.ConsumeCodeResponse plResponse = Passwordless.consumeCode(
                    main, code.deviceId, code.deviceIdHash, code.userInputCode, null);

            // Update to also have phone
            Passwordless.updateUser(main, plResponse.user.getSupertokensUserId(),
                    null, new Passwordless.FieldUpdate("+11234567890"));

            simulateLegacyState(storage, plResponse.user.getSupertokensUserId());

            // Run backfill
            backfillStorage.backfillUsersBatch(appIdentifier, 100);

            // Verify: 2 rows (email + phone)
            String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
            assertEquals(2, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + plResponse.user.getSupertokensUserId() + "'"));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillPopulatesThirdPartyAccountInfo() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            ThirdParty.SignInUpResponse tpResponse = ThirdParty.signInUp(
                    main, "google", "google-user-1", "tp@example.com");

            simulateLegacyState(storage, tpResponse.user.getSupertokensUserId());

            // Run backfill
            backfillStorage.backfillUsersBatch(appIdentifier, 100);

            // Verify: 2 rows (email + tparty)
            String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
            assertEquals(2, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + tpResponse.user.getSupertokensUserId() + "'"));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillPopulatesRecipeUserTenants() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            AuthRecipeUserInfo user = EmailPassword.signUp(main, "tenant@example.com", "password123");
            simulateLegacyState(storage, user.getSupertokensUserId());

            // Run backfill
            backfillStorage.backfillUsersBatch(appIdentifier, 100);

            // Verify recipe_user_tenants populated
            String recipeUserTenantsTable = Config.getConfig(storage).getRecipeUserTenantsTable();
            int tenantRows = countRows(storage, recipeUserTenantsTable,
                    "recipe_user_id = '" + user.getSupertokensUserId() + "'");
            assertTrue("Expected at least 1 recipe_user_tenants row, got " + tenantRows,
                    tenantRows >= 1);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillPopulatesPrimaryUserTenants() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // Create two users and link them
            AuthRecipeUserInfo epUser = EmailPassword.signUp(main, "primary@example.com", "password123");
            AuthRecipeUserInfo epUser2 = EmailPassword.signUp(main, "linked@example.com", "password123");
            AuthRecipe.createPrimaryUser(main, epUser.getSupertokensUserId());
            AuthRecipe.linkAccounts(main, epUser2.getSupertokensUserId(), epUser.getSupertokensUserId());

            // Simulate legacy state for both
            simulateLegacyState(storage, epUser.getSupertokensUserId());
            simulateLegacyState(storage, epUser2.getSupertokensUserId());

            // Also clear primary_user_tenants for the primary
            String primaryUserTenantsTable = Config.getConfig(storage).getPrimaryUserTenantsTable();
            storage.startTransaction(con -> {
                Connection sqlCon = (Connection) con.getConnection();
                try (Statement stmt = sqlCon.createStatement()) {
                    stmt.executeUpdate("DELETE FROM " + primaryUserTenantsTable
                            + " WHERE primary_user_id = '" + epUser.getSupertokensUserId() + "'");
                }
                return null;
            });

            // Run backfill for both users
            backfillStorage.backfillUsersBatch(appIdentifier, 100);

            // Verify primary_user_tenants populated
            int primaryTenantRows = countRows(storage, primaryUserTenantsTable,
                    "primary_user_id = '" + epUser.getSupertokensUserId() + "'");
            // Should have reservations for both primary@example.com and linked@example.com
            assertTrue("Expected at least 2 primary_user_tenants rows, got " + primaryTenantRows,
                    primaryTenantRows >= 2);
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillIsIdempotent() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            AuthRecipeUserInfo user = EmailPassword.signUp(main, "idempotent@example.com", "password123");
            simulateLegacyState(storage, user.getSupertokensUserId());

            // Run backfill twice
            int processed1 = backfillStorage.backfillUsersBatch(appIdentifier, 100);
            assertEquals(1, processed1);

            int processed2 = backfillStorage.backfillUsersBatch(appIdentifier, 100);
            assertEquals(0, processed2); // No more users to process

            // Verify data is correct
            String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
            assertEquals(1, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + user.getSupertokensUserId() + "'"));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillCountReturnsCorrectPending() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // No pending initially
            assertEquals(0, backfillStorage.getBackfillPendingUsersCount(appIdentifier));

            // Create 3 users
            AuthRecipeUserInfo u1 = EmailPassword.signUp(main, "u1@example.com", "password123");
            AuthRecipeUserInfo u2 = EmailPassword.signUp(main, "u2@example.com", "password123");
            AuthRecipeUserInfo u3 = EmailPassword.signUp(main, "u3@example.com", "password123");

            // Still 0 (users created with proper time_joined)
            assertEquals(0, backfillStorage.getBackfillPendingUsersCount(appIdentifier));

            // Simulate legacy for 2 of them
            simulateLegacyState(storage, u1.getSupertokensUserId());
            simulateLegacyState(storage, u2.getSupertokensUserId());

            assertEquals(2, backfillStorage.getBackfillPendingUsersCount(appIdentifier));

            // Backfill 1
            backfillStorage.backfillUsersBatch(appIdentifier, 1);
            assertEquals(1, backfillStorage.getBackfillPendingUsersCount(appIdentifier));

            // Backfill remaining
            backfillStorage.backfillUsersBatch(appIdentifier, 100);
            assertEquals(0, backfillStorage.getBackfillPendingUsersCount(appIdentifier));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void verifyBackfillCompletenessDetectsMissingData() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // Empty DB is consistent
            assertEquals(0, backfillStorage.verifyBackfillCompleteness(appIdentifier));

            // Create user, then simulate legacy state. We additionally wipe
            // recipe_user_account_infos for this recipe_user_id explicitly to
            // guard against any lingering rows from dual-write signup.
            AuthRecipeUserInfo user = EmailPassword.signUp(main, "verify@example.com", "password123");
            simulateLegacyState(storage, user.getSupertokensUserId());
            final String uid = user.getSupertokensUserId();
            final String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
            storage.startTransaction(con -> {
                Connection sqlCon = (Connection) con.getConnection();
                try (Statement stmt = sqlCon.createStatement()) {
                    stmt.executeUpdate("DELETE FROM " + accountInfosTable
                            + " WHERE TRIM(recipe_user_id) = '" + uid + "'");
                }
                return null;
            });

            // User exists in app_id_to_user_id but NOT in recipe_user_account_infos → inconsistent
            assertEquals(1, backfillStorage.verifyBackfillCompleteness(appIdentifier));

            // Backfill the user
            backfillStorage.backfillUsersBatch(appIdentifier, 100);

            // Now consistent
            assertEquals(0, backfillStorage.verifyBackfillCompleteness(appIdentifier));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void backfillMultipleRecipeTypes() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            MigrationBackfillStorage backfillStorage = (MigrationBackfillStorage) storage;
            AppIdentifier appIdentifier = new AppIdentifier(null, null);

            // Create one of each recipe type
            AuthRecipeUserInfo epUser = EmailPassword.signUp(main, "ep@example.com", "password123");

            Passwordless.CreateCodeResponse code = Passwordless.createCode(
                    main, "pl@example.com", null, null, null);
            Passwordless.ConsumeCodeResponse plResponse = Passwordless.consumeCode(
                    main, code.deviceId, code.deviceIdHash, code.userInputCode, null);

            ThirdParty.SignInUpResponse tpResponse = ThirdParty.signInUp(
                    main, "google", "g1", "tp@example.com");

            // Simulate legacy state for all
            simulateLegacyState(storage, epUser.getSupertokensUserId());
            simulateLegacyState(storage, plResponse.user.getSupertokensUserId());
            simulateLegacyState(storage, tpResponse.user.getSupertokensUserId());

            assertEquals(3, backfillStorage.getBackfillPendingUsersCount(appIdentifier));

            // Backfill all
            int processed = backfillStorage.backfillUsersBatch(appIdentifier, 100);
            assertEquals(3, processed);

            // All should be complete
            assertEquals(0, backfillStorage.getBackfillPendingUsersCount(appIdentifier));
            assertEquals(0, backfillStorage.verifyBackfillCompleteness(appIdentifier));

            // Verify each has correct account info count
            String accountInfosTable = Config.getConfig(storage).getRecipeUserAccountInfosTable();
            assertEquals(1, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + epUser.getSupertokensUserId() + "'")); // email
            assertEquals(1, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + plResponse.user.getSupertokensUserId() + "'")); // email
            assertEquals(2, countRows(storage, accountInfosTable,
                    "recipe_user_id = '" + tpResponse.user.getSupertokensUserId() + "'")); // email + tparty
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
