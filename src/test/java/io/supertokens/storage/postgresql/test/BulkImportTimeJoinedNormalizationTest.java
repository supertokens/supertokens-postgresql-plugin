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
import io.supertokens.pluginInterface.authRecipe.sqlStorage.AuthRecipeSQLStorage;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Regression coverage for supertokens/supertokens-postgresql-plugin#348 (context:
 * supertokens/supertokens-core#1347).
 *
 * Bulk import inserts each linked member's row with its own {@code time_joined} copied into
 * {@code primary_or_recipe_user_time_joined} and never normalizes the group afterwards, so a linked
 * group with divergent per-member join times ends up holding multiple distinct
 * {@code primary_or_recipe_user_time_joined} values. That violates the invariant user-list pagination
 * depends on (the DISTINCT cursor on that column vs. the group-MIN next-page token).
 *
 * These tests exercise the new plugin-interface method
 * {@link AuthRecipeSQLStorage#updateTimeJoinedForPrimaryUsers_Transaction}: they reproduce the
 * bulk-import-shaped violated state directly in the tables each migration mode maintains, then call the
 * method and assert every group again satisfies
 * {@code COUNT(DISTINCT primary_or_recipe_user_time_joined) = 1 = MIN(time_joined)} across both
 * {@code all_auth_recipe_users} and {@code app_id_to_user_id}.
 */
public class BulkImportTimeJoinedNormalizationTest {

    // Divergent join times for the three linked members. MIN == the value every row must collapse to.
    private static final long T_MIN = 1_000L;
    private static final long T_MID = 3_000L;
    private static final long T_MAX = 5_000L;

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

    @Test
    public void testNormalizationInLegacyMode() throws Exception {
        runNormalizationTest(MigrationMode.LEGACY);
    }

    @Test
    public void testNormalizationInDualWriteReadOldMode() throws Exception {
        runNormalizationTest(MigrationMode.DUAL_WRITE_READ_OLD);
    }

    @Test
    public void testNormalizationInDualWriteReadNewMode() throws Exception {
        runNormalizationTest(MigrationMode.DUAL_WRITE_READ_NEW);
    }

    @Test
    public void testNormalizationInMigratedMode() throws Exception {
        runNormalizationTest(MigrationMode.MIGRATED);
    }

    private void runNormalizationTest(MigrationMode mode) throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(mode);

            // Build a real linked group of three email-password users. Linking normalizes the group, so
            // right after this the invariant holds; we then deliberately re-violate it below to mimic the
            // bulk-import write shape (which never normalizes).
            AuthRecipeUserInfo primary = EmailPassword.signUp(main, "primary@example.com", "password123");
            AuthRecipeUserInfo member1 = EmailPassword.signUp(main, "member1@example.com", "password123");
            AuthRecipeUserInfo member2 = EmailPassword.signUp(main, "member2@example.com", "password123");
            String primaryId = primary.getSupertokensUserId();
            AuthRecipe.createPrimaryUser(main, primaryId);
            AuthRecipe.linkAccounts(main, member1.getSupertokensUserId(), primaryId);
            AuthRecipe.linkAccounts(main, member2.getSupertokensUserId(), primaryId);

            String usersTable = Config.getConfig(storage).getUsersTable();
            String appIdToUserIdTable = Config.getConfig(storage).getAppIdToUserIdTable();
            // app_id_to_user_id is written in every mode; all_auth_recipe_users only when the mode
            // writes to the old tables (i.e. not MIGRATED) — mirror the method's own branching.
            boolean touchesUsersTable = mode.writesToOldTables();

            // Reproduce the bulk-import-shaped violated state: give each member its own divergent
            // time_joined and set primary_or_recipe_user_time_joined = that same divergent value.
            violate(storage, appIdToUserIdTable, primary.getSupertokensUserId(), T_MID);
            violate(storage, appIdToUserIdTable, member1.getSupertokensUserId(), T_MIN);
            violate(storage, appIdToUserIdTable, member2.getSupertokensUserId(), T_MAX);
            assertEquals("precondition: app_id_to_user_id group must be violated before normalization",
                    3, countDistinctPrimaryTimeJoined(storage, appIdToUserIdTable, primaryId));
            if (touchesUsersTable) {
                violate(storage, usersTable, primary.getSupertokensUserId(), T_MID);
                violate(storage, usersTable, member1.getSupertokensUserId(), T_MIN);
                violate(storage, usersTable, member2.getSupertokensUserId(), T_MAX);
                assertEquals("precondition: all_auth_recipe_users group must be violated before normalization",
                        3, countDistinctPrimaryTimeJoined(storage, usersTable, primaryId));
            }

            // Call the new plugin-interface method, exercising the interface dispatch that #348 wires up.
            AppIdentifier appIdentifier = new AppIdentifier(null, null);
            storage.startTransaction(con -> {
                ((AuthRecipeSQLStorage) storage).updateTimeJoinedForPrimaryUsers_Transaction(
                        appIdentifier, con, Collections.singletonList(primaryId));
                storage.commitTransaction(con);
                return null;
            });

            // Every row in the group must now share a single primary_or_recipe_user_time_joined == MIN.
            assertEquals("app_id_to_user_id group must collapse to a single value in mode " + mode,
                    1, countDistinctPrimaryTimeJoined(storage, appIdToUserIdTable, primaryId));
            assertEquals("app_id_to_user_id group value must equal the group MIN(time_joined) in mode " + mode,
                    T_MIN, primaryTimeJoined(storage, appIdToUserIdTable, primaryId));
            if (touchesUsersTable) {
                assertEquals("all_auth_recipe_users group must collapse to a single value in mode " + mode,
                        1, countDistinctPrimaryTimeJoined(storage, usersTable, primaryId));
                assertEquals("all_auth_recipe_users group value must equal the group MIN(time_joined) in mode " + mode,
                        T_MIN, primaryTimeJoined(storage, usersTable, primaryId));
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // Sets both time_joined and primary_or_recipe_user_time_joined for a single user_id row to the same
    // divergent value — the exact shape bulk import leaves behind for a linked member.
    private void violate(Start storage, String table, String userId, long timeJoined) throws Exception {
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(
                    "UPDATE " + table + " SET time_joined = ?, primary_or_recipe_user_time_joined = ?"
                            + " WHERE app_id = ? AND user_id = ?")) {
                pst.setLong(1, timeJoined);
                pst.setLong(2, timeJoined);
                pst.setString(3, "public");
                pst.setString(4, userId);
                pst.executeUpdate();
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            storage.commitTransaction(con);
            return null;
        });
    }

    private int countDistinctPrimaryTimeJoined(Start storage, String table, String primaryUserId) throws Exception {
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT COUNT(DISTINCT primary_or_recipe_user_time_joined) FROM " + table
                                 + " WHERE app_id = 'public' AND primary_or_recipe_user_id = '" + primaryUserId + "'")) {
                rs.next();
                return rs.getInt(1);
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private long primaryTimeJoined(Start storage, String table, String primaryUserId) throws Exception {
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (Statement stmt = sqlCon.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT MIN(primary_or_recipe_user_time_joined) FROM " + table
                                 + " WHERE app_id = 'public' AND primary_or_recipe_user_id = '" + primaryUserId + "'")) {
                rs.next();
                return rs.getLong(1);
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
