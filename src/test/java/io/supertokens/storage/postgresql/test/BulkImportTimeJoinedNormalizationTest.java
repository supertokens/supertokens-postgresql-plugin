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
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Regression test for the batch time_joined normalization used by bulk import
 * (supertokens/supertokens-core#1347, PLAN-006).
 *
 * <p>Bulk import inserts each member of a linked group with
 * {@code primary_or_recipe_user_time_joined} equal to that member's OWN
 * {@code time_joined} (see {@code EmailPasswordQueries.importUsers_Transaction} and the
 * third-party / passwordless equivalents) and never normalizes the group afterwards. When
 * a group's members have widely divergent {@code time_joined} values this leaves multiple
 * distinct {@code primary_or_recipe_user_time_joined} values within one group, violating the
 * invariant that user-list pagination relies on. The new
 * {@link Start#updateTimeJoinedForPrimaryUsers_Transaction} restores it by collapsing each
 * group to a single value equal to the group's minimum {@code time_joined}.
 *
 * <p>Each test reproduces the bulk-import insert shape directly (so it does not depend on the
 * higher-level import orchestration) for a linked group with widely divergent member times,
 * then verifies the invariant is violated, runs the normalization, and asserts every group in
 * every table the storage maintains for that migration mode now satisfies
 * {@code COUNT(DISTINCT primary_or_recipe_user_time_joined) = 1} equal to the group MIN.
 */
public class BulkImportTimeJoinedNormalizationTest {

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

    private static final String RECIPE_ID = "emailpassword";

    /** A member of a bulk-imported linked group: its own user id and time_joined. */
    private static class Member {
        final String userId;
        final long timeJoined;

        Member(String userId, long timeJoined) {
            this.userId = userId;
            this.timeJoined = timeJoined;
        }
    }

    @Test
    public void testNormalizesLegacyMode() throws Exception {
        runForMode(MigrationMode.LEGACY);
    }

    @Test
    public void testNormalizesDualWriteReadOldMode() throws Exception {
        runForMode(MigrationMode.DUAL_WRITE_READ_OLD);
    }

    @Test
    public void testNormalizesDualWriteReadNewMode() throws Exception {
        runForMode(MigrationMode.DUAL_WRITE_READ_NEW);
    }

    @Test
    public void testNormalizesMigratedMode() throws Exception {
        runForMode(MigrationMode.MIGRATED);
    }

    private void runForMode(MigrationMode mode) throws Exception {
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(new String[]{"../"}, false);
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }
            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(mode);

            AppIdentifier appIdentifier = new AppIdentifier(null, null);
            String usersTable = Config.getConfig(storage).getUsersTable();
            String appTable = Config.getConfig(storage).getAppIdToUserIdTable();

            // A bulk-imported linked group with widely divergent member time_joined values
            // (the core#1347 shape). The primary user's own row is first; its FK on
            // primary_or_recipe_user_id references itself.
            long minTime = 1_000_000_000_000L;
            String primaryId = UUID.randomUUID().toString();
            List<Member> members = Arrays.asList(
                    new Member(primaryId, 3_000_000_000_000L),               // primary user's own row
                    new Member(UUID.randomUUID().toString(), minTime),       // group MIN
                    new Member(UUID.randomUUID().toString(), 9_000_000_000_000L) // large-time member
            );
            insertBulkImportShapedGroup(storage, mode, appIdentifier, primaryId, members);

            // Teeth: at least one table the mode maintains must show the invariant violated
            // (more than one distinct primary_or_recipe_user_time_joined for the group) before we
            // normalize. app_id_to_user_id carries the 0 sentinel in LEGACY, so the divergence
            // shows up there only once the new tables are written; all_auth_recipe_users always
            // carries the real (divergent) times whenever it is written.
            boolean violated = distinctCount(storage, appTable, primaryId) > 1;
            if (mode.writesToOldTables()) {
                violated |= distinctCount(storage, usersTable, primaryId) > 1;
            }
            assertTrue("bulk-import-shaped inserts should violate the group invariant before "
                    + "normalization (mode " + mode + ")", violated);

            // Normalize the group inside a transaction (the exercise under test).
            storage.startTransaction(con -> {
                storage.updateTimeJoinedForPrimaryUsers_Transaction(appIdentifier, con,
                        Collections.singletonList(primaryId));
                storage.commitTransaction(con);
                return null;
            });

            // Every table the mode maintains now holds a single value for the group, equal to the
            // group's MIN(time_joined) as computed within that table.
            if (mode.writesToOldTables()) {
                assertEquals("all_auth_recipe_users not collapsed to one value (mode " + mode + ")",
                        1, distinctCount(storage, usersTable, primaryId));
                assertEquals("all_auth_recipe_users normalized to wrong value (mode " + mode + ")",
                        minTime, singleTimeJoined(storage, usersTable, primaryId));
            }
            assertEquals("app_id_to_user_id not collapsed to one value (mode " + mode + ")",
                    1, distinctCount(storage, appTable, primaryId));
            // In LEGACY the new tables are written with the 0 sentinel, so the group MIN there is 0.
            long expectedApp = mode.writesToNewTables() ? minTime : 0L;
            assertEquals("app_id_to_user_id normalized to wrong value (mode " + mode + ")",
                    expectedApp, singleTimeJoined(storage, appTable, primaryId));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Reproduces the rows bulk import writes for a linked group, mirroring
     * {@code importUsers_Transaction}: each member row's {@code primary_or_recipe_user_time_joined}
     * is its own {@code time_joined}, and the new tables keep the 0 sentinel while writes skip them
     * (LEGACY). No group normalization is performed here — that is what the method under test does.
     */
    private void insertBulkImportShapedGroup(Start storage, MigrationMode mode, AppIdentifier appIdentifier,
                                             String primaryId, List<Member> members) throws Exception {
        String app = appIdentifier.getAppId();
        String appTable = Config.getConfig(storage).getAppIdToUserIdTable();
        String usersTable = Config.getConfig(storage).getUsersTable();
        storage.startTransaction(con -> {
            try {
                Connection sqlCon = (Connection) con.getConnection();

                String appInsert = "INSERT INTO " + appTable
                        + "(app_id, user_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user, "
                        + "recipe_id, time_joined, primary_or_recipe_user_time_joined) VALUES(?, ?, ?, ?, ?, ?, ?)";
                for (Member m : members) {
                    long t = mode.writesToNewTables() ? m.timeJoined : 0L;
                    try (PreparedStatement pst = sqlCon.prepareStatement(appInsert)) {
                        pst.setString(1, app);
                        pst.setString(2, m.userId);
                        pst.setString(3, primaryId);
                        pst.setBoolean(4, true);
                        pst.setString(5, RECIPE_ID);
                        pst.setLong(6, t);
                        pst.setLong(7, t);
                        pst.executeUpdate();
                    }
                }

                if (mode.writesToOldTables()) {
                    String usersInsert = "INSERT INTO " + usersTable
                            + "(app_id, tenant_id, user_id, primary_or_recipe_user_id, "
                            + "is_linked_or_is_a_primary_user, recipe_id, time_joined, "
                            + "primary_or_recipe_user_time_joined) VALUES(?, 'public', ?, ?, ?, ?, ?, ?)";
                    for (Member m : members) {
                        try (PreparedStatement pst = sqlCon.prepareStatement(usersInsert)) {
                            pst.setString(1, app);
                            pst.setString(2, m.userId);
                            pst.setString(3, primaryId);
                            pst.setBoolean(4, true);
                            pst.setString(5, RECIPE_ID);
                            pst.setLong(6, m.timeJoined);
                            pst.setLong(7, m.timeJoined);
                            pst.executeUpdate();
                        }
                    }
                }

                storage.commitTransaction(con);
            } catch (SQLException e) {
                throw new StorageQueryException(e);
            }
            return null;
        });
    }

    private int distinctCount(Start storage, String table, String primaryId) throws Exception {
        return storage.startTransaction(con -> {
            try {
                Connection sqlCon = (Connection) con.getConnection();
                try (PreparedStatement pst = sqlCon.prepareStatement(
                        "SELECT COUNT(DISTINCT primary_or_recipe_user_time_joined) AS c FROM " + table
                                + " WHERE app_id = 'public' AND primary_or_recipe_user_id = ?")) {
                    pst.setString(1, primaryId);
                    try (ResultSet rs = pst.executeQuery()) {
                        rs.next();
                        return rs.getInt("c");
                    }
                }
            } catch (SQLException e) {
                throw new StorageQueryException(e);
            }
        });
    }

    private long singleTimeJoined(Start storage, String table, String primaryId) throws Exception {
        return storage.startTransaction(con -> {
            try {
                Connection sqlCon = (Connection) con.getConnection();
                try (PreparedStatement pst = sqlCon.prepareStatement(
                        "SELECT DISTINCT primary_or_recipe_user_time_joined AS t FROM " + table
                                + " WHERE app_id = 'public' AND primary_or_recipe_user_id = ?")) {
                    pst.setString(1, primaryId);
                    try (ResultSet rs = pst.executeQuery()) {
                        assertTrue(rs.next());
                        return rs.getLong("t");
                    }
                }
            } catch (SQLException e) {
                throw new StorageQueryException(e);
            }
        });
    }
}
