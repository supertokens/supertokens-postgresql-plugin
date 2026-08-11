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
import io.supertokens.ResourceDistributor;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.useridmapping.LockedUser;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.LockedUserImpl;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.queries.AccountInfoQueries;
import io.supertokens.storageLayer.StorageLayer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Behaviour (result-set) tests for the decorrelated tenant-removal reservation cleanup of issue #369,
 * {@code AccountInfoQueries.removeAccountInfoReservationForPrimaryUserWhileRemovingTenant_Transaction}.
 *
 * <p>The rewrite replaces a correlated-OR subquery with a decorrelated member-driven join. The correlated
 * OR was an obfuscated pair-exclusion: for a {@code recipe_user_tenants} row with recipe user {@code ru} and
 * tenant {@code t}, it kept every group member's row EXCEPT the exact {@code (removedMember, removedTenant)}
 * pair. These tests exercise the real production statement (built inside the method, not a copy) against a
 * hand-seeded fixture and assert the surviving {@code primary_user_tenants} tenant set for each of the three
 * row classes and the two boundary cases the rewrite has to preserve:</p>
 *
 * <ul>
 *   <li>the removed member's removed-tenant row is dropped — unless another member also holds it;</li>
 *   <li>the removed member's OTHER-tenant rows survive, even when the removed member is the only member in
 *       that tenant (that tenant must not be swept away); and</li>
 *   <li>other members' rows survive, so a tenant another member also holds — including the removed tenant —
 *       survives.</li>
 * </ul>
 *
 * <p>The cleanup only ever inspects {@code tenant_id} membership (the {@code NOT IN} set is a set of
 * tenant_ids), so the fixture varies tenants and keeps a single third-party account-info value per user.
 * Tenants are inserted directly into the tenants table (the only FK the reservation tables need) under the
 * test app.</p>
 */
public class TenantRemovalReservationCleanupTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    private static final String RECIPE_ID = "thirdparty";
    private static final String AINFO_TYPE = "tparty";

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
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        return process;
    }

    // 36-char deterministic user id (CHAR(36)).
    private static String userId(String k) {
        return "u" + String.format("%035d", Integer.parseInt(k.replaceAll("\\D", "")));
    }

    /**
     * Scenario 1 — removed tenant has no other member; the removed member also sits alone in another tenant.
     *
     * <p>Group {A(primary), B}. A is in {tRemoved, tSoloA}; B is in {tBOnly}. Remove A from tRemoved.</p>
     *
     * <p>Expected surviving tenants for the primary: {tSoloA, tBOnly}. Covers all three row classes:
     * the removed pair (A,tRemoved) is dropped (class 1); A's other-tenant row (A,tSoloA) survives even
     * though A is the ONLY member there (class 2 + boundary); B's row (B,tBOnly) survives (class 3).</p>
     */
    @Test
    public void testRemovedTenantSweptWhenNoOtherMemberHoldsIt() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;
            Start storage = (Start) StorageLayer.getStorage(main);

            TenantIdentifier appTenant = ResourceDistributor.getAppForTesting();
            String appId = appTenant.getAppId();

            String a = userId("0");
            String b = userId("1");
            String tRemoved = "tremoved";
            String tSoloA = "tsoloa";
            String tBOnly = "tbonly";

            Connection con = ConnectionPool.getConnection(storage);
            try {
                con.setAutoCommit(true);
                Tables t = tables(storage);

                createTenants(con, t, appId, tRemoved, tSoloA, tBOnly);
                // group members reserved under primary A
                insertRuai(con, t, appId, a, a);
                insertRuai(con, t, appId, b, a);
                // recipe_user_tenants memberships
                insertRut(con, t, appId, a, tRemoved);
                insertRut(con, t, appId, a, tSoloA);
                insertRut(con, t, appId, b, tBOnly);
                // primary_user_tenants reservations for A across all three tenants
                insertPut(con, t, appId, a, tRemoved);
                insertPut(con, t, appId, a, tSoloA);
                insertPut(con, t, appId, a, tBOnly);

                runCleanup(storage, con, appId, tRemoved, a, a);

                assertEquals("removed tenant must be swept (no other member holds it); "
                                + "the removed member's other tenant and other members' tenants must survive",
                        setOf(tBOnly, tSoloA), survivingTenants(con, t, appId, a));
            } finally {
                con.close();
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Scenario 2 — another member also holds the removed tenant; the removed tenant must survive.
     *
     * <p>Group {A(primary), B}. Both A and B are in tRemoved; A is also in tSoloA. Remove A from tRemoved.</p>
     *
     * <p>Expected surviving tenants for the primary: {tRemoved, tSoloA}. tRemoved survives because B still
     * holds it (class 3) even though the (A,tRemoved) pair itself is excluded.</p>
     */
    @Test
    public void testRemovedTenantSurvivesWhenAnotherMemberHoldsIt() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;
            Start storage = (Start) StorageLayer.getStorage(main);

            TenantIdentifier appTenant = ResourceDistributor.getAppForTesting();
            String appId = appTenant.getAppId();

            String a = userId("0");
            String b = userId("1");
            String tRemoved = "tremoved";
            String tSoloA = "tsoloa";

            Connection con = ConnectionPool.getConnection(storage);
            try {
                con.setAutoCommit(true);
                Tables t = tables(storage);

                createTenants(con, t, appId, tRemoved, tSoloA);
                insertRuai(con, t, appId, a, a);
                insertRuai(con, t, appId, b, a);
                insertRut(con, t, appId, a, tRemoved);
                insertRut(con, t, appId, b, tRemoved);
                insertRut(con, t, appId, a, tSoloA);
                insertPut(con, t, appId, a, tRemoved);
                insertPut(con, t, appId, a, tSoloA);

                runCleanup(storage, con, appId, tRemoved, a, a);

                assertEquals("removed tenant must survive because another linked member still holds it",
                        setOf(tRemoved, tSoloA), survivingTenants(con, t, appId, a));
            } finally {
                con.close();
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Driving the real production statement + fixture helpers.
    // ---------------------------------------------------------------------------------------------

    private void runCleanup(Start storage, Connection con, String appId, String removedTenantId,
                            String removedRecipeUserId, String primaryUserId) throws Exception {
        TenantIdentifier removedTenant = new TenantIdentifier(null, appId, removedTenantId);
        LockedUser lockedUser = new LockedUserImpl(removedRecipeUserId, RECIPE_ID, primaryUserId, con);
        AccountInfoQueries.removeAccountInfoReservationForPrimaryUserWhileRemovingTenant_Transaction(
                storage, con, removedTenant, lockedUser);
    }

    private TreeSet<String> survivingTenants(Connection con, Tables t, String appId, String primaryUserId)
            throws Exception {
        TreeSet<String> out = new TreeSet<>();
        String sql = "SELECT tenant_id FROM " + t.put + " WHERE app_id = ? AND primary_user_id = ?";
        try (PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, appId);
            pst.setString(2, primaryUserId);
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getString(1));
                }
            }
        }
        return out;
    }

    private void createTenants(Connection con, Tables t, String appId, String... tenantIds) throws Exception {
        String sql = "INSERT INTO " + t.tenants + " (app_id, tenant_id, created_at_time) VALUES (?, ?, ?)";
        for (String tenantId : tenantIds) {
            try (PreparedStatement pst = con.prepareStatement(sql)) {
                pst.setString(1, appId);
                pst.setString(2, tenantId);
                pst.setLong(3, 0L);
                pst.executeUpdate();
            }
        }
    }

    private void insertRuai(Connection con, Tables t, String appId, String recipeUserId, String primaryUserId)
            throws Exception {
        String sql = "INSERT INTO " + t.ruai
                + " (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,"
                + "  third_party_id, third_party_user_id, primary_user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, appId);
            pst.setString(2, recipeUserId);
            pst.setString(3, RECIPE_ID);
            pst.setString(4, AINFO_TYPE);
            pst.setString(5, "google::" + recipeUserId);
            pst.setString(6, "google");
            pst.setString(7, recipeUserId);
            pst.setString(8, primaryUserId);
            pst.executeUpdate();
        }
    }

    private void insertRut(Connection con, Tables t, String appId, String recipeUserId, String tenantId)
            throws Exception {
        String sql = "INSERT INTO " + t.rut
                + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type, account_info_value,"
                + "  third_party_id, third_party_user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, appId);
            pst.setString(2, recipeUserId);
            pst.setString(3, tenantId);
            pst.setString(4, RECIPE_ID);
            pst.setString(5, AINFO_TYPE);
            pst.setString(6, "google::" + recipeUserId);
            pst.setString(7, "google");
            pst.setString(8, recipeUserId);
            pst.executeUpdate();
        }
    }

    private void insertPut(Connection con, Tables t, String appId, String primaryUserId, String tenantId)
            throws Exception {
        String sql = "INSERT INTO " + t.put
                + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id)"
                + " VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, appId);
            pst.setString(2, tenantId);
            pst.setString(3, AINFO_TYPE);
            pst.setString(4, "google::" + primaryUserId);
            pst.setString(5, primaryUserId);
            pst.executeUpdate();
        }
    }

    private static TreeSet<String> setOf(String... values) {
        TreeSet<String> out = new TreeSet<>();
        for (String v : values) {
            out.add(v);
        }
        return out;
    }

    private static final class Tables {
        final String tenants;
        final String ruai;
        final String rut;
        final String put;

        Tables(String tenants, String ruai, String rut, String put) {
            this.tenants = tenants;
            this.ruai = ruai;
            this.rut = rut;
            this.put = put;
        }
    }

    private Tables tables(Start storage) {
        return new Tables(
                Config.getConfig(storage).getTenantsTable(),
                Config.getConfig(storage).getRecipeUserAccountInfosTable(),
                Config.getConfig(storage).getRecipeUserTenantsTable(),
                Config.getConfig(storage).getPrimaryUserTenantsTable());
    }
}
