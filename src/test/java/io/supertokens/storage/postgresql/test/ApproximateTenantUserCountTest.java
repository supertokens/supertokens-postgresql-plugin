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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.ResourceDistributor;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.sqlStorage.AuthRecipeSQLStorage;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * PLAN-009 unit 2: correctness and plan-shape guard for the approximate tenant user-count storage
 * contract — {@code computeTenantUserCountAnchor} (a snapshot exact count rebased onto a boundary)
 * and {@code countTenantUsersJoinedSince} (the live "joined since" delta), implemented in
 * GeneralQueries and exposed on {@link AuthRecipeSQLStorage}.
 *
 * <p>The load-bearing invariant is {@code anchor(sinceMs) + countTenantUsersJoinedSince(sinceMs)
 * == getUsersCount(tenant)} for creations, at any moment after the anchor was taken — even for
 * users created between taking the anchor and serving the delta. The exact per-tenant count
 * ({@code getUsersCountForTenant}) is the independent oracle each assertion checks against.
 *
 * <p>The delta's exactness relies on invariant I6 (all rows of a linked group share one
 * {@code primary_or_recipe_user_time_joined}, the group MIN, maintained by
 * {@code updateTimeJoinedForPrimaryUsers_Transaction} on every link): a user linked into a
 * pre-boundary group inherits a time {@code <= sinceMs} and correctly stays out of the delta window.
 */
public class ApproximateTenantUserCountTest {

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

    private static AuthRecipeUserInfo signUp(Main main, String email) throws Exception {
        AuthRecipeUserInfo u = EmailPassword.signUp(main, email, "password123");
        Thread.sleep(15);
        return u;
    }

    private static AuthRecipeUserInfo passwordlessWithEmail(Main main, String email) throws Exception {
        Passwordless.CreateCodeResponse code = Passwordless.createCode(main, email, null, null, null);
        AuthRecipeUserInfo u = Passwordless.consumeCode(main, code.deviceId, code.deviceIdHash,
                code.userInputCode, null).user;
        Thread.sleep(15);
        return u;
    }

    private static long exactCount(TenantIdentifier tenant, Storage storage) throws Exception {
        return AuthRecipe.getUsersCountForTenant(tenant, storage, null);
    }

    private static long anchor(TenantIdentifier tenant, Storage storage, long sinceMs) throws Exception {
        return ((AuthRecipeSQLStorage) storage).computeTenantUserCountAnchor(tenant, sinceMs);
    }

    private static long delta(TenantIdentifier tenant, Storage storage, long sinceMs) throws Exception {
        return ((AuthRecipeSQLStorage) storage).countTenantUsersJoinedSince(tenant, sinceMs);
    }

    // A boundary strictly between the users created before and after it: the sleeps guarantee the
    // last pre-boundary time_joined is < boundary < the first post-boundary time_joined.
    private static long boundaryNow() throws Exception {
        Thread.sleep(30);
        long b = System.currentTimeMillis();
        Thread.sleep(30);
        return b;
    }

    /**
     * The central invariant, exercised with a user created AFTER the anchor is taken (the
     * "concurrent-ish insert" the issue calls out). Population mixes the same shapes the exact-count
     * path cares about: singletons, a linked group, a singleton primary, and a passwordless user with
     * two recipe_user_tenants rows. Verified under both read-from-new migration modes.
     *
     * <p>Layout in time:
     * <pre>
     *   [ batch A: seeded before the boundary ]  |sinceMs|  [ batch B: after ]   [ C: after anchor ]
     * </pre>
     * anchor(sinceMs) captures exactly batch A (C - d0 with d0 = |B|). Adding batch C after the anchor
     * is taken must show up through the live delta, so anchor + delta == the exact count including C.
     */
    @Test
    public void testAnchorPlusDeltaEqualsExactCountWithPostAnchorInsert() throws Exception {
        for (MigrationMode mode : new MigrationMode[]{MigrationMode.MIGRATED, MigrationMode.DUAL_WRITE_READ_NEW}) {
            TestingProcessManager.TestingProcess process = startProcess();
            try {
                Main main = process.getProcess();
                if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

                Start storage = (Start) StorageLayer.getStorage(main);
                Config.getConfig(storage).setMigrationModeForTesting(mode);
                TenantIdentifier tenant = ResourceDistributor.getAppForTesting();

                // ---- batch A (before the boundary) ----
                signUp(main, "a-s0-" + mode + "@t.com");
                signUp(main, "a-s1-" + mode + "@t.com");
                AuthRecipeUserInfo gPrimary = signUp(main, "a-gp-" + mode + "@t.com");
                AuthRecipeUserInfo gMember = signUp(main, "a-gm-" + mode + "@t.com");
                AuthRecipe.createPrimaryUser(main, gPrimary.getSupertokensUserId());
                AuthRecipe.linkAccounts(main, gMember.getSupertokensUserId(), gPrimary.getSupertokensUserId());
                AuthRecipeUserInfo lone = signUp(main, "a-lone-" + mode + "@t.com");
                AuthRecipe.createPrimaryUser(main, lone.getSupertokensUserId());
                AuthRecipeUserInfo pless = passwordlessWithEmail(main, "a-pless-" + mode + "@t.com");
                Passwordless.updateUser(main, pless.getSupertokensUserId(), null,
                        new Passwordless.FieldUpdate("+1555" + (mode == MigrationMode.MIGRATED ? "1" : "2")));
                // distinct users in A: s0, s1, {gp+gm}, lone, pless = 5
                long countA = exactCount(tenant, storage);
                assertEquals("batch A exact count (" + mode + ")", 5, countA);

                long sinceMs = boundaryNow();

                // ---- batch B (after the boundary, before the anchor) ----
                signUp(main, "b-s0-" + mode + "@t.com");
                signUp(main, "b-s1-" + mode + "@t.com");
                long countAB = exactCount(tenant, storage);
                assertEquals("batch A+B exact count (" + mode + ")", 7, countAB);

                // Anchor rebases the snapshot count onto sinceMs: it must equal batch A alone.
                long anchorValue = anchor(tenant, storage, sinceMs);
                assertEquals("anchor must equal the pre-boundary population (" + mode + ")", countA, anchorValue);

                // ---- batch C: created AFTER the anchor was computed ----
                signUp(main, "c-s0-" + mode + "@t.com");

                long exactNow = exactCount(tenant, storage);
                long served = anchorValue + delta(tenant, storage, sinceMs);
                assertEquals("served (anchor + live delta) must equal the exact count including the "
                        + "post-anchor insert (" + mode + ")", exactNow, served);
                assertEquals("exact count must be A+B+C (" + mode + ")", 8, exactNow);
            } finally {
                process.kill();
                assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            }
        }
    }

    /**
     * The three delta scenarios the issue enumerates, asserted on the delta directly:
     * <ol>
     *   <li>a new unlinked user created after the boundary appears in the delta (+1);</li>
     *   <li>a new user linked into a pre-boundary group does NOT re-count — the group's MIN time
     *       stays {@code <= sinceMs}, so the whole group remains outside the window (delta unchanged);</li>
     *   <li>a group formed entirely after the boundary counts exactly once, not once per member.</li>
     * </ol>
     */
    @Test
    public void testDeltaCorrectnessScenarios() throws Exception {
        for (MigrationMode mode : new MigrationMode[]{MigrationMode.MIGRATED, MigrationMode.DUAL_WRITE_READ_NEW}) {
            TestingProcessManager.TestingProcess process = startProcess();
            try {
                Main main = process.getProcess();
                if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

                Start storage = (Start) StorageLayer.getStorage(main);
                Config.getConfig(storage).setMigrationModeForTesting(mode);
                TenantIdentifier tenant = ResourceDistributor.getAppForTesting();

                // Pre-boundary population: a plain singleton plus a linked group of two.
                signUp(main, "pre-s-" + mode + "@t.com");
                AuthRecipeUserInfo gPrimary = signUp(main, "pre-gp-" + mode + "@t.com");
                AuthRecipeUserInfo gMember = signUp(main, "pre-gm-" + mode + "@t.com");
                AuthRecipe.createPrimaryUser(main, gPrimary.getSupertokensUserId());
                AuthRecipe.linkAccounts(main, gMember.getSupertokensUserId(), gPrimary.getSupertokensUserId());

                long sinceMs = boundaryNow();

                // Nothing joined after the boundary yet.
                assertEquals("delta empty before any post-boundary user (" + mode + ")",
                        0, delta(tenant, storage, sinceMs));

                // (1) a new unlinked user after the boundary -> +1.
                signUp(main, "post-u-" + mode + "@t.com");
                assertEquals("new unlinked user must appear in the delta (" + mode + ")",
                        1, delta(tenant, storage, sinceMs));

                // (2) a new user linked into the PRE-boundary group -> delta unchanged (the group's MIN
                // time is the pre-boundary primary's, so it never enters the window; the exact count
                // does not grow either, since the linked user joins an existing group).
                long exactBeforeLink = exactCount(tenant, storage);
                AuthRecipeUserInfo late = signUp(main, "post-late-" + mode + "@t.com");
                AuthRecipe.linkAccounts(main, late.getSupertokensUserId(), gPrimary.getSupertokensUserId());
                assertEquals("linking a post-boundary user into a pre-boundary group must not grow the "
                        + "delta (" + mode + ")", 1, delta(tenant, storage, sinceMs));
                assertEquals("linking into an existing group must not grow the exact count (" + mode + ")",
                        exactBeforeLink, exactCount(tenant, storage));

                // (3) a group formed entirely after the boundary -> counts once, not once per member.
                AuthRecipeUserInfo np = signUp(main, "post-np-" + mode + "@t.com");
                AuthRecipeUserInfo nm = signUp(main, "post-nm-" + mode + "@t.com");
                AuthRecipe.createPrimaryUser(main, np.getSupertokensUserId());
                AuthRecipe.linkAccounts(main, nm.getSupertokensUserId(), np.getSupertokensUserId());
                assertEquals("a wholly-post-boundary group counts once in the delta (" + mode + ")",
                        2, delta(tenant, storage, sinceMs));

                // Sanity: anchor + delta still reconstructs the exact count.
                assertEquals("anchor + delta must equal exact count (" + mode + ")",
                        exactCount(tenant, storage), anchor(tenant, storage, sinceMs) + delta(tenant, storage, sinceMs));
            } finally {
                process.kill();
                assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            }
        }
    }

    /**
     * Legacy read mode (all_auth_recipe_users source): the anchor and delta must stay consistent with
     * {@code getUsersCount_legacy} too, so a deployment that opts into the fast path before migrating
     * still gets a correct reconstruction.
     */
    @Test
    public void testLegacyModeConsistency() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.LEGACY);
            TenantIdentifier tenant = ResourceDistributor.getAppForTesting();

            signUp(main, "l-s0@t.com");
            signUp(main, "l-s1@t.com");
            long sinceMs = boundaryNow();
            signUp(main, "l-s2@t.com");

            long anchorValue = anchor(tenant, storage, sinceMs);
            assertEquals("legacy anchor equals the pre-boundary count", 2, anchorValue);
            assertEquals("legacy delta counts the post-boundary user", 1, delta(tenant, storage, sinceMs));
            assertEquals("legacy anchor + delta must equal exact count",
                    exactCount(tenant, storage), anchorValue + delta(tenant, storage, sinceMs));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Plan shape of the joined-since delta at scale: it must seek the time bound on
     * app_id_to_user_id_pagination_index2 (the bound in an Index Cond, so only the rows joined after
     * the boundary are scanned) and never sequentially scan or fully aggregate the whole tenant.
     * Seeded directly with raw SQL for speed; gated behind SKIP_SCALE_REGRESSION_TESTS like the other
     * scale regression tests.
     */
    @Test
    public void testDeltaPlanShapeIsBoundedIndexSeek() throws Exception {
        Assume.assumeTrue("scale regression tests skipped via SKIP_SCALE_REGRESSION_TESTS",
                !"true".equalsIgnoreCase(System.getenv("SKIP_SCALE_REGRESSION_TESTS")));

        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.MIGRATED);

            TenantIdentifier tenant = ResourceDistributor.getAppForTesting();
            String appId = tenant.getAppId();
            String tenantId = tenant.getTenantId();
            String auid = Config.getConfig(storage).getAppIdToUserIdTable();
            String rut = Config.getConfig(storage).getRecipeUserTenantsTable();

            final int total = 60_000;
            final long timeBase = 1_000_000_000L;
            final int joinedAfter = 40; // users with time_joined strictly greater than the boundary
            final long sinceMs = timeBase + (total - joinedAfter) - 1; // boundary just below the last block

            Connection con = ConnectionPool.getConnection(storage);
            try {
                exec(con, "SET max_parallel_workers_per_gather = 0");

                // Unlinked singletons: one app_id_to_user_id row + one recipe_user_tenants row each,
                // time_joined = timeBase + i so the population is ordered along the pagination index.
                exec(con, "INSERT INTO " + auid
                        + " (app_id, user_id, recipe_id, primary_or_recipe_user_id,"
                        + "  is_linked_or_is_a_primary_user, time_joined, primary_or_recipe_user_time_joined) "
                        + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), 'emailpassword',"
                        + "  'u' || lpad(i::text, 35, '0'), false, " + timeBase + " + i, " + timeBase + " + i "
                        + "FROM generate_series(0, " + (total - 1) + ") AS i");
                exec(con, "INSERT INTO " + rut
                        + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type,"
                        + "  third_party_id, third_party_user_id, account_info_value) "
                        + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), " + q(tenantId) + ","
                        + "  'emailpassword', 'email', '', '', 'e' || i || '@t.com' "
                        + "FROM generate_series(0, " + (total - 1) + ") AS i");
                exec(con, "VACUUM ANALYZE " + auid);
                exec(con, "VACUUM ANALYZE " + rut);

                // Correctness at scale: the delta equals exactly the post-boundary users.
                assertEquals("delta value at scale", joinedAfter, scalar(con, deltaSql(auid, rut, appId, tenantId, sinceMs)));

                JsonObject plan = explain(con, deltaSql(auid, rut, appId, tenantId, sinceMs));

                JsonObject seek = findTimeBoundedAuidScan(plan, auid);
                assertNotNull("delta must scan app_id_to_user_id via an index seek with the "
                        + "primary_or_recipe_user_time_joined bound in the Index Cond; plan was:\n" + plan, seek);

                // The seek must read only the joined-since tail, not the whole tenant. Allow generous
                // slack for index-internal reads but assert it is nowhere near the full population.
                double scanned = seek.get("Actual Rows").getAsDouble();
                assertTrue("delta index seek must read only the post-boundary tail (got " + scanned
                        + " of " + total + ")", scanned <= 20L * joinedAfter);

                assertFalse("delta must not sequentially scan app_id_to_user_id", containsSeqScanOn(plan, auid));
            } finally {
                con.close();
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // The exact SQL GeneralQueries builds for the read-from-new delta (kept in sync with source), with
    // literals inlined so it can be EXPLAINed directly.
    private static String deltaSql(String auid, String rut, String appId, String tenantId, long sinceMs) {
        return "SELECT COUNT(*) FROM ("
                + "SELECT DISTINCT auid.primary_or_recipe_user_id, auid.primary_or_recipe_user_time_joined"
                + " FROM " + auid + " auid"
                + " WHERE auid.app_id = " + q(appId) + " AND auid.primary_or_recipe_user_time_joined > " + sinceMs
                + " AND EXISTS (SELECT 1 FROM " + rut + " rut"
                + " WHERE rut.app_id = auid.app_id AND rut.recipe_user_id = auid.user_id"
                + " AND rut.tenant_id = " + q(tenantId) + ")) u";
    }

    // Finds an (Index Only) Index Scan node on the app_id_to_user_id table whose Index Cond carries the
    // primary_or_recipe_user_time_joined bound — i.e. a sargable seek rather than a scan + Filter.
    private JsonObject findTimeBoundedAuidScan(JsonObject node, String auid) {
        String type = node.get("Node Type").getAsString();
        boolean isIndexScan = type.equals("Index Scan") || type.equals("Index Only Scan");
        String rel = node.has("Relation Name") ? node.get("Relation Name").getAsString() : "";
        String indexCond = node.has("Index Cond") ? node.get("Index Cond").getAsString() : "";
        if (isIndexScan && auid.equals(rel) && indexCond.contains("primary_or_recipe_user_time_joined")) {
            return node;
        }
        for (JsonObject child : children(node)) {
            JsonObject found = findTimeBoundedAuidScan(child, auid);
            if (found != null) return found;
        }
        return null;
    }

    private boolean containsSeqScanOn(JsonObject node, String auid) {
        String rel = node.has("Relation Name") ? node.get("Relation Name").getAsString() : "";
        if (node.get("Node Type").getAsString().equals("Seq Scan") && auid.equals(rel)) {
            return true;
        }
        for (JsonObject child : children(node)) {
            if (containsSeqScanOn(child, auid)) return true;
        }
        return false;
    }

    private JsonObject explain(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)) {
            assertTrue("EXPLAIN returned no rows", rs.next());
            JsonArray arr = new JsonParser().parse(rs.getString(1)).getAsJsonArray();
            return arr.get(0).getAsJsonObject().getAsJsonObject("Plan");
        }
    }

    private List<JsonObject> children(JsonObject node) {
        List<JsonObject> out = new ArrayList<>();
        if (node.has("Plans")) {
            for (int i = 0; i < node.getAsJsonArray("Plans").size(); i++) {
                out.add(node.getAsJsonArray("Plans").get(i).getAsJsonObject());
            }
        }
        return out;
    }

    private void exec(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement()) {
            st.execute(sql);
        }
    }

    private long scalar(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static String q(String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}
