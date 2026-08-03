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
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
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
 * PLAN-005 follow-up (issue #351): the user/bulk-import keyset pagination cursors use the exact but
 * non-sargable predicate {@code A < ? OR (A = ? AND B <= ?)}. Postgres cannot turn an OR of
 * conjuncts into a B-tree seek, so on a deep cursor page it scans the pagination index from the top
 * of the (app_id[, tenant_id]) range and filters every row up to the cursor — per-page cost grows
 * linearly with page depth, making a full walk quadratic. The fix adds a <em>redundant</em> range
 * bound on the leading sort column ({@code AND A >= ?} for ASC, {@code AND A <= ?} for DESC) that is
 * implied by the OR, so the planner seeks straight to the cursor and the residual OR only resolves
 * the equal-time tie run.
 *
 * <p>These are plan-shape regression tests, modelled on {@link MigratedUserScaleRegressionTest}. On a
 * ~40k-user single-tenant fixture seeded directly with SQL, for a <em>deep</em> cursor page each
 * query form is run under {@code EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)} and the executed plan is
 * asserted:
 *
 * <ul>
 *   <li><b>Seek</b>: the sargable form puts the sort-column bound inside an {@code Index Cond} (a
 *       seek) and its pagination-table scan reads only a small multiple of the page size, whereas
 *       the OR-only form leaves the sort column in a {@code Filter} and its scan reads the whole
 *       prefix up to the cursor. Covered for both schema paths ({@code getUsers_new} on
 *       {@code app_id_to_user_id}, {@code getUsers_legacy} on {@code all_auth_recipe_users}), both
 *       directions, and the bulk-import listing (fixed {@code created_at DESC}).</li>
 *   <li><b>Invariance</b>: the sargable and OR-only forms return the identical id sequence and a full
 *       page — the bound must change only the plan, never the result set or the cursor semantics.</li>
 * </ul>
 *
 * <p>The query strings below are copies of the exact SQL that {@code GeneralQueries.getUsers_new} /
 * {@code getUsers_legacy} and {@code BulkImportQueries.getBulkImportUsers} build (kept in sync with
 * that source); the OR-only variants are the pre-fix shapes, kept as the teeth that prove the
 * fixture is deep enough for the non-sargable plan to be expensive. No wall-clock timing is
 * involved — every assertion is on plan structure or exact ids, so the result is deterministic.
 *
 * <p>Heavy fixture (~40k rows across four tables). Runs by default (incl. CI); set
 * {@code SKIP_SCALE_REGRESSION_TESTS=true} to exclude it from quick local runs.
 */
public class SargableKeysetPaginationTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    private static final int TOTAL_USERS = 40_000;
    private static final int PAGE_LIMIT = 50;
    // A deep cursor: everything before it (ASC) / after it (DESC) is the prefix the non-sargable form
    // must scan and discard. 20_000 >> the bound below, so the two plans are unambiguously different.
    private static final int CURSOR_INDEX = 20_000;
    private static final long TIME_BASE = 1_000_000_000L;
    private static final long CURSOR_TIME = TIME_BASE + CURSOR_INDEX;
    private static final String CURSOR_ID = userId(CURSOR_INDEX);
    // The sargable scan may legitimately read the page plus the equal-time tie run; the OR-only scan
    // reads the whole prefix (~CURSOR_INDEX). Anything in between separates the two plans cleanly.
    private static final long ROWS_READ_BOUND = 20L * PAGE_LIMIT;

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
    private static String userId(int k) {
        return "u" + String.format("%035d", k);
    }

    @Test
    public void testKeysetCursorsSeekInsteadOfScanningFromTheTop() throws Exception {
        Assume.assumeTrue("scale regression tests skipped via SKIP_SCALE_REGRESSION_TESTS",
                !"true".equalsIgnoreCase(System.getenv("SKIP_SCALE_REGRESSION_TESTS")));

        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            // getUsers() dispatches to getUsers_new only under a read-from-new mode; the legacy shape
            // is exercised by the legacy query strings directly, so the mode does not gate this test.
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.MIGRATED);

            TenantIdentifier tenant = ResourceDistributor.getAppForTesting();
            String appId = tenant.getAppId();
            String tenantId = tenant.getTenantId();

            String auid = Config.getConfig(storage).getAppIdToUserIdTable();
            String rut = Config.getConfig(storage).getRecipeUserTenantsTable();
            String users = Config.getConfig(storage).getUsersTable();
            String bulk = Config.getConfig(storage).getBulkImportUsersTable();

            Connection con = ConnectionPool.getConnection(storage);
            try {
                con.setAutoCommit(true);
                // Single-worker plans so "Actual Rows" is not split across parallel workers and node
                // selection does not flip on a loaded CI box.
                exec(con, "SET max_parallel_workers_per_gather = 0");

                seed(con, auid, rut, users, bulk, appId, tenantId);

                for (String order : new String[]{"ASC", "DESC"}) {
                    // Migrated path: getUsers_new on app_id_to_user_id (+ EXISTS into recipe_user_tenants).
                    assertSeeksAndEquivalent(con,
                            "getUsers_new " + order,
                            migratedSargable(order, auid, rut, appId, tenantId),
                            migratedOrOnly(order, auid, rut, appId, tenantId),
                            auid);
                    // Legacy path: getUsers_legacy on all_auth_recipe_users.
                    assertSeeksAndEquivalent(con,
                            "getUsers_legacy " + order,
                            legacySargable(order, users, appId, tenantId),
                            legacyOrOnly(order, users, appId, tenantId),
                            users);
                }

                // Bulk-import listing: fixed created_at DESC.
                assertSeeksAndEquivalent(con,
                        "getBulkImportUsers",
                        bulkSargable(bulk, appId),
                        bulkOrOnly(bulk, appId),
                        bulk);
            } finally {
                con.close();
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Fixture: 40k unlinked recipe users, each its own primary, distinct times. Four tables so both
    // schema paths and the bulk-import listing can be EXPLAINed on the same seeded data.
    // ---------------------------------------------------------------------------------------------

    private void seed(Connection con, String auid, String rut, String users, String bulk,
                      String appId, String tenantId) throws Exception {
        String ids = "'u' || lpad(i::text, 35, '0')";
        String gen = "FROM generate_series(0, " + (TOTAL_USERS - 1) + ") AS i";

        // app_id_to_user_id: FK target for all_auth_recipe_users and the migrated pagination scan.
        exec(con, "INSERT INTO " + auid
                + " (app_id, user_id, recipe_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user,"
                + "  time_joined, primary_or_recipe_user_time_joined) "
                + "SELECT " + q(appId) + ", " + ids + ", 'emailpassword', " + ids + ", FALSE, "
                + "  " + TIME_BASE + " + i, " + TIME_BASE + " + i " + gen);

        // recipe_user_tenants: one row per recipe user in the tenant (the EXISTS the migrated query probes).
        exec(con, "INSERT INTO " + rut
                + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type,"
                + "  third_party_id, third_party_user_id, account_info_value) "
                + "SELECT " + q(appId) + ", " + ids + ", " + q(tenantId) + ", 'emailpassword', "
                + "  'email', '', '', 'e' || i || '@t.com' " + gen);

        // all_auth_recipe_users: the legacy pagination table (one row per user in the tenant).
        exec(con, "INSERT INTO " + users
                + " (app_id, tenant_id, user_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user,"
                + "  recipe_id, time_joined, primary_or_recipe_user_time_joined) "
                + "SELECT " + q(appId) + ", " + q(tenantId) + ", " + ids + ", " + ids + ", FALSE, "
                + "  'emailpassword', " + TIME_BASE + " + i, " + TIME_BASE + " + i " + gen);

        // bulk_import_users: the bulk-import listing table (created_at is the sort column).
        exec(con, "INSERT INTO " + bulk
                + " (id, app_id, raw_data, status, created_at, updated_at) "
                + "SELECT " + ids + ", " + q(appId) + ", '{}', 'NEW', "
                + "  " + TIME_BASE + " + i, " + TIME_BASE + " + i " + gen);

        // Stats + visibility map, so the planner picks the index seek plans. VACUUM needs autocommit.
        exec(con, "VACUUM ANALYZE " + auid);
        exec(con, "VACUUM ANALYZE " + rut);
        exec(con, "VACUUM ANALYZE " + users);
        exec(con, "VACUUM ANALYZE " + bulk);
    }

    // ---------------------------------------------------------------------------------------------
    // Query builders — copies of the SQL the source builds for the deep-cursor page. The "sargable"
    // form is the post-fix shape (extra bound on the sort column); the "OrOnly" form is the pre-fix
    // shape, kept as the teeth.
    // ---------------------------------------------------------------------------------------------

    private String migratedSargable(String order, String auid, String rut, String appId, String tenantId) {
        return migrated(order, auid, rut, appId, tenantId, true);
    }

    private String migratedOrOnly(String order, String auid, String rut, String appId, String tenantId) {
        return migrated(order, auid, rut, appId, tenantId, false);
    }

    private String migrated(String order, String auid, String rut, String appId, String tenantId,
                            boolean sargable) {
        String sym = order.equals("ASC") ? ">" : "<";
        String bound = sargable
                ? " AND auid.primary_or_recipe_user_time_joined " + sym + "= " + CURSOR_TIME
                : "";
        return "SELECT DISTINCT auid.primary_or_recipe_user_id, auid.primary_or_recipe_user_time_joined"
                + " FROM " + auid + " auid"
                + " WHERE auid.app_id = " + q(appId)
                + " AND (auid.primary_or_recipe_user_time_joined " + sym + " " + CURSOR_TIME
                + " OR (auid.primary_or_recipe_user_time_joined = " + CURSOR_TIME
                + " AND auid.primary_or_recipe_user_id <= " + q(CURSOR_ID) + "))"
                + bound
                + " AND EXISTS (SELECT 1 FROM " + rut + " rut"
                + " WHERE rut.app_id = auid.app_id AND rut.recipe_user_id = auid.user_id"
                + " AND rut.tenant_id = " + q(tenantId) + ")"
                + " ORDER BY auid.primary_or_recipe_user_time_joined " + order
                + ", auid.primary_or_recipe_user_id DESC LIMIT " + PAGE_LIMIT;
    }

    private String legacySargable(String order, String users, String appId, String tenantId) {
        return legacy(order, users, appId, tenantId, true);
    }

    private String legacyOrOnly(String order, String users, String appId, String tenantId) {
        return legacy(order, users, appId, tenantId, false);
    }

    private String legacy(String order, String users, String appId, String tenantId, boolean sargable) {
        String sym = order.equals("ASC") ? ">" : "<";
        String bound = sargable
                ? " AND primary_or_recipe_user_time_joined " + sym + "= " + CURSOR_TIME
                : "";
        return "SELECT DISTINCT primary_or_recipe_user_id, primary_or_recipe_user_time_joined"
                + " FROM " + users
                + " WHERE (primary_or_recipe_user_time_joined " + sym + " " + CURSOR_TIME
                + " OR (primary_or_recipe_user_time_joined = " + CURSOR_TIME
                + " AND primary_or_recipe_user_id <= " + q(CURSOR_ID) + "))"
                + bound
                + " AND app_id = " + q(appId) + " AND tenant_id = " + q(tenantId)
                + " ORDER BY primary_or_recipe_user_time_joined " + order
                + ", primary_or_recipe_user_id DESC LIMIT " + PAGE_LIMIT;
    }

    private String bulkSargable(String bulk, String appId) {
        return bulk(bulk, appId, true);
    }

    private String bulkOrOnly(String bulk, String appId) {
        return bulk(bulk, appId, false);
    }

    private String bulk(String bulk, String appId, boolean sargable) {
        String bound = sargable ? " AND created_at <= " + CURSOR_TIME : "";
        return "SELECT * FROM " + bulk
                + " WHERE app_id = " + q(appId)
                + " AND (created_at < " + CURSOR_TIME
                + " OR (created_at = " + CURSOR_TIME + " AND id <= " + q(CURSOR_ID) + "))"
                + bound
                + " ORDER BY created_at DESC, id DESC LIMIT " + PAGE_LIMIT;
    }

    // ---------------------------------------------------------------------------------------------
    // Assertions.
    // ---------------------------------------------------------------------------------------------

    private void assertSeeksAndEquivalent(Connection con, String label, String sargableSql,
                                          String orOnlySql, String scanTable) throws Exception {
        JsonObject sargablePlan = explain(con, sargableSql);
        JsonObject orOnlyPlan = explain(con, orOnlySql);

        long sargableRead = scanRowsRead(sargablePlan, scanTable);
        long orOnlyRead = scanRowsRead(orOnlyPlan, scanTable);

        // Teeth: the OR-only form must scan the whole prefix — otherwise the fixture is too shallow to
        // tell a seek from a from-the-top filter and the test could not detect a regression.
        assertTrue(label + ": OR-only form is expected to read the whole prefix from " + scanTable
                        + " (> " + ROWS_READ_BOUND + " rows); read " + orOnlyRead,
                orOnlyRead > ROWS_READ_BOUND);

        // The sargable bound must be an index seek: only a small multiple of the page size read.
        assertTrue(label + ": sargable form must seek to the cursor and read no more than "
                        + ROWS_READ_BOUND + " rows from " + scanTable + "; read " + sargableRead,
                sargableRead <= ROWS_READ_BOUND);

        // The sort-column bound must land in an Index Cond (a seek), not stay a from-the-top Filter.
        String sortCol = scanTable.contains("bulk_import_users") ? "created_at"
                : "primary_or_recipe_user_time_joined";
        assertTrue(label + ": sargable form must place the sort-column bound inside an Index Cond",
                anyIndexCondMentions(sargablePlan, sortCol));
        assertFalse(label + ": OR-only form must not have the sort column in any Index Cond "
                        + "(it stays a Filter — the whole point of the bug)",
                anyIndexCondMentions(orOnlyPlan, sortCol));

        // Invariance: the redundant bound must not change the result set or its order.
        List<String> sargableIds = firstColumn(con, sargableSql);
        List<String> orOnlyIds = firstColumn(con, orOnlySql);
        assertEquals(label + ": sargable and OR-only forms must return the identical id sequence",
                orOnlyIds, sargableIds);
        assertEquals(label + ": page must be full (fixture is deep enough)", PAGE_LIMIT, sargableIds.size());
    }

    // ---------------------------------------------------------------------------------------------
    // EXPLAIN + plan-tree helpers.
    // ---------------------------------------------------------------------------------------------

    private JsonObject explain(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)) {
            assertTrue("EXPLAIN returned no rows", rs.next());
            JsonArray arr = new JsonParser().parse(rs.getString(1)).getAsJsonArray();
            return arr.get(0).getAsJsonObject().getAsJsonObject("Plan");
        }
    }

    // Rows physically read from the given relation's scan node = rows output + rows discarded by a
    // residual filter. For a seek this is ~page size; for a from-the-top filter it is ~the prefix.
    private long scanRowsRead(JsonObject node, String relation) {
        long best = -1;
        if (node.has("Relation Name") && node.get("Relation Name").getAsString().equals(relation)) {
            long output = node.has("Actual Rows") ? node.get("Actual Rows").getAsLong() : 0;
            long removed = node.has("Rows Removed by Filter") ? node.get("Rows Removed by Filter").getAsLong() : 0;
            best = output + removed;
        }
        for (JsonObject child : children(node)) {
            best = Math.max(best, scanRowsRead(child, relation));
        }
        return best;
    }

    private boolean anyIndexCondMentions(JsonObject node, String column) {
        for (String key : new String[]{"Index Cond", "Recheck Cond"}) {
            if (node.has(key) && node.get(key).getAsString().contains(column)) {
                return true;
            }
        }
        for (JsonObject child : children(node)) {
            if (anyIndexCondMentions(child, column)) return true;
        }
        return false;
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

    // ---------------------------------------------------------------------------------------------
    // Raw SQL helpers.
    // ---------------------------------------------------------------------------------------------

    private void exec(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement()) {
            st.execute(sql);
        }
    }

    private List<String> firstColumn(Connection con, String sql) throws Exception {
        List<String> ids = new ArrayList<>();
        try (Statement st = con.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getString(1).trim());
            }
        }
        return ids;
    }

    private static String q(String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}
