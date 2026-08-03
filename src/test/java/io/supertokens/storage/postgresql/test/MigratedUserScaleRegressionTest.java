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
 * PLAN-005 unit 3: scale/plan-shape regression tests for the migrated-schema user pagination
 * (unit 1, {@code getUsers_new}) and per-tenant / app-scoped user count (unit 2,
 * {@code getUsersCount_new}) rewrites.
 *
 * <p>Where {@link MigratedUserPaginationTest} and {@link MigratedUserCountTest} guard <em>results</em>
 * (the rewrites return the same rows / counts as the old queries), these tests guard the <em>plan
 * shape</em> — the actual reason the rewrites exist. They seed ~200k synthetic users directly with
 * SQL into the three tables the queries under test touch ({@code app_id_to_user_id},
 * {@code recipe_user_tenants}, {@code primary_user_tenants}), then run each query form under
 * {@code EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)} and assert on the executed plan:
 *
 * <ul>
 *   <li><b>Pagination</b>: the streaming {@code DISTINCT + EXISTS} form feeds only a small multiple
 *       of the page size into its top {@code Unique} node (index seek + {@code LIMIT}), whereas the
 *       old {@code GROUP BY/MIN} form aggregates the whole tenant before the {@code LIMIT} can apply.
 *       Both first-page and mid-cursor pages, both directions; the two forms must also return the
 *       identical id sequence on the seeded data (the I6-equivalence the rewrite relies on).</li>
 *   <li><b>Count</b>: under {@code work_mem = 64kB} the {@code D - L + G} statements and the
 *       app-scoped streaming count write zero temp blocks and contain no {@code HashAggregate} /
 *       {@code Hash Join} node, while the old join + {@code GROUP BY} spills. The rewritten count
 *       must equal the old query's count.</li>
 * </ul>
 *
 * <p>The query strings below are copies of the exact SQL that {@code GeneralQueries.getUsers_new} /
 * {@code getUsersCount_new} build for the unfiltered, read-from-new path (kept in sync with that
 * source); running them directly is what lets us EXPLAIN the internal query and compare the old and
 * new shapes side by side. Modelled on the MIGRATED read path (the only mode these query shapes are
 * used in). No wall-clock timing is involved — every assertion is on plan structure or exact counts,
 * so the result is deterministic.
 *
 * <p>The fixture is heavy (~200k rows). It runs by default (including in CI); set the environment
 * variable {@code SKIP_SCALE_REGRESSION_TESTS=true} to exclude it from quick local runs.
 */
public class MigratedUserScaleRegressionTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    // 16_700 blocks of 12 app_id_to_user_id rows = 200_400 rows. Each block of 12 recipe users is:
    // 6 unlinked singletons (one of them passwordless with an email AND a phone -> two
    // recipe_user_tenants rows), 1 singleton primary user (createPrimaryUser, never linked),
    // 1 linked group of 2, and 1 linked group of 3. Distinct users per block = 9 -> 150_300 total.
    private static final int NUM_BLOCKS = 16_700;
    private static final int BLOCK = 12;
    private static final int TOTAL_USERS = NUM_BLOCKS * BLOCK; // 200_400
    private static final int PAGE_LIMIT = 50;

    // A mid-tenant cursor anchored on an unlinked user (100000 % 12 == 4), so its
    // primary_or_recipe_user_time_joined is its own time and the (time, id) pair is unambiguous.
    private static final int CURSOR_INDEX = 100_000;
    private static final long TIME_BASE = 1_000_000_000L;
    private static final long CURSOR_TIME = TIME_BASE + CURSOR_INDEX;
    private static final String CURSOR_ID = userId(CURSOR_INDEX);

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

    // 36-char deterministic user id (CHAR(36)), matching the FK'd primary ids by construction.
    private static String userId(int k) {
        return "u" + String.format("%035d", k);
    }

    @Test
    public void testPaginationAndCountPlanShapesAtScale() throws Exception {
        Assume.assumeTrue("scale regression tests skipped via SKIP_SCALE_REGRESSION_TESTS",
                !"true".equalsIgnoreCase(System.getenv("SKIP_SCALE_REGRESSION_TESTS")));

        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            // These query shapes are the ones the plugin uses under a read-from-new migration mode.
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.MIGRATED);

            TenantIdentifier tenant = ResourceDistributor.getAppForTesting();
            String appId = tenant.getAppId();
            String tenantId = tenant.getTenantId();

            String auid = Config.getConfig(storage).getAppIdToUserIdTable();
            String rut = Config.getConfig(storage).getRecipeUserTenantsTable();
            String put = Config.getConfig(storage).getPrimaryUserTenantsTable();

            Connection con = ConnectionPool.getConnection(storage);
            try {
                con.setAutoCommit(true);
                // Deterministic, single-worker plans so "Actual Rows" is not split across parallel
                // workers and node selection does not flip on a loaded CI box.
                exec(con, "SET max_parallel_workers_per_gather = 0");

                seed(con, auid, rut, put, appId, tenantId);

                // -------- Pagination plan shape + equivalence --------
                for (String order : new String[]{"ASC", "DESC"}) {
                    // First page: the new streaming form must feed only ~page-size rows into its top
                    // Unique node; the old GROUP BY/MIN form feeds the whole tenant.
                    assertPaginationBounded(con, order, /*cursor*/ false, auid, rut, appId, tenantId);
                    assertPaginationEquivalent(con, order, /*cursor*/ false, auid, rut, appId, tenantId);
                }
                // Mid-cursor page (ASC): same bound, and the cursor filter (WHERE vs HAVING) must not
                // change the result set.
                assertPaginationBounded(con, "ASC", /*cursor*/ true, auid, rut, appId, tenantId);
                assertPaginationEquivalent(con, "ASC", /*cursor*/ true, auid, rut, appId, tenantId);
                assertPaginationEquivalent(con, "DESC", /*cursor*/ true, auid, rut, appId, tenantId);

                // -------- Count plan shape + equality (work_mem = 64kB) --------
                exec(con, "SET work_mem = '64kB'");
                try {
                    // New per-tenant count: D - L in one merge pass, G a streaming Unique. Both must be
                    // spill-free and hash-free even at 64kB.
                    JsonObject dlPlan = explain(con, tenantCountDL(auid, rut, appId, tenantId));
                    assertSpillFreeAndHashFree("per-tenant D - L", dlPlan);
                    JsonObject gPlan = explain(con, tenantCountG(put, appId, tenantId));
                    assertSpillFreeAndHashFree("per-tenant G", gPlan);

                    // App-scoped streaming count: GROUP BY (time, id) off pagination_index2.
                    JsonObject appPlan = explain(con, appCountNew(auid, appId));
                    assertSpillFreeAndHashFree("app-scoped streaming count", appPlan);

                    // Teeth: the old per-tenant join + GROUP BY must spill (temp blocks) or hash at 64kB.
                    JsonObject oldTenantPlan = explain(con, tenantCountOld(auid, rut, appId, tenantId));
                    assertTrue("old per-tenant count must spill or hash-aggregate at work_mem=64kB "
                                    + "(temp=" + sumTempWritten(oldTenantPlan) + ", hash=" + containsHash(oldTenantPlan) + ")",
                            sumTempWritten(oldTenantPlan) > 0 || containsHash(oldTenantPlan));

                    // Exact-count equality: new (D - L + G) == old join + GROUP BY; new app count == old.
                    long[] dl = runDL(con, tenantCountDL(auid, rut, appId, tenantId));
                    long g = scalar(con, tenantCountG(put, appId, tenantId));
                    long newTenantCount = dl[0] - dl[1] + g;
                    long oldTenantCount = scalar(con, tenantCountOld(auid, rut, appId, tenantId));
                    long newAppCount = scalar(con, appCountNew(auid, appId));
                    long oldAppCount = scalar(con, appCountOld(auid, appId));

                    assertEquals("D - L + G must equal the old join + GROUP BY count",
                            oldTenantCount, newTenantCount);
                    assertEquals("streaming app count must equal the old GROUP BY app count",
                            oldAppCount, newAppCount);
                    // Single-tenant app: per-tenant count and app-scoped count coincide.
                    assertEquals("single-tenant fixture: tenant count == app count",
                            newAppCount, newTenantCount);
                    assertTrue("sanity: the fixture actually seeded users", newTenantCount > 100_000);
                } finally {
                    exec(con, "RESET work_mem");
                }
            } finally {
                con.close();
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Fixture: direct SQL seeding (INSERT ... SELECT FROM generate_series), seconds, deterministic.
    // ---------------------------------------------------------------------------------------------

    private void seed(Connection con, String auid, String rut, String put, String appId, String tenantId)
            throws Exception {
        // Within a block of 12 (r = i % 12): r in 0..5 are unlinked singletons; r == 6 is a singleton
        // primary user (primary == self); r in 7..8 form a linked group of 2 (head at r == 7); r in
        // 9..11 form a linked group of 3 (head at r == 9). The primary index pidx is computed so every
        // member of a linked group shares the head's row -> identical primary_or_recipe_user_time_joined
        // across the group (invariant I6). is_linked_or_is_a_primary_user is true for r >= 6.
        String pidxExpr = "(i - (i % 12) + CASE WHEN i % 12 <= 6 THEN i % 12 WHEN i % 12 IN (7, 8) THEN 7 ELSE 9 END)";
        exec(con, "INSERT INTO " + auid
                + " (app_id, user_id, recipe_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user,"
                + "  time_joined, primary_or_recipe_user_time_joined) "
                + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), "
                + "  CASE WHEN i % 12 = 0 THEN 'passwordless' ELSE 'emailpassword' END, "
                + "  'u' || lpad(pidx::text, 35, '0'), (i % 12) >= 6, "
                + "  " + TIME_BASE + " + i, " + TIME_BASE + " + pidx "
                + "FROM (SELECT i, " + pidxExpr + " AS pidx "
                + "      FROM generate_series(0, " + (TOTAL_USERS - 1) + ") AS i) s");

        // One recipe_user_tenants row per recipe user (account_info_type 'email'), with the empty-string
        // third-party columns the real inserts use.
        exec(con, "INSERT INTO " + rut
                + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type,"
                + "  third_party_id, third_party_user_id, account_info_value) "
                + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), " + q(tenantId) + ", "
                + "  CASE WHEN i % 12 = 0 THEN 'passwordless' ELSE 'emailpassword' END, "
                + "  'email', '', '', 'e' || i || '@t.com' "
                + "FROM generate_series(0, " + (TOTAL_USERS - 1) + ") AS i");

        // The passwordless users (r == 0) also carry a phone -> a second recipe_user_tenants row for the
        // same recipe_user_id, exercising the DISTINCT in the D term and in the old join.
        exec(con, "INSERT INTO " + rut
                + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type,"
                + "  third_party_id, third_party_user_id, account_info_value) "
                + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), " + q(tenantId) + ", "
                + "  'passwordless', 'phone', '', '', '+1' || i "
                + "FROM generate_series(0, " + (TOTAL_USERS - 1) + ") AS i WHERE i % 12 = 0");

        // primary_user_tenants: one row per primary user present in the tenant (r in {6, 7, 9}).
        exec(con, "INSERT INTO " + put
                + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id) "
                + "SELECT " + q(appId) + ", " + q(tenantId) + ", 'email', 'pe' || pidx, "
                + "  'u' || lpad(pidx::text, 35, '0') "
                + "FROM (SELECT (b * 12 + h) AS pidx "
                + "      FROM generate_series(0, " + (NUM_BLOCKS - 1) + ") AS b "
                + "      CROSS JOIN (VALUES (6), (7), (9)) AS hs(h)) s");

        // The group-of-3 primaries (r == 9) also get a second (phone) primary_user_tenants row, so the
        // G term's DISTINCT primary_user_id genuinely collapses duplicates rather than counting rows.
        exec(con, "INSERT INTO " + put
                + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id) "
                + "SELECT " + q(appId) + ", " + q(tenantId) + ", 'phone', 'pp' || pidx, "
                + "  'u' || lpad(pidx::text, 35, '0') "
                + "FROM (SELECT (b * 12 + 9) AS pidx "
                + "      FROM generate_series(0, " + (NUM_BLOCKS - 1) + ") AS b) s");

        // Stats + visibility map, so the planner picks the streaming index plans and index-only scans
        // do not fall back to heap fetches. VACUUM must run outside a transaction (autocommit here).
        exec(con, "VACUUM ANALYZE " + auid);
        exec(con, "VACUUM ANALYZE " + rut);
        exec(con, "VACUUM ANALYZE " + put);
    }

    // ---------------------------------------------------------------------------------------------
    // Query builders — copies of the SQL GeneralQueries builds for the unfiltered read-from-new path.
    // ---------------------------------------------------------------------------------------------

    private String paginationNew(String order, boolean cursor, String auid, String rut,
                                 String appId, String tenantId) {
        String sym = order.equals("ASC") ? ">" : "<";
        String cursorClause = cursor
                ? " AND (auid.primary_or_recipe_user_time_joined " + sym + " " + CURSOR_TIME
                + " OR (auid.primary_or_recipe_user_time_joined = " + CURSOR_TIME
                + " AND auid.primary_or_recipe_user_id <= " + q(CURSOR_ID) + "))"
                : "";
        return "SELECT DISTINCT auid.primary_or_recipe_user_id, auid.primary_or_recipe_user_time_joined"
                + " FROM " + auid + " auid"
                + " WHERE auid.app_id = " + q(appId)
                + cursorClause
                + " AND EXISTS (SELECT 1 FROM " + rut + " rut"
                + " WHERE rut.app_id = auid.app_id AND rut.recipe_user_id = auid.user_id"
                + " AND rut.tenant_id = " + q(tenantId) + ")"
                + " ORDER BY auid.primary_or_recipe_user_time_joined " + order
                + ", auid.primary_or_recipe_user_id DESC LIMIT " + PAGE_LIMIT;
    }

    private String paginationOld(String order, boolean cursor, String auid, String rut,
                                 String appId, String tenantId) {
        String sym = order.equals("ASC") ? ">" : "<";
        String havingClause = cursor
                ? " HAVING (MIN(auid.primary_or_recipe_user_time_joined) " + sym + " " + CURSOR_TIME
                + " OR (MIN(auid.primary_or_recipe_user_time_joined) = " + CURSOR_TIME
                + " AND auid.primary_or_recipe_user_id <= " + q(CURSOR_ID) + "))"
                : "";
        return "SELECT auid.primary_or_recipe_user_id,"
                + " MIN(auid.primary_or_recipe_user_time_joined) AS primary_or_recipe_user_time_joined"
                + " FROM " + auid + " auid"
                + " JOIN " + rut + " rut ON auid.app_id = rut.app_id AND auid.user_id = rut.recipe_user_id"
                + " WHERE auid.app_id = " + q(appId) + " AND rut.tenant_id = " + q(tenantId)
                + " GROUP BY auid.primary_or_recipe_user_id"
                + havingClause
                + " ORDER BY MIN(auid.primary_or_recipe_user_time_joined) " + order
                + ", auid.primary_or_recipe_user_id DESC LIMIT " + PAGE_LIMIT;
    }

    private String tenantCountDL(String auid, String rut, String appId, String tenantId) {
        return "SELECT COUNT(*) AS d, COUNT(*) FILTER (WHERE a.is_linked_or_is_a_primary_user) AS l"
                + " FROM (SELECT DISTINCT recipe_user_id FROM " + rut
                + " WHERE app_id = " + q(appId) + " AND tenant_id = " + q(tenantId) + ") r"
                + " JOIN " + auid + " a ON a.app_id = " + q(appId) + " AND a.user_id = r.recipe_user_id";
    }

    private String tenantCountG(String put, String appId, String tenantId) {
        return "SELECT COUNT(*) AS g FROM (SELECT DISTINCT primary_user_id FROM " + put
                + " WHERE app_id = " + q(appId) + " AND tenant_id = " + q(tenantId) + ") g";
    }

    private String tenantCountOld(String auid, String rut, String appId, String tenantId) {
        return "SELECT COUNT(*) AS total FROM ("
                + "SELECT auid.primary_or_recipe_user_id FROM " + rut + " rut"
                + " JOIN " + auid + " auid ON rut.app_id = auid.app_id AND rut.recipe_user_id = auid.user_id"
                + " WHERE rut.app_id = " + q(appId) + " AND rut.tenant_id = " + q(tenantId)
                + " GROUP BY auid.primary_or_recipe_user_id) AS uniq_users";
    }

    private String appCountNew(String auid, String appId) {
        return "SELECT COUNT(*) AS total FROM ("
                + "SELECT primary_or_recipe_user_time_joined, primary_or_recipe_user_id FROM " + auid
                + " WHERE app_id = " + q(appId) + " GROUP BY 1, 2) AS uniq_users";
    }

    private String appCountOld(String auid, String appId) {
        return "SELECT COUNT(*) AS total FROM ("
                + "SELECT primary_or_recipe_user_id FROM " + auid
                + " WHERE app_id = " + q(appId) + " GROUP BY primary_or_recipe_user_id) AS uniq_users";
    }

    // ---------------------------------------------------------------------------------------------
    // Assertions.
    // ---------------------------------------------------------------------------------------------

    private void assertPaginationBounded(Connection con, String order, boolean cursor,
                                         String auid, String rut, String appId, String tenantId)
            throws Exception {
        String label = "pagination " + order + (cursor ? " mid-cursor" : " first-page");
        double newRows = rowsIntoTopGroupingNode(explain(con, paginationNew(order, cursor, auid, rut, appId, tenantId)));
        double oldRows = rowsIntoTopGroupingNode(explain(con, paginationOld(order, cursor, auid, rut, appId, tenantId)));
        assertTrue(label + ": rewrite must feed the top Unique node no more than 10x the page size, got "
                + newRows, newRows <= 10L * PAGE_LIMIT);
        // Teeth: the old GROUP BY/MIN form feeds the whole tenant into its aggregate.
        assertTrue(label + ": old query is expected to feed the whole tenant into its aggregate (> 10x "
                + "page size); got " + oldRows + " (test would not detect a regression otherwise)",
                oldRows > 10L * PAGE_LIMIT);
    }

    private void assertPaginationEquivalent(Connection con, String order, boolean cursor,
                                            String auid, String rut, String appId, String tenantId)
            throws Exception {
        List<String> newIds = idColumn(con, paginationNew(order, cursor, auid, rut, appId, tenantId));
        List<String> oldIds = idColumn(con, paginationOld(order, cursor, auid, rut, appId, tenantId));
        assertEquals("pagination " + order + (cursor ? " mid-cursor" : " first-page")
                + ": new and old forms must return the identical id sequence", oldIds, newIds);
        assertEquals("pagination page must be full", PAGE_LIMIT, newIds.size());
    }

    private void assertSpillFreeAndHashFree(String label, JsonObject plan) {
        assertEquals(label + ": must write zero temp blocks at work_mem=64kB", 0L, sumTempWritten(plan));
        assertFalse(label + ": must contain no HashAggregate / Hash Join node", containsHash(plan));
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

    // Rows flowing INTO the topmost grouping node (Unique for DISTINCT, Aggregate for GROUP BY) —
    // i.e. the sum of that node's children's actual output rows.
    private double rowsIntoTopGroupingNode(JsonObject node) {
        String type = node.get("Node Type").getAsString();
        if (type.equals("Unique") || type.equals("Aggregate")) {
            double sum = 0;
            for (JsonObject child : children(node)) {
                sum += child.get("Actual Rows").getAsDouble();
            }
            return sum;
        }
        for (JsonObject child : children(node)) {
            double r = rowsIntoTopGroupingNode(child);
            if (r >= 0) return r;
        }
        return -1;
    }

    private long sumTempWritten(JsonObject node) {
        long total = node.has("Temp Written Blocks") ? node.get("Temp Written Blocks").getAsLong() : 0;
        for (JsonObject child : children(node)) {
            total += sumTempWritten(child);
        }
        return total;
    }

    private boolean containsHash(JsonObject node) {
        String type = node.get("Node Type").getAsString();
        String strategy = node.has("Strategy") ? node.get("Strategy").getAsString() : "";
        if (type.equals("Hash Join") || type.equals("Hash")
                || (type.equals("Aggregate") && strategy.equals("Hashed"))) {
            return true;
        }
        for (JsonObject child : children(node)) {
            if (containsHash(child)) return true;
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

    private long scalar(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private long[] runDL(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return new long[]{rs.getLong("d"), rs.getLong("l")};
        }
    }

    private List<String> idColumn(Connection con, String sql) throws Exception {
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
