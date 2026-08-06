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
 * Plan-shape regression tests for the two {@code recipe_user_account_infos} indexes added in this change
 * ({@code (app_id, primary_user_id)} and {@code (app_id, account_info_type, account_info_value)}) and for the
 * {@code app_id} restored on the nested reservation-cleanup subqueries of
 * {@code AccountInfoQueries.removeAccountInfoReservationForPrimaryUserWhileRemovingTenant_Transaction} and
 * {@code updateAccountInfo_Transaction}.
 *
 * <p>The table previously carried only an {@code (app_id, recipe_user_id)} index, so:
 * <ul>
 *   <li>the third-party sign-in lookup ({@code listPrimaryUserIdsByThirdPartyInfo}), which filters
 *       {@code (app_id, account_info_type, account_info_value)}, sequential-scanned the whole app; and</li>
 *   <li>the reservation-cleanup subqueries, which filter {@code primary_user_id} and drop {@code app_id},
 *       could not use any index and scanned {@code recipe_user_account_infos} / {@code recipe_user_tenants}
 *       app-wide.</li>
 * </ul>
 *
 * <p>These tests seed a synthetic single-app / single-tenant dataset directly with SQL, then run each query
 * under {@code EXPLAIN (FORMAT JSON)} and assert on the chosen plan: the fixed queries reach the seeded tables
 * via the intended indexes with <b>no sequential scan</b>, and — the load-bearing property — with {@code app_id}
 * present in the <b>Index Cond</b> of those index scans. The pre-fix shapes (kept alongside as teeth) are
 * asserted the other way around: {@code app_id} absent from any Index Cond over these indexes.
 *
 * <p>Deliberately NOT asserted: that the pre-fix shape sequential-scans. That was true up to PostgreSQL 17
 * (a dropped leading index column made the index unusable), but PostgreSQL 18's B-tree skip scan can service
 * a query that omits the leading {@code app_id} column — cheaply so at low app_id cardinality — so "the broken
 * shape must plan badly" is a planner-version-dependent claim. The presence/absence of {@code app_id} in the
 * index condition is the version-proof signature of the fix, and is what these teeth pin. (Production runs
 * PG &le; 16 where the pre-fix shape still degrades to a sequential scan; on 18+ it degrades with app_id
 * cardinality instead.) {@code EXPLAIN} without {@code ANALYZE} plans but does not execute, so the
 * {@code DELETE} statements never mutate the fixture.
 *
 * <p>The SQL strings below are copies of the statements {@code AccountInfoQueries} builds (kept in sync with
 * that source, with the bind parameters inlined as literals). Heavy-ish fixture; set the environment variable
 * {@code SKIP_SCALE_REGRESSION_TESTS=true} to exclude it from quick local runs.
 */
public class AccountInfoIndexScaleRegressionTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    // Enough rows that a selective index probe is decisively cheaper than a sequential scan, so the
    // planner's choice is a reliable signal of whether a usable index exists.
    private static final int NUM_USERS = 30_000;
    private static final long TIME_BASE = 1_000_000_000L;

    // Index names created by AccountInfoQueries (must match the DDL).
    private static final String IDX_ACCOUNT_INFO = "idx_recipe_user_account_infos_account_info";
    private static final String IDX_APP_PRIMARY_USER = "idx_recipe_user_account_infos_app_primary_user";
    private static final String IDX_RUT_RECIPE_USER = "idx_recipe_user_tenants_recipe_user_id";

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

    // Linked group head for index k: groups of 3, primary == first member of the group.
    private static int primaryIndex(int k) {
        return k - (k % 3);
    }

    @Test
    public void testAccountInfoIndexPlanShapes() throws Exception {
        Assume.assumeTrue("scale regression tests skipped via SKIP_SCALE_REGRESSION_TESTS",
                !"true".equalsIgnoreCase(System.getenv("SKIP_SCALE_REGRESSION_TESTS")));

        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);

            TenantIdentifier tenant = ResourceDistributor.getAppForTesting();
            String appId = tenant.getAppId();
            String tenantId = tenant.getTenantId();

            String auid = Config.getConfig(storage).getAppIdToUserIdTable();
            String rut = Config.getConfig(storage).getRecipeUserTenantsTable();
            String ruai = Config.getConfig(storage).getRecipeUserAccountInfosTable();
            String put = Config.getConfig(storage).getPrimaryUserTenantsTable();

            // Sanity: the two new indexes were actually created at startup on a fresh database.
            Connection con = ConnectionPool.getConnection(storage);
            try {
                con.setAutoCommit(true);
                assertTrue("startup DDL must create " + IDX_ACCOUNT_INFO, indexExists(con, IDX_ACCOUNT_INFO));
                assertTrue("startup DDL must create " + IDX_APP_PRIMARY_USER, indexExists(con, IDX_APP_PRIMARY_USER));

                // Deterministic single-worker plans so node selection does not flip on a loaded CI box.
                exec(con, "SET max_parallel_workers_per_gather = 0");

                seed(con, auid, rut, ruai, put, appId, tenantId);

                // A specific primary user group and a target account-info value to probe.
                String primaryId = userId(0);        // head of group {0,1,2}
                String memberId = userId(1);         // a linked member of that group
                String tpValue = "google::sub12345"; // account_info_value of user 12345

                // ---- (1) Third-party sign-in lookup: (app_id, account_info_type, account_info_value) index ----
                JsonObject tpPlan = explain(con, thirdPartyLookup(auid, ruai, appId, tpValue));
                assertNoSeqScan("third-party sign-in lookup", tpPlan, ruai);
                assertTrue("third-party lookup must use " + IDX_ACCOUNT_INFO + "; plan=" + tpPlan,
                        usesIndex(tpPlan, IDX_ACCOUNT_INFO));

                // Teeth: without the account-info index the same lookup sequential-scans ruai.
                exec(con, "DROP INDEX " + IDX_ACCOUNT_INFO);
                exec(con, "ANALYZE " + ruai);
                try {
                    JsonObject teeth = explain(con, thirdPartyLookup(auid, ruai, appId, tpValue));
                    assertTrue("teeth: without " + IDX_ACCOUNT_INFO + " the lookup must seq-scan "
                                    + ruai + "; plan=" + teeth, hasSeqScan(teeth, ruai));
                } finally {
                    exec(con, "CREATE INDEX IF NOT EXISTS " + IDX_ACCOUNT_INFO + " ON " + ruai
                            + "(app_id, account_info_type, account_info_value)");
                    exec(con, "ANALYZE " + ruai);
                }

                // ---- (2) updateAccountInfo_Transaction QUERY_1: (app_id, primary_user_id) index on ruai +
                //          idx_recipe_user_tenants_recipe_user_id on rut (no correlation — cleanest to assert on) ----
                JsonObject updPlan = explain(con, updateCleanupFixed(put, rut, ruai, appId, primaryId, memberId));
                assertNoSeqScan("updateAccountInfo cleanup (fixed)", updPlan, ruai);
                assertNoSeqScan("updateAccountInfo cleanup (fixed)", updPlan, rut);
                assertTrue("updateAccountInfo cleanup must probe ruai via " + IDX_APP_PRIMARY_USER + "; plan=" + updPlan,
                        usesIndex(updPlan, IDX_APP_PRIMARY_USER));
                assertTrue("updateAccountInfo cleanup must probe rut via " + IDX_RUT_RECIPE_USER + "; plan=" + updPlan,
                        usesIndex(updPlan, IDX_RUT_RECIPE_USER));
                assertIndexCondHasAppId("updateAccountInfo cleanup (fixed)", updPlan, IDX_APP_PRIMARY_USER);
                assertIndexCondHasAppId("updateAccountInfo cleanup (fixed)", updPlan, IDX_RUT_RECIPE_USER);

                // Teeth: the pre-fix shape drops app_id from both subqueries. Whether the planner then
                // sequential-scans (PG <= 17) or rescues the query with a B-tree skip scan over the missing
                // leading column (PG 18+), app_id cannot appear in the index condition — its absence is the
                // version-proof signature of the regression. (Do NOT assert a seq scan here; see class doc.)
                JsonObject updTeeth = explain(con, updateCleanupPreFix(put, rut, ruai, appId, primaryId, memberId));
                assertIndexCondLacksAppId("teeth: app_id-dropped updateAccountInfo cleanup", updTeeth,
                        IDX_APP_PRIMARY_USER, IDX_RUT_RECIPE_USER);

                // ---- (3) tenant-removal reservation cleanup (correlated rut.tenant_id preserved) ----
                JsonObject remPlan = explain(con,
                        tenantRemovalCleanupFixed(put, rut, ruai, appId, primaryId, memberId, tenantId));
                assertNoSeqScan("tenant-removal cleanup (fixed)", remPlan, ruai);
                assertNoSeqScan("tenant-removal cleanup (fixed)", remPlan, rut);
                assertIndexCondHasAppId("tenant-removal cleanup (fixed)", remPlan, IDX_APP_PRIMARY_USER);

                // Teeth: same version-proof signature as (2) — app_id absent from every index condition over
                // the ruai/rut indexes, regardless of whether the planner falls back to a seq scan or a skip scan.
                JsonObject remTeeth = explain(con,
                        tenantRemovalCleanupPreFix(put, rut, ruai, appId, primaryId, memberId, tenantId));
                assertIndexCondLacksAppId("teeth: app_id-dropped tenant-removal cleanup", remTeeth,
                        IDX_APP_PRIMARY_USER, IDX_RUT_RECIPE_USER);
            } finally {
                con.close();
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Fixture: direct SQL seeding, single app / single tenant, linked groups of 3.
    // ---------------------------------------------------------------------------------------------

    private void seed(Connection con, String auid, String rut, String ruai, String put,
                      String appId, String tenantId) throws Exception {
        // primary index for member i: i - (i % 3) -> groups of 3, primary is the group head.
        String pidxExpr = "(i - (i % 3))";

        exec(con, "INSERT INTO " + auid
                + " (app_id, user_id, recipe_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user,"
                + "  time_joined, primary_or_recipe_user_time_joined) "
                + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), 'thirdparty', "
                + "  'u' || lpad(pidx::text, 35, '0'), true, "
                + "  " + TIME_BASE + " + i, " + TIME_BASE + " + pidx "
                + "FROM (SELECT i, " + pidxExpr + " AS pidx "
                + "      FROM generate_series(0, " + (NUM_USERS - 1) + ") AS i) s");

        // One recipe_user_account_infos row per recipe user (third-party), primary_user_id = group head.
        exec(con, "INSERT INTO " + ruai
                + " (app_id, recipe_user_id, recipe_id, account_info_type, account_info_value,"
                + "  third_party_id, third_party_user_id, primary_user_id) "
                + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), 'thirdparty', 'tparty', "
                + "  'google::sub' || i, 'google', 'sub' || i, 'u' || lpad(pidx::text, 35, '0') "
                + "FROM (SELECT i, " + pidxExpr + " AS pidx "
                + "      FROM generate_series(0, " + (NUM_USERS - 1) + ") AS i) s");

        // One recipe_user_tenants row per recipe user (third-party) in the single tenant.
        exec(con, "INSERT INTO " + rut
                + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type, account_info_value,"
                + "  third_party_id, third_party_user_id) "
                + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), " + q(tenantId) + ", "
                + "  'thirdparty', 'tparty', 'google::sub' || i, 'google', 'sub' || i "
                + "FROM generate_series(0, " + (NUM_USERS - 1) + ") AS i");

        // primary_user_tenants: one row per primary user present in the tenant (group heads).
        exec(con, "INSERT INTO " + put
                + " (app_id, tenant_id, account_info_type, account_info_value, primary_user_id) "
                + "SELECT " + q(appId) + ", " + q(tenantId) + ", 'tparty', 'google::sub' || pidx, "
                + "  'u' || lpad(pidx::text, 35, '0') "
                + "FROM (SELECT (b * 3) AS pidx "
                + "      FROM generate_series(0, " + (NUM_USERS / 3 - 1) + ") AS b) s");

        exec(con, "VACUUM ANALYZE " + auid);
        exec(con, "VACUUM ANALYZE " + rut);
        exec(con, "VACUUM ANALYZE " + ruai);
        exec(con, "VACUUM ANALYZE " + put);
    }

    // ---------------------------------------------------------------------------------------------
    // Query builders — copies of the SQL AccountInfoQueries builds (bind params inlined as literals).
    // ---------------------------------------------------------------------------------------------

    // listPrimaryUserIdsByThirdPartyInfo (AccountInfoQueries).
    private String thirdPartyLookup(String auid, String ruai, String appId, String accountInfoValue) {
        return "SELECT DISTINCT auid.primary_or_recipe_user_id"
                + " FROM " + ruai + " ruai"
                + " JOIN " + auid + " auid ON ruai.app_id = auid.app_id AND ruai.recipe_user_id = auid.user_id"
                + " WHERE ruai.app_id = " + q(appId)
                + " AND ruai.account_info_type = 'tparty' AND ruai.account_info_value = " + q(accountInfoValue);
    }

    // updateAccountInfo_Transaction QUERY_1 — fixed (app_id restored, alias-qualified).
    private String updateCleanupFixed(String put, String rut, String ruai, String appId,
                                      String primaryId, String memberId) {
        return "DELETE FROM " + put + " put"
                + " WHERE put.app_id = " + q(appId) + " AND put.primary_user_id = " + q(primaryId)
                + " AND put.account_info_type = 'tparty' AND put.account_info_value NOT IN ("
                + "     SELECT rut.account_info_value"
                + "     FROM " + rut + " rut"
                + "     WHERE rut.app_id = " + q(appId) + " AND rut.recipe_user_id IN ("
                + "         SELECT ruai.recipe_user_id"
                + "         FROM " + ruai + " ruai"
                + "         WHERE ruai.app_id = " + q(appId) + " AND ruai.primary_user_id = " + q(primaryId)
                + "             AND ruai.recipe_user_id != " + q(memberId)
                + "     )"
                + " )";
    }

    // updateAccountInfo_Transaction QUERY_1 — pre-fix (app_id dropped from both subqueries).
    private String updateCleanupPreFix(String put, String rut, String ruai, String appId,
                                       String primaryId, String memberId) {
        return "DELETE FROM " + put
                + " WHERE app_id = " + q(appId) + " AND primary_user_id = " + q(primaryId)
                + " AND account_info_type = 'tparty' AND account_info_value NOT IN ("
                + "     SELECT account_info_value"
                + "     FROM " + rut
                + "     WHERE recipe_user_id IN ("
                + "         SELECT recipe_user_id"
                + "         FROM " + ruai
                + "         WHERE primary_user_id = " + q(primaryId) + " AND recipe_user_id != " + q(memberId)
                + "     )"
                + " )";
    }

    // removeAccountInfoReservationForPrimaryUserWhileRemovingTenant_Transaction — fixed.
    private String tenantRemovalCleanupFixed(String put, String rut, String ruai, String appId,
                                             String primaryId, String memberId, String tenantId) {
        return "DELETE FROM " + put + " put"
                + " WHERE put.app_id = " + q(appId) + " AND put.primary_user_id = " + q(primaryId)
                + " AND (put.tenant_id) NOT IN ("
                + "     SELECT DISTINCT rut.tenant_id"
                + "     FROM " + rut + " rut"
                + "     WHERE rut.app_id = " + q(appId) + " AND rut.recipe_user_id IN ("
                + "         SELECT ruai.recipe_user_id"
                + "         FROM " + ruai + " ruai"
                + "         WHERE ruai.app_id = " + q(appId) + " AND ruai.primary_user_id = " + q(primaryId)
                + "             AND ((ruai.recipe_user_id = " + q(memberId) + " AND rut.tenant_id != " + q(tenantId)
                + "                 ) OR ruai.recipe_user_id != " + q(memberId) + ")"
                + "     )"
                + " )";
    }

    // removeAccountInfoReservationForPrimaryUserWhileRemovingTenant_Transaction — pre-fix (app_id dropped).
    private String tenantRemovalCleanupPreFix(String put, String rut, String ruai, String appId,
                                              String primaryId, String memberId, String tenantId) {
        return "DELETE FROM " + put
                + " WHERE app_id = " + q(appId) + " AND primary_user_id = " + q(primaryId)
                + " AND (tenant_id) NOT IN ("
                + "     SELECT DISTINCT tenant_id"
                + "     FROM " + rut
                + "     WHERE recipe_user_id IN ("
                + "         SELECT recipe_user_id"
                + "         FROM " + ruai
                + "         WHERE primary_user_id = " + q(primaryId)
                + "             AND ((recipe_user_id = " + q(memberId) + " AND tenant_id != " + q(tenantId)
                + "                 ) OR recipe_user_id != " + q(memberId) + ")"
                + "     )"
                + " )";
    }

    // ---------------------------------------------------------------------------------------------
    // EXPLAIN + plan-tree helpers.
    // ---------------------------------------------------------------------------------------------

    private JsonObject explain(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("EXPLAIN (FORMAT JSON) " + sql)) {
            assertTrue("EXPLAIN returned no rows", rs.next());
            JsonArray arr = new JsonParser().parse(rs.getString(1)).getAsJsonArray();
            return arr.get(0).getAsJsonObject().getAsJsonObject("Plan");
        }
    }

    private void assertNoSeqScan(String label, JsonObject plan, String relation) {
        assertFalse(label + ": must not sequential-scan " + relation + "; plan=" + plan,
                hasSeqScan(plan, relation));
    }

    // True if any node is a "Seq Scan" over the given relation (matched by unqualified table name).
    private boolean hasSeqScan(JsonObject node, String relation) {
        String bare = bareName(relation);
        if ("Seq Scan".equals(str(node, "Node Type")) && bare.equals(str(node, "Relation Name"))) {
            return true;
        }
        for (JsonObject child : children(node)) {
            if (hasSeqScan(child, relation)) return true;
        }
        return false;
    }

    // True if any node reads via the given index name.
    private boolean usesIndex(JsonObject node, String indexName) {
        if (indexName.equals(str(node, "Index Name"))) {
            return true;
        }
        for (JsonObject child : children(node)) {
            if (usesIndex(child, indexName)) return true;
        }
        return false;
    }

    // The "Index Cond" text of the first node scanning via the given index, or null if the index is unused
    // (covers Index Scan, Index Only Scan, and Bitmap Index Scan nodes — all carry "Index Name"/"Index Cond").
    private String indexCond(JsonObject node, String indexName) {
        if (indexName.equals(str(node, "Index Name"))) {
            String cond = str(node, "Index Cond");
            return cond != null ? cond : "";
        }
        for (JsonObject child : children(node)) {
            String cond = indexCond(child, indexName);
            if (cond != null) return cond;
        }
        return null;
    }

    // Fixed-shape assertion: the query supplies app_id, so any scan via this index must carry app_id in its
    // index condition. This is the planner-version-proof signature of the fix (see class doc).
    private void assertIndexCondHasAppId(String label, JsonObject plan, String indexName) {
        String cond = indexCond(plan, indexName);
        assertNotNull(label + ": expected a scan via " + indexName + "; plan=" + plan, cond);
        assertTrue(label + ": app_id must appear in the " + indexName + " index condition; cond=" + cond
                + "; plan=" + plan, cond.contains("app_id"));
    }

    // Teeth assertion for the pre-fix shape: the query drops app_id, so no scan over these indexes can carry
    // app_id in its index condition — whether the planner seq-scans (PG <= 17, index unused: vacuously true)
    // or skip-scans the missing leading column (PG 18+). Filters are intentionally ignored: app_id may appear
    // there, but a filter does not bound the index traversal.
    private void assertIndexCondLacksAppId(String label, JsonObject plan, String... indexNames) {
        for (String indexName : indexNames) {
            String cond = indexCond(plan, indexName);
            if (cond != null) {
                assertFalse(label + ": app_id cannot appear in the " + indexName
                                + " index condition when the query drops it; cond=" + cond + "; plan=" + plan,
                        cond.contains("app_id"));
            }
        }
    }

    private boolean indexExists(Connection con, String indexName) throws Exception {
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT 1 FROM pg_indexes WHERE indexname = " + q(indexName) + " LIMIT 1")) {
            return rs.next();
        }
    }

    private static String bareName(String maybeSchemaQualified) {
        int dot = maybeSchemaQualified.lastIndexOf('.');
        return dot >= 0 ? maybeSchemaQualified.substring(dot + 1) : maybeSchemaQualified;
    }

    private String str(JsonObject node, String key) {
        return node.has(key) ? node.get(key).getAsString() : null;
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

    private static String q(String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}
