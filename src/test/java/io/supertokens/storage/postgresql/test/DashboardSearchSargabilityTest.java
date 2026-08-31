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
import io.supertokens.authRecipe.UserPaginationContainer;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.dashboard.DashboardSearchTags;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;

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
 * Covers the dashboard user-search sargability change on {@code GeneralQueries.getUsers_new}: the
 * {@code account_info_value ILIKE} arms became index-sargable {@code LIKE lower(?) || '%'} prefix matches.
 * Emails/phones are normalized to lower case at write time, so those arms match the <b>bare</b> column
 * ({@code account_info_value LIKE lower(?) || '%'}) and are served by an opclass swap of the existing
 * account-info index: {@code idx_recipe_user_tenants_account_info} is recreated as
 * {@code idx_recipe_user_tenants_account_info_pattern} with {@code text_pattern_ops} on
 * {@code account_info_value} (still serves the equality consumers, now also serves prefix range scans).
 * The email domain arm ({@code lower(split_part(account_info_value, '@', 2)) LIKE lower(?) || '%'}) and the
 * case-insensitive provider arm ({@code lower(account_info_value) LIKE lower(?) || '%'}, tparty values are
 * not normalized) keep {@code lower()} and get their own small partial expression indexes
 * ({@code idx_recipe_user_tenants_search_domain} and {@code idx_recipe_user_tenants_search_tparty}).
 *
 * <p>{@link #testDashboardSearchArmsAreSargable()} is a plan-shape regression test: it seeds a synthetic
 * single-app / single-tenant dataset directly with SQL, then runs the exact query {@code getUsers_new}
 * builds (bind params inlined as literals) under {@code EXPLAIN (FORMAT JSON)} and asserts that the search
 * arms reach {@code recipe_user_tenants} through those indexes with <b>no sequential scan</b>. Its teeth
 * drop the indexes and assert the search can no longer be served by them (it degrades to a filter over a
 * broad scan). {@code EXPLAIN} without {@code ANALYZE} only plans, it does not execute.
 *
 * <p>{@link #testDashboardSearchSemanticsPreserved()} drives the real {@code getUsers_new} path end to end
 * (migration mode {@code DUAL_WRITE_READ_NEW}, users created through the recipe APIs) and pins the semantics:
 * a mixed-case third-party value still matches case-insensitively, an email local-part prefix matches, and an
 * email domain prefix matches (the one intentional change — {@code %@term%} became a strict domain prefix —
 * is identical for the single-{@code @} values that email normalization guarantees).
 *
 * <p>The plan-shape fixture is heavy-ish; set {@code SKIP_SCALE_REGRESSION_TESTS=true} to exclude it from
 * quick local runs.
 */
public class DashboardSearchSargabilityTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    // Enough rows that a selective prefix probe is decisively cheaper than a sequential scan, so the
    // planner's choice is a reliable signal of whether a usable index exists.
    private static final int NUM_USERS = 30_000;
    private static final long TIME_BASE = 1_000_000_000L;

    // Index names created by AccountInfoQueries (must match the DDL).
    // The opclass-swapped successor to idx_recipe_user_tenants_account_info; serves the bare-column
    // email/phone prefix arms.
    private static final String IDX_ACCOUNT_INFO_PATTERN = "idx_recipe_user_tenants_account_info_pattern";
    private static final String IDX_ACCOUNT_INFO_OLD = "idx_recipe_user_tenants_account_info";
    private static final String IDX_SEARCH_DOMAIN = "idx_recipe_user_tenants_search_domain";
    private static final String IDX_SEARCH_TPARTY = "idx_recipe_user_tenants_search_tparty";

    // Whole-table expression statistics objects created by AccountInfoQueries (must match the DDL).
    // These are what actually fix the partial-index mis-estimate; PostgreSQL requires 14+ for them.
    private static final String STAT_SEARCH_DOMAIN = "st_recipe_user_tenants_search_domain";
    private static final String STAT_SEARCH_TPARTY = "st_recipe_user_tenants_search_tparty";
    private static final int PG14_VERSION_NUM = 140000;

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
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        return process;
    }

    // ---------------------------------------------------------------------------------------------
    // (1) Plan-shape regression: the search arms must be index range scans, not tenant-wide scans.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testDashboardSearchArmsAreSargable() throws Exception {
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

            Connection con = ConnectionPool.getConnection(storage);
            try {
                con.setAutoCommit(true);

                // Sanity: the search indexes were actually created at startup on a fresh database, and the
                // fresh-install path creates the swapped pattern index directly rather than the old plain one.
                assertTrue("startup DDL must create " + IDX_ACCOUNT_INFO_PATTERN,
                        indexExists(con, IDX_ACCOUNT_INFO_PATTERN));
                assertTrue("startup DDL must create " + IDX_SEARCH_DOMAIN, indexExists(con, IDX_SEARCH_DOMAIN));
                assertTrue("startup DDL must create " + IDX_SEARCH_TPARTY, indexExists(con, IDX_SEARCH_TPARTY));
                assertFalse("fresh install must not create the pre-swap " + IDX_ACCOUNT_INFO_OLD,
                        indexExists(con, IDX_ACCOUNT_INFO_OLD));

                // Deterministic single-worker plans so node selection does not flip on a loaded CI box.
                exec(con, "SET max_parallel_workers_per_gather = 0");

                seed(con, auid, rut, appId, tenantId);

                // ---- Email arm: local-part prefix OR domain prefix, both index-backed (BitmapOr / two ----
                // ---- index scans). The prefixes below are selective (one matching user each). ----
                JsonObject emailPlan = explain(con, emailSearch(auid, rut, appId, tenantId, "user12345", "d12345"));
                assertNoSeqScan("email search", emailPlan, rut);
                assertTrue("email search must use " + IDX_ACCOUNT_INFO_PATTERN + " (local-part arm); plan="
                                + emailPlan, usesIndex(emailPlan, IDX_ACCOUNT_INFO_PATTERN));
                assertTrue("email search must use " + IDX_SEARCH_DOMAIN + " (domain arm); plan=" + emailPlan,
                        usesIndex(emailPlan, IDX_SEARCH_DOMAIN));

                // Teeth: without the pattern index (value arm) and the domain index the search cannot be
                // served by them. A default-opclass index cannot answer a C-collation prefix bound, so the
                // arms fall to a Filter over a broad scan (seq scan on PG <= 17, or a type-prefix index scan
                // filtered on 18+) — either way both indexes are unused. Their absence from the plan is the
                // version-proof signature.
                exec(con, "DROP INDEX " + IDX_ACCOUNT_INFO_PATTERN);
                exec(con, "DROP INDEX " + IDX_SEARCH_DOMAIN);
                exec(con, "ANALYZE " + rut);
                try {
                    JsonObject teeth = explain(con, emailSearch(auid, rut, appId, tenantId, "user12345", "d12345"));
                    assertFalse("teeth: " + IDX_ACCOUNT_INFO_PATTERN + " must be gone; plan=" + teeth,
                            usesIndex(teeth, IDX_ACCOUNT_INFO_PATTERN));
                    assertFalse("teeth: " + IDX_SEARCH_DOMAIN + " must be gone; plan=" + teeth,
                            usesIndex(teeth, IDX_SEARCH_DOMAIN));
                } finally {
                    exec(con, "CREATE INDEX IF NOT EXISTS " + IDX_ACCOUNT_INFO_PATTERN + " ON " + rut
                            + "(app_id, tenant_id, account_info_type, account_info_value text_pattern_ops)");
                    exec(con, "CREATE INDEX IF NOT EXISTS " + IDX_SEARCH_DOMAIN + " ON " + rut
                            + "(app_id, tenant_id, lower(split_part(account_info_value, '@', 2)) text_pattern_ops)"
                            + " WHERE account_info_type = 'email'");
                    exec(con, "ANALYZE " + rut);
                }

                // ---- Provider (tparty) arm: single case-insensitive value-prefix match served by the ----
                // ---- partial IDX_SEARCH_TPARTY. ----
                JsonObject providerPlan = explain(con, providerSearch(auid, rut, appId, tenantId, "google::sub12345"));
                assertNoSeqScan("provider search", providerPlan, rut);
                assertTrue("provider search must use " + IDX_SEARCH_TPARTY + "; plan=" + providerPlan,
                        usesIndex(providerPlan, IDX_SEARCH_TPARTY));
            } finally {
                con.close();
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // (1b) Statistics objects: the whole-table expression statistics that fix the partial-index
    //      mis-estimate must be created at startup on PG14+ (and skipped, without error, below 14).
    //      This is the actual fix of the PR; the plan-shape test above only exercises the indexes,
    //      so without this a refactor of the CREATE STATISTICS DO block would regress silently.
    //      Cheap and deterministic (no large seed), so it is not gated behind SKIP_SCALE_REGRESSION_TESTS.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testDashboardSearchStatisticsCreated() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            String rut = Config.getConfig(storage).getRecipeUserTenantsTable();

            Connection con = ConnectionPool.getConnection(storage);
            try {
                con.setAutoCommit(true);
                int serverVersion = serverVersionNum(con);
                if (serverVersion >= PG14_VERSION_NUM) {
                    // Expression statistics require PG14+; the startup DDL must have created both objects
                    // (the process reaching STARTED already proves the fresh-install batch did not fail).
                    assertTrue("startup DDL must create statistics " + STAT_SEARCH_DOMAIN + " on PG14+ (server_version_num="
                            + serverVersion + ")", statisticsExists(con, rut, STAT_SEARCH_DOMAIN));
                    assertTrue("startup DDL must create statistics " + STAT_SEARCH_TPARTY + " on PG14+ (server_version_num="
                            + serverVersion + ")", statisticsExists(con, rut, STAT_SEARCH_TPARTY));
                } else {
                    // Below 14 the DO block returns early. Startup must still have succeeded (we got here)
                    // and no statistics objects are created for this table.
                    assertFalse("statistics " + STAT_SEARCH_DOMAIN + " must be skipped, not created, below PG14"
                            + " (server_version_num=" + serverVersion + ")", statisticsExists(con, rut, STAT_SEARCH_DOMAIN));
                    assertFalse("statistics " + STAT_SEARCH_TPARTY + " must be skipped, not created, below PG14"
                            + " (server_version_num=" + serverVersion + ")", statisticsExists(con, rut, STAT_SEARCH_TPARTY));
                }
            } finally {
                con.close();
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // Seed one email row and one tparty row per user, single app / single tenant. Distinct local-parts and
    // domains keep the search prefixes selective so the planner's index choice is meaningful.
    private void seed(Connection con, String auid, String rut, String appId, String tenantId) throws Exception {
        exec(con, "INSERT INTO " + auid
                + " (app_id, user_id, recipe_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user,"
                + "  time_joined, primary_or_recipe_user_time_joined) "
                + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), 'thirdparty', "
                + "  'u' || lpad(i::text, 35, '0'), true, " + TIME_BASE + " + i, " + TIME_BASE + " + i "
                + "FROM generate_series(0, " + (NUM_USERS - 1) + ") AS i");

        // Email rows: value 'user<i>@d<i>.com' — distinct local-part and domain per user.
        exec(con, "INSERT INTO " + rut
                + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type, account_info_value,"
                + "  third_party_id, third_party_user_id) "
                + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), " + q(tenantId) + ", "
                + "  'emailpassword', 'email', 'user' || i || '@d' || i || '.com', '', '' "
                + "FROM generate_series(0, " + (NUM_USERS - 1) + ") AS i");

        // Third-party rows: value 'google::sub<i>' — distinct per user.
        exec(con, "INSERT INTO " + rut
                + " (app_id, recipe_user_id, tenant_id, recipe_id, account_info_type, account_info_value,"
                + "  third_party_id, third_party_user_id) "
                + "SELECT " + q(appId) + ", 'u' || lpad(i::text, 35, '0'), " + q(tenantId) + ", "
                + "  'thirdparty', 'tparty', 'google::sub' || i, 'google', 'sub' || i "
                + "FROM generate_series(0, " + (NUM_USERS - 1) + ") AS i");

        exec(con, "VACUUM ANALYZE " + auid);
        exec(con, "VACUUM ANALYZE " + rut);
    }

    // getUsers_new email-only search (bind params inlined as literals). localTerm feeds the local-part arm,
    // domainTerm the domain arm; the code binds the same email term to both, but distinct literals here let
    // each arm be pinned to its own index.
    private String emailSearch(String auid, String rut, String appId, String tenantId,
                               String localTerm, String domainTerm) {
        return "SELECT DISTINCT auid.primary_or_recipe_user_id, auid.primary_or_recipe_user_time_joined"
                + " FROM " + auid + " auid"
                + " JOIN " + rut + " rut ON auid.app_id = rut.app_id AND auid.user_id = rut.recipe_user_id"
                + " WHERE rut.app_id = " + q(appId) + " AND rut.tenant_id = " + q(tenantId)
                + " AND rut.account_info_type = 'email' AND ("
                + "   rut.account_info_value LIKE lower(" + q(localTerm) + ") || '%'"
                + "   OR lower(split_part(rut.account_info_value, '@', 2)) LIKE lower(" + q(domainTerm) + ") || '%'"
                + " )"
                + " ORDER BY auid.primary_or_recipe_user_time_joined ASC, auid.primary_or_recipe_user_id DESC"
                + " LIMIT 1000";
    }

    // getUsers_new provider-only search (bind params inlined as literals).
    private String providerSearch(String auid, String rut, String appId, String tenantId, String term) {
        return "SELECT DISTINCT auid.primary_or_recipe_user_id, auid.primary_or_recipe_user_time_joined"
                + " FROM " + auid + " auid"
                + " JOIN " + rut + " rut ON auid.app_id = rut.app_id AND auid.user_id = rut.recipe_user_id"
                + " WHERE rut.app_id = " + q(appId) + " AND rut.tenant_id = " + q(tenantId)
                + " AND rut.account_info_type = 'tparty' AND ("
                + "   lower(rut.account_info_value) LIKE lower(" + q(term) + ") || '%'"
                + " )"
                + " ORDER BY auid.primary_or_recipe_user_time_joined ASC, auid.primary_or_recipe_user_id DESC"
                + " LIMIT 1000";
    }

    // ---------------------------------------------------------------------------------------------
    // (2) Semantics: drive the real getUsers_new path and pin case-insensitivity + prefix matching.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testDashboardSearchSemanticsPreserved() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            // Read from the new tables so getUsers_new (the changed path) is exercised.
            Config.getConfig((Start) StorageLayer.getStorage(main))
                    .setMigrationModeForTesting(MigrationMode.DUAL_WRITE_READ_NEW);

            // Third-party value is "thirdPartyId::thirdPartyUserId"; use mixed case on both sides.
            ThirdParty.SignInUpResponse tp = ThirdParty.signInUp(main, "Google", "USER-XyZ", "tpuser@other.com");
            String tpUserId = tp.user.getSupertokensUserId();

            AuthRecipeUserInfo ep = EmailPassword.signUp(main, "john@Example.com", "password123");
            String epUserId = ep.getSupertokensUserId();

            // Provider search matches the value prefix case-insensitively — provider id part.
            assertTrue("provider 'google' must match the 'Google::...' third-party value",
                    userIds(search(main, null, null, List.of("google"))).contains(tpUserId));

            // ... and deeper into the mixed-case third-party user-id part.
            assertTrue("provider 'GOOGLE::user-xyz' must match 'Google::USER-XyZ' case-insensitively",
                    userIds(search(main, null, null, List.of("GOOGLE::user-xyz"))).contains(tpUserId));

            // A non-prefix substring must NOT match (prefix semantics, unchanged).
            assertFalse("provider 'oogle' is not a prefix and must not match",
                    userIds(search(main, null, null, List.of("oogle"))).contains(tpUserId));

            // Email local-part prefix.
            assertTrue("email 'john' must match john@example.com",
                    userIds(search(main, List.of("john"), null, null)).contains(epUserId));

            // Email domain prefix (the arm that replaced %@term%): domain 'example.com' starts with 'example'.
            List<String> byDomain = userIds(search(main, List.of("example"), null, null));
            assertTrue("email 'example' must match via the domain arm (example.com)", byDomain.contains(epUserId));
            // The third-party user's email domain is other.com, so it must not appear under an 'example' domain
            // search — confirms the domain arm is a real domain prefix, not a whole-value substring.
            assertFalse("email 'example' must not match the other.com third-party user", byDomain.contains(tpUserId));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    private UserPaginationContainer search(Main main, List<String> emails, List<String> phones,
                                           List<String> providers) throws Exception {
        DashboardSearchTags tags = new DashboardSearchTags(emails, phones, providers);
        return AuthRecipe.getUsers(main, 100, "ASC", null, null, tags);
    }

    private List<String> userIds(UserPaginationContainer result) {
        List<String> ids = new ArrayList<>();
        for (AuthRecipeUserInfo u : result.users) {
            ids.add(u.getSupertokensUserId());
        }
        return ids;
    }

    // ---------------------------------------------------------------------------------------------
    // EXPLAIN + plan-tree helpers (same shape as AccountInfoIndexScaleRegressionTest).
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

    private boolean usesIndex(JsonObject node, String indexName) {
        if (indexName.equals(str(node, "Index Name"))) {
            return true;
        }
        for (JsonObject child : children(node)) {
            if (usesIndex(child, indexName)) return true;
        }
        return false;
    }

    private boolean indexExists(Connection con, String indexName) throws Exception {
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT 1 FROM pg_indexes WHERE indexname = " + q(indexName) + " LIMIT 1")) {
            return rs.next();
        }
    }

    // Scoped to this instance's table via stxrelid, mirroring the production DO-block guard, so it is
    // correct even in a shared-schema deployment where another table carries a same-named stats object.
    private boolean statisticsExists(Connection con, String table, String statsName) throws Exception {
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT 1 FROM pg_statistic_ext WHERE stxrelid = " + q(table) + "::regclass"
                             + " AND stxname = " + q(statsName) + " LIMIT 1")) {
            return rs.next();
        }
    }

    private int serverVersionNum(Connection con) throws Exception {
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("SHOW server_version_num")) {
            assertTrue("SHOW server_version_num returned no rows", rs.next());
            return Integer.parseInt(rs.getString(1).trim());
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
