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
 * Plan-shape regression test for the two additive {@code oauth_sessions} indexes,
 * {@code oauth_session_client_id_index} ({@code (app_id, client_id)}) and
 * {@code oauth_session_session_handle_index} ({@code (app_id, session_handle)}).
 *
 * <p>{@code oauth_sessions} is keyed by {@code gid} only, so before these indexes the two revoke
 * deletes each scanned the whole table:
 *
 * <ul>
 *   <li>{@code deleteOAuthSessionByClientId}: {@code DELETE ... WHERE app_id = ? AND client_id = ?}
 *       — revoke-all-for-client, and the FK-cascade path when an oauth client is deleted;</li>
 *   <li>{@code deleteOAuthSessionBySessionHandle}: {@code DELETE ... WHERE app_id = ? AND
 *       session_handle = ?} — revoke on SuperTokens-session logout.</li>
 * </ul>
 *
 * <p>The test seeds ~50k sessions across many clients directly with SQL, then runs each revoke
 * delete under {@code EXPLAIN (FORMAT JSON)} (plain EXPLAIN — plans, does not execute, so nothing
 * is deleted) and asserts the plan uses an index scan on the corresponding new index and contains
 * no sequential scan. As teeth, it drops the index and re-EXPLAINs, asserting the plan falls back to
 * a sequential scan — proving the index is what drives the plan and the test would catch its removal.
 *
 * <p>The fixture is moderate (~50k rows). It runs by default (including in CI); set the environment
 * variable {@code SKIP_SCALE_REGRESSION_TESTS=true} to exclude it from quick local runs.
 */
public class OAuthSessionRevokeIndexRegressionTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    private static final int NUM_CLIENTS = 500;
    private static final int TOTAL_SESSIONS = 50_000; // 100 sessions per client
    private static final String CLIENT_ID_INDEX = "oauth_session_client_id_index";
    private static final String SESSION_HANDLE_INDEX = "oauth_session_session_handle_index";

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

    @Test
    public void testRevokeDeletesUseTheNewIndexes() throws Exception {
        Assume.assumeTrue("scale regression tests skipped via SKIP_SCALE_REGRESSION_TESTS",
                !"true".equalsIgnoreCase(System.getenv("SKIP_SCALE_REGRESSION_TESTS")));

        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            TenantIdentifier tenant = ResourceDistributor.getAppForTesting();
            String appId = tenant.getAppId();

            String clients = Config.getConfig(storage).getOAuthClientsTable();
            String sessions = Config.getConfig(storage).getOAuthSessionsTable();

            Connection con = ConnectionPool.getConnection(storage);
            try {
                con.setAutoCommit(true);
                // Deterministic single-worker plans so node selection does not flip on a loaded CI box.
                exec(con, "SET max_parallel_workers_per_gather = 0");

                seed(con, clients, sessions, appId);

                // A client that owns a small fraction of the table (100 / 50_000 = 0.2%), and a single
                // session_handle — both selective enough that the planner prefers the index.
                String clientDelete = "DELETE FROM " + sessions
                        + " WHERE app_id = " + q(appId) + " and client_id = " + q("client0") + ";";
                String sessionHandleDelete = "DELETE FROM " + sessions
                        + " WHERE app_id = " + q(appId) + " and session_handle = " + q("sh12345");

                // -------- With the indexes present: index scan, no seq scan --------
                JsonObject clientPlan = explain(con, clientDelete);
                assertTrue("revoke-by-client delete must use " + CLIENT_ID_INDEX + ", plan was:\n" + clientPlan,
                        usesIndexNamed(clientPlan, CLIENT_ID_INDEX));
                assertFalse("revoke-by-client delete must not sequentially scan oauth_sessions",
                        containsSeqScan(clientPlan));

                JsonObject sessionHandlePlan = explain(con, sessionHandleDelete);
                assertTrue("revoke-by-session-handle delete must use " + SESSION_HANDLE_INDEX
                        + ", plan was:\n" + sessionHandlePlan, usesIndexNamed(sessionHandlePlan, SESSION_HANDLE_INDEX));
                assertFalse("revoke-by-session-handle delete must not sequentially scan oauth_sessions",
                        containsSeqScan(sessionHandlePlan));

                // -------- Teeth: dropping each index forces a sequential scan --------
                exec(con, "DROP INDEX " + CLIENT_ID_INDEX);
                JsonObject clientPlanNoIndex = explain(con, clientDelete);
                assertTrue("without " + CLIENT_ID_INDEX + " the revoke-by-client delete is expected to "
                        + "sequentially scan (test would not detect the index's removal otherwise), plan was:\n"
                        + clientPlanNoIndex, containsSeqScan(clientPlanNoIndex));
                assertFalse(usesIndexNamed(clientPlanNoIndex, CLIENT_ID_INDEX));

                exec(con, "DROP INDEX " + SESSION_HANDLE_INDEX);
                JsonObject sessionHandlePlanNoIndex = explain(con, sessionHandleDelete);
                assertTrue("without " + SESSION_HANDLE_INDEX + " the revoke-by-session-handle delete is "
                        + "expected to sequentially scan, plan was:\n" + sessionHandlePlanNoIndex,
                        containsSeqScan(sessionHandlePlanNoIndex));
                assertFalse(usesIndexNamed(sessionHandlePlanNoIndex, SESSION_HANDLE_INDEX));

                // Restore the indexes so the DB is left in its normal state.
                exec(con, "CREATE INDEX IF NOT EXISTS " + CLIENT_ID_INDEX + " ON " + sessions
                        + " (app_id, client_id)");
                exec(con, "CREATE INDEX IF NOT EXISTS " + SESSION_HANDLE_INDEX + " ON " + sessions
                        + " (app_id, session_handle)");
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

    private void seed(Connection con, String clients, String sessions, String appId) throws Exception {
        // One row per client (FK target of oauth_sessions.(app_id, client_id)).
        exec(con, "INSERT INTO " + clients
                + " (app_id, client_id, client_secret, enable_refresh_token_rotation, is_client_credentials_only) "
                + "SELECT " + q(appId) + ", 'client' || c, NULL, false, false "
                + "FROM generate_series(0, " + (NUM_CLIENTS - 1) + ") AS c");

        // Sessions spread evenly across the clients (client_id = 'client' || (i % NUM_CLIENTS)); each
        // has a unique gid, session_handle and refresh tokens (the latter two carry UNIQUE constraints).
        exec(con, "INSERT INTO " + sessions
                + " (gid, app_id, client_id, session_handle, external_refresh_token, internal_refresh_token,"
                + "  jti, exp) "
                + "SELECT 'g' || i, " + q(appId) + ", 'client' || (i % " + NUM_CLIENTS + "), "
                + "  'sh' || i, 'ext' || i, 'int' || i, 'jti', 1000000000 + i "
                + "FROM generate_series(0, " + (TOTAL_SESSIONS - 1) + ") AS i");

        // Fresh stats + visibility map so the planner costs the index plans correctly.
        exec(con, "VACUUM ANALYZE " + clients);
        exec(con, "VACUUM ANALYZE " + sessions);
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

    // Any Index Scan / Index Only Scan / Bitmap Index Scan node referencing the given index by name.
    private boolean usesIndexNamed(JsonObject node, String indexName) {
        if (node.has("Index Name") && node.get("Index Name").getAsString().equals(indexName)) {
            return true;
        }
        for (JsonObject child : children(node)) {
            if (usesIndexNamed(child, indexName)) return true;
        }
        return false;
    }

    private boolean containsSeqScan(JsonObject node) {
        if (node.get("Node Type").getAsString().equals("Seq Scan")) {
            return true;
        }
        for (JsonObject child : children(node)) {
            if (containsSeqScan(child)) return true;
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

    private static String q(String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}
