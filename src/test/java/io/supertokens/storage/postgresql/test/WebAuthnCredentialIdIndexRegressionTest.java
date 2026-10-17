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
import io.supertokens.webauthn.WebAuthN;
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
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Index regression test for {@code webauthn_credentials_app_id_id_index} ({@code (app_id, id)}).
 *
 * <p>WebAuthn looks credentials up by {@code WHERE app_id = ? AND id = ?} (sign in, counter update, and the
 * app-wide credential id uniqueness check at registration). The primary key is {@code (app_id, rp_id, id)}, so
 * with many relying party ids it cannot serve that predicate. The test checks that the index is created on a
 * fresh install and re-created at startup on an existing table (the patch-release backfill path), and that the
 * lookup plans an index scan on it over a seeded table, and is much more expensive without it.
 */
public class WebAuthnCredentialIdIndexRegressionTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    private static final String INDEX = "webauthn_credentials_app_id_id_index";
    private static final int NUM_USERS = 50;
    private static final int TOTAL_CREDENTIALS = 50_000;

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

    private static void stop(TestingProcessManager.TestingProcess process) throws InterruptedException {
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testIndexIsCreatedOnFreshInstallAndBackfilledAtStartup() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Main main = process.getProcess();
        if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
            stop(process);
            return;
        }
        Start storage = (Start) StorageLayer.getStorage(main);
        String credentials = Config.getConfig(storage).getWebAuthNCredentialsTable();
        try (Connection con = ConnectionPool.getConnection(storage)) {
            con.setAutoCommit(true);
            assertEquals("(app_id, id)", indexColumns(con, credentials));
            exec(con, "DROP INDEX " + INDEX);
            assertNull(indexColumns(con, credentials));
        }
        stop(process);

        // restart on the existing schema: the startup backfill re-creates the index
        process = startProcess();
        storage = (Start) StorageLayer.getStorage(process.getProcess());
        try (Connection con = ConnectionPool.getConnection(storage)) {
            con.setAutoCommit(true);
            assertEquals("(app_id, id)", indexColumns(con, credentials));
        }
        stop(process);
    }

    @Test
    public void testCredentialLookupByIdUsesTheIndex() throws Exception {
        Assume.assumeTrue("scale regression tests skipped via SKIP_SCALE_REGRESSION_TESTS",
                !"true".equalsIgnoreCase(System.getenv("SKIP_SCALE_REGRESSION_TESTS")));

        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }
            Start storage = (Start) StorageLayer.getStorage(main);
            TenantIdentifier tenant = ResourceDistributor.getAppForTesting();
            String appId = tenant.getAppId();
            String credentials = Config.getConfig(storage).getWebAuthNCredentialsTable();

            List<String> userIds = new ArrayList<>();
            for (int i = 0; i < NUM_USERS; i++) {
                String userId = UUID.randomUUID().toString();
                WebAuthN.saveUser(storage, tenant, "user" + i + "@example.com", userId, "rp" + i + ".example.com");
                userIds.add(userId);
            }

            try (Connection con = ConnectionPool.getConnection(storage)) {
                con.setAutoCommit(true);
                exec(con, "SET max_parallel_workers_per_gather = 0");

                // credentials spread over many relying party ids, so the (app_id, rp_id, id) key cannot seek on id
                StringBuilder users = new StringBuilder("ARRAY[");
                for (int i = 0; i < userIds.size(); i++) {
                    users.append(i == 0 ? "" : ",").append(q(userIds.get(i)));
                }
                users.append("]");
                exec(con, "INSERT INTO " + credentials
                        + " (id, app_id, rp_id, user_id, counter, public_key, transports, created_at, updated_at) "
                        + "SELECT 'cred' || i, " + q(appId) + ", 'rp' || (i % 5000) || '.example.com', "
                        + "(" + users + ")[1 + (i % " + NUM_USERS + ")], 0, '\\x00'::bytea, '[]', 0, 0 "
                        + "FROM generate_series(0, " + (TOTAL_CREDENTIALS - 1) + ") AS i");
                exec(con, "VACUUM ANALYZE " + credentials);

                String lookup = "SELECT * FROM " + credentials + " WHERE app_id = " + q(appId)
                        + " AND id = " + q("cred12345");

                JsonObject plan = explain(con, lookup);
                assertTrue("credential lookup by id must use " + INDEX + ", plan was:\n" + plan,
                        usesIndexNamed(plan, INDEX));
                assertFalse("credential lookup by id must not sequentially scan", containsSeqScan(plan));

                // teeth: without the index the lookup does not have an index condition on id
                exec(con, "DROP INDEX " + INDEX);
                JsonObject planWithoutIndex = explain(con, lookup);
                assertFalse(usesIndexNamed(planWithoutIndex, INDEX));
                // (PostgreSQL 18 may skip-scan the primary key, so compare costs rather than node types)
                assertTrue("without " + INDEX + " the lookup is expected to be more expensive; with: " + plan
                                + "\nwithout: " + planWithoutIndex,
                        planWithoutIndex.get("Total Cost").getAsDouble() > 10 * plan.get("Total Cost").getAsDouble());

                exec(con, "CREATE INDEX IF NOT EXISTS " + INDEX + " ON " + credentials + " (app_id, id)");
            }
        } finally {
            stop(process);
        }
    }

    // ---------------------------------------------------------------------------------------------

    private String indexColumns(Connection con, String table) throws Exception {
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("SELECT indexdef FROM pg_indexes WHERE indexname = " + q(INDEX)
                     + " AND tablename = " + q(table.contains(".") ? table.substring(table.indexOf('.') + 1)
                     : table))) {
            if (!rs.next()) {
                return null;
            }
            String def = rs.getString(1);
            return def.substring(def.lastIndexOf('('));
        }
    }

    private JsonObject explain(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("EXPLAIN (FORMAT JSON) " + sql)) {
            assertTrue("EXPLAIN returned no rows", rs.next());
            JsonArray arr = new JsonParser().parse(rs.getString(1)).getAsJsonArray();
            return arr.get(0).getAsJsonObject().getAsJsonObject("Plan");
        }
    }

    private boolean usesIndexNamed(JsonObject node, String indexName) {
        if (node.has("Index Name") && node.get("Index Name").getAsString().equals(indexName)) {
            return true;
        }
        for (JsonObject child : children(node)) {
            if (usesIndexNamed(child, indexName)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsSeqScan(JsonObject node) {
        if (node.get("Node Type").getAsString().equals("Seq Scan")) {
            return true;
        }
        for (JsonObject child : children(node)) {
            if (containsSeqScan(child)) {
                return true;
            }
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

    private void exec(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement()) {
            st.execute(sql);
        }
    }

    private static String q(String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}
