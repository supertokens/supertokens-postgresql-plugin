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

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.multitenancy.MultitenancyHelper;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.EmailPasswordConfig;
import io.supertokens.pluginInterface.multitenancy.PasswordlessConfig;
import io.supertokens.pluginInterface.multitenancy.TenantConfig;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.ThirdPartyConfig;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.SchemaVerifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storage.postgresql.queries.GeneralQueries;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Startup schema verification ({@code Storage.verifySchema()} / {@link SchemaVerifier}): a database missing
 * columns that this plugin version needs is reported loudly at startup (ERROR log + SCHEMA_MISMATCH state with
 * the SQL to run), while the core keeps booting and serving everything else; only the queries touching the
 * missing schema fail, with a schema-mismatch hint instead of a raw SQL error (supertokens-core#1386).
 */
public class SchemaVerificationTest {

    private static final String TENANT_SCHEMA = "st_schema_check";
    private static final String DROP_COLUMNS = "ALTER TABLE %s.session_info DROP COLUMN prev_refresh_token_hash_2,"
            + " DROP COLUMN refresh_token_rotated_at";
    private static final String RESTORE_COLUMNS =
            "ALTER TABLE %s.session_info ADD COLUMN IF NOT EXISTS prev_refresh_token_hash_2 VARCHAR(128),"
                    + " ADD COLUMN IF NOT EXISTS refresh_token_rotated_at BIGINT";

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

    /** Raw SQL against the per-worker test database, independent of the core's pool (which may be gated). */
    private static void runSql(String sql) throws Exception {
        try (Connection con = DriverManager.getConnection(DatabaseTestHelper.getTestDatabaseUrl(),
                DatabaseTestHelper.getUser(), DatabaseTestHelper.getPassword());
             Statement st = con.createStatement()) {
            st.execute(sql);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 1. The DDL-derived expectation and a fresh database agree. This pins the parser AND the registry in
    //    GeneralQueries.getAllCreateTableQueries: (a) every table the config knows (PostgreSQLConfig.get*Table())
    //    is in the registry, (b) every registry table exists in the database with exactly the parsed columns.
    //    Tables left behind by other tests (other prefixes/schemas) are ignored on purpose.
    // ---------------------------------------------------------------------------------------------------------
    @Test
    public void expectedSchemaMatchesFreshDatabaseExactly() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        List<SchemaVerifier.ExpectedTable> expected =
                SchemaVerifier.parseExpectedSchema(GeneralQueries.getAllCreateTableQueries(storage));
        Map<String, Set<String>> actual;
        try (Connection con = ConnectionPool.getConnection(storage)) {
            actual = SchemaVerifier.fetchActualSchema(storage, con);
        }

        Map<String, Set<String>> expectedByTable = new HashMap<>();
        Set<String> registryQualifiedNames = new HashSet<>();
        for (SchemaVerifier.ExpectedTable t : expected) {
            assertNull("table listed twice in getAllCreateTableQueries: " + t.tableName,
                    expectedByTable.put(t.tableName, t.columns.keySet()));
            assertFalse("no columns parsed for " + t.tableName, t.columns.isEmpty());
            registryQualifiedNames.add(t.qualifiedName);
        }

        // (a) registry completeness against the config's table-name getters
        PostgreSQLConfig config = Config.getConfig(storage);
        Set<String> configTables = new TreeSet<>();
        for (Method m : PostgreSQLConfig.class.getMethods()) {
            if (m.getName().startsWith("get") && m.getName().endsWith("Table") && m.getParameterCount() == 0
                    && m.getReturnType() == String.class) {
                configTables.add((String) m.invoke(config));
            }
        }
        assertFalse(configTables.isEmpty());
        for (String table : configTables) {
            assertTrue("table known to PostgreSQLConfig but missing from getAllCreateTableQueries: " + table,
                    registryQualifiedNames.contains(table));
        }

        // (b) every registry table exists with exactly the parsed columns
        for (String table : new TreeSet<>(expectedByTable.keySet())) {
            assertNotNull("registry table missing from the database: " + table, actual.get(table));
            assertEquals("columns of " + table, new TreeSet<>(expectedByTable.get(table)),
                    new TreeSet<>(actual.get(table)));
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // ---------------------------------------------------------------------------------------------------------
    // 2. The #1386 scenario on the base database: columns missing -> the core still boots, reports the mismatch
    //    loudly (SCHEMA_MISMATCH state + ERROR log with the SQL to run), keeps serving unaffected queries, and
    //    the affected queries fail with a schema-mismatch hint instead of a raw "column does not exist".
    // ---------------------------------------------------------------------------------------------------------
    @Test
    public void missingColumnsAreReportedAtStartupAndOnlyAffectedQueriesFail() throws Exception {
        String[] args = {"../"};
        // First boot creates the tables (fresh schema), so we can then "downgrade" session_info.
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        process.kill(false); // keep the tables
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        runSql(String.format(DROP_COLUMNS, "public"));
        try {
            process = TestingProcessManager.start(args);
            // boots despite the mismatch
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
            ProcessState.EventAndException e =
                    process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.SCHEMA_MISMATCH);
            assertNotNull(e);

            String msg = e.exception.getMessage();
            assertTrue(msg, msg.contains("session_info"));
            assertTrue(msg, msg.contains("prev_refresh_token_hash_2"));
            assertTrue(msg, msg.contains("refresh_token_rotated_at"));
            assertTrue(msg, msg.contains("### Migration"));
            assertTrue(msg, msg.contains(
                    "ALTER TABLE session_info ADD COLUMN IF NOT EXISTS prev_refresh_token_hash_2 VARCHAR(128);"));
            assertTrue(msg, msg.contains(
                    "ALTER TABLE session_info ADD COLUMN IF NOT EXISTS refresh_token_rotated_at BIGINT;"));

            Start storage = (Start) StorageLayer.getStorage(process.getProcess());
            // unaffected queries keep working
            assertNull(storage.getKeyValue(TenantIdentifier.BASE_TENANT, "nope"));
            // affected queries fail with the hint, not a bare SQL error
            try {
                storage.getSession(TenantIdentifier.BASE_TENANT, "no-such-session");
                fail();
            } catch (StorageQueryException ex) {
                assertTrue(ex.getMessage(), ex.getMessage().contains("Schema mismatch"));
                assertTrue(ex.getMessage(), ex.getMessage().contains("Check the core error logs"));
            }

            // operator applies the migration; affected queries recover without a restart
            runSql(String.format(RESTORE_COLUMNS, "public"));
            assertNull(storage.getSession(TenantIdentifier.BASE_TENANT, "no-such-session"));

            // keep the tables so the finally-block restore (a no-op here) has a relation to run against
            process.kill(false);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        } finally {
            runSql(String.format(RESTORE_COLUMNS, "public"));
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 3. Parser unit test over the DDL shapes used in this plugin.
    // ---------------------------------------------------------------------------------------------------------
    @Test
    public void parserHandlesDdlShapes() {
        List<SchemaVerifier.ExpectedTable> tables = SchemaVerifier.parseExpectedSchema(List.of(
                "CREATE TABLE IF NOT EXISTS myschema.st_widgets ("
                        + "app_id VARCHAR(64) DEFAULT 'public',"
                        + "id BIGINT GENERATED ALWAYS AS IDENTITY,"
                        + "amount NUMERIC(10,2) NOT NULL,"
                        + "note TEXT DEFAULT 'a,b (c)',"
                        + "CONSTRAINT st_widgets_pkey PRIMARY KEY (app_id, id),"
                        + "CONSTRAINT st_widgets_app_id_fkey FOREIGN KEY(app_id) REFERENCES apps (app_id) ON DELETE CASCADE"
                        + ") PARTITION BY RANGE (id);",
                "CREATE TABLE IF NOT EXISTS  st_gadgets(" // double space, no space before '('
                        + " user_id CHAR(36) NOT NULL," // leading spaces (WebAuthNQueries style)
                        + " created_at BIGINT NOT NULL,"
                        + " PRIMARY KEY (user_id),"
                        + " UNIQUE (created_at)"
                        + " );",
                "CREATE TABLE IF NOT EXISTS st_widgets_default PARTITION OF myschema.st_widgets DEFAULT;"));

        assertEquals(2, tables.size());
        SchemaVerifier.ExpectedTable widgets = tables.get(0);
        assertEquals("myschema.st_widgets", widgets.qualifiedName);
        assertEquals("st_widgets", widgets.tableName);
        assertEquals(Set.of("app_id", "id", "amount", "note"), widgets.columns.keySet());
        assertEquals("VARCHAR(64) DEFAULT 'public'", widgets.columns.get("app_id"));
        assertEquals("NUMERIC(10,2) NOT NULL", widgets.columns.get("amount"));
        assertEquals("TEXT DEFAULT 'a,b (c)'", widgets.columns.get("note"));

        SchemaVerifier.ExpectedTable gadgets = tables.get(1);
        assertEquals("st_gadgets", gadgets.qualifiedName);
        assertEquals("st_gadgets", gadgets.tableName);
        assertEquals(Set.of("user_id", "created_at"), gadgets.columns.keySet());
    }

    // ---------------------------------------------------------------------------------------------------------
    // 4. A tenant database (different schema, same server) with missing columns: the core boots and reports the
    //    mismatch (SCHEMA_MISMATCH state names the tenant schema), the base tenant and the tenant's unaffected
    //    queries keep working, only the tenant's affected queries fail - and they recover once the columns are
    //    added.
    // ---------------------------------------------------------------------------------------------------------
    @Test
    public void tenantStorageMismatchIsReportedAndOnlyAffectedTenantQueriesFail() throws Exception {
        String[] args = {"../"};
        TenantIdentifier tid = new TenantIdentifier("abc", null, null);
        JsonObject tenantConfigJson = new JsonObject();
        tenantConfigJson.add("postgresql_connection_uri", new JsonPrimitive(
                "postgresql://" + DatabaseTestHelper.getUser() + ":" + DatabaseTestHelper.getPassword() + "@"
                        + DatabaseTestHelper.getHost() + ":" + DatabaseTestHelper.getPort() + "/"
                        + DatabaseTestHelper.getCurrentTestDatabase() + "?currentSchema=" + TENANT_SCHEMA));
        TenantConfig tenantConfig = new TenantConfig(tid, new EmailPasswordConfig(true),
                new ThirdPartyConfig(false, new ThirdPartyConfig.Provider[0]), new PasswordlessConfig(false),
                null, null, tenantConfigJson);

        try {
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
            FeatureFlagTestContent.getInstance(process.getProcess())
                    .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
            process.startProcess();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantIdentifier(null, null, null),
                    tenantConfig);
            // tenant storage is usable and its tables exist in TENANT_SCHEMA
            assertNull(StorageLayer.getStorage(tid, process.getProcess()).getKeyValue(tid, "nope"));
            assertNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.SCHEMA_MISMATCH, 500));
            process.kill(false);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

            // "downgrade" only the tenant's schema
            runSql(String.format(DROP_COLUMNS, TENANT_SCHEMA));

            process = TestingProcessManager.start(args, false);
            FeatureFlagTestContent.getInstance(process.getProcess())
                    .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
            process.startProcess();
            Main main = process.getProcess();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
            ProcessState.EventAndException mismatch =
                    process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.SCHEMA_MISMATCH);
            assertNotNull(mismatch);
            assertTrue(mismatch.exception.getMessage(),
                    mismatch.exception.getMessage().contains(TENANT_SCHEMA + ".session_info"));

            // base tenant unaffected, including session queries
            Start baseStorage = (Start) StorageLayer.getStorage(main);
            assertNull(baseStorage.getSession(TenantIdentifier.BASE_TENANT, "no-such-session"));
            // the broken tenant still serves everything that does not touch the missing columns
            Start tenantStorage = (Start) StorageLayer.getStorage(tid, main);
            assertNull(tenantStorage.getKeyValue(tid, "nope"));
            // only its affected queries fail, with the hint
            try {
                tenantStorage.getSession(tid, "no-such-session");
                fail();
            } catch (StorageQueryException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("Schema mismatch"));
                assertTrue(e.getMessage(), e.getMessage().contains("Check the core error logs"));
            }

            // operator applies the migration; the tenant recovers without a restart
            runSql(String.format(RESTORE_COLUMNS, TENANT_SCHEMA));
            assertNull(tenantStorage.getSession(tid, "no-such-session"));

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        } finally {
            runSql("DROP SCHEMA IF EXISTS " + TENANT_SCHEMA + " CASCADE");
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 5. Verification runs once per storage at startup - not on the base storage's second initStorage (it is
    //    closed and re-inited through the tenant path during boot) and not on later storage-layer reloads.
    // ---------------------------------------------------------------------------------------------------------
    @Test
    public void verificationRunsOncePerStorageAtStartup() throws Exception {
        String[] args = {"../"};
        int before = SchemaVerifier.verificationRunsForTesting.get();
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        assertEquals(1, SchemaVerifier.verificationRunsForTesting.get() - before);

        MultitenancyHelper.getInstance(process.getProcess()).loadStorageLayer();
        MultitenancyHelper.getInstance(process.getProcess()).loadStorageLayer();
        assertEquals(1, SchemaVerifier.verificationRunsForTesting.get() - before);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
