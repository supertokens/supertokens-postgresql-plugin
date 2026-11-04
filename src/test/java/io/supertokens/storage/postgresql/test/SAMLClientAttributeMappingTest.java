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
 *
 */

package io.supertokens.storage.postgresql.test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.saml.SAMLClient;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.queries.GeneralQueries;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * saml_clients.attribute_mapping holds the optional SAMLClient.attributeMapping as JSON object text (NULL = not
 * configured). Covers the round trip through the write and all three client reads, overwriting and clearing it
 * through the upsert, and the startup backfill adding the column to an existing table.
 */
public class SAMLClientAttributeMappingTest {

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

    @Test
    public void attributeMappingRoundTripsThroughWriteAndAllReads() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        TenantIdentifier tenant = new TenantIdentifier(null, null, null);

        JsonObject mapping = json("{\"userId\": [\"id\", \"NameID\"], \"email\": [\"email\"]}");

        SAMLClient written = storage.createOrUpdateSAMLClient(tenant, client("mapped", "https://idp.example.com/mapped",
                mapping));
        assertEquals(mapping, written.attributeMapping);
        storage.createOrUpdateSAMLClient(tenant, client("unmapped", "https://idp.example.com/unmapped", null));

        // An object comes back equal from every read path.
        assertEquals(mapping, storage.getSAMLClient(tenant, "mapped").attributeMapping);
        assertEquals(mapping,
                storage.getSAMLClientByIDPEntityId(tenant, "https://idp.example.com/mapped").attributeMapping);
        // null stays null, on every read path, and is stored as SQL NULL.
        assertNull(storage.getSAMLClient(tenant, "unmapped").attributeMapping);
        assertNull(storage.getSAMLClientByIDPEntityId(tenant, "https://idp.example.com/unmapped").attributeMapping);
        assertTrue(storedMappingIsNull(storage, "unmapped"));

        List<SAMLClient> clients = storage.getSAMLClients(tenant);
        assertEquals(2, clients.size());
        for (SAMLClient c : clients) {
            if (c.clientId.equals("mapped")) {
                assertEquals(mapping, c.attributeMapping);
            } else {
                assertNull(c.attributeMapping);
            }
        }

        // The ON CONFLICT update replaces the stored mapping with the value given ...
        JsonObject replacement = json("{\"firstName\": [\"givenName\"]}");
        storage.createOrUpdateSAMLClient(tenant, client("mapped", "https://idp.example.com/mapped", replacement));
        assertEquals(replacement, storage.getSAMLClient(tenant, "mapped").attributeMapping);

        // ... including null, which clears it.
        storage.createOrUpdateSAMLClient(tenant, client("mapped", "https://idp.example.com/mapped", null));
        assertNull(storage.getSAMLClient(tenant, "mapped").attributeMapping);
        assertTrue(storedMappingIsNull(storage, "mapped"));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * A database created before the column existed gets it at startup, and existing rows read back with no
     * mapping.
     */
    @Test
    public void startupBackfillAddsColumnToExistingTable() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        TenantIdentifier tenant = new TenantIdentifier(null, null, null);
        String table = Config.getConfig(storage).getSAMLClientsTable();

        storage.createOrUpdateSAMLClient(tenant, client("existing", "https://idp.example.com/existing", null));

        Connection con = ConnectionPool.getConnection(storage);
        try (Statement st = con.createStatement()) {
            con.setAutoCommit(true);
            st.execute("ALTER TABLE " + table + " DROP COLUMN attribute_mapping");
        } finally {
            con.close();
        }
        assertFalse(columnExists(storage, table, "attribute_mapping"));

        Connection migrateCon = ConnectionPool.getConnection(storage);
        try {
            GeneralQueries.createTablesIfNotExists(storage, migrateCon);
        } finally {
            migrateCon.close();
        }

        assertTrue(columnExists(storage, table, "attribute_mapping"));
        assertNull(storage.getSAMLClient(tenant, "existing").attributeMapping);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private static SAMLClient client(String clientId, String idpEntityId, JsonObject attributeMapping) {
        JsonArray redirectURIs = new JsonParser().parse("[\"https://app.example.com/callback\"]").getAsJsonArray();
        return new SAMLClient(clientId, "secret", "https://idp.example.com/sso", redirectURIs,
                "https://app.example.com/callback", idpEntityId, "cert", false, false, null, attributeMapping);
    }

    private static JsonObject json(String text) {
        return new JsonParser().parse(text).getAsJsonObject();
    }

    private static boolean storedMappingIsNull(Start storage, String clientId) throws Exception {
        Connection con = ConnectionPool.getConnection(storage);
        try (PreparedStatement pst = con.prepareStatement("SELECT attribute_mapping IS NULL AS is_null FROM "
                + Config.getConfig(storage).getSAMLClientsTable() + " WHERE client_id = ?")) {
            pst.setString(1, clientId);
            try (ResultSet rs = pst.executeQuery()) {
                assertTrue(rs.next());
                return rs.getBoolean("is_null");
            }
        } finally {
            con.close();
        }
    }

    private static boolean columnExists(Start storage, String table, String column) throws Exception {
        String schema = Config.getConfig(storage).getTableSchema();
        String bareTable = table.contains(".") ? table.substring(table.lastIndexOf('.') + 1) : table;
        Connection con = ConnectionPool.getConnection(storage);
        try (PreparedStatement pst = con.prepareStatement("SELECT 1 FROM information_schema.columns"
                + " WHERE table_schema = ? AND table_name = ? AND column_name = ?")) {
            pst.setString(1, schema);
            pst.setString(2, bareTable);
            pst.setString(3, column);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next();
            }
        } finally {
            con.close();
        }
    }
}
