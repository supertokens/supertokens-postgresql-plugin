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

import io.supertokens.ProcessState;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.oauth.OAuthClient;
import io.supertokens.pluginInterface.oauth.exception.OAuthClientNotFoundException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.junit.Assert.*;

/**
 * {@code getOAuthClientById_Transaction} must read on the caller's transaction connection. The client is
 * written inside an open transaction and read back before commit: only a read on that same connection can
 * see the uncommitted row, so a regression that borrows another pooled connection fails here instead of
 * silently reading committed data.
 */
public class OAuthClientTransactionReadTest {

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

    private static final String CLIENT_ID = "tx-read-client";
    private static final String CLIENT_SECRET = "tx-read-secret";

    @Test
    public void readsUncommittedClientOnCallersConnectionAndThrowsWhenMissing() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.OAUTH});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            return;
        }

        Start start = (Start) StorageLayer.getStorage(process.getProcess());
        AppIdentifier app = new AppIdentifier(null, null);
        String insert = "INSERT INTO " + Config.getConfig(start).getOAuthClientsTable()
                + "(app_id, client_id, client_secret, is_client_credentials_only, enable_refresh_token_rotation)"
                + " VALUES(?, ?, ?, ?, ?)";

        OAuthClient[] readInTx = {null};
        try {
            start.startTransaction(con -> {
                try {
                    Connection sqlCon = (Connection) con.getConnection();
                    try (PreparedStatement pst = sqlCon.prepareStatement(insert)) {
                        pst.setString(1, app.getAppId());
                        pst.setString(2, CLIENT_ID);
                        pst.setString(3, CLIENT_SECRET);
                        pst.setBoolean(4, false);
                        pst.setBoolean(5, true);
                        pst.executeUpdate();
                    }
                    readInTx[0] = start.getOAuthClientById_Transaction(app, con, CLIENT_ID);
                } catch (Exception e) {
                    throw new StorageTransactionLogicException(e);
                }
                // Roll back, so the row was only ever visible on this transaction's connection.
                throw new StorageTransactionLogicException(new Exception("rollback"));
            });
            fail("the transaction should have rolled back");
        } catch (StorageTransactionLogicException e) {
            assertEquals("rollback", e.actualException.getMessage());
        }

        assertNotNull("the uncommitted client must be visible on the caller's connection", readInTx[0]);
        assertEquals(CLIENT_ID, readInTx[0].clientId);
        assertEquals(CLIENT_SECRET, readInTx[0].clientSecret);
        assertFalse(readInTx[0].isClientCredentialsOnly);
        assertTrue(readInTx[0].enableRefreshTokenRotation);

        // The write was rolled back, so the client doesn't exist outside the transaction.
        try {
            start.getOAuthClientById(app, CLIENT_ID);
            fail("the rolled-back client must not exist");
        } catch (OAuthClientNotFoundException expected) {
        }

        // A missing client throws, like the non-transaction form.
        try {
            start.startTransaction(con -> {
                try {
                    return start.getOAuthClientById_Transaction(app, con, "no-such-client");
                } catch (OAuthClientNotFoundException e) {
                    throw new StorageTransactionLogicException(e);
                }
            });
            fail("a missing client must throw OAuthClientNotFoundException");
        } catch (StorageTransactionLogicException e) {
            assertTrue(e.actualException instanceof OAuthClientNotFoundException);
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
