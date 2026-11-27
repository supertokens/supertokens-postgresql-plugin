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
import io.supertokens.pluginInterface.oauth.exception.OAuthClientNotFoundException;
import io.supertokens.storage.postgresql.ConnectionPool;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * {@code createOrUpdateOAuthSession_Transaction} and {@code addOAuthM2MTokenForStats_Transaction} must write
 * the same rows as their auto-commit forms, but on the caller's transaction connection: the write is visible
 * on that connection before commit and disappears with a rollback.
 */
public class OAuthTransactionWriteTwinsTest {

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

    private static final String CLIENT_ID = "tx-write-client";
    private static final long HOUR = 3600L;

    private TestingProcessManager.TestingProcess startProcess() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.OAUTH});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        return process;
    }

    private static List<Object> sessionRow(Start start, String gid) throws Exception {
        String query = "SELECT client_id, app_id, external_refresh_token, internal_refresh_token, session_handle, jti,"
                + " exp FROM " + Config.getConfig(start).getOAuthSessionsTable() + " WHERE gid = ?";
        try (Connection con = ConnectionPool.getConnection(start);
             PreparedStatement pst = con.prepareStatement(query)) {
            pst.setString(1, gid);
            try (ResultSet rs = pst.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return Arrays.asList(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getLong(7));
            }
        }
    }

    @Test
    public void createOrUpdateOAuthSessionTransactionWritesSameRowOnCallersConnection() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
                return;
            }
            Start start = (Start) StorageLayer.getStorage(process.getProcess());
            AppIdentifier app = new AppIdentifier(null, null);
            start.addOrUpdateOauthClient(app, CLIENT_ID, "secret", false, true);
            long exp = System.currentTimeMillis() / 1000 + HOUR;

            // Inside a transaction that rolls back: visible on the caller's connection, gone afterwards.
            boolean[] revokedInTx = {true};
            try {
                start.startTransaction(con -> {
                    try {
                        start.createOrUpdateOAuthSession_Transaction(app, con, "gid-rolled-back", CLIENT_ID,
                                "ext-rb", "int-rb", "sh-rb", "jti-rb", exp);
                        revokedInTx[0] = start.isOAuthTokenRevokedByGID_Transaction(app, con, "gid-rolled-back");
                    } catch (Exception e) {
                        throw new StorageTransactionLogicException(e);
                    }
                    throw new StorageTransactionLogicException(new Exception("rollback"));
                });
                fail("the transaction should have rolled back");
            } catch (StorageTransactionLogicException e) {
                assertEquals("rollback", e.actualException.getMessage());
            }
            assertFalse("the uncommitted session must be visible on the caller's connection", revokedInTx[0]);
            assertNull("the rolled-back session must not exist", sessionRow(start, "gid-rolled-back"));

            // Parity: insert, then update (jti appended), through each form; the rows match. Refresh tokens are
            // unique, so each form uses its own (prefixed) token values.
            start.createOrUpdateOAuthSession(app, "gid-ac", CLIENT_ID, "ac-ext-1", "ac-int-1", "sh-1", "jti-1", exp);
            start.createOrUpdateOAuthSession(app, "gid-ac", CLIENT_ID, "ac-ext-2", "ac-int-2", "sh-2", "jti-2",
                    exp + 1);
            start.startTransaction(con -> {
                try {
                    start.createOrUpdateOAuthSession_Transaction(app, con, "gid-tx", CLIENT_ID, "tx-ext-1",
                            "tx-int-1", "sh-1", "jti-1", exp);
                    start.createOrUpdateOAuthSession_Transaction(app, con, "gid-tx", CLIENT_ID, "tx-ext-2",
                            "tx-int-2", "sh-2", "jti-2", exp + 1);
                } catch (OAuthClientNotFoundException e) {
                    throw new StorageTransactionLogicException(e);
                }
                start.commitTransaction(con);
                return null;
            });
            for (String form : new String[]{"ac", "tx"}) {
                assertEquals(Arrays.asList(CLIENT_ID, app.getAppId(), form + "-ext-2", form + "-int-2", "sh-2",
                        "jti-1,jti-2,", exp + 1), sessionRow(start, "gid-" + form));
            }

            // An unknown client throws, like the auto-commit form.
            try {
                start.startTransaction(con -> {
                    try {
                        start.createOrUpdateOAuthSession_Transaction(app, con, "gid-no-client", "no-such-client",
                                "ext", "int", "sh", "jti", exp);
                    } catch (OAuthClientNotFoundException e) {
                        throw new StorageTransactionLogicException(e);
                    }
                    return null;
                });
                fail("an unknown client must throw OAuthClientNotFoundException");
            } catch (StorageTransactionLogicException e) {
                assertTrue(e.actualException instanceof OAuthClientNotFoundException);
            }
            assertNull(sessionRow(start, "gid-no-client"));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void addOAuthM2MTokenForStatsTransactionCountsLikeAutoCommitOnCallersConnection() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
                return;
            }
            Start start = (Start) StorageLayer.getStorage(process.getProcess());
            AppIdentifier app = new AppIdentifier(null, null);
            long nowSec = System.currentTimeMillis() / 1000;
            long iat = nowSec;
            long exp = nowSec + 24 * HOUR;
            long since = (nowSec - 3 * HOUR) * 1000L;

            start.addOAuthM2MTokenForStats(app, CLIENT_ID, iat, exp);
            assertEquals(1, start.countTotalNumberOfOAuthM2MTokensCreatedSince(app, since));

            // Inside a transaction that rolls back: counted on the caller's connection, gone afterwards.
            int[] countedInTx = {-1};
            try {
                start.startTransaction(con -> {
                    try {
                        start.addOAuthM2MTokenForStats_Transaction(app, con, CLIENT_ID, iat, exp);
                    } catch (OAuthClientNotFoundException e) {
                        throw new StorageTransactionLogicException(e);
                    }
                    String query = "SELECT count FROM " + Config.getConfig(start).getOAuthM2MTokenStatsTable()
                            + " WHERE app_id = ? AND iat_bucket = ? AND exp_bucket = ?";
                    try (PreparedStatement pst = ((Connection) con.getConnection()).prepareStatement(query)) {
                        pst.setString(1, app.getAppId());
                        pst.setLong(2, iat / HOUR);
                        pst.setLong(3, exp / HOUR);
                        try (ResultSet rs = pst.executeQuery()) {
                            assertTrue(rs.next());
                            countedInTx[0] = rs.getInt(1);
                        }
                    } catch (SQLException e) {
                        throw new StorageTransactionLogicException(e);
                    }
                    throw new StorageTransactionLogicException(new Exception("rollback"));
                });
                fail("the transaction should have rolled back");
            } catch (StorageTransactionLogicException e) {
                assertEquals("rollback", e.actualException.getMessage());
            }
            assertEquals("the uncommitted increment must be visible on the caller's connection", 2, countedInTx[0]);
            assertEquals(1, start.countTotalNumberOfOAuthM2MTokensCreatedSince(app, since));

            // Committed: increments the same bucket as the auto-commit form.
            start.startTransaction(con -> {
                try {
                    start.addOAuthM2MTokenForStats_Transaction(app, con, CLIENT_ID, iat, exp);
                } catch (OAuthClientNotFoundException e) {
                    throw new StorageTransactionLogicException(e);
                }
                start.commitTransaction(con);
                return null;
            });
            assertEquals(2, start.countTotalNumberOfOAuthM2MTokensCreatedSince(app, since));
            assertEquals(2, start.countTotalNumberOfOAuthM2MTokensAlive(app));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
