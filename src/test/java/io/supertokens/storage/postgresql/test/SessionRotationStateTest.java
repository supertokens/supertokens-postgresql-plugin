/*
 *    Copyright (c) 2024, VRAI Labs and/or its affiliates. All rights reserved.
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

import com.google.gson.JsonObject;
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.session.SessionInfo;
import io.supertokens.pluginInterface.session.sqlStorage.SessionSQLStorage;
import io.supertokens.session.Session;
import io.supertokens.session.info.SessionInformationHolder;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Verifies the plugin-interface 9.0 refresh-token rotation state round-trips through the
 * {@code prev_refresh_token_hash_2} and {@code refresh_token_rotated_at} columns added to
 * {@code session_info}.
 */
public class SessionRotationStateTest {
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
    public void testRotationStateRoundTrip() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonObject userDataInJWT = new JsonObject();
        userDataInJWT.addProperty("key", "value");
        JsonObject userDataInDatabase = new JsonObject();
        userDataInDatabase.addProperty("key", "value");

        SessionInformationHolder sessionInfo = Session.createNewSession(process.getProcess(), "userId", userDataInJWT,
                userDataInDatabase);
        String sessionHandle = sessionInfo.session.handle;

        SessionSQLStorage storage = (SessionSQLStorage) StorageLayer.getStorage(process.getProcess());
        TenantIdentifier tenant = new TenantIdentifier(null, null, null);

        // A freshly created session has no rotation recorded: both columns are NULL.
        SessionInfo before = storage.getSession(tenant, sessionHandle);
        assertNotNull(before);
        assertNull(before.prevRefreshTokenHash2);
        assertNull(before.refreshTokenRotatedAt);

        // Record rotation state via the extended updateSessionInfo_Transaction.
        String newHash = "newRefreshTokenHash";
        String prevHash = "prevRefreshTokenHash";
        long rotatedAt = System.currentTimeMillis();
        long newExpiry = before.expiry + 1000;
        boolean useStaticKey = before.useStaticKey;
        storage.startTransaction(con -> {
            storage.updateSessionInfo_Transaction(tenant, con, sessionHandle, newHash, prevHash, rotatedAt, newExpiry,
                    useStaticKey);
            storage.commitTransaction(con);
            return null;
        });

        // getSession (non-transactional read) sees the rotation state.
        SessionInfo after = storage.getSession(tenant, sessionHandle);
        assertEquals(newHash, after.refreshTokenHash2);
        assertEquals(prevHash, after.prevRefreshTokenHash2);
        assertEquals(Long.valueOf(rotatedAt), after.refreshTokenRotatedAt);
        assertEquals(newExpiry, after.expiry);

        // getSessionInfo_Transaction (the transactional read core uses on refresh) sees it too.
        SessionInfo[] fromTxn = new SessionInfo[1];
        storage.startTransaction(con -> {
            fromTxn[0] = storage.getSessionInfo_Transaction(tenant, con, sessionHandle);
            storage.commitTransaction(con);
            return null;
        });
        assertEquals(prevHash, fromTxn[0].prevRefreshTokenHash2);
        assertEquals(Long.valueOf(rotatedAt), fromTxn[0].refreshTokenRotatedAt);

        // Rotation state can be cleared back to NULL (null round-trips, not coerced to 0/"").
        storage.startTransaction(con -> {
            storage.updateSessionInfo_Transaction(tenant, con, sessionHandle, newHash, null, null, newExpiry,
                    useStaticKey);
            storage.commitTransaction(con);
            return null;
        });
        SessionInfo cleared = storage.getSession(tenant, sessionHandle);
        assertNull(cleared.prevRefreshTokenHash2);
        assertNull(cleared.refreshTokenRotatedAt);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
