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

import com.google.gson.JsonObject;
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.session.SessionStorage;
import io.supertokens.session.Session;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SessionExistenceProbeTest {

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

    /**
     * isUserIdBeingUsedInNonAuthRecipe uses a SELECT 1 ... LIMIT 1 existence probe for sessions
     * instead of loading every session handle. This pins its semantics: only non-expired sessions
     * count as "the user id is in use".
     */
    @Test
    public void sessionProbeCountsOnlyNonExpiredSessions() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        AppIdentifier appIdentifier = new AppIdentifier(null, null);
        String userId = "session-probe-test-user";

        // no sessions at all -> not in use
        assertFalse(storage.isUserIdBeingUsedInNonAuthRecipe(appIdentifier, SessionStorage.class.getName(), userId));

        Session.createNewSession(process.getProcess(), userId, new JsonObject(), new JsonObject());

        // live session -> in use
        assertTrue(storage.isUserIdBeingUsedInNonAuthRecipe(appIdentifier, SessionStorage.class.getName(), userId));

        // expire the session behind the API's back (updateSessionInfo_Transaction and friends
        // always stamp future expiries, so raw SQL is the only way to backdate expires_at)
        expireAllSessionsOfUser(storage, userId);

        // only expired sessions left -> not in use
        assertFalse(storage.isUserIdBeingUsedInNonAuthRecipe(appIdentifier, SessionStorage.class.getName(), userId));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private void expireAllSessionsOfUser(Start storage, String userId) throws Exception {
        String query = "UPDATE " + Config.getConfig(storage).getSessionInfoTable()
                + " SET expires_at = ? WHERE user_id = ?";
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setLong(1, System.currentTimeMillis() - 1000);
                pst.setString(2, userId);
                pst.executeUpdate();
            }
            return null;
        });
    }
}
