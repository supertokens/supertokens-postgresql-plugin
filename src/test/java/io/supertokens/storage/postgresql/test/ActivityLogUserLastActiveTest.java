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

import io.supertokens.ActiveUsers;
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ActivityLogUserLastActiveTest {

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
     * Every write of user_last_active must also write a `user_last_active` row into the activity_log
     * audit table. This exercises the core ActiveUsers.updateLastActive -> AuditLog.emit ->
     * ActivityLogStorage path end-to-end against PostgreSQL.
     */
    @Test
    public void updateLastActiveMirrorsIntoActivityLog() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        String table = Config.getConfig(storage).getActivityLogTable();
        String userId = "user-last-active-mirror-test";

        // No audit row for this user before the update.
        assertEquals(0, countUserLastActiveEvents(storage, table, userId));

        ActiveUsers.updateLastActive(new AppIdentifier(null, null), process.getProcess(), userId);

        // The user_last_active write mirrored a matching audit row.
        assertEquals(1, countUserLastActiveEvents(storage, table, userId));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private int countUserLastActiveEvents(Start storage, String table, String userId) throws Exception {
        String query = "SELECT COUNT(*) FROM " + table
                + " WHERE event_type = 'user_last_active' AND status = 'success'"
                + " AND primary_or_recipe_user_id = ?";
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setString(1, userId);
                try (ResultSet rs = pst.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        });
    }
}
