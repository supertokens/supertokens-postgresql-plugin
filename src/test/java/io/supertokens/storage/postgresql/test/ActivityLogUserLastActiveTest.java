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
import io.supertokens.pluginInterface.auditlog.ActivityEventType;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
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
     * A call to core's ActiveUsers.updateLastActive must append the concrete activity event to the
     * activity_log audit table (the retired synthetic `user_last_active` event was replaced by the actual
     * interaction — a sign-in, refresh, session-create, ...). This exercises the core
     * ActiveUsers.updateLastActive -> AuditLog.emit -> ActivityLogStorage path end-to-end against PostgreSQL.
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
        ActivityEventType eventType = ActivityEventType.SIGN_IN;

        // No audit row for this user before the update.
        assertEquals(0, countActivityEvents(storage, table, userId, eventType));

        ActiveUsers.updateLastActive(new TenantIdentifier(null, null, null), process.getProcess(), userId,
                eventType);

        // The update appended a matching activity audit row.
        assertEquals(1, countActivityEvents(storage, table, userId, eventType));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private int countActivityEvents(Start storage, String table, String userId, ActivityEventType eventType)
            throws Exception {
        String query = "SELECT COUNT(*) FROM " + table
                + " WHERE event_type = ? AND status = 'success'"
                + " AND primary_or_recipe_user_id = ?";
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setString(1, eventType.getValue());
                pst.setString(2, userId);
                try (ResultSet rs = pst.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        });
    }
}
