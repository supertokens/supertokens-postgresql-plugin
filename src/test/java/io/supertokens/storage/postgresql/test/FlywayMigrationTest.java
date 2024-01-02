/*
 *    Copyright (c) 2023, VRAI Labs and/or its affiliates. All rights reserved.
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
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.storage.postgresql.FlywayMigration;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static io.supertokens.storage.postgresql.queries.GeneralQueries.doesTableExists;
import static io.supertokens.storage.postgresql.queries.GeneralQueries.getAllMigratedScripts;
import static org.junit.Assert.*;

public class FlywayMigrationTest {

    private static final String BASE = "io.supertokens.storage.postgresql.migrations";
    private static final String V1__init_database = BASE + "V1__init_database";
    private static final String V2__plugin_version_3_0_0 = BASE + "V2__plugin_version_3_0_0";
    private static final String V3__plugin_version_4_0_0 = BASE + "V3__plugin_version_4_0_0";
    private static final String V4__plugin_version_5_0_0 = BASE + "V4__plugin_version_5_0_0";
    private static final String V5__plugin_version_5_0_4 = BASE + "V5__plugin_version_5_0_4";
    private static final String V6__core_version_7_0_12 = BASE + "V6__core_version_7_0_12";


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
    public void testThatDefaultConfigLoadsCorrectly() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        checkDatabaseStatus((Start) StorageLayer.getStorage(process.getProcess()), true);
        Set<String> expectedScripts = getExpectedScripts();
        Set<String> scripts = getAllMigratedScripts((Start) StorageLayer.getStorage(process.getProcess()));
        scripts.remove("<< Flyway Baseline >>");
        assertTrue(expectedScripts.equals(scripts));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    public Set<String> getExpectedScripts() {
        Set<String> set = new HashSet<>();
        set.add(V1__init_database);
        set.add(V2__plugin_version_3_0_0);
        set.add(V3__plugin_version_4_0_0);
        set.add(V4__plugin_version_5_0_0);
        set.add(V5__plugin_version_5_0_4);
        set.add(V6__core_version_7_0_12);
        return set;
    }

    public void checkDatabaseStatus(Start start, boolean tableCreated) {
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getAppsTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getTenantsTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getKeyValueTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getAppIdToUserIdTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getUsersTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getUserLastActiveTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getAccessTokenSigningKeysTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getSessionInfoTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getTotpUsedCodesTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getTenantConfigsTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getTenantThirdPartyProvidersTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getTenantThirdPartyProviderClientsTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getEmailPasswordUsersTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getEmailPasswordUserToTenantTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getPasswordResetTokensTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getEmailVerificationTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getEmailVerificationTokensTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getThirdPartyUsersTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getThirdPartyUserToTenantTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getJWTSigningKeysTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getPasswordlessUsersTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getPasswordlessUserToTenantTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getPasswordlessDevicesTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getPasswordlessCodesTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getUserMetadataTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getRolesTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getUserRolesPermissionsTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getUserRolesTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getUserIdMappingTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getDashboardUsersTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getDashboardSessionsTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getTotpUsersTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getTotpUserDevicesTable()));
        assertEquals(tableCreated, doesTableExists(start, Config.getConfig(start).getTotpUsedCodesTable()));
    }
}
