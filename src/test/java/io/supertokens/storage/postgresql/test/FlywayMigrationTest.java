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
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.HashSet;
import java.util.Set;

import static io.supertokens.storage.postgresql.queries.GeneralQueries.getAllMigratedScripts;
import static org.junit.Assert.*;

public class FlywayMigrationTest {

    private static final String BASE = "io.supertokens.storage.postgresql.migrations.";

    private static final String BASELINE_SCRIPT = "<< Flyway Baseline >>";
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

        Set<String> allMigrationScripts = allMigrationScripts();
        Set<String> scripts = getAllMigratedScripts((Start) StorageLayer.getStorage(process.getProcess()));
        assertTrue(allMigrationScripts.equals(scripts));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testWhenV1MigrationFails() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_key_value_table_name", "^#1@");
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Set<String> allMigrationScripts = allMigrationScripts();
        Set<String> scripts = getAllMigratedScripts((Start) StorageLayer.getStorage(process.getProcess()));
        assertTrue(allMigrationScripts.equals(scripts));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    public Set<String> allMigrationScripts() {
        Set<String> set = new HashSet<>();
        set.add(BASELINE_SCRIPT);
        set.add(V1__init_database);
        set.add(V2__plugin_version_3_0_0);
        set.add(V3__plugin_version_4_0_0);
        set.add(V4__plugin_version_5_0_0);
        set.add(V5__plugin_version_5_0_4);
        set.add(V6__core_version_7_0_12);
        return set;
    }
}
