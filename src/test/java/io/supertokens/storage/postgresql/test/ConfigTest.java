/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
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

import io.supertokens.ProcessState;
import io.supertokens.storage.postgresql.ConnectionPoolTestContent;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storageLayer.StorageLayer;
import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConfigTest {

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

        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorageLayer(process.getProcess()));

        checkConfig(config);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

    }

    @Test
    public void testThatCustomConfigLoadsCorrectly() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_connection_pool_size", "5");
        Utils.setValueInConfig("postgresql_past_tokens_table_name", "\"temp_name\"");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorageLayer(process.getProcess()));
        assertEquals(config.getConnectionPoolSize(), 5);
        assertEquals(config.getPastTokensTable(), "temp_name");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testThatInvalidConfigThrowsRightError() throws Exception {
        String[] args = {"../"};

        //'postgresql_user is not set properly in the config file

        Utils.commentConfigValue("postgresql_user");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);

        ProcessState.EventAndException e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE);
        assertNotNull(e);
        System.out.println(e.exception.getMessage());
        TestCase.assertEquals(e.exception.getMessage(),
                "'postgresql_user' is not set in the config.yaml file. Please set this value and restart SuperTokens");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        Utils.reset();


        //'postgresql_password is not set properly in the config file

        Utils.commentConfigValue("postgresql_password");
        process = TestingProcessManager.start(args);

        e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE);
        assertNotNull(e);
        TestCase.assertEquals(e.exception.getMessage(),
                "'postgresql_password' is not set in the config.yaml file. Please set this value and restart " +
                        "SuperTokens");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));


        Utils.reset();


        //postgresql_connection_pool_size is not set properly in the config file

        Utils.setValueInConfig("postgresql_connection_pool_size", "-1");
        process = TestingProcessManager.start(args);

        e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE);
        assertNotNull(e);
        TestCase.assertEquals(e.exception.getMessage(),
                "'postgresql_connection_pool_size' in the config.yaml file must be > 0");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));


    }

    @Test
    public void testThatMissingConfigFileThrowsError() throws Exception {
        String[] args = {"../"};

        ProcessBuilder pb = new ProcessBuilder("rm", "-r", "config.yaml");
        pb.directory(new File(args[0]));
        Process process1 = pb.start();
        process1.waitFor();

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);

        ProcessState.EventAndException e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE);
        assertNotNull(e);
        TestCase.assertEquals(e.exception.getMessage(),
                "java.io.FileNotFoundException: ../config.yaml (No such file or directory)");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));


    }

    @Test
    public void testCustomLocationForConfigLoadsCorrectly() throws Exception {
        String[] args = {"../", "configFile=../temp/config.yaml"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        ProcessState.EventAndException e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE);
        assertNotNull(e);
        TestCase.assertEquals(e.exception.getMessage(), "configPath option must be an absolute path only");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        //absolute path
        File f = new File("../temp/config.yaml");
        args = new String[]{"../", "configFile=" + f.getAbsolutePath()};

        process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorageLayer(process.getProcess()));
        checkConfig(config);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testBadPortInput() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_port", "8989");
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        process.getProcess().waitToInitStorageModule();
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.WAITING_TO_INIT_STORAGE_MODULE));

        ConnectionPoolTestContent.getInstance((Start) StorageLayer.getStorageLayer(process.getProcess()))
                .setKeyValue(ConnectionPoolTestContent.TIME_TO_WAIT_TO_INIT, 5000);
        ConnectionPoolTestContent.getInstance((Start) StorageLayer.getStorageLayer(process.getProcess()))
                .setKeyValue(ConnectionPoolTestContent.RETRY_INTERVAL_IF_INIT_FAILS, 2000);
        process.getProcess().proceedWithInitingStorageModule();

        ProcessState.EventAndException e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE, 7000);
        assertNotNull(e);
        assertEquals(e.exception.getMessage(),
                "Error connecting to PostgreSQL instance. Please make sure that PostgreSQL is running and that you " +
                        "have specified the correct values for 'postgresql_host' and 'postgresql_port' in your config" +
                        " file");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void storageDisabledAndThenEnabled() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        process.getProcess().waitToInitStorageModule();
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.WAITING_TO_INIT_STORAGE_MODULE));

        StorageLayer.getStorageLayer(process.getProcess()).setStorageLayerEnabled(false);
        ConnectionPoolTestContent.getInstance((Start) StorageLayer.getStorageLayer(process.getProcess()))
                .setKeyValue(ConnectionPoolTestContent.TIME_TO_WAIT_TO_INIT, 10000);
        ConnectionPoolTestContent.getInstance((Start) StorageLayer.getStorageLayer(process.getProcess()))
                .setKeyValue(ConnectionPoolTestContent.RETRY_INTERVAL_IF_INIT_FAILS, 2000);
        process.getProcess().proceedWithInitingStorageModule();

        Thread.sleep(5000);
        StorageLayer.getStorageLayer(process.getProcess()).setStorageLayerEnabled(true);

        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testBadHostInput() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_host", "random");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        ProcessState.EventAndException e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE);
        assertNotNull(e);

        assertEquals(
                "Failed to initialize pool: The connection attempt failed.",
                e.exception.getMessage());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

    }

    @Test
    public void testThatChangeInTableNameIsCorrect() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_key_value_table_name", "key_value_table");
        Utils.setValueInConfig("postgresql_session_info_table_name", "session_info_table");
        Utils.setValueInConfig("postgresql_past_tokens_table_name", "past_tokens_table");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorageLayer(process.getProcess()));

        assertEquals("change in PastTokensTable name not reflected", config.getPastTokensTable(), "past_tokens_table");
        assertEquals("change in KeyValueTable name not reflected", config.getKeyValueTable(), "key_value_table");
        assertEquals("change in SessionInfoTable name not reflected", config.getSessionInfoTable(),
                "session_info_table");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    public static void checkConfig(PostgreSQLConfig config) {

        assertEquals("Config connectionPoolSize did not match default", config.getConnectionPoolSize(), 10);
        assertEquals("Config databaseName does not match default", config.getDatabaseName(), "auth_session");
        assertEquals("Config keyValue table does not match default", config.getKeyValueTable(), "key_value");
        assertEquals("Config hostName does not match default ", config.getHostName(), "localhost");
        assertEquals("Config port does not match default", config.getPort(), 5432);
        assertEquals("Config pastTokensTable does not match default", config.getPastTokensTable(), "past_tokens");
        assertEquals("Config sessionInfoTable does not match default", config.getSessionInfoTable(), "session_info");
        assertEquals("Config user does not match default", config.getUser(), "root");
        assertEquals("Config password does not match default", config.getPassword(), "root");
    }

}
