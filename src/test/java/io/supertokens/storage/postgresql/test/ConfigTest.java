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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.JsonObject;
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.session.SessionStorage;
import io.supertokens.session.Session;
import io.supertokens.session.info.SessionInformationHolder;
import io.supertokens.storage.postgresql.ConnectionPoolTestContent;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.annotations.ConnectionPoolProperty;
import io.supertokens.storage.postgresql.annotations.IgnoreForAnnotationCheck;
import io.supertokens.storage.postgresql.annotations.NotConflictingWithinUserPool;
import io.supertokens.storage.postgresql.annotations.UserPoolProperty;
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
import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.Assert.*;

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

        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));

        checkConfig(config);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

    }

    @Test
    public void testThatCustomConfigLoadsCorrectly() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_connection_pool_size", "5");
        Utils.setValueInConfig("postgresql_key_value_table_name", "\"temp_name\"");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));
        assertEquals(config.getConnectionPoolSize(), 5);
        assertEquals(config.getKeyValueTable(), "temp_name");

        process.getProcess().deleteAllInformationForTesting();

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void testThatInvalidConfigThrowsRightError() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_connection_pool_size", "-1");
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);

        ProcessState.EventAndException e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE);
        assertNotNull(e);
        TestCase.assertEquals(e.exception.getCause().getMessage(),
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
                "../config.yaml (No such file or directory)");

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

        // absolute path
        File f = new File("../temp/config.yaml");
        args = new String[]{"../", "configFile=" + f.getAbsolutePath()};

        process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));
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

        ConnectionPoolTestContent.getInstance((Start) StorageLayer.getStorage(process.getProcess()))
                .setKeyValue(ConnectionPoolTestContent.TIME_TO_WAIT_TO_INIT, 5000);
        ConnectionPoolTestContent.getInstance((Start) StorageLayer.getStorage(process.getProcess()))
                .setKeyValue(ConnectionPoolTestContent.RETRY_INTERVAL_IF_INIT_FAILS, 2000);
        process.getProcess().proceedWithInitingStorageModule();

        ProcessState.EventAndException e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE, 7000);
        assertNotNull(e);
        assertEquals(e.exception.getCause().getCause().getMessage(),
                "Error connecting to PostgreSQL instance. Please make sure that PostgreSQL is running and that you "
                        + "have specified the correct values for ('postgresql_host' and 'postgresql_port') or for "
                        + "'postgresql_connection_uri'");

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

        StorageLayer.getStorage(process.getProcess()).setStorageLayerEnabled(false);
        ConnectionPoolTestContent.getInstance((Start) StorageLayer.getStorage(process.getProcess()))
                .setKeyValue(ConnectionPoolTestContent.TIME_TO_WAIT_TO_INIT, 10000);
        ConnectionPoolTestContent.getInstance((Start) StorageLayer.getStorage(process.getProcess()))
                .setKeyValue(ConnectionPoolTestContent.RETRY_INTERVAL_IF_INIT_FAILS, 2000);
        process.getProcess().proceedWithInitingStorageModule();

        Thread.sleep(5000);
        StorageLayer.getStorage(process.getProcess()).setStorageLayerEnabled(true);

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
                "java.sql.SQLException: com.zaxxer.hikari.pool.HikariPool$PoolInitializationException: Failed to " +
                        "initialize pool: The connection attempt failed.",
                e.exception.getCause().getCause().getMessage());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

    }

    @Test
    public void testThatChangeInTableNameIsCorrect() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_key_value_table_name", "key_value_table");
        Utils.setValueInConfig("postgresql_session_info_table_name", "session_info_table");
        Utils.setValueInConfig("postgresql_emailpassword_users_table_name", "users");
        Utils.setValueInConfig("postgresql_emailpassword_pswd_reset_tokens_table_name", "password_reset");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));

        assertEquals("change in KeyValueTable name not reflected", config.getKeyValueTable(), "key_value_table");
        assertEquals("change in SessionInfoTable name not reflected", config.getSessionInfoTable(),
                "session_info_table");
        assertEquals("change in table name not reflected", config.getEmailPasswordUsersTable(), "users");
        assertEquals("change in table name not reflected", config.getPasswordResetTokensTable(), "password_reset");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        // we call this here so that the database is cleared with the modified table names
        // since in postgres, we delete all dbs one by one
        TestingProcessManager.deleteAllInformation();
    }

    @Test
    public void testAddingTableNamePrefixWorks() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_key_value_table_name", "key_value_table");
        Utils.setValueInConfig("postgresql_table_names_prefix", "some_prefix");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));

        assertEquals("change in KeyValueTable name not reflected", config.getKeyValueTable(), "key_value_table");
        assertEquals("change in SessionInfoTable name not reflected", config.getSessionInfoTable(),
                "some_prefix_session_info");
        assertEquals("change in table name not reflected", config.getEmailPasswordUsersTable(),
                "some_prefix_emailpassword_users");
        assertEquals("change in table name not reflected", config.getPasswordResetTokensTable(),
                "some_prefix_emailpassword_pswd_reset_tokens");

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        // we call this here so that the database is cleared with the modified table names
        // since in postgres, we delete all dbs one by one
        TestingProcessManager.deleteAllInformation();
    }

    @Test
    public void testAddingSchemaWorks() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_table_schema", "myschema");
        Utils.setValueInConfig("postgresql_table_names_prefix", "some_prefix");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));

        assertEquals("change in KeyValueTable name not reflected", config.getKeyValueTable(),
                "myschema.some_prefix_key_value");
        assertEquals("change in SessionInfoTable name not reflected", config.getSessionInfoTable(),
                "myschema.some_prefix_session_info");
        assertEquals("change in table name not reflected", config.getEmailPasswordUsersTable(),
                "myschema.some_prefix_emailpassword_users");
        assertEquals("change in table name not reflected", config.getPasswordResetTokensTable(),
                "myschema.some_prefix_emailpassword_pswd_reset_tokens");

        String userId = "userId";
        JsonObject userDataInJWT = new JsonObject();
        userDataInJWT.addProperty("key", "value");
        JsonObject userDataInDatabase = new JsonObject();
        userDataInDatabase.addProperty("key", "value");

        SessionInformationHolder sessionInfo = Session.createNewSession(process.getProcess(), userId, userDataInJWT,
                userDataInDatabase);

        assert sessionInfo.accessToken != null;
        assert sessionInfo.refreshToken != null;
        try {
            TestCase.assertEquals(((SessionStorage) StorageLayer.getStorage(process.getProcess()))
                    .getNumberOfSessions(new TenantIdentifier(null, null, null)), 1);

            // we call this here so that the database is cleared with the modified table names
            // since in postgres, we delete all dbs one by one
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            TestingProcessManager.deleteAllInformation();
        }
    }

    @Test
    public void testAddingSchemaViaConnectionUriWorks() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_connection_uri",
                "postgresql://root:root@localhost:5432/supertokens?currentSchema=myschema");
        Utils.setValueInConfig("postgresql_table_names_prefix", "some_prefix");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));

        assertEquals("change in KeyValueTable name not reflected", config.getKeyValueTable(),
                "myschema.some_prefix_key_value");
        assertEquals("change in SessionInfoTable name not reflected", config.getSessionInfoTable(),
                "myschema.some_prefix_session_info");
        assertEquals("change in table name not reflected", config.getEmailPasswordUsersTable(),
                "myschema.some_prefix_emailpassword_users");
        assertEquals("change in table name not reflected", config.getPasswordResetTokensTable(),
                "myschema.some_prefix_emailpassword_pswd_reset_tokens");

        String userId = "userId";
        JsonObject userDataInJWT = new JsonObject();
        userDataInJWT.addProperty("key", "value");
        JsonObject userDataInDatabase = new JsonObject();
        userDataInDatabase.addProperty("key", "value");

        SessionInformationHolder sessionInfo = Session.createNewSession(process.getProcess(), userId, userDataInJWT,
                userDataInDatabase);

        assert sessionInfo.accessToken != null;
        assert sessionInfo.refreshToken != null;

        TestCase.assertEquals(((SessionStorage) StorageLayer.getStorage(process.getProcess()))
                .getNumberOfSessions(new TenantIdentifier(null, null, null)), 1);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        // we call this here so that the database is cleared with the modified table names
        // since in postgres, we delete all dbs one by one
        TestingProcessManager.deleteAllInformation();
    }

    @Test
    public void testAddingSchemaViaConnectionUriWorks2() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_connection_uri",
                "postgresql://root:root@localhost:5432/supertokens?a=b&currentSchema=myschema");
        Utils.setValueInConfig("postgresql_table_names_prefix", "some_prefix");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));

        assertEquals("change in KeyValueTable name not reflected", config.getKeyValueTable(),
                "myschema.some_prefix_key_value");
        assertEquals("change in SessionInfoTable name not reflected", config.getSessionInfoTable(),
                "myschema.some_prefix_session_info");
        assertEquals("change in table name not reflected", config.getEmailPasswordUsersTable(),
                "myschema.some_prefix_emailpassword_users");
        assertEquals("change in table name not reflected", config.getPasswordResetTokensTable(),
                "myschema.some_prefix_emailpassword_pswd_reset_tokens");

        String userId = "userId";
        JsonObject userDataInJWT = new JsonObject();
        userDataInJWT.addProperty("key", "value");
        JsonObject userDataInDatabase = new JsonObject();
        userDataInDatabase.addProperty("key", "value");

        SessionInformationHolder sessionInfo = Session.createNewSession(process.getProcess(), userId, userDataInJWT,
                userDataInDatabase);

        assert sessionInfo.accessToken != null;
        assert sessionInfo.refreshToken != null;

        TestCase.assertEquals(((SessionStorage) StorageLayer.getStorage(process.getProcess()))
                .getNumberOfSessions(new TenantIdentifier(null, null, null)), 1);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        // we call this here so that the database is cleared with the modified table names
        // since in postgres, we delete all dbs one by one
        TestingProcessManager.deleteAllInformation();
    }

    @Test
    public void testAddingSchemaViaConnectionUriWorks3() throws Exception {
        String[] args = {"../"};

        Utils.setValueInConfig("postgresql_connection_uri",
                "postgresql://root:root@localhost:5432/supertokens?e=f&currentSchema=myschema&a=b&c=d");
        Utils.setValueInConfig("postgresql_table_names_prefix", "some_prefix");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));

        assertEquals("change in KeyValueTable name not reflected", config.getKeyValueTable(),
                "myschema.some_prefix_key_value");
        assertEquals("change in SessionInfoTable name not reflected", config.getSessionInfoTable(),
                "myschema.some_prefix_session_info");
        assertEquals("change in table name not reflected", config.getEmailPasswordUsersTable(),
                "myschema.some_prefix_emailpassword_users");
        assertEquals("change in table name not reflected", config.getPasswordResetTokensTable(),
                "myschema.some_prefix_emailpassword_pswd_reset_tokens");

        String userId = "userId";
        JsonObject userDataInJWT = new JsonObject();
        userDataInJWT.addProperty("key", "value");
        JsonObject userDataInDatabase = new JsonObject();
        userDataInDatabase.addProperty("key", "value");

        SessionInformationHolder sessionInfo = Session.createNewSession(process.getProcess(), userId, userDataInJWT,
                userDataInDatabase);

        assert sessionInfo.accessToken != null;
        assert sessionInfo.refreshToken != null;

        TestCase.assertEquals(((SessionStorage) StorageLayer.getStorage(process.getProcess()))
                .getNumberOfSessions(new TenantIdentifier(null, null, null)), 1);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        // we call this here so that the database is cleared with the modified table names
        // since in postgres, we delete all dbs one by one
        TestingProcessManager.deleteAllInformation();
    }

    @Test
    public void testValidConnectionURI() throws Exception {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        PostgreSQLConfig userConfig = mapper.readValue(new File("../config.yaml"), PostgreSQLConfig.class);
        userConfig.validateAndNormalise();

        String hostname = userConfig.getHostName();
        {
            String[] args = {"../"};

            Utils.setValueInConfig("postgresql_connection_uri",
                    "postgresql://root:root@" + hostname + ":5432/supertokens");
            Utils.commentConfigValue("postgresql_password");
            Utils.commentConfigValue("postgresql_user");
            Utils.commentConfigValue("postgresql_port");
            Utils.commentConfigValue("postgresql_host");
            Utils.commentConfigValue("postgresql_database_name");

            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
            PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));
            checkConfig(config);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }

        {
            Utils.reset();
            String[] args = {"../"};

            Utils.setValueInConfig("postgresql_connection_uri", "postgresql://root:root@" + hostname + "/supertokens");
            Utils.commentConfigValue("postgresql_password");
            Utils.commentConfigValue("postgresql_user");
            Utils.commentConfigValue("postgresql_port");
            Utils.commentConfigValue("postgresql_host");
            Utils.commentConfigValue("postgresql_database_name");

            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
            PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));
            assertEquals(config.getPort(), 5432);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }

        {
            Utils.reset();
            String[] args = {"../"};

            Utils.setValueInConfig("postgresql_connection_uri", "postgresql://" + hostname + ":5432/supertokens");
            Utils.commentConfigValue("postgresql_port");
            Utils.commentConfigValue("postgresql_host");
            Utils.commentConfigValue("postgresql_database_name");

            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
            PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));
            checkConfig(config);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }

        {
            Utils.reset();
            String[] args = {"../"};

            Utils.setValueInConfig("postgresql_connection_uri", "postgresql://root@" + hostname + ":5432/supertokens");
            Utils.commentConfigValue("postgresql_user");
            Utils.commentConfigValue("postgresql_port");
            Utils.commentConfigValue("postgresql_host");
            Utils.commentConfigValue("postgresql_database_name");

            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
            PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));
            checkConfig(config);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }

        {
            Utils.reset();
            String[] args = {"../"};

            Utils.setValueInConfig("postgresql_connection_uri", "postgresql://root:root@" + hostname + ":5432");
            Utils.commentConfigValue("postgresql_password");
            Utils.commentConfigValue("postgresql_user");
            Utils.commentConfigValue("postgresql_port");
            Utils.commentConfigValue("postgresql_host");
            Utils.commentConfigValue("postgresql_database_name");

            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
            PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));
            checkConfig(config);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void testInvalidConnectionURI() throws Exception {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        PostgreSQLConfig userConfig = mapper.readValue(new File("../config.yaml"), PostgreSQLConfig.class);
        userConfig.validateAndNormalise();

        String hostname = userConfig.getHostName();
        {
            String[] args = {"../"};

            Utils.setValueInConfig("postgresql_connection_uri", ":/localhost:5432/supertokens");

            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            ProcessState.EventAndException e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE);
            assertNotNull(e);
            assertEquals(
                    "The provided postgresql connection URI has an incorrect format. Please use a format like "
                            + "postgresql://[user[:[password]]@]host[:port][/dbname][?attr1=val1&attr2=val2...",
                    e.exception.getCause().getMessage());

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }

        {
            Utils.reset();
            String[] args = {"../"};

            Utils.setValueInConfig("postgresql_connection_uri",
                    "postgresql://root:wrongPassword@" + hostname + ":5432/supertokens");
            Utils.commentConfigValue("postgresql_password");
            Utils.commentConfigValue("postgresql_user");
            Utils.commentConfigValue("postgresql_port");
            Utils.commentConfigValue("postgresql_host");
            Utils.commentConfigValue("postgresql_database_name");

            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            ProcessState.EventAndException e = process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.INIT_FAILURE);
            assertNotNull(e);

            TestCase.assertTrue(e.exception.getCause().getMessage().contains("password authentication failed"));

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void testValidConnectionURIAttributes() throws Exception {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        PostgreSQLConfig userConfig = mapper.readValue(new File("../config.yaml"), PostgreSQLConfig.class);
        userConfig.validateAndNormalise();
        String hostname = userConfig.getHostName();
        {
            String[] args = {"../"};

            Utils.setValueInConfig("postgresql_connection_uri",
                    "postgresql://root:root@" + hostname + ":5432/supertokens?key1=value1");

            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
            PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));
            assertEquals(config.getConnectionAttributes(), "key1=value1&allowPublicKeyRetrieval=true");

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }

        {
            Utils.reset();
            String[] args = {"../"};

            Utils.setValueInConfig("postgresql_connection_uri", "postgresql://root:root@" + hostname
                    + ":5432/supertokens?key1=value1&allowPublicKeyRetrieval=false&key2" + "=value2");

            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
            PostgreSQLConfig config = Config.getConfig((Start) StorageLayer.getStorage(process.getProcess()));
            assertEquals(config.getConnectionAttributes(), "key1=value1&allowPublicKeyRetrieval=false&key2=value2");

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void testAllConfigsHaveAnAnnotation() throws Exception {
        for (Field field : PostgreSQLConfig.class.getDeclaredFields()) {
            if (field.isAnnotationPresent(IgnoreForAnnotationCheck.class)) {
                continue;
            }

            if (!(field.isAnnotationPresent(UserPoolProperty.class) || field.isAnnotationPresent(ConnectionPoolProperty.class) || field.isAnnotationPresent(
                    NotConflictingWithinUserPool.class))) {
                fail(field.getName() + " does not have UserPoolProperty, ConnectionPoolProperty or NotConflictingWithinUserPool annotation");
            }
        }
    }

    public static void checkConfig(PostgreSQLConfig config) throws IOException, InvalidConfigException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        PostgreSQLConfig userConfig = mapper.readValue(new File("../config.yaml"), PostgreSQLConfig.class);
        userConfig.validateAndNormalise();

        String hostname = userConfig.getHostName();
        assertEquals("Config getAttributes did not match default", config.getConnectionAttributes(),
                "allowPublicKeyRetrieval=true");
        assertEquals("Config getSchema did not match default", config.getConnectionScheme(), "postgresql");
        assertEquals("Config connectionPoolSize did not match default", config.getConnectionPoolSize(), 10);
        assertEquals("Config databaseName does not match default", config.getDatabaseName(), "supertokens");
        assertEquals("Config keyValue table does not match default", config.getKeyValueTable(), "key_value");
        assertEquals("Config hostName does not match default ", config.getHostName(), hostname);
        assertEquals("Config port does not match default", config.getPort(), 5432);
        assertEquals("Config sessionInfoTable does not match default", config.getSessionInfoTable(), "session_info");
        assertEquals("Config user does not match default", config.getUser(), "root");
        assertEquals("Config password does not match default", config.getPassword(), "root");
        assertEquals("Config keyValue table does not match default", config.getEmailPasswordUsersTable(),
                "emailpassword_users");
        assertEquals("Config keyValue table does not match default", config.getPasswordResetTokensTable(),
                "emailpassword_pswd_reset_tokens");
    }

}
