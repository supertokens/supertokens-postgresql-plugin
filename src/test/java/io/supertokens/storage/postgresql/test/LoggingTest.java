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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.google.gson.JsonObject;
import io.supertokens.ProcessState;
import io.supertokens.config.Config;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.output.Logging;
import io.supertokens.storageLayer.StorageLayer;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import static org.junit.Assert.*;

public class LoggingTest {
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
    public void defaultLogging() throws Exception {
        String[] args = { "../" };
        StorageLayer.close();
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);

        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        File infoLog = new File(Config.getConfig(process.getProcess()).getInfoLogPath(process.getProcess()));
        File errorLog = new File(Config.getConfig(process.getProcess()).getErrorLogPath(process.getProcess()));

        Logging.error((Start) StorageLayer.getStorage(process.getProcess()), "From Test", false);
        Logging.info((Start) StorageLayer.getStorage(process.getProcess()), "From Info", true);

        boolean infoFlag = false;
        boolean errorFlag = false;

        try (Scanner scanner = new Scanner(infoLog, StandardCharsets.UTF_8)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.contains(process.getProcess().getProcessId())) {
                    infoFlag = true;
                    break;
                }
            }
        }

        try (Scanner errorScanner = new Scanner(errorLog, StandardCharsets.UTF_8)) {
            while (errorScanner.hasNextLine()) {
                String line = errorScanner.nextLine();
                if (line.contains(process.getProcess().getProcessId())) {
                    errorFlag = true;
                    break;
                }
            }
        }

        assertTrue(infoFlag && errorFlag);
        process.kill();

        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void customLogging() throws Exception {
        try {
            String[] args = { "../" };

            Utils.setValueInConfig("info_log_path", "\"tempLogging/info.log\"");
            Utils.setValueInConfig("error_log_path", "\"tempLogging/error.log\"");

            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            Logging.error((Start) StorageLayer.getStorage(process.getProcess()), "From Test", false);
            Logging.info((Start) StorageLayer.getStorage(process.getProcess()), "From Info", true);

            boolean infoFlag = false;
            boolean errorFlag = false;

            File infoLog = new File(Config.getConfig(process.getProcess()).getInfoLogPath(process.getProcess()));
            File errorLog = new File(Config.getConfig(process.getProcess()).getErrorLogPath(process.getProcess()));

            try (Scanner scanner = new Scanner(infoLog, StandardCharsets.UTF_8)) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        infoFlag = true;
                        break;
                    }
                }
            }

            try (Scanner errorScanner = new Scanner(errorLog, StandardCharsets.UTF_8)) {
                while (errorScanner.hasNextLine()) {
                    String line = errorScanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        errorFlag = true;
                        break;
                    }
                }
            }

            assertTrue(infoFlag && errorFlag);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        } finally {
            FileUtils.deleteDirectory(new File("tempLogging"));

        }

    }

    @Test
    public void confirmLoggerClosed() throws Exception {
        String[] args = { "../" };
        StorageLayer.close();
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);

        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        ch.qos.logback.classic.Logger postgresqlInfo = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger("io.supertokens.storage.postgresql.Info");
        ch.qos.logback.classic.Logger postgresqlError = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger("io.supertokens.storage.postgresql.Error");

        ch.qos.logback.classic.Logger hikariLogger = (Logger) LoggerFactory.getLogger("com.zaxxer.hikari");

        assertTrue(postgresqlInfo.iteratorForAppenders().hasNext());
        assertTrue(postgresqlError.iteratorForAppenders().hasNext());
        assertTrue(hikariLogger.iteratorForAppenders().hasNext());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        assertTrue(
                !postgresqlInfo.iteratorForAppenders().hasNext() && !postgresqlError.iteratorForAppenders().hasNext());
        assertFalse(hikariLogger.iteratorForAppenders().hasNext());

    }

    @Test
    public void testStandardOutLoggingWithNullStr() throws Exception {
        String[] args = { "../" };
        ByteArrayOutputStream stdOutput = new ByteArrayOutputStream();
        ByteArrayOutputStream errorOutput = new ByteArrayOutputStream();

        Utils.setValueInConfig("log_level", "DEBUG");
        Utils.setValueInConfig("info_log_path", "\"null\"");
        Utils.setValueInConfig("error_log_path", "\"null\"");

        System.setOut(new PrintStream(stdOutput));
        System.setErr(new PrintStream(errorOutput));

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);

        try {

            process.startProcess();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            Logging.debug((Start) StorageLayer.getStorage(process.getProcess()), "outTest-adsvdavasdvas");
            Logging.error((Start) StorageLayer.getStorage(process.getProcess()), "errTest-dsavivilja", true);

            assertTrue(fileContainsString(stdOutput, "outTest-adsvdavasdvas"));
            assertTrue(fileContainsString(errorOutput, "errTest-dsavivilja"));

        } finally {

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
            System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
        }

    }

    @Test
    public void testStandardOutLoggingWithNull() throws Exception {
        String[] args = { "../" };
        ByteArrayOutputStream stdOutput = new ByteArrayOutputStream();
        ByteArrayOutputStream errorOutput = new ByteArrayOutputStream();

        Utils.setValueInConfig("log_level", "DEBUG");
        Utils.setValueInConfig("info_log_path", "null");
        Utils.setValueInConfig("error_log_path", "null");

        System.setOut(new PrintStream(stdOutput));
        System.setErr(new PrintStream(errorOutput));

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);

        try {

            process.startProcess();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            Logging.debug((Start) StorageLayer.getStorage(process.getProcess()), "outTest-adsvdavasdvas");
            Logging.error((Start) StorageLayer.getStorage(process.getProcess()), "errTest-dsavivilja", true);

            assertTrue(fileContainsString(stdOutput, "outTest-adsvdavasdvas"));
            assertTrue(fileContainsString(errorOutput, "errTest-dsavivilja"));

        } finally {

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
            System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
        }

    }

    @Test
    public void confirmHikariLoggerClosedOnlyWhenProcessEnds() throws Exception {
        StorageLayer.close();
        String[] args = { "../" };
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});

        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        ch.qos.logback.classic.Logger postgresqlInfo = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger("io.supertokens.storage.postgresql.Info");
        ch.qos.logback.classic.Logger postgresqlError = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger("io.supertokens.storage.postgresql.Error");
        ch.qos.logback.classic.Logger hikariLogger = (Logger) LoggerFactory.getLogger("com.zaxxer.hikari");

        assertEquals(1, countAppenders(postgresqlError));
        assertEquals(1, countAppenders(postgresqlInfo));
        assertEquals(1, countAppenders(hikariLogger));

        TenantIdentifier tenant = new TenantIdentifier(null, null, "t1");
        JsonObject config = new JsonObject();
        StorageLayer.getBaseStorage(process.getProcess()).modifyConfigToAddANewUserPoolForTesting(config, 1);
        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                tenant,
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null,
                config
        ), false);

        // No new appenders were added
        assertEquals(1, countAppenders(postgresqlError));
        assertEquals(1, countAppenders(postgresqlInfo));
        assertEquals(1, countAppenders(hikariLogger));

        Multitenancy.deleteTenant(tenant, process.getProcess());

        // No appenders were removed
        assertEquals(1, countAppenders(postgresqlError));
        assertEquals(1, countAppenders(postgresqlInfo));
        assertEquals(1, countAppenders(hikariLogger));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        assertEquals(0, countAppenders(postgresqlError));
        assertEquals(0, countAppenders(postgresqlInfo));
        assertEquals(0, countAppenders(hikariLogger));

        assertFalse(hikariLogger.iteratorForAppenders().hasNext());
    }

    private static int countAppenders(ch.qos.logback.classic.Logger logger) {
        int count = 0;
        Iterator<Appender<ILoggingEvent>> appenderIter = logger.iteratorForAppenders();
        while (appenderIter.hasNext()) {
            Appender<ILoggingEvent> appender = appenderIter.next();
            count++;
        }
        return count;
    }

    private static boolean fileContainsString(ByteArrayOutputStream log, String value) throws IOException {
        boolean containsString = false;
        try (BufferedReader reader = new BufferedReader(new StringReader(log.toString()))) {
            String currentReadingLine = reader.readLine();
            while (currentReadingLine != null) {
                if (currentReadingLine.contains(value)) {
                    containsString = true;
                    break;
                }
                currentReadingLine = reader.readLine();
            }
        }
        return containsString;
    }
}
