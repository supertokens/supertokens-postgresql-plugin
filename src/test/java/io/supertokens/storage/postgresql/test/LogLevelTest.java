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
import io.supertokens.config.Config;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.output.Logging;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import static org.junit.Assert.*;

public class LogLevelTest {
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
    public void testLogLevelNoneOutput() throws Exception {
        {
            Utils.setValueInConfig("log_level", "NONE");
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);

            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            File infoLog = new File(Config.getConfig(process.getProcess()).getInfoLogPath(process.getProcess()));
            File errorLog = new File(Config.getConfig(process.getProcess()).getErrorLogPath(process.getProcess()));
            boolean didOutput = false;
            Logging.error((Start) StorageLayer.getStorage(process.getProcess()), "some message", false);
            Logging.warn((Start) StorageLayer.getStorage(process.getProcess()), "some message");
            Logging.info((Start) StorageLayer.getStorage(process.getProcess()), "some message", true);
            Logging.debug((Start) StorageLayer.getStorage(process.getProcess()), "some message");

            try (Scanner scanner = new Scanner(infoLog, StandardCharsets.UTF_8)) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        didOutput = true;
                        break;
                    }
                }
            }

            try (Scanner errorScanner = new Scanner(errorLog, StandardCharsets.UTF_8)) {
                while (errorScanner.hasNextLine()) {
                    String line = errorScanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        didOutput = true;
                        break;
                    }
                }
            }

            assertFalse(didOutput);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void testLogLevelErrorOutput() throws Exception {
        {
            Utils.setValueInConfig("log_level", "ERROR");
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);

            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            File infoLog = new File(Config.getConfig(process.getProcess()).getInfoLogPath(process.getProcess()));
            File errorLog = new File(Config.getConfig(process.getProcess()).getErrorLogPath(process.getProcess()));
            boolean errorOutput = false;
            boolean warnOutput = false;
            boolean infoOutput = false;
            boolean debugOutput = false;

            Logging.error((Start) StorageLayer.getStorage(process.getProcess()), "some error", false);
            Logging.warn((Start) StorageLayer.getStorage(process.getProcess()), "some warn");
            Logging.info((Start) StorageLayer.getStorage(process.getProcess()), "some info", true);
            Logging.debug((Start) StorageLayer.getStorage(process.getProcess()), "some debug");

            try (Scanner scanner = new Scanner(infoLog, StandardCharsets.UTF_8)) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        if (line.contains("some info")) {
                            infoOutput = true;
                        } else if (line.contains("some debug")) {
                            debugOutput = true;
                        }
                    }
                }
            }

            try (Scanner errorScanner = new Scanner(errorLog, StandardCharsets.UTF_8)) {
                while (errorScanner.hasNextLine()) {
                    String line = errorScanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        if (line.contains("some error")) {
                            errorOutput = true;
                        } else if (line.contains("some warn")) {
                            warnOutput = true;
                        }
                    }
                }
            }

            assertTrue(errorOutput && !warnOutput && !infoOutput && !debugOutput);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void testLogLevelWarnOutput() throws Exception {
        {
            Utils.setValueInConfig("log_level", "WARN");
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);

            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            File infoLog = new File(Config.getConfig(process.getProcess()).getInfoLogPath(process.getProcess()));
            File errorLog = new File(Config.getConfig(process.getProcess()).getErrorLogPath(process.getProcess()));
            boolean errorOutput = false;
            boolean warnOutput = false;
            boolean infoOutput = false;
            boolean debugOutput = false;

            Logging.error((Start) StorageLayer.getStorage(process.getProcess()), "some error", false);
            Logging.warn((Start) StorageLayer.getStorage(process.getProcess()), "some warn");
            Logging.info((Start) StorageLayer.getStorage(process.getProcess()), "some info", true);
            Logging.debug((Start) StorageLayer.getStorage(process.getProcess()), "some debug");

            try (Scanner scanner = new Scanner(infoLog, StandardCharsets.UTF_8)) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        if (line.contains("some info")) {
                            infoOutput = true;
                        } else if (line.contains("some debug")) {
                            debugOutput = true;
                        }
                    }
                }
            }

            try (Scanner errorScanner = new Scanner(errorLog, StandardCharsets.UTF_8)) {
                while (errorScanner.hasNextLine()) {
                    String line = errorScanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        if (line.contains("some error")) {
                            errorOutput = true;
                        } else if (line.contains("some warn")) {
                            warnOutput = true;
                        }
                    }
                }
            }

            assertTrue(errorOutput && warnOutput && !infoOutput && !debugOutput);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void testLogLevelInfoOutput() throws Exception {
        {
            Utils.setValueInConfig("log_level", "INFO");
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);

            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            File infoLog = new File(Config.getConfig(process.getProcess()).getInfoLogPath(process.getProcess()));
            File errorLog = new File(Config.getConfig(process.getProcess()).getErrorLogPath(process.getProcess()));
            boolean errorOutput = false;
            boolean warnOutput = false;
            boolean infoOutput = false;
            boolean debugOutput = false;

            Logging.error((Start) StorageLayer.getStorage(process.getProcess()), "some error", false);
            Logging.warn((Start) StorageLayer.getStorage(process.getProcess()), "some warn");
            Logging.info((Start) StorageLayer.getStorage(process.getProcess()), "some info", true);
            Logging.debug((Start) StorageLayer.getStorage(process.getProcess()), "some debug");

            try (Scanner scanner = new Scanner(infoLog, StandardCharsets.UTF_8)) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        if (line.contains("some info")) {
                            infoOutput = true;
                        } else if (line.contains("some debug")) {
                            debugOutput = true;
                        }
                    }
                }
            }

            try (Scanner errorScanner = new Scanner(errorLog, StandardCharsets.UTF_8)) {
                while (errorScanner.hasNextLine()) {
                    String line = errorScanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        if (line.contains("some error")) {
                            errorOutput = true;
                        } else if (line.contains("some warn")) {
                            warnOutput = true;
                        }
                    }
                }
            }

            assertTrue(errorOutput && warnOutput && infoOutput && !debugOutput);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void testLogLevelDebugOutput() throws Exception {
        {
            Utils.setValueInConfig("log_level", "DEBUG");
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);

            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            File infoLog = new File(Config.getConfig(process.getProcess()).getInfoLogPath(process.getProcess()));
            File errorLog = new File(Config.getConfig(process.getProcess()).getErrorLogPath(process.getProcess()));
            boolean errorOutput = false;
            boolean warnOutput = false;
            boolean infoOutput = false;
            boolean debugOutput = false;

            Logging.error((Start) StorageLayer.getStorage(process.getProcess()), "some error", false);
            Logging.warn((Start) StorageLayer.getStorage(process.getProcess()), "some warn");
            Logging.info((Start) StorageLayer.getStorage(process.getProcess()), "some info", true);
            Logging.debug((Start) StorageLayer.getStorage(process.getProcess()), "some debug");

            try (Scanner scanner = new Scanner(infoLog, StandardCharsets.UTF_8)) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        if (line.contains("some info")) {
                            infoOutput = true;
                        } else if (line.contains("some debug")) {
                            debugOutput = true;
                        }
                    }
                }
            }

            try (Scanner errorScanner = new Scanner(errorLog, StandardCharsets.UTF_8)) {
                while (errorScanner.hasNextLine()) {
                    String line = errorScanner.nextLine();
                    if (line.contains(process.getProcess().getProcessId())) {
                        if (line.contains("some error")) {
                            errorOutput = true;
                        } else if (line.contains("some warn")) {
                            warnOutput = true;
                        }
                    }
                }
            }

            assertTrue(errorOutput && warnOutput && infoOutput && debugOutput);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
