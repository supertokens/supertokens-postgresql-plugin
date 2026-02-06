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

import io.supertokens.Main;
import io.supertokens.pluginInterface.PluginInterfaceTesting;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.queries.MultitenancyQueries;
import io.supertokens.storageLayer.StorageLayer;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.Mockito;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public abstract class Utils extends Mockito {

    private static ByteArrayOutputStream byteArrayOutputStream;

    // Track the current test database for cleanup
    private static final ThreadLocal<String> currentTestDatabaseName = new ThreadLocal<>();

    public static void afterTesting() {
        String installDir = "../";
        try {
            // Drop the test-specific database
            String testDb = currentTestDatabaseName.get();
            if (testDb != null) {
                DatabaseTestHelper.dropTestDatabase(testDb);
                currentTestDatabaseName.remove();
            }

            // we remove the license key file
            ProcessBuilder pb = new ProcessBuilder("rm", "licenseKey");
            pb.directory(new File(installDir));
            Process process = pb.start();
            process.waitFor();

            // remove config.yaml file
            String workerId = System.getProperty("org.gradle.test.worker");
            pb = new ProcessBuilder("rm", "config" + workerId + ".yaml");
            pb.directory(new File(installDir));
            process = pb.start();
            process.waitFor();

            // remove webserver-temp folders created by tomcat
            final File webserverTemp = new File(installDir + "webserver-temp");
            try {
                FileUtils.deleteDirectory(webserverTemp);
            } catch (Exception ignored) {
            }

            // remove .started folder created by processes
            final File dotStartedFolder = new File(installDir + ".started" + workerId);
            try {
                FileUtils.deleteDirectory(dotStartedFolder);
            } catch (Exception ignored) {
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void reset() {
        Main.isTesting = true;
        PluginInterfaceTesting.isTesting = true;
        Start.isTesting = true;
        Main.makeConsolePrintSilent = true;
        MultitenancyQueries.simulateErrorInAddingTenantIdInTargetStorage_forTesting = false;

        String installDir = "../";
        String workerId = System.getProperty("org.gradle.test.worker");
        try {
            // IMPORTANT: Kill all processes FIRST to close database connections
            // This must happen before we try to drop the database
            TestingProcessManager.killAll();

            // Now close the storage layer
            StorageLayer.close();

            // Now it's safe to drop previous test database (connections are closed)
            String previousDb = currentTestDatabaseName.get();
            if (previousDb != null) {
                DatabaseTestHelper.dropTestDatabase(previousDb);
                currentTestDatabaseName.remove();
            }

            // Create a new test-specific database
            String testDbName = DatabaseTestHelper.createTestDatabase();
            currentTestDatabaseName.set(testDbName);

            // Copy base config file
            ProcessBuilder pb = new ProcessBuilder("cp", "temp/config.yaml", "./config" + workerId + ".yaml");
            pb.directory(new File(installDir));
            Process process = pb.start();
            process.waitFor();

            // Update config with test-specific database name and connection details
            setValueInConfigDirectly("postgresql_database_name", "\"" + testDbName + "\"", workerId);
            setValueInConfigDirectly("postgresql_host", "\"" + DatabaseTestHelper.getHost() + "\"", workerId);
            setValueInConfigDirectly("postgresql_port", DatabaseTestHelper.getPort(), workerId);
            setValueInConfigDirectly("postgresql_user", "\"" + DatabaseTestHelper.getUser() + "\"", workerId);
            setValueInConfigDirectly("postgresql_password", "\"" + DatabaseTestHelper.getPassword() + "\"", workerId);

            // Kill again and delete info (now on the new database context)
            TestingProcessManager.killAll();
            TestingProcessManager.deleteAllInformation();
            TestingProcessManager.killAll();

            byteArrayOutputStream = new ByteArrayOutputStream();
            System.setErr(new PrintStream(byteArrayOutputStream));
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.gc();
    }

    /**
     * Internal method to set a config value without closing StorageLayer.
     * Used during reset() to configure the test database.
     */
    private static void setValueInConfigDirectly(String key, String value, String workerId) throws IOException {
        String oldStr = "\n((#\\s)?)" + key + "(:|((:\\s).+))\n";
        String newStr = "\n" + key + ": " + value + "\n";
        StringBuilder originalFileContent = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader("../config" + workerId + ".yaml"))) {
            String currentReadingLine = reader.readLine();
            while (currentReadingLine != null) {
                originalFileContent.append(currentReadingLine).append(System.lineSeparator());
                currentReadingLine = reader.readLine();
            }
            String modifiedFileContent = originalFileContent.toString().replaceAll(oldStr, newStr);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter("../config" + workerId + ".yaml"))) {
                writer.write(modifiedFileContent);
            }
        }
    }

    static void stopLicenseKeyFromUpdatingToLatest(TestingProcessManager.TestingProcess process) {
        try {
            List<String> licenseKey = Files.readAllLines(Paths.get("../licenseKey"));
            String escapedKey = licenseKey.get(0).replaceAll("\"", "\\\\\"");
            final HttpsURLConnection mockCon = mock(HttpsURLConnection.class);
            InputStream inputStrm = new ByteArrayInputStream(
                    ("{\"latestLicenseKey\": \"" + escapedKey + "\"}").getBytes(StandardCharsets.UTF_8));
            when(mockCon.getInputStream()).thenReturn(inputStrm);
            when(mockCon.getResponseCode()).thenReturn(200);
            when(mockCon.getOutputStream()).thenReturn(new OutputStream() {
                @Override
                public void write(int b) {
                }
            });

        } catch (Exception ignored) {

        }
    }

    public static void setValueInConfig(String key, String value) throws FileNotFoundException, IOException {
        // we close the storage layer since there might be a change in the db related
        // config.
        StorageLayer.close();

        String oldStr = "\n((#\\s)?)" + key + "(:|((:\\s).+))\n";
        String newStr = "\n" + key + ": " + value + "\n";
        StringBuilder originalFileContent = new StringBuilder();
        String workerId = System.getProperty("org.gradle.test.worker");
        try (BufferedReader reader = new BufferedReader(new FileReader("../config" + workerId + ".yaml"))) {
            String currentReadingLine = reader.readLine();
            while (currentReadingLine != null) {
                originalFileContent.append(currentReadingLine).append(System.lineSeparator());
                currentReadingLine = reader.readLine();
            }
            String modifiedFileContent = originalFileContent.toString().replaceAll(oldStr, newStr);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter("../config" + workerId + ".yaml"))) {
                writer.write(modifiedFileContent);
            }
        }
    }

    public static void commentConfigValue(String key) throws IOException {
        // we close the storage layer since there might be a change in the db related
        // config.
        StorageLayer.close();

        String oldStr = "\n((#\\s)?)" + key + "(:|((:\\s).+))\n";
        String newStr = "\n# " + key + ":";

        StringBuilder originalFileContent = new StringBuilder();
        String workerId = System.getProperty("org.gradle.test.worker");
        try (BufferedReader reader = new BufferedReader(new FileReader("../config" + workerId + ".yaml"))) {
            String currentReadingLine = reader.readLine();
            while (currentReadingLine != null) {
                originalFileContent.append(currentReadingLine).append(System.lineSeparator());
                currentReadingLine = reader.readLine();
            }
            String modifiedFileContent = originalFileContent.toString().replaceAll(oldStr, newStr);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter("../config" + workerId + ".yaml"))) {
                writer.write(modifiedFileContent);
            }
        }

    }

    public static TestRule getOnFailure() {
        return new TestWatcher() {
            @Override
            protected void failed(Throwable e, Description description) {
                System.out.println(byteArrayOutputStream.toString(StandardCharsets.UTF_8));
            }
        };
    }

    /**
     * Checks if file logging is enabled (i.e., log paths are not set to "null" via environment variables).
     * When INFO_LOG_PATH or ERROR_LOG_PATH envvars are set to "null", logging goes to console instead of files.
     *
     * @return true if file logging is enabled, false if logging is configured to go to console
     */
    public static boolean isFileLoggingEnabled() {
        String infoLogPath = System.getenv("INFO_LOG_PATH");
        String errorLogPath = System.getenv("ERROR_LOG_PATH");

        // If either envvar is set to "null" (case-insensitive), file logging is disabled
        boolean infoLogNull = infoLogPath != null && infoLogPath.equalsIgnoreCase("null");
        boolean errorLogNull = errorLogPath != null && errorLogPath.equalsIgnoreCase("null");

        return !infoLogNull && !errorLogNull;
    }

    public static TestRule retryFlakyTest() {
        return new TestRule() {
            private final int retryCount = 10;

            public Statement apply(Statement base, Description description) {
                return statement(base, description);
            }

            private Statement statement(final Statement base, final Description description) {
                return new Statement() {
                    @Override
                    public void evaluate() throws Throwable {
                        Throwable caughtThrowable = null;

                        // implement retry logic here
                        for (int i = 0; i < retryCount; i++) {
                            try {
                                base.evaluate();
                                return;
                            } catch (Throwable t) {
                                caughtThrowable = t;
                                System.err.println(description.getDisplayName() + ": run " + (i+1) + " failed");
                                TestingProcessManager.killAll();
                                Thread.sleep(1000 + new Random().nextInt(3000));
                            }
                        }
                        System.err.println(description.getDisplayName() + ": giving up after " + retryCount + " failures");
                        throw caughtThrowable;
                    }
                };
            }
        };
    }

}
