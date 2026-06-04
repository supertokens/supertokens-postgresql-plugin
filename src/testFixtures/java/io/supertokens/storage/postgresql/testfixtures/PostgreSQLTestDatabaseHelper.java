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

package io.supertokens.storage.postgresql.testfixtures;

import io.supertokens.pluginInterface.testUtils.TestDatabaseHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PostgreSQL-backed implementation of {@link TestDatabaseHelper}.
 * Discovered by core's test infrastructure via ServiceLoader so that the PostgreSQL
 * JDBC driver stays in the plugin layer and never enters supertokens-core's dependencies.
 *
 * One database is created per Gradle test worker (identified by {@code org.gradle.test.worker})
 * and reused across all tests in that worker. Data cleanup between tests is handled by the
 * process restart in SharedProcess / TestingProcess.
 */
public class PostgreSQLTestDatabaseHelper implements TestDatabaseHelper {

    private static final ThreadLocal<String> currentTestDatabase = new ThreadLocal<>();

    private static volatile String workerDatabase = null;
    private static volatile boolean workerDatabaseInitialized = false;
    private static final Object lock = new Object();

    private static final String PG_HOST     = cfg("TEST_PG_HOST", "localhost");
    private static final String PG_PORT     = cfg("TEST_PG_PORT", "5432");
    private static final String PG_USER     = cfg("TEST_PG_USER", "root");
    private static final String PG_PASSWORD = cfg("TEST_PG_PASSWORD", "root");
    private static final String PG_ADMIN_DB = "postgres";

    private static String cfg(String name, String def) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) v = System.getProperty(name);
        return (v != null && !v.isEmpty()) ? v : def;
    }

    /** ServiceLoader requires a public no-arg constructor. */
    public PostgreSQLTestDatabaseHelper() {}

    @Override
    public String createTestDatabase() {
        synchronized (lock) {
            if (workerDatabaseInitialized && workerDatabase != null) {
                currentTestDatabase.set(workerDatabase);
                return workerDatabase;
            }
        }

        String workerId = System.getProperty("org.gradle.test.worker", "1");
        String dbName = "test_core_w" + workerId;

        String adminUrl = String.format("jdbc:postgresql://%s:%s/%s", PG_HOST, PG_PORT, PG_ADMIN_DB);

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL driver not found", e);
        }

        try (Connection conn = DriverManager.getConnection(adminUrl, PG_USER, PG_PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE DATABASE " + dbName);
        } catch (SQLException e) {
            // "database already exists" (SQLState 42P04) is expected on reruns — ignore it.
            if (!"42P04".equals(e.getSQLState())) {
                throw new RuntimeException("Failed to create test database: " + dbName, e);
            }
        }

        synchronized (lock) {
            workerDatabase = dbName;
            workerDatabaseInitialized = true;
        }
        currentTestDatabase.set(dbName);
        return dbName;
    }

    @Override
    public void dropCurrentTestDatabase() {
        currentTestDatabase.remove();
    }

    @Override
    public String getCurrentTestDatabase() {
        return currentTestDatabase.get();
    }

    @Override public String getHost()     { return PG_HOST; }
    @Override public String getPort()     { return PG_PORT; }
    @Override public String getUser()     { return PG_USER; }
    @Override public String getPassword() { return PG_PASSWORD; }
}
