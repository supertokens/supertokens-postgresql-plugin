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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class for managing test-specific PostgreSQL databases.
 * Each test gets its own isolated database to prevent interference.
 */
public class DatabaseTestHelper {

    private static final AtomicInteger testCounter = new AtomicInteger(0);

    // Thread-local storage for the current test's database name
    private static final ThreadLocal<String> currentTestDatabase = new ThreadLocal<>();

    // PostgreSQL connection details - read from environment or use defaults
    private static final String PG_HOST = getConfigValue("TEST_PG_HOST", "pg");
    private static final String PG_PORT = getConfigValue("TEST_PG_PORT", "5432");
    private static final String PG_USER = getConfigValue("TEST_PG_USER", "root");
    private static final String PG_PASSWORD = getConfigValue("TEST_PG_PASSWORD", "root");
    private static final String PG_ADMIN_DATABASE = "postgres"; // Database to connect to for admin operations

    private static String getConfigValue(String envName, String defaultValue) {
        String value = System.getenv(envName);
        if (value == null || value.isEmpty()) {
            value = System.getProperty(envName);
        }
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    /**
     * Generate a unique database name for the current test.
     * Format: test_w{workerId}_t{timestamp}_{counter}
     */
    public static String generateTestDatabaseName() {
        String workerId = System.getProperty("org.gradle.test.worker", "0");
        long timestamp = System.currentTimeMillis();
        int counter = testCounter.incrementAndGet();

        // PostgreSQL database names must be lowercase and can't start with a number
        // Max length is 63 characters
        String dbName = String.format("test_w%s_%d_%d", workerId, timestamp % 1000000, counter);
        return dbName.toLowerCase();
    }

    /**
     * Create a new test database.
     * Connects to the admin database to create the test database.
     */
    public static String createTestDatabase() {
        String dbName = generateTestDatabaseName();

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL driver not found", e);
        }

        String adminUrl = String.format("jdbc:postgresql://%s:%s/%s", PG_HOST, PG_PORT, PG_ADMIN_DATABASE);

        try (Connection conn = DriverManager.getConnection(adminUrl, PG_USER, PG_PASSWORD);
             Statement stmt = conn.createStatement()) {

            // Create the database
            stmt.executeUpdate("CREATE DATABASE " + dbName);
            // System.out.println("[DatabaseTestHelper] Created test database: " + dbName);

            currentTestDatabase.set(dbName);
            return dbName;

        } catch (SQLException e) {
            System.err.println("[DatabaseTestHelper] Failed to create database " + dbName + ": " + e.getMessage());
            throw new RuntimeException("Failed to create test database: " + dbName, e);
        }
    }

    /**
     * Drop the current test database.
     * Should be called after the test completes.
     */
    public static void dropCurrentTestDatabase() {
        String dbName = currentTestDatabase.get();
        if (dbName == null) {
            return;
        }

        dropTestDatabase(dbName);
        currentTestDatabase.remove();
    }

    /**
     * Drop a specific test database.
     */
    public static void dropTestDatabase(String dbName) {
        if (dbName == null || dbName.isEmpty()) {
            return;
        }

        // Don't drop the admin database or any non-test databases
        if (!dbName.startsWith("test_")) {
            System.err.println("[DatabaseTestHelper] Refusing to drop non-test database: " + dbName);
            return;
        }

        String adminUrl = String.format("jdbc:postgresql://%s:%s/%s", PG_HOST, PG_PORT, PG_ADMIN_DATABASE);

        try (Connection conn = DriverManager.getConnection(adminUrl, PG_USER, PG_PASSWORD);
             Statement stmt = conn.createStatement()) {

            // Terminate all connections to the database first
            stmt.executeUpdate(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + dbName + "'"
            );

            // Drop the database
            stmt.executeUpdate("DROP DATABASE IF EXISTS " + dbName);
            // System.out.println("[DatabaseTestHelper] Dropped test database: " + dbName);

        } catch (SQLException e) {
            // Log but don't fail - database might already be dropped
            System.err.println("[DatabaseTestHelper] Warning: Could not drop database " + dbName + ": " + e.getMessage());
        }
    }

    /**
     * Get the current test database name.
     */
    public static String getCurrentTestDatabase() {
        return currentTestDatabase.get();
    }

    /**
     * Set the current test database name (for cases where it's set externally).
     */
    public static void setCurrentTestDatabase(String dbName) {
        currentTestDatabase.set(dbName);
    }

    /**
     * Clean up any stale test databases that might have been left from previous runs.
     * This can be called at the start of a test run.
     */
    public static void cleanupStaleTestDatabases() {
        String adminUrl = String.format("jdbc:postgresql://%s:%s/%s", PG_HOST, PG_PORT, PG_ADMIN_DATABASE);

        try (Connection conn = DriverManager.getConnection(adminUrl, PG_USER, PG_PASSWORD);
             Statement stmt = conn.createStatement()) {

            // Find all test databases
            var rs = stmt.executeQuery(
                "SELECT datname FROM pg_database WHERE datname LIKE 'test_%'"
            );

            while (rs.next()) {
                String dbName = rs.getString(1);
                // System.out.println("[DatabaseTestHelper] Cleaning up stale database: " + dbName);
                dropTestDatabase(dbName);
            }

        } catch (SQLException e) {
            System.err.println("[DatabaseTestHelper] Warning: Could not cleanup stale databases: " + e.getMessage());
        }
    }

    /**
     * Get the JDBC URL for the current test database.
     */
    public static String getTestDatabaseUrl() {
        String dbName = currentTestDatabase.get();
        if (dbName == null) {
            throw new IllegalStateException("No test database has been created. Call createTestDatabase() first.");
        }
        return String.format("jdbc:postgresql://%s:%s/%s", PG_HOST, PG_PORT, dbName);
    }

    /**
     * Get the PostgreSQL host.
     */
    public static String getHost() {
        return PG_HOST;
    }

    /**
     * Get the PostgreSQL port.
     */
    public static String getPort() {
        return PG_PORT;
    }

    /**
     * Get the PostgreSQL user.
     */
    public static String getUser() {
        return PG_USER;
    }

    /**
     * Get the PostgreSQL password.
     */
    public static String getPassword() {
        return PG_PASSWORD;
    }
}
