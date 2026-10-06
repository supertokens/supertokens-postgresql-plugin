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

package io.supertokens.storage.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.supertokens.pluginInterface.exceptions.DbInitException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storage.postgresql.output.Logging;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPool extends ResourceDistributor.SingletonResource {

    private static final String RESOURCE_KEY = "io.supertokens.storage.postgresql.ConnectionPool";
    // volatile: getConnection() checks it for null outside initLock (a racy read before this change too)
    private volatile HikariDataSource hikariDataSource;
    private final Start start;
    private PostConnectCallback postConnectCallback;
    // A ReentrantLock rather than a `synchronized` method: the core initialises storages from worker threads,
    // and on JDK 21 a virtual thread that parks inside a synchronized section is pinned to its carrier. Pool
    // setup parks a lot (TCP connect, DDL, and every Hikari DEBUG line contends for the shared logging locks);
    // enough pinned waiters left the core's scheduler with no carrier to run the lock owner and hung startup.
    private final ReentrantLock initLock = new ReentrantLock();

    private ConnectionPool(Start start, PostConnectCallback postConnectCallback) {
        this.start = start;
        this.postConnectCallback = postConnectCallback;
    }

    private void initialiseHikariDataSource() throws SQLException, StorageQueryException {
        initLock.lock();
        try {
            initialiseHikariDataSourceLocked();
        } finally {
            initLock.unlock();
        }
    }

    // only ever called with initLock held
    private void initialiseHikariDataSourceLocked() throws SQLException, StorageQueryException {
        if (this.hikariDataSource != null) {
            return;
        }
        if (!start.enabled) {
            throw new RuntimeException("Connection to refused"); // emulates exception thrown by Hikari
        }

        PostgreSQLConfig userConfig = Config.getConfig(start);
        HikariConfig config = newHikariConfig(userConfig, start.getUserPoolId() + "~" + start.getConnectionPoolId());
        config.setMaximumPoolSize(userConfig.getConnectionPoolSize());
        if (userConfig.getMinimumIdleConnections() != null) {
            config.setMinimumIdle(userConfig.getMinimumIdleConnections());
            config.setIdleTimeout(userConfig.getIdleConnectionTimeout());
        }
        try {
            hikariDataSource = new HikariDataSource(config);
        } catch (Exception e) {
            throw new SQLException(e);
        }

        try {
            try (Connection con = hikariDataSource.getConnection()) {
                this.postConnectCallback.apply(con);
            }
        } catch (StorageQueryException e) {
            // if an exception happens here, we want to set the hikariDataSource to null once again so that
            // whenever the getConnection is called again, we want to re-attempt creation of tables and tenant
            // entries for this storage
            hikariDataSource.close();
            hikariDataSource = null;
            throw e;
        }
    }

    /**
     * Builds the Hikari configuration shared by every pool this plugin opens against a database described by
     * {@code userConfig}: JDBC URL, credentials, driver properties, connection-init SQL and timeouts. Pool
     * sizing is deliberately left to the caller — the live pool sizes itself from the user config, while the
     * bulk import pool ({@link BulkImportConnectionPool}) is sized by the import parallelism.
     */
    static HikariConfig newHikariConfig(PostgreSQLConfig userConfig, String poolName) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.postgresql.Driver");

        String scheme = userConfig.getConnectionScheme();

        String hostName = userConfig.getHostName();

        String port = userConfig.getPort() + "";
        if (!port.equals("-1")) {
            port = ":" + port;
        } else {
            port = "";
        }

        String databaseName = userConfig.getDatabaseName();

        String attributes = userConfig.getConnectionAttributes();
        if (!attributes.equals("")) {
            attributes = "?" + attributes;
        }

        String jdbcUrl = "jdbc:" + scheme + "://" + hostName + port + "/" + databaseName + attributes;
        config.setJdbcUrl(jdbcUrl);

        if (userConfig.getUser() != null) {
            config.setUsername(userConfig.getUser());
        }

        if (userConfig.getPassword() != null && !userConfig.getPassword().equals("")) {
            config.setPassword(userConfig.getPassword());
        }
        config.setConnectionTimeout(5000);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("tcpKeepAlive", "true");
        config.addDataSourceProperty("socketTimeout", "60");
        config.setConnectionInitSql(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED");
        // TODO: set maxLifetimeValue to lesser than 10 mins so that the following error doesnt happen:
        // io.supertokens.storage.postgresql.HikariLoggingAppender.doAppend(HikariLoggingAppender.java:117) |
        // SuperTokens
        // - Failed to validate connection org.mariadb.jdbc.MariaDbConnection@79af83ae (Connection.setNetworkTimeout
        // cannot be called on a closed connection). Possibly consider using a shorter maxLifetime value.
        config.setPoolName(poolName);
        return config;
    }

    private static int getTimeToWaitToInit(Start start) {
        int actualValue = 3600 * 1000;
        if (Start.isTesting) {
            Integer testValue = ConnectionPoolTestContent.getInstance(start)
                    .getValue(ConnectionPoolTestContent.TIME_TO_WAIT_TO_INIT);
            return Objects.requireNonNullElse(testValue, actualValue);
        }
        return actualValue;
    }

    private static int getRetryIntervalIfInitFails(Start start) {
        int actualValue = 10 * 1000;
        if (Start.isTesting) {
            Integer testValue = ConnectionPoolTestContent.getInstance(start)
                    .getValue(ConnectionPoolTestContent.RETRY_INTERVAL_IF_INIT_FAILS);
            return Objects.requireNonNullElse(testValue, actualValue);
        }
        return actualValue;
    }

    private static ConnectionPool getInstance(Start start) {
        return (ConnectionPool) start.getResourceDistributor().getResource(RESOURCE_KEY);
    }

    private static void removeInstance(Start start) {
        start.getResourceDistributor().removeResource(RESOURCE_KEY);
    }

    static boolean isAlreadyInitialised(Start start) {
        return getInstance(start) != null && getInstance(start).hikariDataSource != null;
    }

    static void initPool(Start start, boolean shouldWait, PostConnectCallback postConnectCallback)
            throws DbInitException {
        if (isAlreadyInitialised(start)) {
            return;
        }
        Logging.info(start, "Setting up PostgreSQL connection pool.", true);
        boolean longMessagePrinted = false;
        long maxTryTime = System.currentTimeMillis() + getTimeToWaitToInit(start);
        String errorMessage =
                "Error connecting to PostgreSQL instance. Please make sure that PostgreSQL is running and that "
                        + "you have" +
                        " specified the correct values for ('postgresql_host' and 'postgresql_port') or for "
                        + "'postgresql_connection_uri'";
        try {
            ConnectionPool con = new ConnectionPool(start, postConnectCallback);
            start.getResourceDistributor().setResource(RESOURCE_KEY, con);
            while (true) {
                try {
                    con.initialiseHikariDataSource();
                    break;
                } catch (Exception e) {
                    if (!shouldWait) {
                        throw new DbInitException(e);
                    }
                    if (e.getMessage().contains("Connection to") && e.getMessage().contains("refused")
                            || e.getMessage().contains("the database system is starting up")) {
                        start.handleKillSignalForWhenItHappens();
                        if (System.currentTimeMillis() > maxTryTime) {
                            throw new DbInitException(errorMessage);
                        }
                        if (!longMessagePrinted) {
                            longMessagePrinted = true;
                            Logging.info(start, errorMessage, true);
                        }
                        double minsRemaining = (maxTryTime - System.currentTimeMillis()) / (1000.0 * 60);
                        NumberFormat formatter = new DecimalFormat("#0.0");
                        Logging.info(start,
                                "Trying again in a few seconds for " + formatter.format(minsRemaining) + " mins...",
                                true);
                        try {
                            if (Thread.interrupted()) {
                                throw new InterruptedException();
                            }
                            Thread.sleep(getRetryIntervalIfInitFails(start));
                        } catch (InterruptedException ex) {
                            throw new DbInitException(errorMessage);
                        }
                    } else {
                        throw new DbInitException(e);
                    }
                }
            }
        } finally {
            start.removeShutdownHook();
        }
    }

    private static Connection getNewConnection(Start start) throws SQLException, StorageQueryException {
        if (getInstance(start) == null) {
            throw new IllegalStateException("Please call initPool before getConnection");
        }
        if (!start.enabled) {
            throw new SQLException("Storage layer disabled");
        }
        if (getInstance(start).hikariDataSource == null) {
            getInstance(start).initialiseHikariDataSource();
        }
        return getInstance(start).hikariDataSource.getConnection();
    }

    public static Connection getConnectionForProxyStorage(Start start) throws SQLException, StorageQueryException {
        return getNewConnection(start);
    }

    // ── Test-only guard: two connections from the same pool in one call chain ──────────────────────
    // Under a small connection pool, a call chain that holds one connection (inside a startTransaction)
    // and borrows a SECOND from the same pool causes hold-and-wait exhaustion — the deadlock class behind
    // the OAuth non-rotating-refresh regression. This tripwire turns that otherwise-silent deadlock into a
    // located test failure at the exact nested borrow, through any depth of helper indirection. It is keyed
    // per pool, so a nested borrow on a DIFFERENT tenant's pool is allowed; BulkImportProxyStorage is
    // naturally exempt (it reuses its transaction connection and never reaches getNewConnection). Active
    // only under Start.isTesting — a no-op in production.
    private static final ThreadLocal<Map<String, Integer>> TXN_DEPTH_BY_POOL =
            ThreadLocal.withInitial(HashMap::new);

    // During the PLAN-018 cleanup the guard WARNS by default so the suite stays green while the pre-existing
    // instances are fixed; flip this to fail fast (intended to become the default once the cleanup lands).
    private static volatile boolean throwOnNestedAcquisition = false;

    // Test hook: when true, a nested same-pool acquisition throws instead of only warning.
    public static void setThrowOnNestedAcquisition(boolean value) {
        throwOnNestedAcquisition = value;
    }

    private static String poolKey(Start start) {
        return start.getUserPoolId() + "~" + start.getConnectionPoolId();
    }

    // Called by Start.startTransactionHelper AFTER it has taken its own connection, wrapping the callback.
    static void enterTransaction(Start start) {
        if (!Start.isTesting) {
            return;
        }
        TXN_DEPTH_BY_POOL.get().merge(poolKey(start), 1, Integer::sum);
    }

    static void exitTransaction(Start start) {
        if (!Start.isTesting) {
            return;
        }
        Map<String, Integer> depths = TXN_DEPTH_BY_POOL.get();
        String key = poolKey(start);
        Integer depth = depths.get(key);
        if (depth == null) {
            return;
        }
        if (depth <= 1) {
            depths.remove(key);
        } else {
            depths.put(key, depth - 1);
        }
    }

    private static void assertNoNestedPoolAcquisition(Start start) {
        if (!Start.isTesting) {
            return;
        }
        Integer depth = TXN_DEPTH_BY_POOL.get().get(poolKey(start));
        if (depth == null || depth <= 0) {
            return;
        }
        String message = "Nested same-pool connection acquisition on pool '" + poolKey(start) + "' at "
                + nestedAcquisitionSite() + ": a helper borrows a SECOND connection while a startTransaction on"
                + " this pool is open — the hold-and-wait pool-exhaustion (OAuth-refresh deadlock) class. Thread"
                + " the transaction's connection through the helper (use its *_Transaction overload), or resolve"
                + " the value before opening the transaction.";
        if (throwOnNestedAcquisition) {
            throw new IllegalStateException(message);
        }
        // Warn-mode default during the PLAN-018 cleanup: surface it without failing the suite.
        System.err.println("[nested-conn-guard][WARN] " + message);
    }

    // The nearest application frame that borrowed the second connection — for locating the site in warn-mode.
    private static String nestedAcquisitionSite() {
        for (StackTraceElement f : Thread.currentThread().getStackTrace()) {
            String cn = f.getClassName();
            if (!cn.startsWith("io.supertokens.")) {
                continue;
            }
            if (cn.endsWith(".ConnectionPool") || cn.endsWith(".QueryExecutorTemplate")
                    || (cn.endsWith(".Start") && f.getMethodName().startsWith("startTransaction"))) {
                continue;
            }
            return cn.substring(cn.lastIndexOf('.') + 1) + "." + f.getMethodName()
                    + "(" + f.getFileName() + ":" + f.getLineNumber() + ")";
        }
        return "unknown";
    }

    public static Connection getConnection(Start start) throws SQLException, StorageQueryException {
        if (start.schemaMismatchMessage != null) {
            // Start.verifySchema() ran in strict mode (schema_check_strict_mode) and found missing
            // tables/columns: fail every query on this storage consistently with the operator-facing message
            // until a later verification succeeds.
            throw new SQLException(start.schemaMismatchMessage);
        }
        if (start instanceof BulkImportProxyStorage) {
            return ((BulkImportProxyStorage) start).getTransactionConnection();
        }
        assertNoNestedPoolAcquisition(start);
        return getNewConnection(start);
    }

    /** Bypasses the strict-mode schema-mismatch gate above so that a failed storage can be re-verified. */
    static Connection getConnectionForSchemaVerification(Start start) throws SQLException, StorageQueryException {
        return getNewConnection(start);
    }

    static void close(Start start) {
        if (getInstance(start) == null) {
            return;
        }
        if (getInstance(start).hikariDataSource != null) {
            try {
                getInstance(start).hikariDataSource.close();
            } finally {
                // we mark it as null so that next time it's being initialised, it will be initialised again
                getInstance(start).hikariDataSource = null;
                removeInstance(start);
            }
        }
    }

    @FunctionalInterface
    public static interface PostConnectCallback {
        void apply(Connection connection) throws StorageQueryException;
    }
}
