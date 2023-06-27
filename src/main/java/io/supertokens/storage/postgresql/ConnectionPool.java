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
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storage.postgresql.output.Logging;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Objects;

public class ConnectionPool extends ResourceDistributor.SingletonResource {

    private static final String RESOURCE_KEY = "io.supertokens.storage.postgresql.ConnectionPool";
    private HikariDataSource hikariDataSource;
    private final Start start;

    private ConnectionPool(Start start) {
        this.start = start;
    }

    private synchronized void initialiseHikariDataSource() throws SQLException {
        if (this.hikariDataSource != null) {
            return;
        }
        if (!start.enabled) {
            throw new RuntimeException("Connection to refused"); // emulates exception thrown by Hikari
        }

        HikariConfig config = new HikariConfig();
        PostgreSQLConfig userConfig = Config.getConfig(start);
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

        config.setJdbcUrl("jdbc:" + scheme + "://" + hostName + port + "/" + databaseName + attributes);

        if (userConfig.getUser() != null) {
            config.setUsername(userConfig.getUser());
        }

        if (userConfig.getPassword() != null && !userConfig.getPassword().equals("")) {
            config.setPassword(userConfig.getPassword());
        }
        config.setMaximumPoolSize(userConfig.getConnectionPoolSize());
        config.setConnectionTimeout(5000);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        // TODO: set maxLifetimeValue to lesser than 10 mins so that the following error doesnt happen:
        // io.supertokens.storage.postgresql.HikariLoggingAppender.doAppend(HikariLoggingAppender.java:117) |
        // SuperTokens
        // - Failed to validate connection org.mariadb.jdbc.MariaDbConnection@79af83ae (Connection.setNetworkTimeout
        // cannot be called on a closed connection). Possibly consider using a shorter maxLifetime value.
        config.setPoolName(start.getUserPoolId() + "~" + start.getConnectionPoolId());
        try {
            hikariDataSource = new HikariDataSource(config);
        } catch (Exception e) {
            throw new SQLException(e);
        }
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

    static void initPool(Start start, boolean shouldWait) throws DbInitException {
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
            ConnectionPool con = new ConnectionPool(start);
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

    public static Connection getConnection(Start start) throws SQLException {
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
}
