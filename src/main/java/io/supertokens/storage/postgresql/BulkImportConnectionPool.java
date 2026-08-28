/*
 *    Copyright (c) 2026, VRAI Labs and/or its affiliates. All rights reserved.
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

package io.supertokens.storage.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.supertokens.pluginInterface.bulkimport.sqlStorage.BulkImportProxySQLStorage;
import io.supertokens.pluginInterface.bulkimport.sqlStorage.BulkImportProxyStoragePool;
import io.supertokens.pluginInterface.exceptions.DbInitException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storage.postgresql.output.Logging;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The bulk import connection pool: a bounded Hikari pool, separate from the live {@link ConnectionPool} of
 * the storage it was opened from, that exists only while an import is running.
 *
 * <p>It is sized by the caller (the import parallelism), never pre-fills idle connections, and does not run
 * the startup DDL — the live storage already created the schema, and replaying the idempotent
 * {@code CREATE INDEX IF NOT EXISTS} batch on a table that is being written by the import itself only
 * buys lock waits. Connections identify themselves to the server with {@code ApplicationName}
 * {@value #APPLICATION_NAME} so operators can tell import backends from API backends in
 * {@code pg_stat_activity}.
 *
 * <p>Every {@link BulkImportProxyStorage} created by {@link #createProxyStorage()} borrows exactly one
 * connection from this pool (lazily, on first use) and shares the owning storage's configuration.
 */
public class BulkImportConnectionPool implements BulkImportProxyStoragePool {

    public static final String APPLICATION_NAME = "supertokens-bulk-import";

    /** Connections are cheap to re-open; do not keep idle import connections around between rounds. */
    private static final long IDLE_TIMEOUT_MS = 10_000; // Hikari's minimum

    private final Start owner;
    private final HikariDataSource dataSource;
    private final List<BulkImportProxyStorage> proxyStorages = new ArrayList<>();
    private volatile boolean closed = false;

    private BulkImportConnectionPool(Start owner, HikariDataSource dataSource) {
        this.owner = owner;
        this.dataSource = dataSource;
    }

    static BulkImportConnectionPool open(Start owner, int maxConnections) throws DbInitException {
        if (maxConnections < 1) {
            throw new IllegalArgumentException("maxConnections must be >= 1, got " + maxConnections);
        }
        PostgreSQLConfig userConfig = Config.getConfig(owner);
        HikariConfig config = ConnectionPool.newHikariConfig(userConfig, owner.getUserPoolId() + "~bulk-import");
        config.setMaximumPoolSize(maxConnections);
        config.setMinimumIdle(0);
        config.setIdleTimeout(IDLE_TIMEOUT_MS);
        config.addDataSourceProperty("ApplicationName", APPLICATION_NAME);
        try {
            Logging.info(owner, "Opening bulk import connection pool (max " + maxConnections + " connections).",
                    false);
            return new BulkImportConnectionPool(owner, new HikariDataSource(config));
        } catch (Exception e) {
            throw new DbInitException(e);
        }
    }

    @Override
    public synchronized BulkImportProxySQLStorage createProxyStorage() throws StorageQueryException {
        assertOpen();
        BulkImportProxyStorage proxy = new BulkImportProxyStorage(this);
        proxy.constructor(owner.getProcessId(), Start.silent, Start.isTesting);
        Config.shareConfig(owner, proxy);
        proxyStorages.add(proxy);
        return proxy;
    }

    /**
     * Borrows a connection; blocks for Hikari's connection timeout when all are in use. Deliberately not
     * synchronized: Hikari is thread-safe and a waiting borrower must not hold up the others.
     */
    Connection borrowConnection() throws SQLException, StorageQueryException {
        assertOpen();
        return dataSource.getConnection();
    }

    @Override
    public synchronized void close() throws StorageQueryException {
        if (closed) {
            return;
        }
        closed = true;
        StorageQueryException firstFailure = null;
        for (BulkImportProxyStorage proxy : proxyStorages) {
            try {
                proxy.closeConnectionForBulkImportProxyStorage();
            } catch (StorageQueryException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                }
            }
        }
        proxyStorages.clear();
        dataSource.close();
        Logging.info(owner, "Closed bulk import connection pool.", false);
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private void assertOpen() throws StorageQueryException {
        if (closed) {
            throw new StorageQueryException(new IllegalStateException("Bulk import connection pool is closed"));
        }
    }

    // visible for tests
    int getActiveConnections() {
        return dataSource.getHikariPoolMXBean().getActiveConnections();
    }

    int getTotalConnections() {
        return dataSource.getHikariPoolMXBean().getTotalConnections();
    }
}
