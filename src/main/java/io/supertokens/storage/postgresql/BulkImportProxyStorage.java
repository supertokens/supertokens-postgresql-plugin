/*
 *    Copyright (c) 2024, VRAI Labs and/or its affiliates. All rights reserved.
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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.List;

import io.supertokens.pluginInterface.bulkimport.sqlStorage.BulkImportProxySQLStorage;
import io.supertokens.pluginInterface.exceptions.DbInitException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.pluginInterface.sqlStorage.TransactionConnection;

/**
 * A {@link Start} bound to a single connection borrowed from a {@link BulkImportConnectionPool}.
 *
 * <p>Bulk import reuses the ordinary recipe code, which opens (nested) transactions and commits as it goes.
 * This proxy routes every one of those queries onto its one connection and turns the implicit commits into
 * no-ops (see {@link BulkImportProxyConnection}), so a worker can claim rows, import users and finalise the
 * claimed rows as one unit, committing only when it explicitly says so. Savepoints allow a failed import to
 * be undone without giving up the claim.
 *
 * <p>The connection is borrowed lazily on first use and given back by
 * {@link #closeConnectionForBulkImportProxyStorage()} (or when the pool closes). Not thread-safe by design:
 * one worker, one proxy.
 */
public class BulkImportProxyStorage extends Start implements BulkImportProxySQLStorage {

    private final BulkImportConnectionPool pool;
    private BulkImportProxyConnection connection;

    BulkImportProxyStorage(BulkImportConnectionPool pool) {
        this.pool = pool;
    }

    public synchronized Connection getTransactionConnection() throws SQLException, StorageQueryException {
        if (this.connection == null) {
            Connection con = pool.borrowConnection();
            // READ COMMITTED (the core-wide default since 12.0): statement-level snapshots mean that after a
            // ROLLBACK TO SAVEPOINT a retried statement sees current data. Under REPEATABLE READ the
            // transaction would keep its original snapshot and a retry after a serialization failure could
            // never succeed.
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            this.connection = new BulkImportProxyConnection(con);
        }
        return this.connection;
    }

    @Override
    protected <T> T startTransactionHelper(TransactionLogic<T> logic, TransactionIsolationLevel isolationLevel)
            throws StorageQueryException, StorageTransactionLogicException, SQLException, TenantOrAppNotFoundException {
        return logic.mainLogicAndCommit(new TransactionConnection(getTransactionConnection()));
    }

    @Override
    public void commitTransaction(TransactionConnection con) throws StorageQueryException {
        // Intentionally a no-op: nothing is committed until the bulk import worker calls
        // commitTransactionForBulkImportProxyStorage(), so that a failure anywhere can still undo everything.
    }

    @Override
    public void initStorage(boolean shouldWait, List<TenantIdentifier> tenantIdentifiers) throws DbInitException {
        throw new UnsupportedOperationException(
                "BulkImportProxyStorage is initialised by its BulkImportConnectionPool; do not call initStorage()");
    }

    @Override
    public synchronized void closeConnectionForBulkImportProxyStorage() throws StorageQueryException {
        if (this.connection == null) {
            return;
        }
        try {
            // Hikari would roll back a dirty connection on return anyway; be explicit so the locks of an
            // abandoned claim are released deterministically.
            this.connection.rollbackForBulkImportProxyStorage();
        } catch (SQLException ignored) {
            // connection may already be broken; closing it below is what matters
        }
        try {
            this.connection.closeForBulkImportProxyStorage();
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        } finally {
            this.connection = null;
        }
    }

    @Override
    public synchronized void commitTransactionForBulkImportProxyStorage() throws StorageQueryException {
        try {
            if (this.connection != null) {
                this.connection.commitForBulkImportProxyStorage();
            }
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public synchronized void rollbackTransactionForBulkImportProxyStorage() throws StorageQueryException {
        try {
            if (this.connection != null) {
                this.connection.rollbackForBulkImportProxyStorage();
            }
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public synchronized Savepoint createSavepointForBulkImportProxyStorage() throws StorageQueryException {
        try {
            return getTransactionConnection().setSavepoint();
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public synchronized void rollbackToSavepointForBulkImportProxyStorage(Savepoint savepoint)
            throws StorageQueryException {
        try {
            getTransactionConnection().rollback(savepoint);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public synchronized void releaseSavepointForBulkImportProxyStorage(Savepoint savepoint)
            throws StorageQueryException {
        try {
            getTransactionConnection().releaseSavepoint(savepoint);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }
}
