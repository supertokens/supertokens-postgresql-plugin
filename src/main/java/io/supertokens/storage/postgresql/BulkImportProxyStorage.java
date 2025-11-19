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
import java.util.List;

import io.supertokens.pluginInterface.exceptions.DbInitException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.pluginInterface.sqlStorage.TransactionConnection;


/**
* BulkImportProxyStorage is a class extending Start, serving as a Storage instance in the bulk import user cronjob.
* This cronjob extensively utilizes existing queries to import users, all of which internally operate within transactions.
* 
* For the purpose of bulkimport cronjob, we aim to employ a single connection for all queries and rollback any operations in case of query failures.
* To achieve this, we override the startTransactionHelper method to utilize the same connection and prevent automatic query commits even upon transaction success.
* Subsequently, the cronjob is responsible for committing the transaction after ensuring the successful execution of all queries.
*/

public class BulkImportProxyStorage extends Start {
    private BulkImportProxyConnection connection;

    public synchronized Connection getTransactionConnection() throws SQLException, StorageQueryException {
        if (this.connection == null) {
            Connection con = ConnectionPool.getConnectionForProxyStorage(this);
            this.connection = new BulkImportProxyConnection(con);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
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
        // We do not want to commit the queries when using the BulkImportProxyStorage to be able to rollback everything
        // if any query fails while importing the user
        //super.commitTransaction(con);
    }

    @Override
    public void initStorage(boolean shouldWait, List<TenantIdentifier> tenantIdentifiers) throws DbInitException {
        super.initStorage(shouldWait, tenantIdentifiers);

        // `BulkImportProxyStorage` uses `BulkImportProxyConnection`, which overrides the `.commit()` method on the Connection object.
        // The `initStorage()` method runs `select * from table_name limit 1` queries to check if the tables exist but these queries
        // don't get committed due to the overridden `.commit()`, so we need to manually commit the transaction to remove any locks on the tables.

        // Without this commit, a call to `select * from bulk_import_users limit 1` in `doesTableExist()` locks the `bulk_import_users` table,
        try {
            this.commitTransactionForBulkImportProxyStorage();
        } catch (StorageQueryException e) {
            throw new DbInitException(e);
        }
    }

    @Override
    public void closeConnectionForBulkImportProxyStorage() throws StorageQueryException {
        try {
            if (this.connection != null) {
                this.connection.closeForBulkImportProxyStorage();
                this.connection = null;
            }
            ConnectionPool.close(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void commitTransactionForBulkImportProxyStorage() throws StorageQueryException {
        try {
            if (this.connection != null) {
                this.connection.commitForBulkImportProxyStorage();
            }
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void rollbackTransactionForBulkImportProxyStorage() throws StorageQueryException {
        try {
            this.connection.rollbackForBulkImportProxyStorage();
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }
}
