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
import java.sql.SQLTransactionRollbackException;
import java.util.Set;

import org.postgresql.util.PSQLException;

import com.google.gson.JsonObject;

import io.supertokens.pluginInterface.LOG_LEVEL;
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.sqlStorage.TransactionConnection;
import io.supertokens.storage.postgresql.config.Config;


/**
* BulkImportProxyStorage is a class extending Start, serving as a Storage instance in the bulk import user cronjob.
* This cronjob extensively utilizes existing queries to import users, all of which internally operate within transactions.
* 
* For the purpose of bulkimport cronjob, we aim to employ a single connection for all queries and rollback any operations in case of query failures.
* To achieve this, we override the startTransaction method to utilize the same connection and prevent automatic query commits even upon transaction success.
* Subsequently, the cronjob is responsible for committing the transaction after ensuring the successful execution of all queries.
*/

public class BulkImportProxyStorage extends Start {
    private Connection transactionConnection;

    public Connection getTransactionConnection() throws SQLException {
        if (this.transactionConnection == null || this.transactionConnection.isClosed()) {
            this.transactionConnection = ConnectionPool.getConnectionForProxyStorage(this);
        }
        return this.transactionConnection;
    }

    @Override
    public <T> T startTransaction(TransactionLogic<T> logic)
            throws StorageTransactionLogicException, StorageQueryException {
        return startTransaction(logic, TransactionIsolationLevel.SERIALIZABLE);
    }

    @Override
    public <T> T startTransaction(TransactionLogic<T> logic, TransactionIsolationLevel isolationLevel)
            throws StorageTransactionLogicException, StorageQueryException {
        final int NUM_TRIES = 50;
        int tries = 0;
        while (true) {
            tries++;
            try {
                return startTransactionHelper(logic, isolationLevel);
            } catch (SQLException | StorageQueryException | StorageTransactionLogicException e) {
                Throwable actualException = e;
                if (e instanceof StorageQueryException) {
                    actualException = e.getCause();
                } else if (e instanceof StorageTransactionLogicException) {
                    actualException = ((StorageTransactionLogicException) e).actualException;
                }
                String exceptionMessage = actualException.getMessage();
                if (exceptionMessage == null) {
                    exceptionMessage = "";
                }

                // see: https://github.com/supertokens/supertokens-postgresql-plugin/pull/3

                // We set this variable to the current (or cause) exception casted to
                // PSQLException if we can safely cast it
                PSQLException psqlException = actualException instanceof PSQLException ? (PSQLException) actualException
                        : null;

                // PSQL error class 40 is transaction rollback. See:
                // https://www.postgresql.org/docs/12/errcodes-appendix.html
                boolean isPSQLRollbackException = psqlException != null
                        && psqlException.getServerErrorMessage().getSQLState().startsWith("40");

                // We keep the old exception detection logic to ensure backwards compatibility.
                // We could get here if the new logic hits a false negative,
                // e.g., in case someone renamed constraints/tables
                boolean isDeadlockException = actualException instanceof SQLTransactionRollbackException
                        || exceptionMessage.toLowerCase().contains("concurrent update")
                        || exceptionMessage.toLowerCase().contains("concurrent delete")
                        || exceptionMessage.toLowerCase().contains("the transaction might succeed if retried") ||

                        // we have deadlock as well due to the DeadlockTest.java
                        exceptionMessage.toLowerCase().contains("deadlock");

                if ((isPSQLRollbackException || isDeadlockException) && tries < NUM_TRIES) {
                    try {
                        Thread.sleep((long) (10 + (250 + Math.min(Math.pow(2, tries), 3000)) * Math.random()));
                    } catch (InterruptedException ignored) {
                    }
                    ProcessState.getInstance(this).addState(ProcessState.PROCESS_STATE.DEADLOCK_FOUND, e);
                    // this because deadlocks are not necessarily a result of faulty logic. They can
                    // happen
                    continue;
                }

                if ((isPSQLRollbackException || isDeadlockException) && tries == NUM_TRIES) {
                    ProcessState.getInstance(this).addState(ProcessState.PROCESS_STATE.DEADLOCK_NOT_RESOLVED, e);
                }
                if (e instanceof StorageQueryException) {
                    throw (StorageQueryException) e;
                } else if (e instanceof StorageTransactionLogicException) {
                    throw (StorageTransactionLogicException) e;
                }
                throw new StorageQueryException(e);
            }
        }
    }

    private <T> T startTransactionHelper(TransactionLogic<T> logic, TransactionIsolationLevel isolationLevel)
            throws StorageQueryException, StorageTransactionLogicException, SQLException {
        Connection con = null;
        try {
            con = (Connection) getTransactionConnection();
            int libIsolationLevel = Connection.TRANSACTION_SERIALIZABLE;
            switch (isolationLevel) {
                case SERIALIZABLE:
                    libIsolationLevel = Connection.TRANSACTION_SERIALIZABLE;
                    break;
                case REPEATABLE_READ:
                    libIsolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
                    break;
                case READ_COMMITTED:
                    libIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
                    break;
                case READ_UNCOMMITTED:
                    libIsolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
                    break;
                case NONE:
                    libIsolationLevel = Connection.TRANSACTION_NONE;
                    break;
            }

            if (libIsolationLevel != Connection.TRANSACTION_SERIALIZABLE) {
                con.setTransactionIsolation(libIsolationLevel);
            }
            con.setAutoCommit(false);
            return logic.mainLogicAndCommit(new TransactionConnection(con));
        } catch (Exception e) {
            if (con != null) {
                con.rollback();
            }
            throw e;
        }
    }

    @Override
    public void commitTransaction(TransactionConnection con) throws StorageQueryException {
        // We do not want to commit the queries when using the BulkImportProxyStorage to be able to rollback everything 
        // if any query fails while importing the user
    }

    @Override
    public void loadConfig(JsonObject configJson, Set<LOG_LEVEL> logLevels, TenantIdentifier tenantIdentifier)
            throws InvalidConfigException {
        // We are overriding the loadConfig method to set the connection pool size
        // to 1 to avoid creating many connections for the bulk import cronjob
        configJson.addProperty("postgresql_connection_pool_size", 1);
        Config.loadConfig(this, configJson, logLevels, tenantIdentifier);
    }
}
