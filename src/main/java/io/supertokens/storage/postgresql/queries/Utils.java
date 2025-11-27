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
 */

package io.supertokens.storage.postgresql.queries;

import java.sql.Connection;
import java.sql.SQLException;

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.storage.postgresql.LockFailure;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;

public class Utils {

    /**
     * Acquires a PostgreSQL advisory lock using two string keys.
     * Uses pg_try_advisory_xact_lock which is transaction-scoped (automatically released on commit/rollback).
     *
     * @param con  The database connection (must be within a transaction)
     * @param key Key for the lock (e.g., appId)
     * @throws SQLException          If a database error occurs
     * @throws StorageQueryException If a query error occurs
     * @throws LockFailure           If the lock could not be acquired
     */
    public static void takeAdvisoryLock(Connection con, String key)
            throws SQLException, StorageQueryException {
        String LOCK_QUERY = "SELECT pg_try_advisory_xact_lock(hashtext(?))";
        boolean lockAcquired = execute(con, LOCK_QUERY, pst -> {
            pst.setString(1, key);
        }, result -> {
            if (result.next()) {
                return result.getBoolean(1);
            }
            return false;
        });
        if (!lockAcquired) {
            throw new StorageQueryException(new LockFailure());
        }
    }
}
