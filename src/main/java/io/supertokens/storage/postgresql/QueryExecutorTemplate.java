/*
 *    Copyright (c) 2022, VRAI Labs and/or its affiliates. All rights reserved.
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

import io.supertokens.pluginInterface.exceptions.StorageQueryException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface QueryExecutorTemplate {

    static <T> T execute(Start start, String QUERY, PreparedStatementValueSetter setter,
                         ResultSetValueExtractor<T> mapper) throws SQLException, StorageQueryException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            return execute(con, QUERY, setter, mapper);
        }
    }

    static <T> T execute(Connection con, String QUERY, PreparedStatementValueSetter setter,
                         ResultSetValueExtractor<T> mapper) throws SQLException, StorageQueryException {
        if (setter == null)
            setter = PreparedStatementValueSetter.NO_OP_SETTER;
        try (PreparedStatement pst = con.prepareStatement(QUERY)) {
            setter.setValues(pst);
            try (ResultSet result = pst.executeQuery()) {
                return mapper.extract(result);
            }
        } catch (SQLException e) {
            throw withSchemaMismatchHint(e);
        }
    }

    static void executeBatch(Connection connection, String QUERY, List<PreparedStatementValueSetter> setters)
            throws SQLException, StorageQueryException {
        if(setters == null || setters.isEmpty()) {
            return;
        }
        try (PreparedStatement pst = connection.prepareStatement(QUERY)) {
            int counter = 0;
            for(PreparedStatementValueSetter setter: setters) {
                setter.setValues(pst);
                pst.addBatch();
                counter++;

                if(counter % 100 == 0) {
                    pst.executeBatch();
                }
            }
            pst.executeBatch(); //for the possible remaining ones
        } catch (SQLException e) {
            throw withSchemaMismatchHint(e);
        }
    }

    static int update(Start start, String QUERY, PreparedStatementValueSetter setter)
            throws SQLException, StorageQueryException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            return update(con, QUERY, setter);
        }
    }

    static int update(Connection con, String QUERY, PreparedStatementValueSetter setter) throws SQLException {
        try (PreparedStatement pst = con.prepareStatement(QUERY)) {
            setter.setValues(pst);
            return pst.executeUpdate();
        } catch (SQLException e) {
            throw withSchemaMismatchHint(e);
        }
    }

    static <T> T update(Start start, String QUERY, PreparedStatementValueSetter setter, ResultSetValueExtractor<T> mapper)
            throws SQLException, StorageQueryException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            try (PreparedStatement pst = con.prepareStatement(QUERY)) {
                setter.setValues(pst);
                try (ResultSet result = pst.executeQuery()) {
                    return mapper.extract(result);
                }
            } catch (SQLException e) {
                throw withSchemaMismatchHint(e);
            }
        }
    }

    /**
     * PostgreSQL SQLSTATEs for "column does not exist" / "relation does not exist". When one of these comes out
     * of a query, the schema is missing something this plugin version expects - i.e. a release's manual
     * "### Migration" step was not applied (see {@link SchemaVerifier}). The raw driver message ("column
     * sess.prev_refresh_token_hash_2 does not exist") ends up in API responses where it is cryptic, so it is
     * wrapped with a hint pointing the operator at the core logs, which carry the full startup report.
     */
    static SQLException withSchemaMismatchHint(SQLException e) {
        String state = e.getSQLState();
        if ("42703".equals(state) || "42P01".equals(state)) {
            return new SQLException("Schema mismatch: the database is missing a table or column required by "
                    + "this version of SuperTokens. Check the core error logs and the \"### Migration\" "
                    + "sections of the CHANGELOG for the SQL to run. (" + e.getMessage() + ")", state, e);
        }
        return e;
    }

}
