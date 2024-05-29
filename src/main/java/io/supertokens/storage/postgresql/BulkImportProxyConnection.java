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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
* BulkImportProxyConnection is a class implementing the Connection interface, serving as a Connection instance in the bulk import user cronjob.
* This cron extensively utilizes existing queries to import users, all of which internally operate within transactions and those query sometimes 
* call the commit/rollback method on the connection.
* 
* For the purpose of bulkimport cronjob, we aim to employ a single connection for all queries and rollback any operations in case of query failures.
* To achieve this, we use our own proxy Connection instance and override the commit/rollback/close methods to do nothing. 
*/

public class BulkImportProxyConnection implements Connection {
    private Connection con = null;

    public BulkImportProxyConnection(Connection con) {
        this.con = con;
    }

    @Override
    public void close() throws SQLException {
        // We simply ignore when close is called BulkImportProxyConnection
    }

    @Override
    public void commit() throws SQLException {
        // We simply ignore when commit is called BulkImportProxyConnection
    }

    @Override
    public void rollback() throws SQLException {
        // We simply ignore when rollback is called BulkImportProxyConnection
    }

    public void closeForBulkImportProxyStorage() throws SQLException {
        this.con.close();
    }

    public void commitForBulkImportProxyStorage() throws SQLException {
        this.con.commit();
    }

    public void rollbackForBulkImportProxyStorage() throws SQLException {
        this.con.rollback();
    }

    /* Following methods are unchaged */

    @Override
    public Statement createStatement() throws SQLException {
        return this.con.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return this.con.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return this.con.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return this.con.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.con.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.con.getAutoCommit();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.con.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return this.con.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.con.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.con.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.con.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return this.con.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.con.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return this.con.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.con.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.con.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.con.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return this.con.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.con.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return this.con.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        this.con.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        this.con.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return this.con.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return this.con.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return this.con.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        this.con.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        this.con.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return this.con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return this.con.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return this.con.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return this.con.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return this.con.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return this.con.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return this.con.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return this.con.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return this.con.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return this.con.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return this.con.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        this.con.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.con.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return this.con.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return this.con.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return this.con.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return this.con.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.con.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return this.con.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.con.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        this.con.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return this.con.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.con.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.con.isWrapperFor(iface);
    }
}