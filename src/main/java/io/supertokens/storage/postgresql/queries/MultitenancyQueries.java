/*
 *    Copyright (c) 2023, VRAI Labs and/or its affiliates. All rights reserved.
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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.emailpassword.PasswordResetTokenInfo;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.pluginInterface.multitenancy.exceptions.DuplicateTenantException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class MultitenancyQueries {

    private static class TenantConfigRowMapper implements RowMapper<TenantConfig, ResultSet> {
        public static final MultitenancyQueries.TenantConfigRowMapper INSTANCE = new MultitenancyQueries.TenantConfigRowMapper();

        private TenantConfigRowMapper() {
        }

        private static MultitenancyQueries.TenantConfigRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public TenantConfig map(ResultSet result) throws StorageQueryException {
            try {
                JsonParser jp = new JsonParser();

                TenantConfig config = new TenantConfig(
                        new TenantIdentifier(result.getString("connection_uri_domain"), result.getString("app_id"), result.getString("tenant_id")),
                        EmailPasswordConfig.fromJSONString(result.getString("emailpassword")),
                        ThirdPartyConfig.fromJSONString(result.getString("thirdparty")),
                        PasswordlessConfig.fromJSONString(result.getString("passwordless")),
                        jp.parse(result.getString("core_config")).getAsJsonObject()
                );
                return config;
            } catch (Exception e) {
                throw new StorageQueryException(e);
            }
        }
    }

    static String getQueryToCreateTenantConfigsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantConfigsTable = Config.getConfig(start).getTenantConfigsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tenantConfigsTable + " ("
                + "connection_uri_domain CHAR(128) DEFAULT '',"
                + "app_id CHAR(64) DEFAULT 'public',"
                + "tenant_id CHAR(64) DEFAULT 'public',"
                + "core_config TEXT,"
                + "emailpassword TEXT,"
                + "passwordless TEXT,"
                + "thirdparty TEXT,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tenantConfigsTable, null, "pkey") + " PRIMARY KEY (connection_uri_domain, app_id, tenant_id)"
                + ");";
        // @formatter:on
    }

    public static void createTenant(Start start, TenantConfig tenantConfig) throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getTenantConfigsTable()
                + "(connection_uri_domain, app_id, tenant_id, core_config, emailpassword, passwordless, thirdparty)" + " VALUES(?, ?, ?, ?, ?, ?, ?)";
        update(start, QUERY, pst -> {
            pst.setString(1, tenantConfig.tenantIdentifier.getConnectionUriDomain());
            pst.setString(2, tenantConfig.tenantIdentifier.getAppId());
            pst.setString(3, tenantConfig.tenantIdentifier.getTenantId());
            pst.setString(4, tenantConfig.coreConfig.toString());
            pst.setString(5, tenantConfig.emailPasswordConfig.toJSON().toString());
            pst.setString(6, tenantConfig.passwordlessConfig.toJSON().toString());
            pst.setString(7, tenantConfig.thirdPartyConfig.toJSON().toString());
        });
    }

    public static void updateTenant(Start start, TenantConfig tenantConfig) throws SQLException, StorageQueryException {
        String QUERY = "UPDATE " + getConfig(start).getTenantConfigsTable()
                + " SET core_config = ?, emailpassword = ?, passwordless = ?, thirdparty = ?"
                + " WHERE connection_uri_domain = ? AND app_id = ? AND tenant_id = ?;";
        update(start, QUERY, pst -> {
            pst.setString(1, tenantConfig.coreConfig.toString());
            pst.setString(2, tenantConfig.emailPasswordConfig.toJSON().toString());
            pst.setString(3, tenantConfig.passwordlessConfig.toJSON().toString());
            pst.setString(4, tenantConfig.thirdPartyConfig.toJSON().toString());
            pst.setString(5, tenantConfig.tenantIdentifier.getConnectionUriDomain());
            pst.setString(6, tenantConfig.tenantIdentifier.getAppId());
            pst.setString(7, tenantConfig.tenantIdentifier.getTenantId());
        });
    }

    public static TenantConfig[] getAllTenants(Start start) throws SQLException, StorageQueryException {
        String QUERY = "SELECT connection_uri_domain, app_id, tenant_id, core_config, emailpassword, passwordless, thirdparty FROM "
                + getConfig(start).getTenantConfigsTable() + ";";

        return execute(start, QUERY, pst -> {}, result -> {
            List<TenantConfig> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(MultitenancyQueries.TenantConfigRowMapper.getInstance().mapOrThrow(result));
            }
            TenantConfig[] finalResult = new TenantConfig[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        });
    }

    public static void addTenantIdInUserPool(Start start, TenantIdentifier tenantIdentifier) throws
            StorageTransactionLogicException, StorageQueryException {
        {
            start.startTransaction(con -> {
                Connection sqlCon = (Connection) con.getConnection();
                try {
                    {
                        String QUERY = "INSERT INTO " + getConfig(start).getAppsTable()
                                + "(app_id, created_at_time)" + " VALUES(?, ?) ON CONFLICT DO NOTHING";
                        update(sqlCon, QUERY, pst -> {
                            pst.setString(1, tenantIdentifier.getAppId());
                            pst.setLong(2,  System.currentTimeMillis());
                        });
                    }

                    {
                        String QUERY = "INSERT INTO " + getConfig(start).getTenantsTable()
                                + "(app_id, tenant_id, created_at_time)" + " VALUES(?, ?, ?)";

                        update(sqlCon, QUERY, pst -> {
                            pst.setString(1, tenantIdentifier.getAppId());
                            pst.setString(2, tenantIdentifier.getTenantId());
                            pst.setLong(3,  System.currentTimeMillis());
                        });
                    }

                    sqlCon.commit();
                } catch (SQLException throwables) {
                    throw new StorageTransactionLogicException(throwables);
                }
                return null;
            });
        }
    }
}
