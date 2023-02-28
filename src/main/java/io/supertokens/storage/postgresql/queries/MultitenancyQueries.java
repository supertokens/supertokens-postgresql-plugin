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
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.queries.multitenancy.TenantConfigSQLHelper;
import io.supertokens.storage.postgresql.queries.multitenancy.ThirdPartyProviderClientSQLHelper;
import io.supertokens.storage.postgresql.queries.multitenancy.ThirdPartyProviderSQLHelper;
import io.supertokens.storage.postgresql.utils.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class MultitenancyQueries {
    static String getQueryToCreateTenantConfigsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantConfigsTable = Config.getConfig(start).getTenantConfigsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tenantConfigsTable + " ("
                + "connection_uri_domain VARCHAR(256) DEFAULT '',"
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "core_config TEXT,"
                + "email_password_enabled BOOLEAN,"
                + "passwordless_enabled BOOLEAN,"
                + "third_party_enabled BOOLEAN,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tenantConfigsTable, null, "pkey") + " PRIMARY KEY (connection_uri_domain, app_id, tenant_id)"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreateTenantThirdPartyProvidersTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantThirdPartyProvidersTable = Config.getConfig(start).getTenantThirdPartyProvidersTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tenantThirdPartyProvidersTable + " ("
                + "connection_uri_domain VARCHAR(256) DEFAULT '',"
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "third_party_id VARCHAR(28) NOT NULL,"
                + "name VARCHAR(64),"
                + "authorization_endpoint TEXT,"
                + "authorization_endpoint_query_params TEXT,"
                + "token_endpoint TEXT,"
                + "token_endpoint_body_params TEXT,"
                + "user_info_endpoint TEXT,"
                + "user_info_endpoint_query_params TEXT,"
                + "user_info_endpoint_headers TEXT,"
                + "jwks_uri TEXT,"
                + "oidc_discovery_endpoint TEXT,"
                + "require_email BOOLEAN,"
                + "user_info_map_from_id_token_payload_user_id VARCHAR(64),"
                + "user_info_map_from_id_token_payload_email VARCHAR(64),"
                + "user_info_map_from_id_token_payload_email_verified VARCHAR(64),"
                + "user_info_map_from_user_info_endpoint_user_id VARCHAR(64),"
                + "user_info_map_from_user_info_endpoint_email VARCHAR(64),"
                + "user_info_map_from_user_info_endpoint_email_verified VARCHAR(64),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tenantThirdPartyProvidersTable, null, "pkey") + " PRIMARY KEY (connection_uri_domain, app_id, tenant_id, third_party_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tenantThirdPartyProvidersTable, "tenant_id", "fkey")
                + " FOREIGN KEY(connection_uri_domain, app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantConfigsTable() +  " (connection_uri_domain, app_id, tenant_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreateTenantThirdPartyProviderClientsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tenantThirdPartyProvidersTable = Config.getConfig(start).getTenantThirdPartyProviderClientsTable();
        return "CREATE TABLE IF NOT EXISTS " + tenantThirdPartyProvidersTable + " ("
                + "connection_uri_domain VARCHAR(256) DEFAULT '',"
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "third_party_id VARCHAR(28) NOT NULL,"
                + "client_type VARCHAR(64) NOT NULL DEFAULT '',"
                + "client_id VARCHAR(256) NOT NULL,"
                + "client_secret TEXT,"
                + "scope VARCHAR(128)[],"
                + "force_pkce BOOLEAN,"
                + "additional_config TEXT,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tenantThirdPartyProvidersTable, null, "pkey") + " PRIMARY KEY (connection_uri_domain, app_id, tenant_id, third_party_id, client_type),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tenantThirdPartyProvidersTable, "third_party_id", "fkey")
                + " FOREIGN KEY(connection_uri_domain, app_id, tenant_id, third_party_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantThirdPartyProvidersTable() +  " (connection_uri_domain, app_id, tenant_id, third_party_id) ON DELETE CASCADE"
                + ");";
    }

    private static void executeCreateTenantQueries(Start start, Connection sqlCon, TenantConfig tenantConfig)
            throws SQLException, StorageQueryException {

        TenantConfigSQLHelper.create(start, sqlCon, tenantConfig);

        if (tenantConfig.thirdPartyConfig.providers != null) {
            for (ThirdPartyConfig.Provider provider : tenantConfig.thirdPartyConfig.providers) {
                ThirdPartyProviderSQLHelper.create(start, sqlCon, tenantConfig, provider);

                if (provider.clients != null) {
                    for (ThirdPartyConfig.ProviderClient providerClient : provider.clients) {
                        ThirdPartyProviderClientSQLHelper.create(start, sqlCon, tenantConfig, provider, providerClient);
                    }
                }
            }
        }
    }

    public static void createTenantConfig(Start start, TenantConfig tenantConfig) throws StorageQueryException, StorageTransactionLogicException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            {
                try {
                    executeCreateTenantQueries(start, sqlCon, tenantConfig);
                    sqlCon.commit();
                } catch (SQLException throwables) {
                    throw new StorageTransactionLogicException(throwables);
                }
            }

            return null;
        });
    }

    public static void overwriteTenantConfig(Start start, TenantConfig tenantConfig) throws StorageQueryException, StorageTransactionLogicException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            {
                try {
                    {
                        String QUERY = "DELETE FROM " + getConfig(start).getTenantConfigsTable()
                                + " WHERE connection_uri_domain = ? AND app_id = ? AND tenant_id = ?;";
                        int rowsAffected = update(start, QUERY, pst -> {
                            pst.setString(1, tenantConfig.tenantIdentifier.getConnectionUriDomain());
                            pst.setString(2, tenantConfig.tenantIdentifier.getAppId());
                            pst.setString(3, tenantConfig.tenantIdentifier.getTenantId());
                        });
                        if (rowsAffected == 0) {
                            throw new StorageTransactionLogicException(new TenantOrAppNotFoundException(tenantConfig.tenantIdentifier));
                        }
                    }

                    {
                        executeCreateTenantQueries(start, sqlCon, tenantConfig);
                    }

                    sqlCon.commit();

                } catch (SQLException throwables) {
                    throw new StorageTransactionLogicException(throwables);
                }
            }

            return null;
        });
    }

    public static TenantConfig[] getAllTenants(Start start) throws StorageQueryException {
        try {

            // Map TenantIdentifier -> thirdPartyId -> clientType
            HashMap<TenantIdentifier, HashMap<String, HashMap<String, ThirdPartyConfig.ProviderClient>>> providerClientsMap = new HashMap<>();
            {
                // Read all provider clients from the third party provider clients table
                String QUERY = "SELECT connection_uri_domain, app_id, tenant_id, third_party_id, client_type, client_id, client_secret, scope, force_pkce, additional_config FROM "
                        + getConfig(start).getTenantThirdPartyProviderClientsTable() + ";";

                execute(start, QUERY, pst -> {}, result -> {
                    while (result.next()) {
                        TenantIdentifier tenantIdentifier = new TenantIdentifier(result.getString("connection_uri_domain"), result.getString("app_id"), result.getString("tenant_id"));
                        ThirdPartyConfig.ProviderClient providerClient = ThirdPartyProviderClientSQLHelper.TenantThirdPartyProviderClientRowMapper.getInstance().mapOrThrow(result);
                        if (!providerClientsMap.containsKey(tenantIdentifier)) {
                            providerClientsMap.put(tenantIdentifier, new HashMap<>());
                        }

                        if(!providerClientsMap.get(tenantIdentifier).containsKey(result.getString("third_party_id"))) {
                            providerClientsMap.get(tenantIdentifier).put(result.getString("third_party_id"), new HashMap<>());
                        }

                        providerClientsMap.get(tenantIdentifier).get(result.getString("third_party_id")).put(providerClient.clientType, providerClient);
                    }
                    return null;
                });
            }

            // Map (tenantIdentifier) -> thirdPartyId -> provider
            HashMap<TenantIdentifier, HashMap<String, ThirdPartyConfig.Provider>> providerMap = new HashMap<>();
            {
                // Read all providers from the third party providers table
                String QUERY = "SELECT connection_uri_domain, app_id, tenant_id, third_party_id, name, authorization_endpoint, authorization_endpoint_query_params, token_endpoint, token_endpoint_body_params, user_info_endpoint, user_info_endpoint_query_params, user_info_endpoint_headers, jwks_uri, oidc_discovery_endpoint, require_email, user_info_map_from_id_token_payload_user_id, user_info_map_from_id_token_payload_email, user_info_map_from_id_token_payload_email_verified, user_info_map_from_user_info_endpoint_user_id, user_info_map_from_user_info_endpoint_email, user_info_map_from_user_info_endpoint_email_verified FROM "
                        + getConfig(start).getTenantThirdPartyProvidersTable() + ";";

                execute(start, QUERY, pst -> {}, result -> {
                    while (result.next()) {
                        TenantIdentifier tenantIdentifier = new TenantIdentifier(result.getString("connection_uri_domain"), result.getString("app_id"), result.getString("tenant_id"));
                        ThirdPartyConfig.ProviderClient[] clients = null;
                        if (providerClientsMap.containsKey(tenantIdentifier) && providerClientsMap.get(tenantIdentifier).containsKey(result.getString("third_party_id"))) {
                            clients = providerClientsMap.get(tenantIdentifier).get(result.getString("third_party_id")).values().toArray(new ThirdPartyConfig.ProviderClient[0]);
                        }
                        ThirdPartyConfig.Provider provider = ThirdPartyProviderSQLHelper.TenantThirdPartyProviderRowMapper.getInstance(clients).mapOrThrow(result);

                        if (!providerMap.containsKey(tenantIdentifier)) {
                            providerMap.put(tenantIdentifier, new HashMap<>());
                        }
                        providerMap.get(tenantIdentifier).put(provider.thirdPartyId, provider);
                    }
                    return null;
                });
            }

            {
                // Read all tenant configs
                String QUERY = "SELECT connection_uri_domain, app_id, tenant_id, core_config, email_password_enabled, passwordless_enabled, third_party_enabled FROM "
                        + getConfig(start).getTenantConfigsTable() + ";";

                TenantConfig[] tenantConfigs = execute(start, QUERY, pst -> {}, result -> {
                    List<TenantConfig> temp = new ArrayList<>();
                    while (result.next()) {
                        TenantIdentifier tenantIdentifier = new TenantIdentifier(result.getString("connection_uri_domain"), result.getString("app_id"), result.getString("tenant_id"));
                        ThirdPartyConfig.Provider[] providers = null;
                        if (providerMap.containsKey(tenantIdentifier)) {
                            providers = providerMap.get(tenantIdentifier).values().toArray(new ThirdPartyConfig.Provider[0]);
                        }
                        temp.add(TenantConfigSQLHelper.TenantConfigRowMapper.getInstance(providers).mapOrThrow(result));
                    }
                    TenantConfig[] finalResult = new TenantConfig[temp.size()];
                    for (int i = 0; i < temp.size(); i++) {
                        finalResult[i] = temp.get(i);
                    }
                    return finalResult;
                });
                return tenantConfigs;
            }
        } catch (SQLException throwables) {
            throw new StorageQueryException(throwables);
        }
    }

    public static void addTenantIdInUserPool(Start start, TenantIdentifier tenantIdentifier) throws
            StorageTransactionLogicException, StorageQueryException {
        {
            start.startTransaction(con -> {
                Connection sqlCon = (Connection) con.getConnection();
                long currentTime = System.currentTimeMillis();
                try {
                    {
                        String QUERY = "INSERT INTO " + getConfig(start).getAppsTable()
                                + "(app_id, created_at_time)" + " VALUES(?, ?) ON CONFLICT DO NOTHING";
                        update(sqlCon, QUERY, pst -> {
                            pst.setString(1, tenantIdentifier.getAppId());
                            pst.setLong(2,  currentTime);
                        });
                    }

                    {
                        String QUERY = "INSERT INTO " + getConfig(start).getTenantsTable()
                                + "(app_id, tenant_id, created_at_time)" + " VALUES(?, ?, ?)";

                        update(sqlCon, QUERY, pst -> {
                            pst.setString(1, tenantIdentifier.getAppId());
                            pst.setString(2, tenantIdentifier.getTenantId());
                            pst.setLong(3,  currentTime);
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
