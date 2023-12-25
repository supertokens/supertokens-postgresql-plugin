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

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.queries.multitenancy.MfaSqlHelper;
import io.supertokens.storage.postgresql.queries.multitenancy.TenantConfigSQLHelper;
import io.supertokens.storage.postgresql.queries.multitenancy.ThirdPartyProviderClientSQLHelper;
import io.supertokens.storage.postgresql.queries.multitenancy.ThirdPartyProviderSQLHelper;
import io.supertokens.storage.postgresql.utils.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
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

    public static String getQueryToCreateTenantIdIndexForTenantThirdPartyProvidersTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS tenant_thirdparty_providers_tenant_id_index ON "
                + getConfig(start).getTenantThirdPartyProvidersTable() + " (connection_uri_domain, app_id, tenant_id);";
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

    public static String getQueryToCreateThirdPartyIdIndexForTenantThirdPartyProviderClientsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS tenant_thirdparty_provider_clients_third_party_id_index ON "
                + getConfig(start).getTenantThirdPartyProviderClientsTable() + " (connection_uri_domain, app_id, tenant_id, third_party_id);";
    }

    public static String getQueryToCreateFirstFactorsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getTenantFirstFactorsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "connection_uri_domain VARCHAR(256) DEFAULT '',"
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "factor_id VARCHAR(128),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey")
                + " PRIMARY KEY (connection_uri_domain, app_id, tenant_id, factor_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "tenant_id", "fkey")
                + " FOREIGN KEY (connection_uri_domain, app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantConfigsTable() +  " (connection_uri_domain, app_id, tenant_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static String getQueryToCreateTenantIdIndexForFirstFactorsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS tenant_first_factors_tenant_id_index ON "
                + getConfig(start).getTenantFirstFactorsTable() + " (connection_uri_domain, app_id, tenant_id);";
    }

    public static String getQueryToCreateRequiredSecondaryFactorsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getTenantRequiredSecondaryFactorsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "connection_uri_domain VARCHAR(256) DEFAULT '',"
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "factor_id VARCHAR(128),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey")
                + " PRIMARY KEY (connection_uri_domain, app_id, tenant_id, factor_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "tenant_id", "fkey")
                + " FOREIGN KEY (connection_uri_domain, app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantConfigsTable() +  " (connection_uri_domain, app_id, tenant_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static String getQueryToCreateTenantIdIndexForRequiredSecondaryFactorsTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS tenant_default_required_factor_ids_tenant_id_index ON "
                + getConfig(start).getTenantRequiredSecondaryFactorsTable() + " (connection_uri_domain, app_id, tenant_id);";
    }


    private static void executeCreateTenantQueries(Start start, Connection sqlCon, TenantConfig tenantConfig)
            throws SQLException, StorageQueryException {

        TenantConfigSQLHelper.create(start, sqlCon, tenantConfig);

        for (ThirdPartyConfig.Provider provider : tenantConfig.thirdPartyConfig.providers) {
            ThirdPartyProviderSQLHelper.create(start, sqlCon, tenantConfig, provider);

            for (ThirdPartyConfig.ProviderClient providerClient : provider.clients) {
                ThirdPartyProviderClientSQLHelper.create(start, sqlCon, tenantConfig, provider, providerClient);
            }
        }

        MfaSqlHelper.createFirstFactors(start, sqlCon, tenantConfig.tenantIdentifier, tenantConfig.firstFactors);
        MfaSqlHelper.createRequiredSecondaryFactors(start, sqlCon, tenantConfig.tenantIdentifier, tenantConfig.requiredSecondaryFactors);
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

    public static boolean deleteTenantConfig(Start start, TenantIdentifier tenantIdentifier) throws StorageQueryException {
        try {
            String QUERY = "DELETE FROM " + getConfig(start).getTenantConfigsTable()
                    + " WHERE connection_uri_domain = ? AND app_id = ? AND tenant_id = ?";

            int numRows = update(start, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getConnectionUriDomain());
                pst.setString(2, tenantIdentifier.getAppId());
                pst.setString(3, tenantIdentifier.getTenantId());
            });

            return numRows > 0;

        } catch (SQLException throwables) {
            throw new StorageQueryException(throwables);
        }
    }

    public static void overwriteTenantConfig(Start start, TenantConfig tenantConfig) throws StorageQueryException, StorageTransactionLogicException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            {
                try {
                    {
                        String QUERY = "DELETE FROM " + getConfig(start).getTenantConfigsTable()
                                + " WHERE connection_uri_domain = ? AND app_id = ? AND tenant_id = ?;";
                        int rowsAffected = update(sqlCon, QUERY, pst -> {
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
            HashMap<TenantIdentifier, HashMap<String, HashMap<String, ThirdPartyConfig.ProviderClient>>> providerClientsMap = ThirdPartyProviderClientSQLHelper.selectAll(start);

            // Map (tenantIdentifier) -> thirdPartyId -> provider
            HashMap<TenantIdentifier, HashMap<String, ThirdPartyConfig.Provider>> providerMap = ThirdPartyProviderSQLHelper.selectAll(start, providerClientsMap);

            // Map (tenantIdentifier) -> firstFactors
            HashMap<TenantIdentifier, String[]> firstFactorsMap = MfaSqlHelper.selectAllFirstFactors(start);

            // Map (tenantIdentifier) -> requiredSecondaryFactors
            HashMap<TenantIdentifier, String[]> requiredSecondaryFactorsMap = MfaSqlHelper.selectAllRequiredSecondaryFactors(start);

            return TenantConfigSQLHelper.selectAll(start, providerMap, firstFactorsMap, requiredSecondaryFactorsMap);
        } catch (SQLException throwables) {
            throw new StorageQueryException(throwables);
        }
    }

    public static void addTenantIdInTargetStorage(Start start, TenantIdentifier tenantIdentifier) throws
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

    public static void deleteTenantIdInTargetStorage(Start start, TenantIdentifier tenantIdentifier)
        throws StorageQueryException {
        try {
            if (tenantIdentifier.getTenantId().equals(TenantIdentifier.DEFAULT_TENANT_ID)) {
                // Delete the app
                String QUERY = "DELETE FROM " + getConfig(start).getAppsTable()
                        + " WHERE app_id = ?";

                update(start, QUERY, pst -> {
                    pst.setString(1, tenantIdentifier.getAppId());
                });
            } else {
                // Delete the tenant
                String QUERY = "DELETE FROM " + getConfig(start).getTenantsTable()
                        + " WHERE app_id = ? AND tenant_id = ?";

                update(start, QUERY, pst -> {
                    pst.setString(1, tenantIdentifier.getAppId());
                    pst.setString(2, tenantIdentifier.getTenantId());
                });
            }

        } catch (SQLException throwables) {
            throw new StorageQueryException(throwables);
        }
    }
}
