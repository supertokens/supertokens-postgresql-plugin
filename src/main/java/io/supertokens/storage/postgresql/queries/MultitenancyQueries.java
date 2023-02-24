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

import com.google.gson.JsonParser;
import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
                        new EmailPasswordConfig(result.getBoolean("email_password_enabled")),
                        new ThirdPartyConfig(result.getBoolean("third_party_enabled"), null), // Providers will be populated later
                        new PasswordlessConfig(result.getBoolean("passwordless_enabled")),
                        jp.parse(result.getString("core_config")).getAsJsonObject()
                );
                return config;
            } catch (Exception e) {
                throw new StorageQueryException(e);
            }
        }
    }

    private static class TenantThirdPartyProviderRowMapper implements RowMapper<ThirdPartyConfig.Provider, ResultSet> {
        public static final MultitenancyQueries.TenantThirdPartyProviderRowMapper INSTANCE = new MultitenancyQueries.TenantThirdPartyProviderRowMapper();

        private TenantThirdPartyProviderRowMapper() {
        }

        private static MultitenancyQueries.TenantThirdPartyProviderRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public ThirdPartyConfig.Provider map(ResultSet result) throws StorageQueryException {
            try {
                JsonParser jp = new JsonParser();
                ThirdPartyConfig.Provider provider = new ThirdPartyConfig.Provider(
                    result.getString("third_party_id"),
                    result.getString("name"),
                    null, // to be added later
                    result.getString("authorization_endpoint"),
                    jp.parse(result.getString("authorization_endpoint_query_params")).getAsJsonObject(),
                    result.getString("token_endpoint"),
                    jp.parse(result.getString("token_endpoint_body_params")).getAsJsonObject(),
                    result.getString("user_info_endpoint"),
                    jp.parse(result.getString("user_info_endpoint_query_params")).getAsJsonObject(),
                    jp.parse(result.getString("user_info_endpoint_headers")).getAsJsonObject(),
                    result.getString("jwks_uri"),
                    result.getString("oidc_discovery_endpoint"),
                    result.getBoolean("require_email"),
                    new ThirdPartyConfig.UserInfoMap(
                        new ThirdPartyConfig.UserInfoMapKeyValue(
                            result.getString("user_info_map_from_id_token_payload_user_id"),
                            result.getString("user_info_map_from_id_token_payload_email"),
                            result.getString("user_info_map_from_id_token_payload_email_verified")
                        ),
                        new ThirdPartyConfig.UserInfoMapKeyValue(
                            result.getString("user_info_map_from_user_info_endpoint_user_id"),
                            result.getString("user_info_map_from_user_info_endpoint_email"),
                            result.getString("user_info_map_from_user_info_endpoint_email_verified")
                        )
                    )
                );
                return provider;
            } catch (Exception e) {
                throw new StorageQueryException(e);
            }
        }
    }

    private static class TenantThirdPartyProviderClientRowMapper implements RowMapper<ThirdPartyConfig.ProviderClient, ResultSet> {
        public static final MultitenancyQueries.TenantThirdPartyProviderClientRowMapper INSTANCE = new MultitenancyQueries.TenantThirdPartyProviderClientRowMapper();

        private TenantThirdPartyProviderClientRowMapper() {
        }

        private static MultitenancyQueries.TenantThirdPartyProviderClientRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public ThirdPartyConfig.ProviderClient map(ResultSet result) throws StorageQueryException {
            try {
                JsonParser jp = new JsonParser();
                Array scopeArray = result.getArray("scope");
                ThirdPartyConfig.ProviderClient providerClient = new ThirdPartyConfig.ProviderClient(
                        result.getString("client_type"),
                        result.getString("client_id"),
                        result.getString("client_secret"),
                        (String[]) scopeArray.getArray(),
                        result.getBoolean("force_pkce"),
                        jp.parse(result.getString("additional_config")).getAsJsonObject()
                );
                scopeArray.free();
                return providerClient;
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
                + "connection_uri_domain CHAR(256) DEFAULT '',"
                + "app_id CHAR(64) DEFAULT 'public',"
                + "tenant_id CHAR(64) DEFAULT 'public',"
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
                + "connection_uri_domain CHAR(256) DEFAULT '',"
                + "app_id CHAR(64) DEFAULT 'public',"
                + "tenant_id CHAR(64) DEFAULT 'public',"
                + "third_party_id CHAR(64) NOT NULL,"
                + "name CHAR(64),"
                + "authorization_endpoint CHAR(256),"
                + "authorization_endpoint_query_params TEXT,"
                + "token_endpoint CHAR(256),"
                + "token_endpoint_body_params TEXT,"
                + "user_info_endpoint CHAR(256),"
                + "user_info_endpoint_query_params TEXT,"
                + "user_info_endpoint_headers TEXT,"
                + "jwks_uri CHAR(256),"
                + "oidc_discovery_endpoint CHAR(256),"
                + "require_email BOOLEAN,"
                + "user_info_map_from_id_token_payload_user_id CHAR(64),"
                + "user_info_map_from_id_token_payload_email CHAR(64),"
                + "user_info_map_from_id_token_payload_email_verified CHAR(64),"
                + "user_info_map_from_user_info_endpoint_user_id CHAR(64),"
                + "user_info_map_from_user_info_endpoint_email CHAR(64),"
                + "user_info_map_from_user_info_endpoint_email_verified CHAR(64),"
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
                + "connection_uri_domain CHAR(256) DEFAULT '',"
                + "app_id CHAR(64) DEFAULT 'public',"
                + "tenant_id CHAR(64) DEFAULT 'public',"
                + "third_party_id CHAR(64) NOT NULL,"
                + "client_type CHAR(64) NOT NULL DEFAULT '',"
                + "client_id CHAR(256) NOT NULL,"
                + "client_secret TEXT,"
                + "scope CHAR(128)[],"
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
        {
            String QUERY = "INSERT INTO " + getConfig(start).getTenantConfigsTable()
                    + "(connection_uri_domain, app_id, tenant_id, core_config, email_password_enabled, passwordless_enabled, third_party_enabled)" + " VALUES(?, ?, ?, ?, ?, ?, ?)";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantConfig.tenantIdentifier.getConnectionUriDomain());
                pst.setString(2, tenantConfig.tenantIdentifier.getAppId());
                pst.setString(3, tenantConfig.tenantIdentifier.getTenantId());
                pst.setString(4, tenantConfig.coreConfig.toString());
                pst.setBoolean(5, tenantConfig.emailPasswordConfig.enabled);
                pst.setBoolean(6, tenantConfig.passwordlessConfig.enabled);
                pst.setBoolean(7, tenantConfig.thirdPartyConfig.enabled);
            });
        }

        {
            if (tenantConfig.thirdPartyConfig.providers == null) {
                return;
            }

            for (ThirdPartyConfig.Provider provider : tenantConfig.thirdPartyConfig.providers) {
                String QUERY = "INSERT INTO " + getConfig(start).getTenantThirdPartyProvidersTable()
                        + "(connection_uri_domain, app_id, tenant_id, third_party_id, name, authorization_endpoint, authorization_endpoint_query_params, token_endpoint, token_endpoint_body_params, user_info_endpoint, user_info_endpoint_query_params, user_info_endpoint_headers, jwks_uri, oidc_discovery_endpoint, require_email, user_info_map_from_id_token_payload_user_id, user_info_map_from_id_token_payload_email, user_info_map_from_id_token_payload_email_verified, user_info_map_from_user_info_endpoint_user_id, user_info_map_from_user_info_endpoint_email, user_info_map_from_user_info_endpoint_email_verified)" + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                String user_info_map_from_id_token_payload_user_id;
                String user_info_map_from_id_token_payload_email;
                String user_info_map_from_id_token_payload_email_verified;

                if (provider.userInfoMap != null && provider.userInfoMap.fromIdTokenPayload != null) {
                    user_info_map_from_id_token_payload_user_id = provider.userInfoMap.fromIdTokenPayload.userId;
                    user_info_map_from_id_token_payload_email = provider.userInfoMap.fromIdTokenPayload.email;
                    user_info_map_from_id_token_payload_email_verified = provider.userInfoMap.fromIdTokenPayload.emailVerified;
                } else {
                    user_info_map_from_id_token_payload_user_id = null;
                    user_info_map_from_id_token_payload_email = null;
                    user_info_map_from_id_token_payload_email_verified = null;
                }

                String user_info_map_from_user_info_endpoint_user_id;
                String user_info_map_from_user_info_endpoint_email;
                String user_info_map_from_user_info_endpoint_email_verified;

                if (provider.userInfoMap != null && provider.userInfoMap.fromUserInfoAPI != null) {
                    user_info_map_from_user_info_endpoint_user_id = provider.userInfoMap.fromUserInfoAPI.userId;
                    user_info_map_from_user_info_endpoint_email = provider.userInfoMap.fromUserInfoAPI.email;
                    user_info_map_from_user_info_endpoint_email_verified = provider.userInfoMap.fromUserInfoAPI.emailVerified;
                } else {
                    user_info_map_from_user_info_endpoint_user_id = null;
                    user_info_map_from_user_info_endpoint_email = null;
                    user_info_map_from_user_info_endpoint_email_verified = null;
                }

                update(sqlCon, QUERY, pst -> {
                    pst.setString(1, tenantConfig.tenantIdentifier.getConnectionUriDomain());
                    pst.setString(2, tenantConfig.tenantIdentifier.getAppId());
                    pst.setString(3, tenantConfig.tenantIdentifier.getTenantId());
                    pst.setString(4, provider.thirdPartyId);
                    pst.setString(5, provider.name);
                    pst.setString(6, provider.authorizationEndpoint);
                    pst.setString(7, provider.authorizationEndpointQueryParams.toString());
                    pst.setString(8, provider.tokenEndpoint);
                    pst.setString(9, provider.tokenEndpointBodyParams.toString());
                    pst.setString(10, provider.userInfoEndpoint);
                    pst.setString(11, provider.userInfoEndpointQueryParams.toString());
                    pst.setString(12, provider.userInfoEndpointHeaders.toString());
                    pst.setString(13, provider.jwksURI);
                    pst.setString(14, provider.oidcDiscoveryEndpoint);
                    pst.setBoolean(15, provider.requireEmail);
                    pst.setString(16, user_info_map_from_id_token_payload_user_id);
                    pst.setString(17, user_info_map_from_id_token_payload_email);
                    pst.setString(18, user_info_map_from_id_token_payload_email_verified);
                    pst.setString(19, user_info_map_from_user_info_endpoint_user_id);
                    pst.setString(20, user_info_map_from_user_info_endpoint_email);
                    pst.setString(21, user_info_map_from_user_info_endpoint_email_verified);
                });

                if (provider.clients == null) {
                    continue;
                }

                for (ThirdPartyConfig.ProviderClient providerClient : provider.clients) {
                    String QUERY2 = "INSERT INTO " + getConfig(start).getTenantThirdPartyProviderClientsTable()
                            + "(connection_uri_domain, app_id, tenant_id, third_party_id, client_type, client_id, client_secret, scope, force_pkce, additional_config)" + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    update(sqlCon, QUERY2, pst -> {
                        pst.setString(1, tenantConfig.tenantIdentifier.getConnectionUriDomain());
                        pst.setString(2, tenantConfig.tenantIdentifier.getAppId());
                        pst.setString(3, tenantConfig.tenantIdentifier.getTenantId());
                        pst.setString(4, provider.thirdPartyId);
                        pst.setString(5, providerClient.clientType);
                        pst.setString(6, providerClient.clientId);
                        pst.setString(7, providerClient.clientSecret);
                        pst.setArray(8, sqlCon.createArrayOf("CHAR", providerClient.scope));
                        pst.setBoolean(9, providerClient.forcePKCE);
                        pst.setString(10, providerClient.additionalConfig.toString());
                    });
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
                    /*
                    This may not be required as delete will cascade
                    {
                        String QUERY = "DELETE FROM " + getConfig(start).getTenantThirdPartyProviderClientsTable()
                                + " WHERE connection_uri_domain = ? AND app_id = ? AND tenant_id = ?;";
                        update(start, QUERY, pst -> {
                            pst.setString(1, tenantConfig.tenantIdentifier.getConnectionUriDomain());
                            pst.setString(2, tenantConfig.tenantIdentifier.getAppId());
                            pst.setString(3, tenantConfig.tenantIdentifier.getTenantId());
                        });
                    }

                    {
                        String QUERY = "DELETE FROM " + getConfig(start).getTenantThirdPartyProvidersTable()
                                + " WHERE connection_uri_domain = ? AND app_id = ? AND tenant_id = ?;";
                        update(start, QUERY, pst -> {
                            pst.setString(1, tenantConfig.tenantIdentifier.getConnectionUriDomain());
                            pst.setString(2, tenantConfig.tenantIdentifier.getAppId());
                            pst.setString(3, tenantConfig.tenantIdentifier.getTenantId());
                        });
                    }
                    */

                    {
                        String QUERY = "DELETE FROM " + getConfig(start).getTenantConfigsTable()
                                + " WHERE connection_uri_domain = ? AND app_id = ? AND tenant_id = ?;";
                        int rowsAffected = update(start, QUERY, pst -> {
                            pst.setString(1, tenantConfig.tenantIdentifier.getConnectionUriDomain());
                            pst.setString(2, tenantConfig.tenantIdentifier.getAppId());
                            pst.setString(3, tenantConfig.tenantIdentifier.getTenantId());
                        });
                        if (rowsAffected == 0) {
                            sqlCon.rollback();
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
        TenantConfig[] tenantConfigs;

        try {
            {
                // Read all tenant configs
                String QUERY = "SELECT connection_uri_domain, app_id, tenant_id, core_config, email_password_enabled, passwordless_enabled, third_party_enabled FROM "
                        + getConfig(start).getTenantConfigsTable() + ";";

                tenantConfigs = execute(start, QUERY, pst -> {}, result -> {
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

            HashMap<TenantIdentifier, HashMap<String, ThirdPartyConfig.Provider>> providerMap = new HashMap<>();

            {
                // Read all providers
                String QUERY = "SELECT connection_uri_domain, app_id, tenant_id, third_party_id, name, authorization_endpoint, authorization_endpoint_query_params, token_endpoint, token_endpoint_body_params, user_info_endpoint, user_info_endpoint_query_params, user_info_endpoint_headers, jwks_uri, oidc_discovery_endpoint, require_email, user_info_map_from_id_token_payload_user_id, user_info_map_from_id_token_payload_email, user_info_map_from_id_token_payload_email_verified, user_info_map_from_user_info_endpoint_user_id, user_info_map_from_user_info_endpoint_email, user_info_map_from_user_info_endpoint_email_verified FROM "
                        + getConfig(start).getTenantThirdPartyProvidersTable() + ";";

                execute(start, QUERY, pst -> {}, result -> {
                    List<ThirdPartyConfig.Provider> temp = new ArrayList<>();
                    while (result.next()) {
                        TenantIdentifier tenantIdentifier = new TenantIdentifier(result.getString("connection_uri_domain"), result.getString("app_id"), result.getString("tenant_id"));
                        ThirdPartyConfig.Provider provider = MultitenancyQueries.TenantThirdPartyProviderRowMapper.getInstance().mapOrThrow(result);

                        if (!providerMap.containsKey(tenantIdentifier)) {
                            providerMap.put(tenantIdentifier, new HashMap<>());
                        }
                        providerMap.get(tenantIdentifier).put(provider.thirdPartyId, provider);
                    }
                    return null;
                });
            }

            {
                // Read all provider clients
                String QUERY = "SELECT connection_uri_domain, app_id, tenant_id, third_party_id, client_type, client_id, client_secret, scope, force_pkce, additional_config FROM "
                        + getConfig(start).getTenantThirdPartyProviderClientsTable() + ";";

                execute(start, QUERY, pst -> {}, result -> {
                    List<ThirdPartyConfig.Provider> temp = new ArrayList<>();
                    while (result.next()) {
                        TenantIdentifier tenantIdentifier = new TenantIdentifier(result.getString("connection_uri_domain"), result.getString("app_id"), result.getString("tenant_id"));
                        ThirdPartyConfig.Provider provider = providerMap.get(tenantIdentifier).get(result.getString("third_party_id"));
                        ThirdPartyConfig.ProviderClient providerClient = MultitenancyQueries.TenantThirdPartyProviderClientRowMapper.getInstance().mapOrThrow(result);

                        ThirdPartyConfig.ProviderClient[] clients;
                        if (provider.clients == null) {
                            clients = new ThirdPartyConfig.ProviderClient[1];
                        } else {
                            clients = Arrays.copyOf(provider.clients, provider.clients.length + 1);
                        }
                        clients[clients.length-1] = providerClient;
                        providerMap.get(tenantIdentifier).put(result.getString("third_party_id"), new ThirdPartyConfig.Provider(
                                provider.thirdPartyId,
                                provider.name,
                                clients,
                                provider.authorizationEndpoint,
                                provider.authorizationEndpointQueryParams,
                                provider.tokenEndpoint,
                                provider.tokenEndpointBodyParams,
                                provider.userInfoEndpoint,
                                provider.userInfoEndpointQueryParams,
                                provider.userInfoEndpointHeaders,
                                provider.jwksURI,
                                provider.oidcDiscoveryEndpoint,
                                provider.requireEmail,
                                provider.userInfoMap
                        ));
                    }
                    return null;
                });
            }

            for (int i=0; i<tenantConfigs.length; i++) {
                TenantConfig tenantConfig = tenantConfigs[i];

                if (!providerMap.containsKey(tenantConfig.tenantIdentifier)) {
                    continue;
                }

                Object[] providerObjects = providerMap.get(tenantConfig.tenantIdentifier).values().toArray();
                ThirdPartyConfig.Provider[] providers = new ThirdPartyConfig.Provider[providerObjects.length];
                for (int ii=0; ii<providerObjects.length; ii++) {
                    providers[ii] = (ThirdPartyConfig.Provider) providerObjects[ii];
                }

                tenantConfigs[i] = new TenantConfig(
                        tenantConfig.tenantIdentifier,
                        tenantConfig.emailPasswordConfig,
                        new ThirdPartyConfig(
                                tenantConfig.thirdPartyConfig.enabled,
                                providers
                        ),
                        tenantConfig.passwordlessConfig,
                        tenantConfig.coreConfig
                );
            }

            return tenantConfigs;
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
