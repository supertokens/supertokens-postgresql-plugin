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

package io.supertokens.storage.postgresql.queries.multitenancy;

import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.TenantConfig;
import io.supertokens.pluginInterface.multitenancy.ThirdPartyConfig;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.queries.utils.JsonUtils;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class ThirdPartyProviderClientSQLHelper {
    public static class TenantThirdPartyProviderClientRowMapper implements
            RowMapper<ThirdPartyConfig.ProviderClient, ResultSet> {
        public static final ThirdPartyProviderClientSQLHelper.TenantThirdPartyProviderClientRowMapper INSTANCE = new ThirdPartyProviderClientSQLHelper.TenantThirdPartyProviderClientRowMapper();

        private TenantThirdPartyProviderClientRowMapper() {
        }

        public static ThirdPartyProviderClientSQLHelper.TenantThirdPartyProviderClientRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public ThirdPartyConfig.ProviderClient map(ResultSet result) throws StorageQueryException {
            try {
                Array scopeArray = result.getArray("scope");
                String[] scopeStringArray;
                if (scopeArray == null) {
                    scopeStringArray = null;
                } else {
                    scopeStringArray = (String[]) scopeArray.getArray();
                    scopeArray.free();
                }
                String clientType = result.getString("client_type");
                if (clientType.equals("")) {
                    clientType = null;
                }

                return new ThirdPartyConfig.ProviderClient(
                        clientType,
                        result.getString("client_id"),
                        result.getString("client_secret"),
                        scopeStringArray,
                        result.getBoolean("force_pkce"),
                        JsonUtils.stringToJsonObject(result.getString("additional_config"))
                );
            } catch (Exception e) {
                throw new StorageQueryException(e);
            }
        }
    }

    public static void create(Start start, Connection sqlCon, TenantConfig tenantConfig, ThirdPartyConfig.Provider provider, ThirdPartyConfig.ProviderClient providerClient)
            throws SQLException, StorageQueryException {

        String QUERY = "INSERT INTO " + getConfig(start).getTenantThirdPartyProviderClientsTable()
                + "(connection_uri_domain, app_id, tenant_id, third_party_id, client_type, client_id, client_secret, scope, force_pkce, additional_config)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Array scopeArray;
        if (providerClient.scope != null) {
            scopeArray = sqlCon.createArrayOf("VARCHAR", providerClient.scope);
        } else {
            scopeArray = null;
        }
        update(sqlCon, QUERY, pst -> {
            pst.setString(1, tenantConfig.tenantIdentifier.getConnectionUriDomain());
            pst.setString(2, tenantConfig.tenantIdentifier.getAppId());
            pst.setString(3, tenantConfig.tenantIdentifier.getTenantId());
            pst.setString(4, provider.thirdPartyId);
            pst.setString(5, Objects.requireNonNullElse(providerClient.clientType, ""));
            pst.setString(6, providerClient.clientId);
            pst.setString(7, providerClient.clientSecret);
            pst.setArray(8, scopeArray);
            pst.setBoolean(9, providerClient.forcePKCE);
            pst.setString(10, JsonUtils.jsonObjectToString(providerClient.additionalConfig));
        });
    }

}
