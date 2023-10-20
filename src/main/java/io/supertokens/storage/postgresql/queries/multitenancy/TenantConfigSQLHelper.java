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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.queries.utils.JsonUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class TenantConfigSQLHelper {
    public static class TenantConfigRowMapper implements RowMapper<TenantConfig, ResultSet> {
        ThirdPartyConfig.Provider[] providers;

        private TenantConfigRowMapper(ThirdPartyConfig.Provider[] providers) {
            this.providers = providers;
        }

        public static TenantConfigSQLHelper.TenantConfigRowMapper getInstance(ThirdPartyConfig.Provider[] providers) {
            return new TenantConfigSQLHelper.TenantConfigRowMapper(providers);
        }

        private static String[] getStringArrayFromJsonString(String input) {
            if (input == null) {
                return new String[0];
            }
            return new Gson().fromJson(input, String[].class);
        }

        private static JsonArray getJsonArrayFromJsonString(String input) {
            if (input == null) {
                return new JsonArray();
            }
            return new Gson().fromJson(input, JsonArray.class);
        }

        @Override
        public TenantConfig map(ResultSet result) throws StorageQueryException {
            try {
                String[] firstFactors = getStringArrayFromJsonString(result.getString("first_factors"));
                String[] defaultRequiredFactorIds = getStringArrayFromJsonString(result.getString("default_required_factors"));

                return new TenantConfig(
                        new TenantIdentifier(result.getString("connection_uri_domain"), result.getString("app_id"), result.getString("tenant_id")),
                        new EmailPasswordConfig(result.getBoolean("email_password_enabled")),
                        new ThirdPartyConfig(result.getBoolean("third_party_enabled"), this.providers),
                        new PasswordlessConfig(result.getBoolean("passwordless_enabled")),
                        new TotpConfig(result.getBoolean("totp_enabled")),
                        new MfaConfig(firstFactors, defaultRequiredFactorIds),
                        JsonUtils.stringToJsonObject(result.getString("core_config"))
                );
            } catch (Exception e) {
                throw new StorageQueryException(e);
            }
        }
    }

    public static TenantConfig[] selectAll(Start start, HashMap<TenantIdentifier, HashMap<String, ThirdPartyConfig.Provider>> providerMap)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT connection_uri_domain, app_id, tenant_id, core_config,"
                + " email_password_enabled, passwordless_enabled, third_party_enabled,"
                + " totp_enabled, first_factors, default_required_factors FROM "
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

    public static void create(Start start, Connection sqlCon, TenantConfig tenantConfig)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getTenantConfigsTable()
                + "(connection_uri_domain, app_id, tenant_id, core_config,"
                + " email_password_enabled, passwordless_enabled, third_party_enabled,"
                + " totp_enabled, first_factors, default_required_factors)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        update(sqlCon, QUERY, pst -> {
            pst.setString(1, tenantConfig.tenantIdentifier.getConnectionUriDomain());
            pst.setString(2, tenantConfig.tenantIdentifier.getAppId());
            pst.setString(3, tenantConfig.tenantIdentifier.getTenantId());
            pst.setString(4, tenantConfig.coreConfig.toString());
            pst.setBoolean(5, tenantConfig.emailPasswordConfig.enabled);
            pst.setBoolean(6, tenantConfig.passwordlessConfig.enabled);
            pst.setBoolean(7, tenantConfig.thirdPartyConfig.enabled);
            pst.setBoolean(8, tenantConfig.totpConfig.enabled);
            pst.setString(9, new GsonBuilder().serializeNulls().create().toJson(tenantConfig.mfaConfig.defaultRequiredFactorIds));
            pst.setString(10, new GsonBuilder().serializeNulls().create().toJson(tenantConfig.mfaConfig.defaultRequiredFactorIds));
        });
    }

}
