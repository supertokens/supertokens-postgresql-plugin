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

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.TenantConfig;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.ThirdPartyConfig;
import io.supertokens.storage.postgresql.Start;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class MfaSqlHelper {
    public static HashMap<TenantIdentifier, String[]> selectAllFirstFactors(Start start)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT connection_uri_domain, app_id, tenant_id, factor_id FROM "
                + getConfig(start).getFirstFactorsTable() + ";";
        return execute(start, QUERY, pst -> {}, result -> {
            HashMap<TenantIdentifier, List<String>> firstFactors = new HashMap<>();

            while (result.next()) {
                TenantIdentifier tenantIdentifier = new TenantIdentifier(result.getString("connection_uri_domain"), result.getString("app_id"), result.getString("tenant_id"));
                if (!firstFactors.containsKey(tenantIdentifier)) {
                    firstFactors.put(tenantIdentifier, new ArrayList<>());
                }

                firstFactors.get(tenantIdentifier).add(result.getString("factor_id"));
            }

            HashMap<TenantIdentifier, String[]> finalResult = new HashMap<>();
            for (TenantIdentifier tenantIdentifier : firstFactors.keySet()) {
                finalResult.put(tenantIdentifier, firstFactors.get(tenantIdentifier).toArray(new String[0]));
            }

            return finalResult;
        });
    }

    public static HashMap<TenantIdentifier, String[]> selectAllDefaultRequiredFactorIds(Start start)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT connection_uri_domain, app_id, tenant_id, factor_id FROM "
                + getConfig(start).getDefaultRequiredFactorIdsTable() + ";";
        return execute(start, QUERY, pst -> {}, result -> {
            HashMap<TenantIdentifier, List<String>> defaultRequiredFactors = new HashMap<>();

            while (result.next()) {
                TenantIdentifier tenantIdentifier = new TenantIdentifier(result.getString("connection_uri_domain"),
                        result.getString("app_id"), result.getString("tenant_id"));
                if (!defaultRequiredFactors.containsKey(tenantIdentifier)) {
                    defaultRequiredFactors.put(tenantIdentifier, new ArrayList<>());
                }

                defaultRequiredFactors.get(tenantIdentifier).add(result.getString("factor_id"));
            }

            HashMap<TenantIdentifier, String[]> finalResult = new HashMap<>();
            for (TenantIdentifier tenantIdentifier : defaultRequiredFactors.keySet()) {
                finalResult.put(tenantIdentifier, defaultRequiredFactors.get(tenantIdentifier).toArray(new String[0]));
            }

            return finalResult;
        });
    }

    public static void createFirstFactors(Start start, Connection sqlCon, TenantIdentifier tenantIdentifier, String[] firstFactors)
            throws SQLException, StorageQueryException {
        if (firstFactors == null || firstFactors.length == 0) {
            return;
        }

        String QUERY = "INSERT INTO " + getConfig(start).getFirstFactorsTable() + "(connection_uri_domain, app_id, tenant_id, factor_id) VALUES (?, ?, ?, ?);";
        for (String factorId : Set.of(firstFactors)) {
            update(sqlCon, QUERY, pst -> {
               pst.setString(1, tenantIdentifier.getConnectionUriDomain());
                pst.setString(2, tenantIdentifier.getAppId());
                pst.setString(3, tenantIdentifier.getTenantId());
                pst.setString(4, factorId);
            });
        }
    }

    public static void createDefaultRequiredFactorIds(Start start, Connection sqlCon, TenantIdentifier tenantIdentifier, String[] defaultRequiredFactorIds)
            throws SQLException, StorageQueryException {
        if (defaultRequiredFactorIds == null || defaultRequiredFactorIds.length == 0) {
            return;
        }

        String QUERY = "INSERT INTO " + getConfig(start).getDefaultRequiredFactorIdsTable() + "(connection_uri_domain, app_id, tenant_id, factor_id) VALUES (?, ?, ?, ?);";
        for (String factorId : Set.of(defaultRequiredFactorIds)) {
            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getConnectionUriDomain());
                pst.setString(2, tenantIdentifier.getAppId());
                pst.setString(3, tenantIdentifier.getTenantId());
                pst.setString(4, factorId);
            });
        }
    }
}
