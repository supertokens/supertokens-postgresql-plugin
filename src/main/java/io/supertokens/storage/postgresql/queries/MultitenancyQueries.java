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

import io.supertokens.pluginInterface.emailpassword.exceptions.UnknownUserIdException;
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
import java.util.HashMap;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class MultitenancyQueries {

    private static void executeCreateTenantQueries(Start start, Connection sqlCon, TenantConfig tenantConfig)
            throws SQLException, StorageQueryException {

        TenantConfigSQLHelper.create(start, sqlCon, tenantConfig);

        for (ThirdPartyConfig.Provider provider : tenantConfig.thirdPartyConfig.providers) {
            ThirdPartyProviderSQLHelper.create(start, sqlCon, tenantConfig, provider);

            for (ThirdPartyConfig.ProviderClient providerClient : provider.clients) {
                ThirdPartyProviderClientSQLHelper.create(start, sqlCon, tenantConfig, provider, providerClient);
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

            return TenantConfigSQLHelper.selectAll(start, providerMap);
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
