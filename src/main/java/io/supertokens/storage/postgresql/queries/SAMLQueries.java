/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
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
 *
 */

package io.supertokens.storage.postgresql.queries;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.saml.SAMLClaimsInfo;
import io.supertokens.pluginInterface.saml.SAMLClient;
import io.supertokens.pluginInterface.saml.SAMLRelayStateInfo;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import static io.supertokens.storage.postgresql.config.Config.getConfig;
import io.supertokens.storage.postgresql.utils.Utils;

public class SAMLQueries {
    public static String getQueryToCreateSAMLClientsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getSAMLClientsTable();
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "client_id VARCHAR(256) NOT NULL,"
                + "client_secret TEXT,"
                + "sso_login_url TEXT NOT NULL,"
                + "redirect_uris TEXT NOT NULL,"
                + "default_redirect_uri TEXT NOT NULL,"
                + "idp_entity_id VARCHAR(256) NOT NULL,"
                + "idp_signing_certificate TEXT NOT NULL,"
                + "allow_idp_initiated_login BOOLEAN NOT NULL DEFAULT FALSE,"
                + "enable_request_signing BOOLEAN NOT NULL DEFAULT FALSE,"
                + "created_at BIGINT NOT NULL,"
                + "updated_at BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey")
                + " PRIMARY KEY(app_id, tenant_id, client_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "idp_entity_id", "key")
                + " UNIQUE (app_id, tenant_id, idp_entity_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "app_id", "fkey") + " "
                + "FOREIGN KEY(app_id) "
                + "REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "tenant_id", "fkey") + " "
                + "FOREIGN KEY(app_id, tenant_id) "
                + "REFERENCES " + Config.getConfig(start).getTenantsTable() + " (app_id, tenant_id) ON DELETE CASCADE"
                + " );";
    }

    public static String getQueryToCreateSAMLClientsAppIdTenantIdIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS saml_clients_app_id_tenant_id_index ON "
                + Config.getConfig(start).getSAMLClientsTable() + " (app_id, tenant_id)";
    }

    public static String getQueryToCreateSAMLRelayStateTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getSAMLRelayStateTable();
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "relay_state VARCHAR(256) NOT NULL,"
                + "client_id VARCHAR(256) NOT NULL,"
                + "state TEXT NOT NULL,"
                + "redirect_uri TEXT NOT NULL,"
                + "created_at BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey")
                + " PRIMARY KEY(app_id, tenant_id, relay_state),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "app_id", "fkey") + " "
                + "FOREIGN KEY(app_id) "
                + "REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "tenant_id", "fkey") + " "
                + "FOREIGN KEY(app_id, tenant_id) "
                + "REFERENCES " + Config.getConfig(start).getTenantsTable() + " (app_id, tenant_id) ON DELETE CASCADE"
                + " );";
    }

    public static String getQueryToCreateSAMLRelayStateAppIdTenantIdIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS saml_relay_state_app_id_tenant_id_index ON "
                + Config.getConfig(start).getSAMLRelayStateTable() + " (app_id, tenant_id)";
    }

    public static String getQueryToCreateSAMLClaimsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = Config.getConfig(start).getSAMLClaimsTable();
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "app_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "tenant_id VARCHAR(64) NOT NULL DEFAULT 'public',"
                + "client_id VARCHAR(256) NOT NULL,"
                + "code VARCHAR(256) NOT NULL,"
                + "claims TEXT NOT NULL,"
                + "created_at BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey")
                + " PRIMARY KEY(app_id, tenant_id, code),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "app_id", "fkey") + " "
                + "FOREIGN KEY(app_id) "
                + "REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "tenant_id", "fkey") + " "
                + "FOREIGN KEY(app_id, tenant_id) "
                + "REFERENCES " + Config.getConfig(start).getTenantsTable() + " (app_id, tenant_id) ON DELETE CASCADE"
                + " );";
    }

    public static String getQueryToCreateSAMLClaimsAppIdTenantIdIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS saml_claims_app_id_tenant_id_index ON "
                + Config.getConfig(start).getSAMLClaimsTable() + " (app_id, tenant_id)";
    }

    public static void saveRelayStateInfo(Start start, TenantIdentifier tenantIdentifier,
                                          String relayState, String clientId, String state, String redirectURI)
            throws StorageQueryException, SQLException {
        String QUERY = "INSERT INTO " + getConfig(start).getSAMLRelayStateTable()
                + "(app_id, tenant_id, relay_state, client_id, state, redirect_uri, created_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?)";

        update(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, relayState);
            pst.setString(4, clientId);
            pst.setString(5, state);
            pst.setString(6, redirectURI);
            pst.setLong(7, System.currentTimeMillis());
        });
    }

    public static SAMLRelayStateInfo getRelayStateInfo(Start start, TenantIdentifier tenantIdentifier, String relayState)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT client_id, state, redirect_uri FROM " + getConfig(start).getSAMLRelayStateTable()
                + " WHERE app_id = ? AND tenant_id = ? AND relay_state = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, relayState);
        }, result -> {
            if (result.next()) {
                return new SAMLRelayStateInfo(
                        relayState,
                        result.getString("client_id"),
                        result.getString("state"),
                        result.getString("redirect_uri")
                );
            }
            return null;
        });
    }

    public static void saveSAMLClaims(Start start, TenantIdentifier tenantIdentifier, String clientId, String code, String claimsJson)
            throws StorageQueryException, SQLException {
        String QUERY = "INSERT INTO " + getConfig(start).getSAMLClaimsTable()
                + "(app_id, tenant_id, client_id, code, claims, created_at)"
                + " VALUES (?, ?, ?, ?, ?, ?)";

        update(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, clientId);
            pst.setString(4, code);
            pst.setString(5, claimsJson);
            pst.setLong(6, System.currentTimeMillis());
        });
    }

    public static SAMLClaimsInfo getSAMLClaimsAndRemoveCode(Start start, TenantIdentifier tenantIdentifier, String code)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT client_id, claims FROM " + getConfig(start).getSAMLClaimsTable()
                + " WHERE app_id = ? AND tenant_id = ? AND code = ?";

        SAMLClaimsInfo result = execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, code);
        }, rs -> {
            if (rs.next()) {
                try {
                    JsonParser jp = new JsonParser();
                    JsonObject claims = jp.parse(rs.getString("claims")).getAsJsonObject();
                    return new SAMLClaimsInfo(
                            rs.getString("client_id"),
                            claims
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        });

        if (result != null) {
            String DELETE_QUERY = "DELETE FROM " + getConfig(start).getSAMLClaimsTable()
                    + " WHERE app_id = ? AND tenant_id = ? AND code = ?";
            update(start, DELETE_QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, code);
            });
        }

        return result;
    }

    public static SAMLClient createOrUpdateSAMLClient(
            Start start,
            TenantIdentifier tenantIdentifier,
            String clientId,
            String clientSecret,
            String ssoLoginURL,
            String redirectURIsJson,
            String defaultRedirectURI,
            String idpEntityId,
            String idpSigningCertificate,
            boolean allowIDPInitiatedLogin,
            boolean enableRequestSigning)
            throws StorageQueryException, SQLException {
        String QUERY = "INSERT INTO " + getConfig(start).getSAMLClientsTable()
                + "(app_id, tenant_id, client_id, client_secret, sso_login_url, redirect_uris, default_redirect_uri,"
                + " idp_entity_id, idp_signing_certificate, allow_idp_initiated_login, enable_request_signing,"
                + " created_at, updated_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT (app_id, tenant_id, client_id)"
                + " DO UPDATE SET"
                + " client_secret = EXCLUDED.client_secret,"
                + " sso_login_url = EXCLUDED.sso_login_url,"
                + " redirect_uris = EXCLUDED.redirect_uris,"
                + " default_redirect_uri = EXCLUDED.default_redirect_uri,"
                + " idp_entity_id = EXCLUDED.idp_entity_id,"
                + " idp_signing_certificate = EXCLUDED.idp_signing_certificate,"
                + " allow_idp_initiated_login = EXCLUDED.allow_idp_initiated_login,"
                + " enable_request_signing = EXCLUDED.enable_request_signing,"
                + " updated_at = EXCLUDED.updated_at";

        long now = System.currentTimeMillis();
        update(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, clientId);
            pst.setString(4, clientSecret);
            pst.setString(5, ssoLoginURL);
            pst.setString(6, redirectURIsJson);
            pst.setString(7, defaultRedirectURI);
            pst.setString(8, idpEntityId);
            pst.setString(9, idpSigningCertificate);
            pst.setBoolean(10, allowIDPInitiatedLogin);
            pst.setBoolean(11, enableRequestSigning);
            pst.setLong(12, now);
            pst.setLong(13, now);
        });

        // Return the created/updated client
        return getSAMLClient(start, tenantIdentifier, clientId);
    }

    public static SAMLClient getSAMLClient(Start start, TenantIdentifier tenantIdentifier, String clientId)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT client_id, client_secret, sso_login_url, redirect_uris, default_redirect_uri,"
                + " idp_entity_id, idp_signing_certificate, allow_idp_initiated_login, enable_request_signing,"
                + " created_at, updated_at FROM " + getConfig(start).getSAMLClientsTable()
                + " WHERE app_id = ? AND tenant_id = ? AND client_id = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, clientId);
        }, result -> {
            if (result.next()) {
                try {
                    JsonParser jp = new JsonParser();
                    JsonArray redirectURIs = jp.parse(result.getString("redirect_uris")).getAsJsonArray();
                    return new SAMLClient(
                            result.getString("client_id"),
                            result.getString("client_secret"),
                            result.getString("sso_login_url"),
                            redirectURIs,
                            result.getString("default_redirect_uri"),
                            result.getString("idp_entity_id"),
                            result.getString("idp_signing_certificate"),
                            result.getBoolean("allow_idp_initiated_login"),
                            result.getBoolean("enable_request_signing")
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        });
    }

    public static SAMLClient getSAMLClientByIDPEntityId(Start start, TenantIdentifier tenantIdentifier, String idpEntityId) throws StorageQueryException, SQLException {
        String QUERY = "SELECT client_id, client_secret, sso_login_url, redirect_uris, default_redirect_uri,"
                + " idp_entity_id, idp_signing_certificate, allow_idp_initiated_login, enable_request_signing,"
                + " created_at, updated_at FROM " + getConfig(start).getSAMLClientsTable()
                + " WHERE app_id = ? AND tenant_id = ? AND idp_entity_id = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, idpEntityId);
        }, result -> {
            if (result.next()) {
                try {
                    JsonParser jp = new JsonParser();
                    JsonArray redirectURIs = jp.parse(result.getString("redirect_uris")).getAsJsonArray();
                    return new SAMLClient(
                            result.getString("client_id"),
                            result.getString("client_secret"),
                            result.getString("sso_login_url"),
                            redirectURIs,
                            result.getString("default_redirect_uri"),
                            result.getString("idp_entity_id"),
                            result.getString("idp_signing_certificate"),
                            result.getBoolean("allow_idp_initiated_login"),
                            result.getBoolean("enable_request_signing")
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        });
    }

    public static List<SAMLClient> getSAMLClients(Start start, TenantIdentifier tenantIdentifier)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT client_id, client_secret, sso_login_url, redirect_uris, default_redirect_uri,"
                + " idp_entity_id, idp_signing_certificate, allow_idp_initiated_login, enable_request_signing,"
                + " created_at, updated_at FROM " + getConfig(start).getSAMLClientsTable()
                + " WHERE app_id = ? AND tenant_id = ? ORDER BY created_at ASC";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
        }, result -> {
            List<SAMLClient> clients = new ArrayList<>();
            while (result.next()) {
                try {
                    JsonParser jp = new JsonParser();
                    JsonArray redirectURIs = jp.parse(result.getString("redirect_uris")).getAsJsonArray();
                    clients.add(new SAMLClient(
                            result.getString("client_id"),
                            result.getString("client_secret"),
                            result.getString("sso_login_url"),
                            redirectURIs,
                            result.getString("default_redirect_uri"),
                            result.getString("idp_entity_id"),
                            result.getString("idp_signing_certificate"),
                            result.getBoolean("allow_idp_initiated_login"),
                            result.getBoolean("enable_request_signing")
                    ));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return clients;
        });
    }

    public static boolean removeSAMLClient(Start start, TenantIdentifier tenantIdentifier, String clientId)
            throws StorageQueryException, SQLException {
        String QUERY = "DELETE FROM " + getConfig(start).getSAMLClientsTable()
                + " WHERE app_id = ? AND tenant_id = ? AND client_id = ?";

        int rowsAffected = update(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, clientId);
        });

        return rowsAffected > 0;
    }

    public static void removeExpiredSAMLCodesAndRelayStates(Start start) throws StorageQueryException, SQLException {
        long now = System.currentTimeMillis();
        long expiredBefore = now - (24 * 60 * 60 * 1000); // 24 hours

        // Remove expired relay states
        String RELAY_STATE_QUERY = "DELETE FROM " + getConfig(start).getSAMLRelayStateTable()
                + " WHERE created_at < ?";
        update(start, RELAY_STATE_QUERY, pst -> {
            pst.setLong(1, expiredBefore);
        });

        // Remove expired claims
        String CLAIMS_QUERY = "DELETE FROM " + getConfig(start).getSAMLClaimsTable()
                + " WHERE created_at < ?";
        update(start, CLAIMS_QUERY, pst -> {
            pst.setLong(1, expiredBefore);
        });
    }

    public static int countSAMLClients(Start start, TenantIdentifier tenantIdentifier) throws StorageQueryException, SQLException {
        String QUERY = "SELECT COUNT(*) as c FROM " + getConfig(start).getSAMLClientsTable()
                + " WHERE app_id = ? AND tenant_id = ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
        }, result -> {
            if (result.next()) {
                return result.getInt("c");
            }
            return 0;
        });
    }
}
