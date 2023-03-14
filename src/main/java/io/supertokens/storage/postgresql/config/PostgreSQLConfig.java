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

package io.supertokens.storage.postgresql.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.supertokens.pluginInterface.exceptions.QuitProgramFromPluginException;

import java.net.URI;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PostgreSQLConfig {

    @JsonProperty
    private int postgresql_config_version = -1;

    @JsonProperty
    private int postgresql_connection_pool_size = 10;

    @JsonProperty
    private String postgresql_host = null;

    @JsonProperty
    private int postgresql_port = -1;

    @JsonProperty
    private String postgresql_user = null;

    @JsonProperty
    private String postgresql_password = null;

    @JsonProperty
    private String postgresql_database_name = null;

    @JsonProperty
    private String postgresql_key_value_table_name = null;

    @JsonProperty
    private String postgresql_session_info_table_name = null;

    @JsonProperty
    private String postgresql_emailpassword_users_table_name = null;

    @JsonProperty
    private String postgresql_emailpassword_pswd_reset_tokens_table_name = null;

    @JsonProperty
    private String postgresql_emailverification_tokens_table_name = null;

    @JsonProperty
    private String postgresql_emailverification_verified_emails_table_name = null;

    @JsonProperty
    private String postgresql_thirdparty_users_table_name = null;

    @JsonProperty
    private String postgresql_table_names_prefix = "";

    @JsonProperty
    private String postgresql_table_schema = "public";

    @JsonProperty
    private String postgresql_connection_uri = null;

    public String getTableSchema() {
        return postgresql_table_schema;
    }

    public int getConnectionPoolSize() {
        return postgresql_connection_pool_size;
    }

    public String getConnectionScheme() {
        if (postgresql_connection_uri != null) {
            URI uri = URI.create(postgresql_connection_uri);

            // sometimes if the scheme is missing, the host is returned as the scheme. To
            // prevent that,
            // we have a check
            String host = this.getHostName();
            if (uri.getScheme() != null && !uri.getScheme().equals(host)) {
                return uri.getScheme();
            }
        }
        return "postgresql";
    }

    public String getConnectionAttributes() {
        if (postgresql_connection_uri != null) {
            URI uri = URI.create(postgresql_connection_uri);
            String query = uri.getQuery();
            if (query != null) {
                if (query.contains("allowPublicKeyRetrieval=")) {
                    return query;
                } else {
                    return query + "&allowPublicKeyRetrieval=true";
                }
            }
        }
        return "allowPublicKeyRetrieval=true";
    }

    public String getHostName() {
        if (postgresql_host == null) {
            if (postgresql_connection_uri != null) {
                URI uri = URI.create(postgresql_connection_uri);
                if (uri.getHost() != null) {
                    return uri.getHost();
                }
            }
            return "localhost";
        }
        return postgresql_host;
    }

    public int getPort() {
        if (postgresql_port == -1) {
            if (postgresql_connection_uri != null) {
                URI uri = URI.create(postgresql_connection_uri);
                return uri.getPort();
            }
            return 5432;
        }
        return postgresql_port;
    }

    public String getUser() {
        if (postgresql_user == null) {
            if (postgresql_connection_uri != null) {
                URI uri = URI.create(postgresql_connection_uri);
                String userInfo = uri.getUserInfo();
                if (userInfo != null) {
                    String[] userInfoArray = userInfo.split(":");
                    if (userInfoArray.length > 0 && !userInfoArray[0].equals("")) {
                        return userInfoArray[0];
                    }
                }
            }
            return null;
        }
        return postgresql_user;
    }

    public String getPassword() {
        if (postgresql_password == null) {
            if (postgresql_connection_uri != null) {
                URI uri = URI.create(postgresql_connection_uri);
                String userInfo = uri.getUserInfo();
                if (userInfo != null) {
                    String[] userInfoArray = userInfo.split(":");
                    if (userInfoArray.length > 1 && !userInfoArray[1].equals("")) {
                        return userInfoArray[1];
                    }
                }
            }
            return null;
        }
        return postgresql_password;
    }

    public String getDatabaseName() {
        if (postgresql_database_name == null) {
            if (postgresql_connection_uri != null) {
                URI uri = URI.create(postgresql_connection_uri);
                String path = uri.getPath();
                if (path != null && !path.equals("") && !path.equals("/")) {
                    if (path.startsWith("/")) {
                        return path.substring(1);
                    }
                    return path;
                }
            }
            return "supertokens";
        }
        return postgresql_database_name;
    }

    public String getConnectionURI() {
        return postgresql_connection_uri;
    }

    public String getUsersTable() {
        return addSchemaAndPrefixToTableName("all_auth_recipe_users");
    }

    public String getKeyValueTable() {
        String tableName = "key_value";
        if (postgresql_key_value_table_name != null) {
            return addSchemaToTableName(postgresql_key_value_table_name);
        }
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getAccessTokenSigningKeysTable() {
        return addSchemaAndPrefixToTableName("session_access_token_signing_keys");
    }

    public String getSessionInfoTable() {
        String tableName = "session_info";
        if (postgresql_session_info_table_name != null) {
            return addSchemaToTableName(postgresql_session_info_table_name);
        }
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getEmailPasswordUsersTable() {
        String tableName = "emailpassword_users";
        if (postgresql_emailpassword_users_table_name != null) {
            return addSchemaToTableName(postgresql_emailpassword_users_table_name);
        }
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getPasswordResetTokensTable() {
        String tableName = "emailpassword_pswd_reset_tokens";
        if (postgresql_emailpassword_pswd_reset_tokens_table_name != null) {
            return addSchemaToTableName(postgresql_emailpassword_pswd_reset_tokens_table_name);
        }
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getEmailVerificationTokensTable() {
        String tableName = "emailverification_tokens";
        if (postgresql_emailverification_tokens_table_name != null) {
            return addSchemaToTableName(postgresql_emailverification_tokens_table_name);
        }
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getEmailVerificationTable() {
        String tableName = "emailverification_verified_emails";
        if (postgresql_emailverification_verified_emails_table_name != null) {
            return addSchemaToTableName(postgresql_emailverification_verified_emails_table_name);
        }
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getThirdPartyUsersTable() {
        String tableName = "thirdparty_users";
        if (postgresql_thirdparty_users_table_name != null) {
            return addSchemaToTableName(postgresql_thirdparty_users_table_name);
        }
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getPasswordlessUsersTable() {
        String tableName = "passwordless_users";
        return addSchemaAndPrefixToTableName(tableName);

    }

    public String getPasswordlessDevicesTable() {
        String tableName = "passwordless_devices";
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getPasswordlessCodesTable() {
        String tableName = "passwordless_codes";
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getJWTSigningKeysTable() {
        return addSchemaAndPrefixToTableName("jwt_signing_keys");
    }

    public String getUserMetadataTable() {
        return addSchemaAndPrefixToTableName("user_metadata");
    }

    public String getRolesTable() {
        return addSchemaAndPrefixToTableName("roles");
    }

    public String getUserRolesPermissionsTable() {
        return addSchemaAndPrefixToTableName("role_permissions");
    }

    public String getUserRolesTable() {
        return addSchemaAndPrefixToTableName("user_roles");
    }

    public String getUserIdMappingTable() {
        return addSchemaAndPrefixToTableName("userid_mapping");
    }

    public String getDashboardUsersTable() {
        return addSchemaAndPrefixToTableName("dashboard_users");
    }

    public String getDashboardSessionsTable() {
        return addSchemaAndPrefixToTableName("dashboard_user_sessions");
    }

    public String getTotpUsersTable() {
        return addSchemaAndPrefixToTableName("totp_users");
    }

    public String getTotpUserDevicesTable() {
        return addSchemaAndPrefixToTableName("totp_user_devices");
    }

    public String getTotpUsedCodesTable() {
        return addSchemaAndPrefixToTableName("totp_used_codes");
    }

    private String addSchemaAndPrefixToTableName(String tableName) {
        String name = tableName;
        if (!postgresql_table_names_prefix.trim().equals("")) {
            name = postgresql_table_names_prefix.trim() + "_" + name;
        }
        return addSchemaToTableName(name);
    }

    private String addSchemaToTableName(String tableName) {
        String name = tableName;
        if (!postgresql_table_schema.trim().equals("public")) {
            name = postgresql_table_schema.trim() + "." + name;
        }
        return name;
    }

    void validateAndInitialise() {
        if (postgresql_connection_uri != null) {
            try {
                URI ignored = URI.create(postgresql_connection_uri);
            } catch (Exception e) {
                throw new QuitProgramFromPluginException(
                        "The provided postgresql connection URI has an incorrect format. Please use a format like "
                                + "postgresql://[user[:[password]]@]host[:port][/dbname][?attr1=val1&attr2=val2...");
            }
        } else {
            if (this.getUser() == null) {
                throw new QuitProgramFromPluginException(
                        "'postgresql_user' and 'postgresql_connection_uri' are not set. Please set at least one of "
                                + "these values");
            }
        }

        if (getConnectionPoolSize() <= 0) {
            throw new QuitProgramFromPluginException(
                    "'postgresql_connection_pool_size' in the config.yaml file must be > 0");
        }
    }

}
