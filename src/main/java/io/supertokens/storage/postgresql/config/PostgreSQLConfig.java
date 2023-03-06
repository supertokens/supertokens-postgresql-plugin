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
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;

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
        if (postgresql_connection_uri != null) {
            String connectionAttributes = getConnectionAttributes();
            if (connectionAttributes.contains("currentSchema=")) {
                String[] splitted = connectionAttributes.split("currentSchema=");
                String valueStr = splitted[1];
                if (valueStr.contains("&")) {
                    return valueStr.split("&")[0];
                }
                return valueStr.trim();
            }
        }
        return postgresql_table_schema.trim();
    }

    public int getConnectionPoolSize() {
        return postgresql_connection_pool_size;
    }

    public String getConnectionScheme() {
        if (postgresql_connection_uri != null) {
            URI uri = URI.create(postgresql_connection_uri);

            // sometimes if the scheme is missing, the host is returned as the scheme. To prevent that,
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
        if (postgresql_connection_uri != null) {
            URI uri = URI.create(postgresql_connection_uri);
            if (uri.getHost() != null) {
                return uri.getHost();
            }
        }

        if (postgresql_host != null) {
            return postgresql_host;
        }
        return "localhost";
    }

    public int getPort() {
        if (postgresql_connection_uri != null) {
            URI uri = URI.create(postgresql_connection_uri);
            if (uri.getPort() > 0) {
                return uri.getPort();
            }
        }

        if (postgresql_port != -1) {
            return postgresql_port;
        }
        return 5432;
    }

    public String getUser() {
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

        if (postgresql_user != null) {
            return postgresql_user;
        }
        return null;
    }

    public String getPassword() {
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

        if (postgresql_password != null) {
            return postgresql_password;
        }
        return null;
    }

    public String getDatabaseName() {
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

        if (postgresql_database_name != null) {
            return postgresql_database_name;
        }
        return "supertokens";
    }

    public String getConnectionURI() {
        return postgresql_connection_uri;
    }

    public String getUsersTable() {
        return addSchemaAndPrefixToTableName("all_auth_recipe_users");
    }

    public String getAppsTable() {
        String tableName = "apps";
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getTenantsTable() {
        String tableName = "tenants";
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getTenantConfigsTable() {
        String tableName = "tenant_configs";
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getTenantThirdPartyProvidersTable() {
        String tableName = "tenant_thirdparty_providers";
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getTenantThirdPartyProviderClientsTable() {
        String tableName = "tenant_thirdparty_provider_clients";
        return addSchemaAndPrefixToTableName(tableName);
    }


    public String getKeyValueTable() {
        String tableName = "key_value";
        if (postgresql_key_value_table_name != null) {
            return addSchemaToTableName(postgresql_key_value_table_name);
        }
        return addSchemaAndPrefixToTableName(tableName);
    }

    public String getAppIdToUserIdTable() {
        return addSchemaAndPrefixToTableName("app_id_to_user_id");
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

    public String getEmailPasswordUserToTenantTable() {
        String tableName = "emailpassword_user_to_tenant";
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

    public String getTablePrefix() {
        return postgresql_table_names_prefix.trim();
    }

    private String addSchemaAndPrefixToTableName(String tableName) {
        String name = tableName;
        if (!getTablePrefix().equals("")) {
            name = getTablePrefix() + "_" + name;
        }
        return addSchemaToTableName(name);
    }

    private String addSchemaToTableName(String tableName) {
        String name = tableName;
        if (!getTableSchema().equals("public")) {
            name = getTableSchema() + "." + name;
        }
        return name;
    }

    void validate() throws InvalidConfigException {
        if (postgresql_connection_uri != null) {
            try {
                URI ignored = URI.create(postgresql_connection_uri);
            } catch (Exception e) {
                throw new InvalidConfigException(
                        "The provided postgresql connection URI has an incorrect format. Please use a format like "
                                + "postgresql://[user[:[password]]@]host[:port][/dbname][?attr1=val1&attr2=val2...");
            }
        } else {
            if (this.getUser() == null) {
                throw new InvalidConfigException(
                        "'postgresql_user' and 'postgresql_connection_uri' are not set. Please set at least one of "
                                + "these values");
            }
        }

        if (getConnectionPoolSize() <= 0) {
            throw new InvalidConfigException(
                    "'postgresql_connection_pool_size' in the config.yaml file must be > 0");
        }
    }

    void assertThatConfigFromSameUserPoolIsNotConflicting(PostgreSQLConfig otherConfig) throws InvalidConfigException {
        if (!otherConfig.getTablePrefix().equals(getTablePrefix())) {
            throw new InvalidConfigException(
                    "You cannot set different name for table prefix for the same user pool");
        }

        if (!otherConfig.getKeyValueTable().equals(getKeyValueTable())) {
            throw new InvalidConfigException(
                    "You cannot set different name for table " + getKeyValueTable() +
                            " for the same user pool");
        }
        if (!otherConfig.getSessionInfoTable().equals(getSessionInfoTable())) {
            throw new InvalidConfigException(
                    "You cannot set different name for table " + getSessionInfoTable() +
                            " for the same user pool");
        }

        if (!otherConfig.getEmailPasswordUsersTable().equals(getEmailPasswordUsersTable())) {
            throw new InvalidConfigException(
                    "You cannot set different name for table " + getEmailPasswordUsersTable() +
                            " for the same user pool");
        }

        if (!otherConfig.getPasswordResetTokensTable().equals(
                getPasswordResetTokensTable())) {
            throw new InvalidConfigException(
                    "You cannot set different name for table " + getPasswordResetTokensTable() +
                            " for the same user pool");
        }

        if (!otherConfig.getEmailVerificationTokensTable().equals(
                getEmailVerificationTokensTable())) {
            throw new InvalidConfigException(
                    "You cannot set different name for table " + getEmailVerificationTokensTable() +
                            " for the same user pool");
        }

        if (!otherConfig.getEmailVerificationTable().equals(
                getEmailVerificationTable())) {
            throw new InvalidConfigException(
                    "You cannot set different name for table " +
                            getEmailVerificationTable() +
                            " for the same user pool");
        }

        if (!otherConfig.getThirdPartyUsersTable().equals(getThirdPartyUsersTable())) {
            throw new InvalidConfigException(
                    "You cannot set different name for table " + getThirdPartyUsersTable() +
                            " for the same user pool");
        }
    }
}