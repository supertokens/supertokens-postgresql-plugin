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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;
import io.supertokens.storage.postgresql.annotations.ConnectionPoolProperty;
import io.supertokens.storage.postgresql.annotations.IgnoreForAnnotationCheck;
import io.supertokens.storage.postgresql.annotations.NotConflictingWithinUserPool;
import io.supertokens.storage.postgresql.annotations.UserPoolProperty;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PostgreSQLConfig {

    @JsonProperty
    @IgnoreForAnnotationCheck
    private int postgresql_config_version = -1;

    @JsonProperty
    @ConnectionPoolProperty
    private int postgresql_connection_pool_size = 10;

    @JsonProperty
    @UserPoolProperty
    private String postgresql_host = null;

    @JsonProperty
    @UserPoolProperty
    private int postgresql_port = -1;

    @JsonProperty
    @ConnectionPoolProperty
    private String postgresql_user = null;

    @JsonProperty
    @ConnectionPoolProperty
    private String postgresql_password = null;

    @JsonProperty
    @UserPoolProperty
    private String postgresql_database_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    private String postgresql_table_names_prefix = "";

    @JsonProperty
    @NotConflictingWithinUserPool
    private String postgresql_key_value_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    private String postgresql_session_info_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    private String postgresql_emailpassword_users_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    private String postgresql_emailpassword_pswd_reset_tokens_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    private String postgresql_emailverification_tokens_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    private String postgresql_emailverification_verified_emails_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    private String postgresql_thirdparty_users_table_name = null;

    @JsonProperty
    @UserPoolProperty
    private String postgresql_table_schema = "public";

    @JsonProperty
    @IgnoreForAnnotationCheck
    private String postgresql_connection_uri = null;

    @ConnectionPoolProperty
    private String postgresql_connection_attributes = "allowPublicKeyRetrieval=true";

    @ConnectionPoolProperty
    private String postgresql_connection_scheme = "postgresql";

    @IgnoreForAnnotationCheck
    boolean isValidAndNormalised = false;

    public static Set<String> getValidFields() {
        PostgreSQLConfig config = new PostgreSQLConfig();
        JsonObject configObj = new GsonBuilder().serializeNulls().create().toJsonTree(config).getAsJsonObject();

        Set<String> validFields = new HashSet<>();
        for (Map.Entry<String, JsonElement> entry : configObj.entrySet()) {
            validFields.add(entry.getKey());
        }
        return validFields;
    }

    public String getTableSchema() {
        return postgresql_table_schema;
    }

    public int getConnectionPoolSize() {
        return postgresql_connection_pool_size;
    }

    public String getConnectionScheme() {
        return postgresql_connection_scheme;
    }

    public String getConnectionAttributes() {
        return postgresql_connection_attributes;
    }

    public String getHostName() {
        return postgresql_host;
    }

    public int getPort() {
        return postgresql_port;
    }

    public String getUser() {
        return postgresql_user;
    }

    public String getPassword() {
        return postgresql_password;
    }

    public String getDatabaseName() {
        return postgresql_database_name;
    }

    public String getConnectionURI() {
        return postgresql_connection_uri;
    }

    public String getUsersTable() {
        return addSchemaAndPrefixToTableName("all_auth_recipe_users");
    }

    public String getAppsTable() {
        return addSchemaAndPrefixToTableName("apps");
    }

    public String getTenantsTable() {
        return addSchemaAndPrefixToTableName("tenants");
    }

    public String getTenantConfigsTable() {
        return addSchemaAndPrefixToTableName("tenant_configs");
    }

    public String getTenantFirstFactorsTable() {
        return addSchemaAndPrefixToTableName("tenant_first_factors");
    }

    public String getTenantRequiredSecondaryFactorsTable() {
        return addSchemaAndPrefixToTableName("tenant_required_secondary_factors");
    }

    public String getTenantThirdPartyProvidersTable() {
        return addSchemaAndPrefixToTableName("tenant_thirdparty_providers");
    }

    public String getTenantThirdPartyProviderClientsTable() {
        return addSchemaAndPrefixToTableName("tenant_thirdparty_provider_clients");
    }

    public String getKeyValueTable() {
        return postgresql_key_value_table_name;
    }

    public String getAppIdToUserIdTable() {
        return addSchemaAndPrefixToTableName("app_id_to_user_id");
    }

    public String getUserLastActiveTable() {
        return addSchemaAndPrefixToTableName("user_last_active");
    }

    public String getAccessTokenSigningKeysTable() {
        return addSchemaAndPrefixToTableName("session_access_token_signing_keys");
    }

    public String getSessionInfoTable() {
        return postgresql_session_info_table_name;
    }

    public String getEmailPasswordUserToTenantTable() {
        return addSchemaAndPrefixToTableName("emailpassword_user_to_tenant");
    }

    public String getEmailPasswordUsersTable() {
        return postgresql_emailpassword_users_table_name;
    }

    public String getPasswordResetTokensTable() {
        return postgresql_emailpassword_pswd_reset_tokens_table_name;
    }

    public String getEmailVerificationTokensTable() {
        return postgresql_emailverification_tokens_table_name;
    }

    public String getEmailVerificationTable() {
        return postgresql_emailverification_verified_emails_table_name;
    }

    public String getThirdPartyUsersTable() {
        return postgresql_thirdparty_users_table_name;
    }

    public String getThirdPartyUserToTenantTable() {
        return addSchemaAndPrefixToTableName("thirdparty_user_to_tenant");
    }

    public String getPasswordlessUsersTable() {
        return addSchemaAndPrefixToTableName("passwordless_users");
    }

    public String getPasswordlessUserToTenantTable() {
        return addSchemaAndPrefixToTableName("passwordless_user_to_tenant");
    }

    public String getPasswordlessDevicesTable() {
        return addSchemaAndPrefixToTableName("passwordless_devices");
    }

    public String getPasswordlessCodesTable() {
        return addSchemaAndPrefixToTableName("passwordless_codes");
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
        return postgresql_table_names_prefix;
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
        return addSchemaToTableName(postgresql_table_names_prefix + tableName);
    }

    private String addSchemaToTableName(String tableName) {
        String name = tableName;
        if (!getTableSchema().equals("public")) {
            name = getTableSchema() + "." + name;
        }
        return name;
    }

    public void validateAndNormalise() throws InvalidConfigException {
        if (isValidAndNormalised) {
            return;
        }

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

        if (postgresql_connection_pool_size <= 0) {
            throw new InvalidConfigException(
                    "'postgresql_connection_pool_size' in the config.yaml file must be > 0");
        }

        // Normalisation
        if (postgresql_connection_uri != null) {
            { // postgresql_connection_attributes
                URI uri = URI.create(postgresql_connection_uri);
                String query = uri.getQuery();
                if (query != null) {
                    if (query.contains("allowPublicKeyRetrieval=")) {
                        postgresql_connection_attributes = query;
                    } else {
                        postgresql_connection_attributes = query + "&allowPublicKeyRetrieval=true";
                    }
                }
            }

            { // postgresql_table_schema
                String connectionAttributes = postgresql_connection_attributes;
                if (connectionAttributes.contains("currentSchema=")) {
                    String[] splitted = connectionAttributes.split("currentSchema=");
                    String valueStr = splitted[1];
                    if (valueStr.contains("&")) {
                        postgresql_table_schema = valueStr.split("&")[0];
                    } else {
                        postgresql_table_schema = valueStr;
                    }
                    postgresql_table_schema = postgresql_table_schema.trim();
                }
            }

            { // postgresql_host
                URI uri = URI.create(postgresql_connection_uri);
                if (uri.getHost() != null) {
                    postgresql_host = uri.getHost();
                }
            }

            { // postgresql_port
                URI uri = URI.create(postgresql_connection_uri);
                if (uri.getPort() > 0) {
                    postgresql_port = uri.getPort();
                }
            }

            { // postgresql_connection_scheme
                URI uri = URI.create(postgresql_connection_uri);

                // sometimes if the scheme is missing, the host is returned as the scheme. To
                // prevent that,
                // we have a check
                String host = postgresql_host;
                if (uri.getScheme() != null && !uri.getScheme().equals(host)) {
                    postgresql_connection_scheme = uri.getScheme();
                }
            }

            {
                URI uri = URI.create(postgresql_connection_uri);
                String userInfo = uri.getUserInfo();
                if (userInfo != null) {
                    String[] userInfoArray = userInfo.split(":");
                    if (userInfoArray.length > 0 && !userInfoArray[0].equals("")) {
                        postgresql_user = userInfoArray[0];
                    }
                }
            }

            {
                URI uri = URI.create(postgresql_connection_uri);
                String userInfo = uri.getUserInfo();
                if (userInfo != null) {
                    String[] userInfoArray = userInfo.split(":");
                    if (userInfoArray.length > 1 && !userInfoArray[1].equals("")) {
                        postgresql_password = userInfoArray[1];
                    }
                }
            }

            {
                URI uri = URI.create(postgresql_connection_uri);
                String path = uri.getPath();
                if (path != null && !path.equals("") && !path.equals("/")) {
                    if (path.startsWith("/")) {
                        postgresql_database_name = path.substring(1);
                    } else {
                        postgresql_database_name = path;
                    }
                }
            }
        }

        { // postgresql_table_names_prefix
            if (postgresql_table_names_prefix == null) {
                postgresql_table_names_prefix = "";
            }
            postgresql_table_names_prefix = postgresql_table_names_prefix.trim();
            if (!postgresql_table_names_prefix.isEmpty()) {
                postgresql_table_names_prefix = postgresql_table_names_prefix + "_";
            }
        }

        { // postgresql_table_schema
            postgresql_table_schema = postgresql_table_schema.trim();
        }

        { // postgresql_connection_scheme
            postgresql_connection_scheme = postgresql_connection_scheme.trim();
        }

        { // postgresql_host
            if (postgresql_host == null) {
                postgresql_host = "localhost";
            }
        }

        { // postgresql_port
            if (postgresql_port < 0) {
                postgresql_port = 5432;
            }
        }

        { // postgresql_database_name
            if (postgresql_database_name == null) {
                postgresql_database_name = "supertokens";
            }
            postgresql_database_name = postgresql_database_name.trim();
        }

        if (postgresql_key_value_table_name != null) {
            postgresql_key_value_table_name = addSchemaToTableName(postgresql_key_value_table_name);
        } else {
            postgresql_key_value_table_name = addSchemaAndPrefixToTableName("key_value");
        }

        if (postgresql_session_info_table_name != null) {
            postgresql_session_info_table_name = addSchemaToTableName(postgresql_session_info_table_name);
        } else {
            postgresql_session_info_table_name = addSchemaAndPrefixToTableName("session_info");
        }

        if (postgresql_emailpassword_users_table_name != null) {
            postgresql_emailpassword_users_table_name = addSchemaToTableName(postgresql_emailpassword_users_table_name);
        } else {
            postgresql_emailpassword_users_table_name = addSchemaAndPrefixToTableName("emailpassword_users");
        }

        if (postgresql_emailpassword_pswd_reset_tokens_table_name != null) {
            postgresql_emailpassword_pswd_reset_tokens_table_name = addSchemaToTableName(postgresql_emailpassword_pswd_reset_tokens_table_name);
        } else {
            postgresql_emailpassword_pswd_reset_tokens_table_name = addSchemaAndPrefixToTableName("emailpassword_pswd_reset_tokens");
        }

        if (postgresql_emailverification_tokens_table_name != null) {
            postgresql_emailverification_tokens_table_name = addSchemaToTableName(postgresql_emailverification_tokens_table_name);
        } else {
            postgresql_emailverification_tokens_table_name = addSchemaAndPrefixToTableName("emailverification_tokens");
        }

        if (postgresql_emailverification_verified_emails_table_name != null) {
            postgresql_emailverification_verified_emails_table_name = addSchemaToTableName(postgresql_emailverification_verified_emails_table_name);
        } else {
            postgresql_emailverification_verified_emails_table_name = addSchemaAndPrefixToTableName("emailverification_verified_emails");
        }

        if (postgresql_thirdparty_users_table_name != null) {
            postgresql_thirdparty_users_table_name = addSchemaToTableName(postgresql_thirdparty_users_table_name);
        } else {
            postgresql_thirdparty_users_table_name = addSchemaAndPrefixToTableName("thirdparty_users");
        }

        isValidAndNormalised = true;
    }

    void assertThatConfigFromSameUserPoolIsNotConflicting(PostgreSQLConfig otherConfig) throws InvalidConfigException {
        for (Field field : PostgreSQLConfig.class.getDeclaredFields()) {
            if (field.isAnnotationPresent(NotConflictingWithinUserPool.class)) {
                try {
                    if (!Objects.equals(field.get(this), field.get(otherConfig))) {
                        throw new InvalidConfigException(
                                "You cannot set different values for " + field.getName() +
                                        " for the same user pool");
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public String getUserPoolId() {
        StringBuilder userPoolId = new StringBuilder();
        for (Field field : PostgreSQLConfig.class.getDeclaredFields()) {
            if (field.isAnnotationPresent(UserPoolProperty.class)) {
                userPoolId.append("|");
                try {
                    if (field.get(this) != null) {
                        userPoolId.append(field.get(this).toString());
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return userPoolId.toString();
    }

    public String getConnectionPoolId() {
        StringBuilder connectionPoolId = new StringBuilder();
        for (Field field : PostgreSQLConfig.class.getDeclaredFields()) {
            if (field.isAnnotationPresent(ConnectionPoolProperty.class)) {
                connectionPoolId.append("|");
                try {
                    if (field.get(this) != null) {
                        connectionPoolId.append(field.get(this).toString());
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return connectionPoolId.toString();
    }
}
