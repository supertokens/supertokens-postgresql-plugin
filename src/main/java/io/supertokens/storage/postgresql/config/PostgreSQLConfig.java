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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.supertokens.pluginInterface.ConfigFieldInfo;
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.annotations.*;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PostgreSQLConfig {

    @JsonProperty
    @IgnoreForAnnotationCheck
    private int postgresql_config_version = -1;

    @JsonProperty
    @ConnectionPoolProperty
    @DashboardInfo(
            description = "Defines the connection pool size to PostgreSQL. Please see https://github" +
                    ".com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing",
            defaultValue = "10", isOptional = true, isEditable = true)
    private int postgresql_connection_pool_size = 10;

    @JsonProperty
    @UserPoolProperty
    @DashboardInfo(
            description = "Specify the postgresql host url here. For example: - \"localhost\" - \"192.168.0.1\" - " +
                    "\"<IP to cloud instance>\" - \"example.com\"",
            defaultValue = "\"localhost\"", isOptional = true)
    private String postgresql_host = null;

    @JsonProperty
    @UserPoolProperty
    @DashboardInfo(description = "Specify the port to use when connecting to PostgreSQL instance.",
            defaultValue = "5432", isOptional = true)
    private int postgresql_port = -1;

    @JsonProperty
    @ConnectionPoolProperty
    @DashboardInfo(
            description = "The PostgreSQL user to use to query the database. If the relevant tables are not already " +
                    "created by you, this user should have the ability to create new tables. To see the tables " +
                    "needed, visit: https://supertokens.com/docs/thirdpartyemailpassword/pre-built-ui/setup/database" +
                    "-setup/postgresql",
            defaultValue = "null")
    private String postgresql_user = null;

    @JsonProperty
    @ConnectionPoolProperty
    @DashboardInfo(
            description = "Password for the PostgreSQL user. If you have not set a password make this an empty string.",
            defaultValue = "null")
    private String postgresql_password = null;

    @JsonProperty
    @UserPoolProperty
    @DashboardInfo(description = "The database name to store SuperTokens related data.",
            defaultValue = "\"supertokens\"", isOptional = true)
    private String postgresql_database_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    @DashboardInfo(
            description = "A prefix to add to all table names managed by SuperTokens. An \"_\" will be added between " +
                    "this prefix and the actual table name if the prefix is defined.",
            defaultValue = "\"\"", isOptional = true)
    private String postgresql_table_names_prefix = "";

    @JsonProperty
    @NotConflictingWithinUserPool
    @DashboardInfo(
            description = "Specify the name of the table that will store secret keys and app info necessary for the " +
                    "functioning sessions.",
            defaultValue = "\"key_value\"", isOptional = true)
    private String postgresql_key_value_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    @DashboardInfo(description = "Specify the name of the table that will store the session info for users.",
            defaultValue = "\"session_info\"", isOptional = true)
    private String postgresql_session_info_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    @DashboardInfo(
            description = "Specify the name of the table that will store the user information, along with their email" +
                    " and hashed password.",
            defaultValue = "\"emailpassword_users\"", isOptional = true)
    private String postgresql_emailpassword_users_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    @DashboardInfo(description = "Specify the name of the table that will store the password reset tokens for users.",
            defaultValue = "\"emailpassword_pswd_reset_tokens\"", isOptional = true)
    private String postgresql_emailpassword_pswd_reset_tokens_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    @DashboardInfo(
            description = "Specify the name of the table that will store the email verification tokens for users.",
            defaultValue = "\"emailverification_tokens\"", isOptional = true)
    private String postgresql_emailverification_tokens_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    @DashboardInfo(description = "Specify the name of the table that will store the verified email addresses.",
            defaultValue = "\"emailverification_verified_emails\"", isOptional = true)
    private String postgresql_emailverification_verified_emails_table_name = null;

    @JsonProperty
    @NotConflictingWithinUserPool
    @DashboardInfo(description = "Specify the name of the table that will store the thirdparty recipe users.",
            defaultValue = "\"thirdparty_users\"", isOptional = true)
    private String postgresql_thirdparty_users_table_name = null;

    @JsonProperty
    @UserPoolProperty
    @DashboardInfo(description = "The schema for tables.", defaultValue = "\"public\"", isOptional = true)
    private String postgresql_table_schema = "public";

    @JsonProperty
    @IgnoreForAnnotationCheck
    @DashboardInfo(
            description = "Specify the PostgreSQL connection URI in the following format: " +
                    "postgresql://[user[:[password]]@]host[:port][/dbname][?attr1=val1&attr2=val2... Values provided " +
                    "via other configs will override values provided by this config.",
            defaultValue = "null", isOptional = true)
    private String postgresql_connection_uri = null;

    @ConnectionPoolProperty
    @DashboardInfo(description = "The connection attributes of the PostgreSQL database.",
            defaultValue = "\"allowPublicKeyRetrieval=true\"", isOptional = true)
    private String postgresql_connection_attributes = "allowPublicKeyRetrieval=true";

    @ConnectionPoolProperty
    @DashboardInfo(description = "The scheme of the PostgreSQL database.", defaultValue = "\"postgresql\"",
            isOptional = true)
    private String postgresql_connection_scheme = "postgresql";

    @JsonProperty
    @ConnectionPoolProperty
    @DashboardInfo(description = "Timeout in milliseconds for the idle connections to be closed.",
            defaultValue = "60000", isOptional = true, isEditable = true)
    private long postgresql_idle_connection_timeout = 60000;

    @JsonProperty
    @ConnectionPoolProperty
    @DashboardInfo(
            description = "Minimum number of idle connections to be kept active. If not set, minimum idle connections" +
                    " will be same as the connection pool size.",
            defaultValue = "null", isOptional = true, isEditable = true)
    private Integer postgresql_minimum_idle_connections = null;

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

    public static ArrayList<ConfigFieldInfo> getConfigFieldsInfoForDashboard(Start start) {
        ArrayList<ConfigFieldInfo> result = new ArrayList<ConfigFieldInfo>();

        JsonObject tenantConfig = new Gson().toJsonTree(Config.getConfig(start)).getAsJsonObject();

        PostgreSQLConfig defaultConfigObj = new PostgreSQLConfig();
        try {
            defaultConfigObj.validateAndNormalise(true); // skip validation and just populate defaults
        } catch (InvalidConfigException e) {
            throw new IllegalStateException(e); // should never happen
        }

        JsonObject defaultConfig = new Gson().toJsonTree(defaultConfigObj).getAsJsonObject();

        for (String fieldId : PostgreSQLConfig.getValidFields()) {
            try {
                Field field = PostgreSQLConfig.class.getDeclaredField(fieldId);
                if (!field.isAnnotationPresent(DashboardInfo.class)) {
                    continue;
                }

                if (field.getName().endsWith("_table_name")) {
                    continue; // do not show
                }

                String key = field.getName();
                String description = field.isAnnotationPresent(DashboardInfo.class)
                        ? field.getAnnotation(DashboardInfo.class).description()
                        : "";
                boolean isDifferentAcrossTenants = true;

                String valueType = null;

                Class<?> fieldType = field.getType();

                if (fieldType == String.class) {
                    valueType = "string";
                } else if (fieldType == boolean.class) {
                    valueType = "boolean";
                } else if (fieldType == int.class || fieldType == long.class || fieldType == Integer.class) {
                    valueType = "number";
                } else {
                    throw new RuntimeException("Unknown field type " + fieldType.getName());
                }

                JsonElement value = tenantConfig.get(field.getName());

                JsonElement defaultValue = defaultConfig.get(field.getName());
                boolean isNullable = defaultValue == null;

                boolean isEditable = field.getAnnotation(DashboardInfo.class).isEditable();

                result.add(new ConfigFieldInfo(
                        key, valueType, value, description, isDifferentAcrossTenants,
                        null, isNullable, defaultValue, true, isEditable));

            } catch (NoSuchFieldException e) {
                continue;
            }
        }
        return result;
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

    public long getIdleConnectionTimeout() {
        return postgresql_idle_connection_timeout;
    }

    public Integer getMinimumIdleConnections() {
        return postgresql_minimum_idle_connections;
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
    public String getOAuthClientsTable() {
        return addSchemaAndPrefixToTableName("oauth_clients");
    }

    public String getOAuthM2MTokensTable() {
        return addSchemaAndPrefixToTableName("oauth_m2m_tokens");
    }

    public String getOAuthSessionsTable() {
        return addSchemaAndPrefixToTableName("oauth_sessions");
    }

    public String getOAuthLogoutChallengesTable() {
        return addSchemaAndPrefixToTableName("oauth_logout_challenges");
    }

    public String getBulkImportUsersTable() {
        return addSchemaAndPrefixToTableName("bulk_import_users");
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
        validateAndNormalise(false);
    }

    private void validateAndNormalise(boolean skipValidation) throws InvalidConfigException {
        if (isValidAndNormalised) {
            return;
        }

        if (!skipValidation) {
            if (postgresql_connection_uri != null) {
                try {
                    URI ignored = URI.create(postgresql_connection_uri);
                } catch (Exception e) {
                    throw new InvalidConfigException(
                            "The provided postgresql connection URI has an incorrect format. Please use a format like "
                                    +
                                    "postgresql://[user[:[password]]@]host[:port][/dbname][?attr1=val1&attr2=val2...");
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

            if (postgresql_minimum_idle_connections != null) {
                if (postgresql_minimum_idle_connections < 0) {
                    throw new InvalidConfigException(
                            "'postgresql_minimum_idle_connections' must be >= 0");
                }

                if (postgresql_minimum_idle_connections > postgresql_connection_pool_size) {
                    throw new InvalidConfigException(
                            "'postgresql_minimum_idle_connections' must be less than or equal to "
                                    + "'postgresql_connection_pool_size'");
                }
            }
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
            postgresql_emailpassword_pswd_reset_tokens_table_name = addSchemaToTableName(
                    postgresql_emailpassword_pswd_reset_tokens_table_name);
        } else {
            postgresql_emailpassword_pswd_reset_tokens_table_name = addSchemaAndPrefixToTableName(
                    "emailpassword_pswd_reset_tokens");
        }

        if (postgresql_emailverification_tokens_table_name != null) {
            postgresql_emailverification_tokens_table_name = addSchemaToTableName(
                    postgresql_emailverification_tokens_table_name);
        } else {
            postgresql_emailverification_tokens_table_name = addSchemaAndPrefixToTableName("emailverification_tokens");
        }

        if (postgresql_emailverification_verified_emails_table_name != null) {
            postgresql_emailverification_verified_emails_table_name = addSchemaToTableName(
                    postgresql_emailverification_verified_emails_table_name);
        } else {
            postgresql_emailverification_verified_emails_table_name = addSchemaAndPrefixToTableName(
                    "emailverification_verified_emails");
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
                try {
                    String fieldName = field.getName();
                    String fieldValue = field.get(this) != null ? field.get(this).toString() : null;
                    if (fieldValue == null) {
                        continue;
                    }
                    // To ensure a unique connectionPoolId we include the database password and use the "|db_pass|"
                    // identifier.
                    // This facilitates easy removal of the password from logs when necessary.
                    if (fieldName.equals("postgresql_password")) {
                        connectionPoolId.append("|db_pass|" + fieldValue + "|db_pass");
                    } else {
                        connectionPoolId.append("|" + fieldValue);
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return connectionPoolId.toString();
    }
}
