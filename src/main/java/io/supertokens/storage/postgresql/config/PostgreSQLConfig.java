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

@JsonIgnoreProperties(ignoreUnknown = true)
public class PostgreSQLConfig {

    @JsonProperty
    private int postgresql_config_version = -1;

    @JsonProperty
    private int postgresql_connection_pool_size = 10;

    @JsonProperty
    private String postgresql_host = "localhost";

    @JsonProperty
    private int postgresql_port = 5432;

    @JsonProperty
    private String postgresql_user = null;

    @JsonProperty
    private String postgresql_password = null;

    @JsonProperty
    private String postgresql_database_name = "supertokens";

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

    public int getConnectionPoolSize() {
        return postgresql_connection_pool_size;
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

    public String getKeyValueTable() {
        String tableName = "key_value";
        if (postgresql_key_value_table_name != null) {
            return postgresql_key_value_table_name;
        }
        return addPrefixToTableName(tableName);
    }

    public String getSessionInfoTable() {
        String tableName = "session_info";
        if (postgresql_session_info_table_name != null) {
            return postgresql_session_info_table_name;
        }
        return addPrefixToTableName(tableName);
    }

    public String getUsersTable() {
        String tableName = "emailpassword_users";
        if (postgresql_emailpassword_users_table_name != null) {
            return postgresql_emailpassword_users_table_name;
        }
        return addPrefixToTableName(tableName);
    }

    public String getPasswordResetTokensTable() {
        String tableName = "emailpassword_pswd_reset_tokens";
        if (postgresql_emailpassword_pswd_reset_tokens_table_name != null) {
            return postgresql_emailpassword_pswd_reset_tokens_table_name;
        }
        return addPrefixToTableName(tableName);
    }

    public String getEmailVerificationTokensTable() {
        String tableName = "emailverification_tokens";
        if (postgresql_emailverification_tokens_table_name != null) {
            return postgresql_emailverification_tokens_table_name;
        }
        return addPrefixToTableName(tableName);
    }

    public String getEmailVerificationTable() {
        String tableName = "emailverification_verified_emails";
        if (postgresql_emailverification_verified_emails_table_name != null) {
            return postgresql_emailverification_verified_emails_table_name;
        }
        return addPrefixToTableName(tableName);
    }

    public String getThirdPartyUsersTable() {
        String tableName = "thirdparty_users";
        if (postgresql_thirdparty_users_table_name != null) {
            return postgresql_thirdparty_users_table_name;
        }
        return addPrefixToTableName(tableName);
    }

    private String addPrefixToTableName(String tableName) {
        if (!postgresql_table_names_prefix.trim().equals("")) {
            return postgresql_table_names_prefix.trim() + "_" + tableName;
        }
        return tableName;
    }

    void validateAndInitialise() {
        if (getUser() == null) {
            throw new QuitProgramFromPluginException(
                    "'postgresql_user' is not set in the config.yaml file. Please set this value and restart " +
                            "SuperTokens");
        }

        if (getPassword() == null) {
            throw new QuitProgramFromPluginException(
                    "'postgresql_password' is not set in the config.yaml file. Please set this value and restart " +
                            "SuperTokens");
        }
        if (getConnectionPoolSize() <= 0) {
            throw new QuitProgramFromPluginException(
                    "'postgresql_connection_pool_size' in the config.yaml file must be > 0");
        }
    }

}