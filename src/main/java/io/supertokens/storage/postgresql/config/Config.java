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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.JsonObject;
import io.supertokens.pluginInterface.LOG_LEVEL;
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.ResourceDistributor;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.output.Logging;
import io.supertokens.storage.postgresql.utils.ConfigMapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Config extends ResourceDistributor.SingletonResource {

    private static final String RESOURCE_KEY = "io.supertokens.storage.postgresql.config.Config";
    private final PostgreSQLConfig config;
    private final Start start;
    private Set<LOG_LEVEL> logLevels;

    private Config(Start start, JsonObject configJson, Set<LOG_LEVEL> logLevels) throws InvalidConfigException {
        this.start = start;
        this.logLevels = logLevels;
        try {
            config = loadPostgreSQLConfig(configJson);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Config getInstance(Start start) {
        return (Config) start.getResourceDistributor().getResource(RESOURCE_KEY);
    }

    public static void loadConfig(Start start, JsonObject configJson, Set<LOG_LEVEL> logLevels, TenantIdentifier tenantIdentifier)
            throws InvalidConfigException {
        if (getInstance(start) != null) {
            return;
        }
        start.getResourceDistributor().setResource(RESOURCE_KEY, new Config(start, configJson, logLevels));
        Logging.info(start, "Loading PostgreSQL config.", tenantIdentifier.equals(TenantIdentifier.BASE_TENANT));
    }

    public static String getUserPoolId(Start start) {
        // TODO: The way things are implemented right now, this function has the issue that if the user points to the
        //  same database, but with a different host (cause the db is reachable via two hosts as an example),
        //  then it will return two different user pool IDs - which is technically the wrong thing to do.
        return getConfig(start).getUserPoolId();
    }

    public static String getConnectionPoolId(Start start) {
        return getConfig(start).getConnectionPoolId();
    }

    public static void assertThatConfigFromSameUserPoolIsNotConflicting(Start start, JsonObject otherConfigJson)
            throws InvalidConfigException {
        Set<LOG_LEVEL> temp = new HashSet();
        temp.add(LOG_LEVEL.NONE);
        PostgreSQLConfig otherConfig = new Config(start, otherConfigJson, temp).config;
        PostgreSQLConfig thisConfig = getConfig(start);
        thisConfig.assertThatConfigFromSameUserPoolIsNotConflicting(otherConfig);
    }

    public static PostgreSQLConfig getConfig(Start start) {
        if (getInstance(start) == null) {
            throw new IllegalStateException("Please call loadConfig() before calling getConfig()");
        }
        return getInstance(start).config;
    }

    public static Set<LOG_LEVEL> getLogLevels(Start start) {
        return getInstance(start).logLevels;
    }

    public static void setLogLevels(Start start, Set<LOG_LEVEL> logLevels) {
        getInstance(start).logLevels = logLevels;
    }

    private PostgreSQLConfig loadPostgreSQLConfig(JsonObject configJson) throws IOException, InvalidConfigException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        PostgreSQLConfig config = mapper.readValue(configJson.toString(), PostgreSQLConfig.class);
        config.validateAndNormalise();
        return config;
    }

    public static boolean canBeUsed(JsonObject configJson) throws InvalidConfigException {
        try {
            PostgreSQLConfig config = ConfigMapper.mapConfig(configJson, PostgreSQLConfig.class);
            return config.getConnectionURI() != null || config.getUser() != null || config.getPassword() != null;
        } catch (InvalidConfigException e) {
            throw e;
        } catch (Exception e) {
            return false;
        }
    }

}
