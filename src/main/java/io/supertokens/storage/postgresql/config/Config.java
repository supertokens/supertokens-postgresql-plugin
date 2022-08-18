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
import io.supertokens.pluginInterface.LOG_LEVEL;
import io.supertokens.pluginInterface.exceptions.QuitProgramFromPluginException;
import io.supertokens.storage.postgresql.ResourceDistributor;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.output.Logging;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class Config extends ResourceDistributor.SingletonResource {

    private static final String RESOURCE_KEY = "io.supertokens.storage.postgresql.config.Config";
    private final PostgreSQLConfig config;
    private final Start start;
    private final Set<LOG_LEVEL> logLevels;

    private Config(Start start, String configFilePath, Set<LOG_LEVEL> logLevels) {
        this.start = start;
        this.logLevels = logLevels;
        try {
            config = loadPostgreSQLConfig(configFilePath);
        } catch (IOException e) {
            throw new QuitProgramFromPluginException(e);
        }
    }

    private static Config getInstance(Start start) {
        return (Config) start.getResourceDistributor().getResource(RESOURCE_KEY);
    }

    public static void loadConfig(Start start, String configFilePath, Set<LOG_LEVEL> logLevels) {
        if (getInstance(start) != null) {
            return;
        }
        start.getResourceDistributor().setResource(RESOURCE_KEY, new Config(start, configFilePath, logLevels));
        Logging.info(start, "Loading PostgreSQL config.", true);
    }

    public static PostgreSQLConfig getConfig(Start start) {
        if (getInstance(start) == null) {
            throw new QuitProgramFromPluginException("Please call loadConfig() before calling getConfig()");
        }
        return getInstance(start).config;
    }

    public static Set<LOG_LEVEL> getLogLevels(Start start) {
        return getInstance(start).logLevels;
    }

    private PostgreSQLConfig loadPostgreSQLConfig(String configFilePath) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        PostgreSQLConfig config = mapper.readValue(new File(configFilePath), PostgreSQLConfig.class);
        config.validateAndInitialise();
        return config;
    }

    public static boolean canBeUsed(String configFilePath) {
        try {
            final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            PostgreSQLConfig config = mapper.readValue(new File(configFilePath), PostgreSQLConfig.class);
            return config.getUser() != null || config.getPassword() != null || config.getConnectionURI() != null;
        } catch (Exception e) {
            return false;
        }
    }

}
