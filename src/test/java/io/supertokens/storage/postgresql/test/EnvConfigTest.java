/*
 *    Copyright (c) 2025, VRAI Labs and/or its affiliates. All rights reserved.
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

package io.supertokens.storage.postgresql.test;

import com.google.gson.JsonObject;
import io.supertokens.storage.postgresql.Start;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.Assert.*;

public class EnvConfigTest {

    private static void setEnv(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

    private static void removeEnv(String key) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.remove(key);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to remove environment variable", e);
        }
    }

    @Test
    public void testUpdateConfigJsonFromEnvHandlesBoxedTypes() {
        // Regression test: Integer fields like postgresql_minimum_idle_connections
        // were silently dropped because updateConfigJsonFromEnv only checked
        // primitive types (int.class) not boxed types (Integer.class).
        Start start = new Start();
        try {
            setEnv("POSTGRESQL_MINIMUM_IDLE_CONNECTIONS", "5");

            JsonObject configJson = new JsonObject();
            start.updateConfigJsonFromEnv(configJson);

            assertTrue("POSTGRESQL_MINIMUM_IDLE_CONNECTIONS env var should be loaded into configJson",
                    configJson.has("postgresql_minimum_idle_connections"));
            assertEquals(5, configJson.get("postgresql_minimum_idle_connections").getAsInt());
        } finally {
            removeEnv("POSTGRESQL_MINIMUM_IDLE_CONNECTIONS");
        }
    }

    @Test
    public void testUpdateConfigJsonFromEnvHandlesBoxedTypeWithZero() {
        // Tests that Integer value of 0 is correctly loaded (not confused with null/unset)
        Start start = new Start();
        try {
            setEnv("POSTGRESQL_MINIMUM_IDLE_CONNECTIONS", "0");

            JsonObject configJson = new JsonObject();
            start.updateConfigJsonFromEnv(configJson);

            assertTrue("POSTGRESQL_MINIMUM_IDLE_CONNECTIONS env var with value 0 should be loaded",
                    configJson.has("postgresql_minimum_idle_connections"));
            assertEquals(0, configJson.get("postgresql_minimum_idle_connections").getAsInt());
        } finally {
            removeEnv("POSTGRESQL_MINIMUM_IDLE_CONNECTIONS");
        }
    }
}
