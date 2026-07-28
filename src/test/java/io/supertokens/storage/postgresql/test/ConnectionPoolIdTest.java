/*
 *    Copyright (c) 2026, VRAI Labs and/or its affiliates. All rights reserved.
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

package io.supertokens.storage.postgresql.test;

import com.google.gson.JsonObject;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storage.postgresql.utils.ConfigMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

/**
 * getConnectionPoolId() becomes the HikariCP pool name, which leaks into Hikari log lines,
 * exception messages and telemetry exports. These tests pin the security property that the raw
 * database password never appears in it, while still contributing to pool identity.
 *
 * Pure unit tests: getConnectionPoolId() only reflects over the config fields, so no running
 * core or database is needed.
 */
public class ConnectionPoolIdTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    private static PostgreSQLConfig configWithPassword(String password) throws Exception {
        JsonObject json = new JsonObject();
        json.addProperty("postgresql_user", "test_user");
        json.addProperty("postgresql_password", password);
        json.addProperty("postgresql_database_name", "supertokens");
        return ConfigMapper.mapConfig(json, PostgreSQLConfig.class);
    }

    @Test
    public void connectionPoolIdDoesNotContainRawPassword() throws Exception {
        String password = "s3cr3t-Db-Passw0rd!";
        String poolId = configWithPassword(password).getConnectionPoolId();

        assertFalse(poolId.contains(password));
    }

    @Test
    public void passwordStillContributesToConnectionPoolIdUniqueness() throws Exception {
        // identical configs -> identical pool ids (pool reuse must keep working)
        assertEquals(
                configWithPassword("password-one").getConnectionPoolId(),
                configWithPassword("password-one").getConnectionPoolId());

        // configs identical except for the password -> different pool ids, so rotating the
        // password still tears down and replaces the connection pool
        assertNotEquals(
                configWithPassword("password-one").getConnectionPoolId(),
                configWithPassword("password-two").getConnectionPoolId());
    }
}
