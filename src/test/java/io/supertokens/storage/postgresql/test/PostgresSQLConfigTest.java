/*
 *    Copyright (c) 2024, VRAI Labs and/or its affiliates. All rights reserved.
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

import io.supertokens.ProcessState;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storageLayer.StorageLayer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import static org.junit.Assert.*;

public class PostgresSQLConfigTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    @AfterClass
    public static void afterTesting() {
        Utils.afterTesting();
    }

    @Before
    public void beforeEach() {
        Utils.reset();
    }

    @Test
    public void testRetreivingConfigProperties() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        JsonArray configArray = ((Start) StorageLayer.getStorage(process.getProcess())).getConfigFieldsJson();

        for (int i = 0; i < configArray.size(); i++) {
            JsonObject config = configArray.get(i).getAsJsonObject();
            assertTrue(config.get("name").getAsJsonPrimitive().isString());
            assertTrue(config.get("description").getAsJsonPrimitive().isString());
            assertTrue(config.get("isDifferentAcrossTenants").getAsJsonPrimitive().isBoolean());
            assertTrue(config.get("type").getAsJsonPrimitive().isString());
            String type = config.get("type").getAsString();
            assertTrue(type.equals("number") || type.equals("boolean") || type.equals("string"));
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

}
