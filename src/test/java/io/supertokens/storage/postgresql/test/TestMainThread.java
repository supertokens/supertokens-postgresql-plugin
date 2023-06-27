/*
 *    Copyright (c) 2023, VRAI Labs and/or its affiliates. All rights reserved.
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
import io.supertokens.ProcessState;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.test.httpRequest.HttpRequestForTesting;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.*;

public class TestMainThread {
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
    public void testThatMainThreadIsSameThroughout() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            return;
        }

        if (StorageLayer.isInMemDb(process.getProcess())) {
            return;
        }

        Storage storage = StorageLayer.getBaseStorage(process.getProcess());
        Thread mainThread = ((Start) storage).getMainThread();

        // Create a new app
        JsonObject config = new JsonObject();
        storage.modifyConfigToAddANewUserPoolForTesting(config, 1);
        JsonObject requestBody = new JsonObject();
        requestBody.addProperty("appId", "a1");
        requestBody.add("coreConfig", config);
        HttpRequestForTesting.sendJsonPUTRequest(process.getProcess(), "", "http://localhost:3567/recipe/multitenancy/app",
                requestBody, 1000, 2500, null,  "3.0", "multitenancy");

        Storage storage2 = StorageLayer.getStorage(new TenantIdentifier(null, "a1", null), process.getProcess());

        assertNotEquals(storage, storage2);
        assertEquals(mainThread, ((Start) storage2).getMainThread());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
