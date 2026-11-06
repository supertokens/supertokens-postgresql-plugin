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
 */

package io.supertokens.storage.postgresql.test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.saml.SAMLClaimsInfo;
import io.supertokens.pluginInterface.saml.SAMLStorage;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * A SAML code (a row in saml_claims) must be redeemable exactly once, also when several callers redeem it
 * at the same time.
 */
public class SAMLClaimsCodeSingleUseTest {
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
    public void testIssue28_samlCodeRedeemedOnce() throws Exception {
        int port;
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        // The webserver is not used by this test; a free port keeps it independent of other cores on this host.
        String[] args = {"../", "port=" + port};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED, 60000));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            return;
        }

        SAMLStorage storage = (SAMLStorage) StorageLayer.getStorage(process.getProcess());
        TenantIdentifier tenant = TenantIdentifier.BASE_TENANT;

        JsonObject claims = new JsonObject();
        JsonArray nameId = new JsonArray();
        nameId.add(new com.google.gson.JsonPrimitive("user@example.com"));
        claims.add("NameID", nameId);

        int threads = 8;
        int trials = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            for (int trial = 0; trial < trials; trial++) {
                String code = UUID.randomUUID().toString();
                storage.saveSAMLClaims(tenant, "client-1", code, claims, 300000);

                CyclicBarrier barrier = new CyclicBarrier(threads);
                List<Future<SAMLClaimsInfo>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    futures.add(executor.submit(() -> {
                        barrier.await(10, TimeUnit.SECONDS);
                        return storage.getSAMLClaimsAndRemoveCode(tenant, code);
                    }));
                }

                int redeemed = 0;
                for (Future<SAMLClaimsInfo> f : futures) {
                    SAMLClaimsInfo info = f.get(30, TimeUnit.SECONDS);
                    if (info != null) {
                        redeemed++;
                        assertEquals("client-1", info.clientId);
                        assertEquals("user@example.com",
                                info.claims.getAsJsonArray("NameID").get(0).getAsString());
                    }
                }
                assertEquals("code must be redeemed exactly once (trial " + trial + ")", 1, redeemed);
                assertNull(storage.getSAMLClaimsAndRemoveCode(tenant, code));
            }
        } finally {
            executor.shutdownNow();
        }

        // An expired code is never returned.
        String expired = UUID.randomUUID().toString();
        storage.saveSAMLClaims(tenant, "client-1", expired, claims, -1000);
        assertNull(storage.getSAMLClaimsAndRemoveCode(tenant, expired));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
