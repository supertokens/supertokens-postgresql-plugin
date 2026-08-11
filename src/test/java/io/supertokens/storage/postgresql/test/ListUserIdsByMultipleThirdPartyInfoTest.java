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

import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.queries.ThirdPartyQueries;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Regression test for {@code ThirdPartyQueries.listUserIdsByMultipleThirdPartyInfo_Transaction}.
 *
 * <p>The query used to filter with two independent {@code IN} clauses
 * ({@code third_party_id IN (...) AND third_party_user_id IN (...)}), which matches the
 * cross-product of the two requested lists rather than the individual pairs. When a batch
 * contains distinct users whose provider ids and provider-user-ids overlap under different
 * pairings, the cross-product returns spurious matches. The fix filters on the row-value
 * tuple {@code (third_party_id, third_party_user_id)}, matching each requested pair exactly.
 */
public class ListUserIdsByMultipleThirdPartyInfoTest {

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

    /**
     * Seeds a fixture where the requested pairs and the cross-product of their two lists differ:
     *
     * <pre>
     *   requested:  (google, u1)   -> userA
     *               (facebook, u2) -> userB
     *   decoys:     (google, u2)   -> userC   // shares the provider of the first request
     *                                          //  and the provider-user-id of the second
     *               (facebook, u1) -> userD   // the other off-diagonal pairing
     * </pre>
     *
     * A pairwise match must return exactly {userA, userB}. The old cross-product form
     * ({@code third_party_id IN (google, facebook) AND third_party_user_id IN (u1, u2)})
     * would additionally match the two decoy rows, returning {userA, userB, userC, userD}.
     */
    @Test
    public void pairwiseMatchDoesNotReturnCrossProduct() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        AppIdentifier appIdentifier = new AppIdentifier(null, null);

        // Distinct emails so account-linking (disabled here anyway) can never merge the users;
        // each recipe user is therefore its own primary_or_recipe_user_id.
        String userA = ThirdParty.signInUp(process.getProcess(), "google", "u1", "a@example.com")
                .user.getSupertokensUserId();
        String userB = ThirdParty.signInUp(process.getProcess(), "facebook", "u2", "b@example.com")
                .user.getSupertokensUserId();
        String userC = ThirdParty.signInUp(process.getProcess(), "google", "u2", "c@example.com")
                .user.getSupertokensUserId();
        String userD = ThirdParty.signInUp(process.getProcess(), "facebook", "u1", "d@example.com")
                .user.getSupertokensUserId();

        // Sanity: the four sign-ups produced four distinct users.
        assertEquals(4, new HashSet<>(java.util.Arrays.asList(userA, userB, userC, userD)).size());

        // Request only the two diagonal pairs. Map is keyed by third_party_user_id -> third_party_id.
        Map<String, String> thirdPartyUserIdToThirdPartyId = new HashMap<>();
        thirdPartyUserIdToThirdPartyId.put("u1", "google");
        thirdPartyUserIdToThirdPartyId.put("u2", "facebook");

        List<String> result = storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            return ThirdPartyQueries.listUserIdsByMultipleThirdPartyInfo_Transaction(
                    storage, sqlCon, appIdentifier, thirdPartyUserIdToThirdPartyId);
        });

        Set<String> actual = new HashSet<>(result);
        Set<String> expected = new HashSet<>();
        expected.add(userA);
        expected.add(userB);

        assertEquals("must match the requested pairs exactly, not the cross-product of the two lists",
                expected, actual);
        assertFalse("decoy (google, u2) must not match", actual.contains(userC));
        assertFalse("decoy (facebook, u1) must not match", actual.contains(userD));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    /**
     * An empty / null request must short-circuit to an empty result without touching the DB.
     */
    @Test
    public void emptyRequestReturnsEmpty() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            return;
        }

        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        AppIdentifier appIdentifier = new AppIdentifier(null, null);

        List<String> empty = storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            return ThirdPartyQueries.listUserIdsByMultipleThirdPartyInfo_Transaction(
                    storage, sqlCon, appIdentifier, new HashMap<>());
        });
        assertTrue(empty.isEmpty());

        List<String> nullResult = storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            return ThirdPartyQueries.listUserIdsByMultipleThirdPartyInfo_Transaction(
                    storage, sqlCon, appIdentifier, null);
        });
        assertTrue(nullResult.isEmpty());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
