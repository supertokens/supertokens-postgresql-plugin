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

import com.google.gson.JsonObject;

import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.ResourceDistributor;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.ACCOUNT_INFO_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.*;

/**
 * Regression guard for issue #360: associating an existing ThirdParty user with a second tenant must
 * record membership in {@code recipe_user_tenants} (rut) in every mode that writes the new tables, and
 * the user must be visible in that tenant on the read path (listing, count, tenant-scoped ThirdParty
 * lookup).
 *
 * <p>The recipe-level {@code recipe_user_tenants} inserts that used to live in
 * {@code ThirdPartyQueries}/{@code EmailPasswordQueries}/{@code PasswordlessQueries}
 * {@code addUserIdToTenant_Transaction} were removed: they were redundant belt-and-suspenders. The
 * single writer of rut on this path is the orchestration-level
 * {@code Start.addUserIdToTenant_Transaction ->
 * AccountInfoQueries.addTenantIdToRecipeUser_Transaction}, which seeds every target tenant from the
 * app-scoped {@code recipe_user_account_infos} for every {@code writesToNewTables()} mode. This test
 * pins that behavior — with the ThirdParty recipe specifically, which never had a reachable
 * recipe-level rut insert to begin with (its former gate required {@code writesToOldTables()} too, and
 * the old-tables branch returns first).
 */
public class ThirdPartyTenantAssociationRutTest {

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

    private TestingProcessManager.TestingProcess startProcess() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES,
                        new EE_FEATURES[]{EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        return process;
    }

    // Count of rut rows for (app, recipeUserId, tenant) with the given account_info_type, read straight
    // from the table so the assertion does not depend on the read path.
    private static int rutRowCount(Start storage, String appId, String recipeUserId, String tenantId,
                                   ACCOUNT_INFO_TYPE type) throws Exception {
        String rut = Config.getConfig(storage).getRecipeUserTenantsTable();
        String query = "SELECT count(*) AS c FROM " + rut
                + " WHERE app_id = ? AND recipe_user_id = ? AND tenant_id = ? AND account_info_type = ?";
        Connection con = ConnectionPool.getConnection(storage);
        try (PreparedStatement pst = con.prepareStatement(query)) {
            pst.setString(1, appId);
            pst.setString(2, recipeUserId);
            pst.setString(3, tenantId);
            pst.setString(4, type.toString());
            try (ResultSet rs = pst.executeQuery()) {
                rs.next();
                return rs.getInt("c");
            }
        } finally {
            con.close();
        }
    }

    private static boolean listingContains(TenantIdentifier tenant, Storage storage, String userId)
            throws Exception {
        AuthRecipeUserInfo[] users = AuthRecipe.getUsers(tenant, storage, 100000, "ASC", null, null, null).users;
        for (AuthRecipeUserInfo u : users) {
            if (u.getSupertokensUserId().equals(userId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * For every migration mode: sign a ThirdParty user up in the public tenant, then associate it with a
     * second tenant {@code t1}. Assert that
     * <ul>
     *   <li>in every {@code writesToNewTables()} mode, {@code recipe_user_tenants} has both the email row
     *       and the third-party row for the user in {@code t1} (the exact rows the issue feared were lost),
     *       and none in the write-old-only {@code LEGACY} mode;</li>
     *   <li>in every mode, the user is visible in {@code t1}: it appears in the tenant listing and count,
     *       and the tenant-scoped ThirdParty lookup finds it — via whichever tables the mode reads.</li>
     * </ul>
     */
    @Test
    public void testThirdPartyAssociationRecordsMembershipInEveryMode() throws Exception {
        for (MigrationMode mode : MigrationMode.values()) {
            TestingProcessManager.TestingProcess process = startProcess();
            try {
                Main main = process.getProcess();
                if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

                // Create t1 BEFORE capturing the storage handle: addNewOrUpdateAppOrTenant reloads the
                // storage layer (tearing down the previous connection pool), so a handle captured earlier
                // would go stale. t1 uses the base config, so it shares the one DB; count/listing queries
                // run through the main storage scoped by tenant id, and the tenant's own handle is used
                // only for the association write (mirrors MigratedUserCountTest).
                TenantIdentifier t1 = new TenantIdentifier(null, null, "t1");
                Multitenancy.addNewOrUpdateAppOrTenant(main, new TenantConfig(
                        t1,
                        new EmailPasswordConfig(true),
                        new ThirdPartyConfig(true, null),
                        new PasswordlessConfig(true),
                        null, null, new JsonObject()
                ), false);

                Start storage = (Start) StorageLayer.getStorage(main);
                Config.getConfig(storage).setMigrationModeForTesting(mode);
                Storage t1Storage = StorageLayer.getStorage(t1, main);

                String tpId = "google";
                String tpUserId = "g-" + mode;
                ThirdParty.SignInUpResponse resp = ThirdParty.signInUp(main, tpId, tpUserId,
                        "tp-" + mode + "@test.com");
                String userId = resp.user.getSupertokensUserId();

                // Associate the existing ThirdParty user with t1.
                Multitenancy.addUserIdToTenant(main, t1, t1Storage, userId);

                String appId = t1.getAppId();
                if (mode.writesToNewTables()) {
                    assertEquals("email rut row must exist in t1 (" + mode + ")",
                            1, rutRowCount(storage, appId, userId, "t1", ACCOUNT_INFO_TYPE.EMAIL));
                    assertEquals("third-party rut row must exist in t1 (" + mode + ")",
                            1, rutRowCount(storage, appId, userId, "t1", ACCOUNT_INFO_TYPE.THIRD_PARTY));
                } else {
                    assertEquals("no rut rows are written in write-old-only mode (" + mode + ")",
                            0, rutRowCount(storage, appId, userId, "t1", ACCOUNT_INFO_TYPE.EMAIL)
                                    + rutRowCount(storage, appId, userId, "t1", ACCOUNT_INFO_TYPE.THIRD_PARTY));
                }

                // Read path: the user must be visible in t1 regardless of which tables the mode reads.
                assertTrue("user must appear in t1 listing (" + mode + ")",
                        listingContains(t1, storage, userId));
                assertEquals("t1 count must include the associated user (" + mode + ")",
                        1, AuthRecipe.getUsersCountForTenant(t1, storage, null));

                AuthRecipeUserInfo found = ThirdParty.getUser(t1, storage, tpId, tpUserId);
                assertNotNull("tenant-scoped ThirdParty lookup must find the user in t1 (" + mode + ")",
                        found);
                assertEquals("tenant-scoped lookup returns the associated user (" + mode + ")",
                        userId, found.getSupertokensUserId());
            } finally {
                process.kill();
                assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            }
        }
    }
}
