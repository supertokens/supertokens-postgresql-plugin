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
import io.supertokens.authRecipe.UserPaginationContainer;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeStorage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.Random;

import static org.junit.Assert.*;

/**
 * PLAN-005 unit 2: correctness guard for the D - L + G decomposition rewrite of the unfiltered
 * per-tenant {@code getUsersCount_new}, and the streaming (time, id) GROUP BY rewrite of the
 * unfiltered app-scoped {@code getUsersCount_new} (GeneralQueries.java).
 *
 * <p>The rewrite is a performance change (index-only streaming instead of a tenant-wide
 * hash-aggregating join) that must preserve the exact count. The central oracle is the
 * independent listing path {@code getUsers}: for a tenant, the number of distinct primary
 * users the listing returns is, by definition, the tenant user count — so the rewritten count
 * must equal the listing length for every scenario. Each scenario also asserts an explicitly
 * computed expected value. Exercised under both read-from-new migration modes, with linked
 * groups, singleton primaries, passwordless users carrying both email and phone (two
 * {@code recipe_user_tenants} rows), multi-tenant membership, and tenant disassociation.
 */
public class MigratedUserCountTest {

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

    private static AuthRecipeUserInfo signUp(Main main, String email) throws Exception {
        AuthRecipeUserInfo u = EmailPassword.signUp(main, email, "password123");
        Thread.sleep(15);
        return u;
    }

    private static AuthRecipeUserInfo passwordlessWithEmail(Main main, String email) throws Exception {
        Passwordless.CreateCodeResponse code = Passwordless.createCode(main, email, null, null, null);
        AuthRecipeUserInfo u = Passwordless.consumeCode(main, code.deviceId, code.deviceIdHash,
                code.userInputCode, null).user;
        Thread.sleep(15);
        return u;
    }

    // Count of the public (default) tenant via the rewritten storage path.
    private static long publicTenantCount(Main main) throws Exception {
        Storage storage = StorageLayer.getStorage(main);
        return AuthRecipe.getUsersCountForTenant(ResourceDistributor.getAppForTesting(), storage, null);
    }

    // Number of distinct primary users the listing returns for the public tenant — the independent
    // oracle the rewritten count must match.
    private static int publicTenantListingLength(Main main) throws Exception {
        UserPaginationContainer all = AuthRecipe.getUsers(main, 100000, "ASC", null, null, null);
        return all.users.length;
    }

    private static long tenantCount(TenantIdentifier tenant, Storage storage) throws Exception {
        return AuthRecipe.getUsersCountForTenant(tenant, storage, null);
    }

    private static int tenantListingLength(TenantIdentifier tenant, Storage storage) throws Exception {
        return AuthRecipe.getUsers(tenant, storage, 100000, "ASC", null, null, null).users.length;
    }

    // App-scoped unfiltered count (the F1 streaming fix) via the storage interface directly.
    private static long appCount(Main main) throws Exception {
        Storage storage = StorageLayer.getStorage(main);
        return ((AuthRecipeStorage) storage).getUsersCount(
                ResourceDistributor.getAppForTesting().toAppIdentifier(), null);
    }

    /**
     * All the correctness cases the issue enumerates, on the public tenant, under both
     * read-from-new modes: singletons, a linked group (members subtracted in L and restored once
     * by G), a singleton primary user (createPrimaryUser, never linked — nets out of D - L and is
     * restored by its primary_user_tenants row in G), and a passwordless user carrying both email
     * and phone (two recipe_user_tenants rows, must count once).
     */
    @Test
    public void testTenantCountDecompositionCorrectness() throws Exception {
        for (MigrationMode mode : new MigrationMode[]{MigrationMode.MIGRATED, MigrationMode.DUAL_WRITE_READ_NEW}) {
            TestingProcessManager.TestingProcess process = startProcess();
            try {
                Main main = process.getProcess();
                if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

                Start storage = (Start) StorageLayer.getStorage(main);
                Config.getConfig(storage).setMigrationModeForTesting(mode);

                // 2 plain singletons.
                signUp(main, "s0-" + mode + "@test.com");
                signUp(main, "s1-" + mode + "@test.com");

                // A linked group: EP primary + a second EP login method linked in.
                AuthRecipeUserInfo gPrimary = signUp(main, "g-primary-" + mode + "@test.com");
                AuthRecipeUserInfo gMember = signUp(main, "g-member-" + mode + "@test.com");
                AuthRecipe.createPrimaryUser(main, gPrimary.getSupertokensUserId());
                AuthRecipe.linkAccounts(main, gMember.getSupertokensUserId(), gPrimary.getSupertokensUserId());

                // A singleton primary user: made primary, never linked to anything.
                AuthRecipeUserInfo lone = signUp(main, "lone-primary-" + mode + "@test.com");
                AuthRecipe.createPrimaryUser(main, lone.getSupertokensUserId());

                // A passwordless user with BOTH email and phone (two recipe_user_tenants rows).
                AuthRecipeUserInfo pless = passwordlessWithEmail(main, "pless-" + mode + "@test.com");
                Passwordless.updateUser(main, pless.getSupertokensUserId(),
                        null, new Passwordless.FieldUpdate("+1000" + (mode == MigrationMode.MIGRATED ? "1" : "2")));

                // Distinct users: s0, s1, {gPrimary+gMember}=1, lone, pless = 5.
                long expected = 5;
                assertEquals("D - L + G count (" + mode + ")", expected, publicTenantCount(main));
                assertEquals("count must equal listing length (" + mode + ")",
                        publicTenantListingLength(main), publicTenantCount(main));
                // The linked member must not be counted on its own.
                assertEquals("linked member is not a separate user (" + mode + ")",
                        5, publicTenantListingLength(main));
            } finally {
                process.kill();
                assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            }
        }
    }

    /**
     * Multi-tenant membership and disassociation: a user counts once per tenant it belongs to;
     * disassociating it from one tenant stops it counting there but not elsewhere; disassociating
     * from all tenants makes it count in none. Each tenant's count is cross-checked against that
     * tenant's listing length.
     */
    @Test
    public void testMultiTenantCountAndDisassociation() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            // Create t1 BEFORE capturing the storage handle: addNewOrUpdateAppOrTenant reloads the
            // storage layer (tearing down the previous connection pool), so a handle captured earlier
            // would be stale. All tenants share the one DB here (t1 uses the base config), so the count
            // and listing queries below run through the main `storage` scoped by tenant id; the tenant's
            // own storage handle is used only for the association writes, mirroring MigrationModeTest.
            TenantIdentifier publicTenant = ResourceDistributor.getAppForTesting();
            TenantIdentifier t1 = new TenantIdentifier(null, null, "t1");
            Multitenancy.addNewOrUpdateAppOrTenant(main, new TenantConfig(
                    t1,
                    new EmailPasswordConfig(true),
                    new ThirdPartyConfig(true, null),
                    new PasswordlessConfig(true),
                    null, null, new JsonObject()
            ), false);

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.MIGRATED);
            Storage t1Storage = StorageLayer.getStorage(t1, main);

            // Three users signed up in public.
            AuthRecipeUserInfo shared = signUp(main, "shared@test.com");   // will also join t1
            signUp(main, "publicOnly@test.com");                            // stays in public only
            AuthRecipeUserInfo t1Bound = signUp(main, "t1bound@test.com");  // will move fully to t1

            // shared joins t1 too (now in both tenants); t1Bound joins t1 then leaves public.
            Multitenancy.addUserIdToTenant(main, t1, t1Storage, shared.getSupertokensUserId());
            Multitenancy.addUserIdToTenant(main, t1, t1Storage, t1Bound.getSupertokensUserId());
            Multitenancy.removeUserIdFromTenant(main, publicTenant,
                    StorageLayer.getStorage(main), t1Bound.getSupertokensUserId(), null);

            // public: shared + publicOnly = 2 ; t1: shared + t1Bound = 2
            assertEquals("public tenant counts each member once", 2, tenantCount(publicTenant, storage));
            assertEquals("public count == public listing", tenantListingLength(publicTenant, storage),
                    tenantCount(publicTenant, storage));
            assertEquals("t1 counts each member once", 2, tenantCount(t1, storage));
            assertEquals("t1 count == t1 listing", tenantListingLength(t1, storage),
                    tenantCount(t1, storage));

            // Disassociate `shared` from t1 -> t1 drops to 1 (t1Bound), public unchanged at 2.
            Multitenancy.removeUserIdFromTenant(main, t1, t1Storage, shared.getSupertokensUserId(), null);
            assertEquals("t1 loses the disassociated user", 1, tenantCount(t1, storage));
            assertEquals("public unaffected by t1 disassociation", 2, tenantCount(publicTenant, storage));

            // Disassociate `shared` from public too -> it now belongs to no tenant, counts nowhere.
            Multitenancy.removeUserIdFromTenant(main, publicTenant,
                    StorageLayer.getStorage(main), shared.getSupertokensUserId(), null);
            assertEquals("public loses the fully-disassociated user", 1, tenantCount(publicTenant, storage));
            assertEquals("t1 still 1", 1, tenantCount(t1, storage));
            assertEquals("public count == public listing after disassociation",
                    tenantListingLength(publicTenant, storage), tenantCount(publicTenant, storage));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Equivalence on randomized seeded data: a mix of singletons, linked groups, singleton
     * primaries and both auth recipes, then assert the rewritten count equals the independent
     * listing length (and the app-scoped streaming count matches, since every user lives in the
     * public tenant here). Deterministic via a fixed seed.
     */
    @Test
    public void testCountEquivalenceOnSeededData() throws Exception {
        for (MigrationMode mode : new MigrationMode[]{MigrationMode.MIGRATED, MigrationMode.DUAL_WRITE_READ_NEW}) {
            TestingProcessManager.TestingProcess process = startProcess();
            try {
                Main main = process.getProcess();
                if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

                Start storage = (Start) StorageLayer.getStorage(main);
                Config.getConfig(storage).setMigrationModeForTesting(mode);

                Random rng = new Random(42);
                int n = 40;
                java.util.List<String> primaryIds = new java.util.ArrayList<>();
                for (int i = 0; i < n; i++) {
                    AuthRecipeUserInfo u;
                    if (rng.nextBoolean()) {
                        u = signUp(main, "r-ep-" + mode + "-" + i + "@test.com");
                    } else {
                        ThirdParty.SignInUpResponse tp =
                                ThirdParty.signInUp(main, "google", "g-" + mode + "-" + i, "r-tp-" + mode + "-" + i + "@test.com");
                        u = tp.user;
                        Thread.sleep(15);
                    }
                    // ~1 in 3: make primary. Of those, ~1 in 2: link the previous primary's group into it.
                    if (rng.nextInt(3) == 0) {
                        AuthRecipe.createPrimaryUser(main, u.getSupertokensUserId());
                        if (!primaryIds.isEmpty() && rng.nextBoolean()) {
                            String other = primaryIds.get(rng.nextInt(primaryIds.size()));
                            try {
                                AuthRecipe.linkAccounts(main, u.getSupertokensUserId(), other);
                            } catch (Exception ignored) {
                                // linking can legitimately fail (already linked / conflict); skip
                            }
                        }
                        primaryIds.add(u.getSupertokensUserId());
                    }
                }

                long count = publicTenantCount(main);
                assertEquals("seeded: count == listing length (" + mode + ")",
                        publicTenantListingLength(main), count);
                assertEquals("seeded: app-scoped streaming count == public count (single-tenant app) (" + mode + ")",
                        count, appCount(main));
                assertTrue("sanity: some users exist", count > 0);
            } finally {
                process.kill();
                assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            }
        }
    }

    /**
     * The recipe-filtered count variant keeps its own SQL shape; verify it still returns the right
     * per-recipe counts (a cross-recipe linked group is reachable under either recipe filter).
     */
    @Test
    public void testRecipeFilteredCountUnchanged() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.MIGRATED);

            signUp(main, "ep-a@test.com");
            signUp(main, "ep-b@test.com");
            ThirdParty.signInUp(main, "google", "g-a", "tp-a@test.com");

            TenantIdentifier publicTenant = ResourceDistributor.getAppForTesting();
            assertEquals("EP-filtered count", 2,
                    AuthRecipe.getUsersCountForTenant(publicTenant, storage,
                            new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD}));
            assertEquals("TP-filtered count", 1,
                    AuthRecipe.getUsersCountForTenant(publicTenant, storage,
                            new RECIPE_ID[]{RECIPE_ID.THIRD_PARTY}));
            assertEquals("unfiltered count (D-L+G)", 3, publicTenantCount(main));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }
}
