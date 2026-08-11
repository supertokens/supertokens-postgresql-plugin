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
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.EmailPasswordConfig;
import io.supertokens.pluginInterface.multitenancy.PasswordlessConfig;
import io.supertokens.pluginInterface.multitenancy.TenantConfig;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.ThirdPartyConfig;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.queries.WebAuthNQueries;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.webauthn.WebAuthN;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Regression test for supertokens/supertokens-postgresql-plugin#361 (PLAN-006).
 *
 * <p>{@code getPrimaryUserIdForAppUsingEmail_Transaction_legacy} joined
 * {@code webauthn_user_to_tenant} to {@code all_auth_recipe_users} on {@code user_id} alone,
 * omitting {@code app_id} from the join condition. The {@code WHERE} clause scopes the
 * webauthn side to the queried app, but the unqualified join then matches
 * {@code all_auth_recipe_users} rows from <em>every</em> app that happens to carry the same
 * {@code user_id} string. Results are correct today only because user ids are core-generated
 * UUIDs that never collide across apps — nothing enforces that, and this sits in an
 * auth-decision path, so any id reuse across apps returns another app's primary user id.
 *
 * <p>The test seeds the same {@code user_id} string in two apps directly, mapped to different
 * primary users, and asserts the legacy lookup for one app returns only that app's primary
 * user. The buggy query is a two-row {@code SELECT DISTINCT} with no {@code ORDER BY}, so which
 * primary the single-value method returns is otherwise plan-dependent. To make the cross-app
 * leak deterministically observable, the foreign app's primary id is chosen to sort ahead of
 * the real one and the lookup transaction pins {@code enable_hashagg = off} so {@code DISTINCT}
 * resolves via a sort: on the unscoped join the foreign primary sorts first and the assertion
 * fails, while the app-scoped join never matches the foreign row at all.
 */
public class WebAuthnLegacyEmailLookupAppScopingTest {

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

    @Rule
    public Retry retry = new Retry(3);

    private static final String OTHER_APP = "othertestapp";
    private static final String RP_ID = "example.com";

    @Test
    public void testLegacyLookupIsScopedToQueriedApp() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            return;
        }

        Main main = process.getProcess();
        Storage storage = StorageLayer.getStorage(main);
        Start start = (Start) storage;

        // LEGACY = write-old + read-old, so all_auth_recipe_users is populated and the legacy
        // lookup path (getPrimaryUserIdForAppUsingEmail_Transaction_legacy) is exercised.
        Config.getConfig(start).setMigrationModeForTesting(MigrationMode.LEGACY);

        // A second app whose "public" tenant we can seed into.
        createApp(main, new TenantIdentifier(null, OTHER_APP, null));

        String email = "shared@webauthn.com";
        String sharedUserId = io.supertokens.utils.Utils.getUUID();
        // An all-zero UUID sorts ahead of any core-generated user id, so under the sort-based
        // DISTINCT pinned below it is the first row the buggy unscoped join surfaces.
        String otherAppPrimaryId = "00000000-0000-0000-0000-000000000000";

        // Seed the OTHER app: the same user_id string mapped to a DIFFERENT primary user.
        seedCrossAppUser(start, OTHER_APP, sharedUserId, otherAppPrimaryId, email);

        // The real user in the default app: a standalone webauthn user whose primary id is its
        // own user id. This is the only row the app-scoped lookup should ever return.
        AppIdentifier defaultApp = new AppIdentifier(null, null);
        WebAuthN.saveUser(storage, TenantIdentifier.BASE_TENANT, email, sharedUserId, RP_ID);

        String result = start.startTransaction(con -> {
            try {
                Connection sqlCon = (Connection) con.getConnection();
                // The production query has no ORDER BY; pin a sort-based DISTINCT (scoped to this
                // transaction) so the leaked foreign primary is deterministically first when the
                // join is unscoped. With the fix the foreign row is never joined, so this has no
                // effect on the correct result.
                try (java.sql.Statement st = sqlCon.createStatement()) {
                    st.execute("SET LOCAL enable_hashagg = off");
                }
                return WebAuthNQueries.getPrimaryUserIdForAppUsingEmail_Transaction(
                        start, sqlCon, defaultApp, email);
            } catch (Exception e) {
                throw new StorageQueryException(e);
            }
        });

        assertEquals("legacy webauthn email lookup must return only the queried app's primary user "
                        + "(cross-app join leak, issue #361)",
                sharedUserId, result);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private void createApp(Main main, TenantIdentifier app) throws Exception {
        TenantConfig config = new TenantConfig(
                app,
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null, new JsonObject()
        );
        Multitenancy.addNewOrUpdateAppOrTenant(main, config, false);
    }

    /**
     * Seeds, in {@code appId}'s public tenant, a webauthn recipe user with id {@code userId}
     * linked under primary user {@code primaryId}, mirroring the rows the storage maintains in a
     * write-old mode. No webauthn row is needed on this side — the leak is in the join to
     * {@code all_auth_recipe_users}, so only the identity rows carrying {@code userId} matter.
     */
    private void seedCrossAppUser(Start start, String appId, String userId, String primaryId,
                                  String email) throws Exception {
        String appTable = Config.getConfig(start).getAppIdToUserIdTable();
        String usersTable = Config.getConfig(start).getUsersTable();
        long timeJoined = 1_000_000_000_000L;
        start.startTransaction(con -> {
            try {
                Connection sqlCon = (Connection) con.getConnection();

                // The primary user's own app_id_to_user_id row (self-referential primary).
                insertAppUser(sqlCon, appTable, appId, primaryId, primaryId, true, timeJoined);
                // The recipe user, linked under that primary, sharing the default app's user_id.
                insertAppUser(sqlCon, appTable, appId, userId, primaryId, false, timeJoined);
                // Its tenant-scoped row in all_auth_recipe_users — the row the buggy join leaks.
                insertRecipeUser(sqlCon, usersTable, appId, userId, primaryId, timeJoined);

                start.commitTransaction(con);
            } catch (SQLException e) {
                throw new StorageQueryException(e);
            }
            return null;
        });
    }

    private void insertAppUser(Connection sqlCon, String appTable, String appId, String userId,
                               String primaryId, boolean isPrimary, long timeJoined) throws SQLException {
        String q = "INSERT INTO " + appTable
                + "(app_id, user_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user, "
                + "recipe_id, time_joined, primary_or_recipe_user_time_joined) VALUES(?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pst = sqlCon.prepareStatement(q)) {
            pst.setString(1, appId);
            pst.setString(2, userId);
            pst.setString(3, primaryId);
            pst.setBoolean(4, isPrimary);
            pst.setString(5, "webauthn");
            pst.setLong(6, timeJoined);
            pst.setLong(7, timeJoined);
            pst.executeUpdate();
        }
    }

    private void insertRecipeUser(Connection sqlCon, String usersTable, String appId, String userId,
                                  String primaryId, long timeJoined) throws SQLException {
        String q = "INSERT INTO " + usersTable
                + "(app_id, tenant_id, user_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user, "
                + "recipe_id, time_joined, primary_or_recipe_user_time_joined) VALUES(?, 'public', ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pst = sqlCon.prepareStatement(q)) {
            pst.setString(1, appId);
            pst.setString(2, userId);
            pst.setString(3, primaryId);
            pst.setBoolean(4, false);
            pst.setString(5, "webauthn");
            pst.setLong(6, timeJoined);
            pst.setLong(7, timeJoined);
            pst.executeUpdate();
        }
    }
}
