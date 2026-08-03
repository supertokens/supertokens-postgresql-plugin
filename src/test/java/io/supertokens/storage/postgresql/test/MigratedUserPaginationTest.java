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

import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.authRecipe.UserPaginationContainer;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * PLAN-005 unit 1: equivalence guard for the streaming DISTINCT + EXISTS rewrite of the
 * non-search {@code getUsers_new} pagination variants (GeneralQueries.java).
 *
 * The rewrite is a performance change that must preserve behaviour exactly: same rows, same
 * order, same cursor semantics as the previous GROUP BY/MIN + HAVING form. These tests exercise
 * both read-from-new-tables migration modes with linked users whose groups straddle page
 * boundaries, ascending and descending, and with recipe-id filters. The central assertion is
 * that walking the listing one page at a time (following the pagination token) yields exactly
 * the same sequence as a single large page — which is only true if the cursor predicate and
 * tie-break are correct — and that each linked group surfaces once (its primary), never its
 * individual login methods.
 */
public class MigratedUserPaginationTest {

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

    // Signs up an emailpassword user; the small sleep keeps time_joined strictly increasing so the
    // expected ordering is deterministic (the cursor tie-break on equal times is still exercised by
    // linked groups, which collapse many member rows onto a single group time).
    private static AuthRecipeUserInfo signUp(Main main, String email) throws Exception {
        AuthRecipeUserInfo u = EmailPassword.signUp(main, email, "password123");
        Thread.sleep(15);
        return u;
    }

    private static List<String> idsFrom(AuthRecipeUserInfo[] users) {
        List<String> ids = new ArrayList<>();
        for (AuthRecipeUserInfo u : users) {
            ids.add(u.getSupertokensUserId());
        }
        return ids;
    }

    // Walks the whole listing one small page at a time, following the pagination token, and returns
    // the ids in the order the server produced them.
    private static List<String> collectPaginated(Main main, String order, RECIPE_ID[] includeRecipeIds,
                                                  int pageSize) throws Exception {
        List<String> ids = new ArrayList<>();
        String token = null;
        int safety = 0;
        do {
            UserPaginationContainer page = AuthRecipe.getUsers(main, pageSize, order, token, includeRecipeIds, null);
            ids.addAll(idsFrom(page.users));
            token = page.nextPaginationToken;
            if (++safety > 10000) {
                fail("pagination did not terminate");
            }
        } while (token != null);
        return ids;
    }

    private static void assertSortedByTimeThenIdDesc(AuthRecipeUserInfo[] users, String order) {
        for (int i = 1; i < users.length; i++) {
            AuthRecipeUserInfo prev = users[i - 1];
            AuthRecipeUserInfo cur = users[i];
            if (prev.timeJoined == cur.timeJoined) {
                // tie-break is always id DESC regardless of time order
                assertTrue("tie-break must be id DESC at index " + i,
                        prev.getSupertokensUserId().compareTo(cur.getSupertokensUserId()) >= 0);
            } else if (order.equals("ASC")) {
                assertTrue("time must be ascending at index " + i, prev.timeJoined < cur.timeJoined);
            } else {
                assertTrue("time must be descending at index " + i, prev.timeJoined > cur.timeJoined);
            }
        }
    }

    private static void assertNoDuplicates(List<String> ids) {
        Set<String> seen = new HashSet<>(ids);
        assertEquals("listing must not contain duplicate users", ids.size(), seen.size());
    }

    // Directly rewrites one app_id_to_user_id row's primary_or_recipe_user_time_joined, bypassing the
    // transactional updater that normally keeps a linked group's values equal (invariant I6). This is
    // the only way to synthesise an I6 violation from a test — the public linking API cannot produce
    // one — and it reproduces the concrete production window the rewrite depends on NOT occurring: a
    // group left with mixed times mid-migration (e.g. a member still carrying the LEGACY `0` sentinel
    // because the MIGRATED backfill has not yet reached it) while a read-from-new mode is live.
    private static void forceTimeJoinedForRow(Start storage, String userId, long timeJoined) throws Exception {
        String table = Config.getConfig(storage).getAppIdToUserIdTable();
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (Statement stmt = sqlCon.createStatement()) {
                stmt.executeUpdate("UPDATE " + table + " SET primary_or_recipe_user_time_joined = "
                        + timeJoined + " WHERE app_id = 'public' AND user_id = '" + userId + "'");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            storage.commitTransaction(con);
            return null;
        });
    }

    /**
     * Singletons interleaved with linked groups; the group members are created at different times
     * so that, without invariant I6, a group could surface at more than one cursor position. The
     * rewrite must still return each group exactly once (as its primary), in the same order whether
     * read as one page or paginated, ascending and descending — for every read-from-new mode.
     */
    @Test
    public void testPaginationEquivalenceWithLinkedGroups() throws Exception {
        for (MigrationMode mode : new MigrationMode[]{MigrationMode.MIGRATED, MigrationMode.DUAL_WRITE_READ_NEW}) {
            TestingProcessManager.TestingProcess process = startProcess();
            try {
                Main main = process.getProcess();
                if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

                Start storage = (Start) StorageLayer.getStorage(main);
                Config.getConfig(storage).setMigrationModeForTesting(mode);

                // 8 users; two linked groups whose members were created at different times.
                AuthRecipeUserInfo u0 = signUp(main, "u0-" + mode + "@test.com"); // singleton
                AuthRecipeUserInfo u1 = signUp(main, "u1-" + mode + "@test.com"); // group A primary
                AuthRecipeUserInfo u2 = signUp(main, "u2-" + mode + "@test.com"); // singleton
                AuthRecipeUserInfo u3 = signUp(main, "u3-" + mode + "@test.com"); // -> linked into A
                AuthRecipeUserInfo u4 = signUp(main, "u4-" + mode + "@test.com"); // singleton
                AuthRecipeUserInfo u5 = signUp(main, "u5-" + mode + "@test.com"); // group B primary
                AuthRecipeUserInfo u6 = signUp(main, "u6-" + mode + "@test.com"); // -> linked into B
                AuthRecipeUserInfo u7 = signUp(main, "u7-" + mode + "@test.com"); // singleton

                AuthRecipe.createPrimaryUser(main, u1.getSupertokensUserId());
                AuthRecipe.linkAccounts(main, u3.getSupertokensUserId(), u1.getSupertokensUserId());
                AuthRecipe.createPrimaryUser(main, u5.getSupertokensUserId());
                AuthRecipe.linkAccounts(main, u6.getSupertokensUserId(), u5.getSupertokensUserId());

                Set<String> expectedPrimaries = new HashSet<>();
                Collections.addAll(expectedPrimaries,
                        u0.getSupertokensUserId(), u1.getSupertokensUserId(), u2.getSupertokensUserId(),
                        u4.getSupertokensUserId(), u5.getSupertokensUserId(), u7.getSupertokensUserId());
                Set<String> linkedAway = new HashSet<>();
                Collections.addAll(linkedAway, u3.getSupertokensUserId(), u6.getSupertokensUserId());

                for (String order : new String[]{"ASC", "DESC"}) {
                    // Single large page = the reference the paginated walk must reproduce.
                    UserPaginationContainer ref = AuthRecipe.getUsers(main, 1000, order, null, null, null);
                    List<String> refIds = idsFrom(ref.users);

                    assertSortedByTimeThenIdDesc(ref.users, order);
                    assertNoDuplicates(refIds);
                    assertEquals("one entry per group/singleton in " + mode + " " + order,
                            expectedPrimaries.size(), refIds.size());
                    assertEquals(expectedPrimaries, new HashSet<>(refIds));
                    for (String gone : linkedAway) {
                        assertFalse("linked login method must not appear as its own row",
                                refIds.contains(gone));
                    }

                    // Cursor equivalence: paginating one row at a time must reproduce the reference
                    // order exactly (page boundaries fall between and around the linked groups).
                    assertEquals("page-size-1 walk must equal single page (" + mode + " " + order + ")",
                            refIds, collectPaginated(main, order, null, 1));
                    // A page size that does not divide the total, to land boundaries mid-group.
                    assertEquals("page-size-3 walk must equal single page (" + mode + " " + order + ")",
                            refIds, collectPaginated(main, order, null, 3));
                }
            } finally {
                process.kill();
                assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            }
        }
    }

    /**
     * The recipe-id-filtered pagination variant keeps its own SQL shape; verify the DISTINCT + EXISTS
     * rewrite of that branch also paginates equivalently. A cross-recipe linked group (EP + TP) is
     * included so the filter selects the group via whichever member matches, and the group still
     * appears once under its primary.
     */
    @Test
    public void testPaginationEquivalenceWithRecipeIdFilter() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

            Start storage = (Start) StorageLayer.getStorage(main);
            Config.getConfig(storage).setMigrationModeForTesting(MigrationMode.MIGRATED);

            AuthRecipeUserInfo ep1 = signUp(main, "ep1@test.com");
            ThirdParty.SignInUpResponse tp1 = ThirdParty.signInUp(main, "google", "g-1", "tp1@test.com");
            Thread.sleep(15);
            AuthRecipeUserInfo ep2 = signUp(main, "ep2@test.com");
            ThirdParty.SignInUpResponse tp2 = ThirdParty.signInUp(main, "google", "g-2", "tp2@test.com");
            Thread.sleep(15);
            // Cross-recipe linked group: EP primary + TP login method.
            AuthRecipeUserInfo epPrimary = signUp(main, "ep-primary@test.com");
            ThirdParty.SignInUpResponse tpLinked = ThirdParty.signInUp(main, "google", "g-3", "tp3@test.com");
            Thread.sleep(15);
            AuthRecipe.createPrimaryUser(main, epPrimary.getSupertokensUserId());
            AuthRecipe.linkAccounts(main, tpLinked.user.getSupertokensUserId(), epPrimary.getSupertokensUserId());

            RECIPE_ID[][] filters = new RECIPE_ID[][]{
                    new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD},
                    new RECIPE_ID[]{RECIPE_ID.THIRD_PARTY},
                    new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD, RECIPE_ID.THIRD_PARTY},
            };

            for (String order : new String[]{"ASC", "DESC"}) {
                for (RECIPE_ID[] filter : filters) {
                    UserPaginationContainer ref = AuthRecipe.getUsers(main, 1000, order, null, filter, null);
                    List<String> refIds = idsFrom(ref.users);
                    assertSortedByTimeThenIdDesc(ref.users, order);
                    assertNoDuplicates(refIds);
                    assertEquals("recipe-filtered page-size-1 walk must equal single page",
                            refIds, collectPaginated(main, order, filter, 1));
                    assertEquals("recipe-filtered page-size-2 walk must equal single page",
                            refIds, collectPaginated(main, order, filter, 2));
                }
            }

            // The cross-recipe group's primary is reachable under both an EP-only and a TP-only filter,
            // and appears exactly once each time.
            List<String> epOnly = idsFrom(
                    AuthRecipe.getUsers(main, 1000, "ASC", null, new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD}, null).users);
            List<String> tpOnly = idsFrom(
                    AuthRecipe.getUsers(main, 1000, "ASC", null, new RECIPE_ID[]{RECIPE_ID.THIRD_PARTY}, null).users);
            assertTrue("cross-recipe group primary visible via EP filter",
                    epOnly.contains(epPrimary.getSupertokensUserId()));
            assertTrue("cross-recipe group primary visible via TP filter",
                    tpOnly.contains(epPrimary.getSupertokensUserId()));
            assertEquals("group primary appears once under EP filter", 1,
                    Collections.frequency(epOnly, epPrimary.getSupertokensUserId()));
            assertEquals("group primary appears once under TP filter", 1,
                    Collections.frequency(tpOnly, epPrimary.getSupertokensUserId()));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * Characterises what the streaming rewrite does when its load-bearing precondition — invariant I6,
     * "every row of a linked group shares one {@code primary_or_recipe_user_time_joined}" — is
     * violated. The old GROUP BY/MIN form collapsed a mixed-time group to a single (mis-sorted) row
     * via MIN; the DISTINCT form cannot, so the group's primary surfaces once per distinct time value,
     * i.e. a duplicate listing entry.
     *
     * This is not a reachable production state: I6 is transactionally enforced on link/unlink/bulk-link
     * and the MIGRATED backfill copies the already-consistent group time, so a read-from-new mode never
     * observes a mixed-time group — and PLAN-005's rollout adds a pre-deploy I6 data check + repair as a
     * belt-and-braces guard. The test forces the violation directly (raw UPDATE, bypassing the updater)
     * purely to PIN the failure mode so it is not silent: if a future change regresses I6 enforcement,
     * or reintroduces a defensive query shape, this assertion changes and forces a conscious decision.
     * It also documents that the damage is bounded — only the offending group duplicates; every
     * consistent user in the same listing is untouched, nothing is dropped, and no login method leaks as
     * its own row.
     */
    @Test
    public void testI6ViolationYieldsBoundedDuplicateNotCorruption() throws Exception {
        for (MigrationMode mode : new MigrationMode[]{MigrationMode.MIGRATED, MigrationMode.DUAL_WRITE_READ_NEW}) {
            TestingProcessManager.TestingProcess process = startProcess();
            try {
                Main main = process.getProcess();
                if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;

                Start storage = (Start) StorageLayer.getStorage(main);
                Config.getConfig(storage).setMigrationModeForTesting(mode);

                AuthRecipeUserInfo before = signUp(main, "before-" + mode + "@test.com"); // singleton
                AuthRecipeUserInfo primary = signUp(main, "primary-" + mode + "@test.com"); // group primary
                AuthRecipeUserInfo member = signUp(main, "member-" + mode + "@test.com"); // linked into the group
                AuthRecipeUserInfo after = signUp(main, "after-" + mode + "@test.com"); // singleton

                AuthRecipe.createPrimaryUser(main, primary.getSupertokensUserId());
                AuthRecipe.linkAccounts(main, member.getSupertokensUserId(), primary.getSupertokensUserId());

                // Baseline: while I6 holds the group is a single, non-duplicated row.
                List<String> healthy = idsFrom(AuthRecipe.getUsers(main, 1000, "ASC", null, null, null).users);
                assertNoDuplicates(healthy);
                assertEquals("group is one row while I6 holds", 1,
                        Collections.frequency(healthy, primary.getSupertokensUserId()));

                // Break I6: leave the member's row at the LEGACY `0` sentinel while the primary keeps its
                // real time — the exact shape of a group the MIGRATED backfill has only partially reached.
                forceTimeJoinedForRow(storage, member.getSupertokensUserId(), 0L);

                for (String order : new String[]{"ASC", "DESC"}) {
                    List<String> ids = idsFrom(AuthRecipe.getUsers(main, 1000, order, null, null, null).users);

                    // The group's primary now appears once per distinct time in the group (here: 2).
                    assertEquals("mixed-time group duplicates the primary (" + mode + " " + order + ")", 2,
                            Collections.frequency(ids, primary.getSupertokensUserId()));
                    // Damage is confined to the offending group: singletons stay single, and no login
                    // method leaks as its own row (both duplicate rows carry the primary's id).
                    assertEquals("unaffected singleton stays single: before (" + mode + " " + order + ")", 1,
                            Collections.frequency(ids, before.getSupertokensUserId()));
                    assertEquals("unaffected singleton stays single: after (" + mode + " " + order + ")", 1,
                            Collections.frequency(ids, after.getSupertokensUserId()));
                    assertFalse("linked login method never appears as its own row",
                            ids.contains(member.getSupertokensUserId()));
                }
            } finally {
                process.kill();
                assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            }
        }
    }
}
