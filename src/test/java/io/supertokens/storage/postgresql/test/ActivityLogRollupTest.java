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

import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.auditlog.AuditLogEvent;
import io.supertokens.pluginInterface.auditlog.RollupEventTypes;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.queries.ActiveUsersQueries;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The fold+reconcile rollup that derives {@code user_last_active} from the {@code activity_log}, plus the
 * transactional audit insert. Covers: fold idempotency, {@code GREATEST} monotonicity, the reconcile that
 * drops a recipe user linked away within the window, and the atomicity of a transactional audit write with
 * its surrounding mutation.
 */
public class ActivityLogRollupTest {

    private static final String APP_ID = "public";

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
     * Folding the same window twice yields the same projection — the fold is idempotent, so overlapping
     * windows and concurrent passes are harmless.
     */
    @Test
    public void foldIsIdempotentAcrossRepeatedPasses() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        if (storage == null) {
            return;
        }
        String log = Config.getConfig(storage).getActivityLogTable();

        long base = System.currentTimeMillis();
        String userA = "rollup-idempotent-A";
        String userB = "rollup-idempotent-B";
        registerUser(storage, APP_ID, userA);
        registerUser(storage, APP_ID, userB);

        insertActivityEvent(storage, log, userA, base + 1000);
        insertActivityEvent(storage, log, userA, base + 2000); // A's most recent
        insertActivityEvent(storage, log, userB, base + 3000);

        runRollup(storage, base - 10000);
        assertEquals(Long.valueOf(base + 2000), getLastActive(storage, userA));
        assertEquals(Long.valueOf(base + 3000), getLastActive(storage, userB));

        // Same window again — the projection must be unchanged.
        runRollup(storage, base - 10000);
        assertEquals(Long.valueOf(base + 2000), getLastActive(storage, userA));
        assertEquals(Long.valueOf(base + 3000), getLastActive(storage, userB));

        stopProcess(process);
    }

    /**
     * A fold must never lower an already-stored last-active timestamp when the window's most-recent
     * activity for that user is older than what is stored — e.g. a value written directly (the Phase-1
     * direct writer) or by an earlier, wider fold. {@code ON CONFLICT ... GREATEST} keeps it monotonic.
     * The window's MAX alone would not catch this: teeth require the stored value to exceed everything
     * the fold sees.
     */
    @Test
    public void foldNeverLowersAStoredTimestamp() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        if (storage == null) {
            return;
        }
        String log = Config.getConfig(storage).getActivityLogTable();
        String ula = Config.getConfig(storage).getUserLastActiveTable();

        long base = System.currentTimeMillis();
        String user = "rollup-monotonic";
        registerUser(storage, APP_ID, user);

        // Projection already holds a recent value (as if written directly, ahead of the log).
        seedUserLastActive(storage, ula, user, base + 9000);

        // The only activity the fold can see for this user is older than the stored value.
        insertActivityEvent(storage, log, user, base + 1000);
        runRollup(storage, base - 10000);

        // GREATEST(stored, folded) keeps the higher stored timestamp.
        assertEquals(Long.valueOf(base + 9000), getLastActive(storage, user));

        stopProcess(process);
    }

    /**
     * A recipe user active just before being linked to a primary: after fold+reconcile in one pass, only
     * the primary user's projection row remains (the linked-away recipe user's row is reconciled away). The
     * {@code account_linking} event also folds — crediting the primary ({@code primary_or_recipe_user_id})
     * at the link's timestamp — so the primary's last-active is the link time, above its own earlier activity.
     */
    @Test
    public void reconcileRemovesRecipeUserLinkedAwayInWindow() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        if (storage == null) {
            return;
        }
        String log = Config.getConfig(storage).getActivityLogTable();

        long base = System.currentTimeMillis();
        String recipeUser = "rollup-reconcile-R";
        String primaryUser = "rollup-reconcile-P";
        registerUser(storage, APP_ID, recipeUser);
        registerUser(storage, APP_ID, primaryUser);

        // R was active on its own, then P was active, then R got linked into P — all within the window.
        insertActivityEvent(storage, log, recipeUser, base + 1000);
        insertActivityEvent(storage, log, primaryUser, base + 2000);
        insertAccountLinkingEvent(storage, log, recipeUser, primaryUser, base + 3000);

        runRollup(storage, base - 10000);

        // The recipe user's row is gone (folded then reconciled away); the primary user's row remains, credited
        // up to the account_linking event (base + 3000), which counts as activity for the primary.
        assertNull(getLastActive(storage, recipeUser));
        assertEquals(Long.valueOf(base + 3000), getLastActive(storage, primaryUser));

        stopProcess(process);
    }

    /**
     * The fold must never resurrect a projection row for an app that no longer exists. {@code activity_log}
     * rows are intentionally retained after an app is deleted (the table has no app foreign key), while
     * {@code user_last_active} cascades on app delete via its {@code app_id -> apps} FK. Without a guard the
     * fold re-inserts the retained events and the INSERT violates that FK — exactly the failure that
     * surfaced across the suite once the test DB stopped being reset. The {@code EXISTS (apps)} guard
     * confines the fold to still-existing apps: an event whose app_id is absent from {@code apps} is
     * skipped (no FK violation, no resurrected row) while a concurrent event for an existing app still
     * folds normally.
     */
    @Test
    public void foldSkipsActivityForAppMissingFromApps() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        if (storage == null) {
            return;
        }
        String log = Config.getConfig(storage).getActivityLogTable();

        long base = System.currentTimeMillis();
        String existingAppUser = "rollup-app-guard-existing";
        String deletedAppUser = "rollup-app-guard-deleted";
        // "public" is present in the apps table; this id never is. activity_log has no app FK, so the row
        // inserts fine, standing in for a deleted app whose activity_log rows still linger.
        String missingAppId = "app-that-was-deleted";
        // The existing-app user is a real user; the deleted-app user cannot be registered (its app_id is
        // absent from apps), which is fine — the apps guard skips it before the user guard is reached.
        registerUser(storage, APP_ID, existingAppUser);

        insertActivityEventForApp(storage, log, APP_ID, existingAppUser, base + 1000);
        insertActivityEventForApp(storage, log, missingAppId, deletedAppUser, base + 2000);

        // Without the guard this fold would throw a user_last_active -> apps FK violation on the missing app.
        runRollup(storage, base - 10000);

        // The existing app's user is folded; the missing app's user is skipped (no resurrected row).
        assertEquals(Long.valueOf(base + 1000), getLastActiveForApp(storage, APP_ID, existingAppUser));
        assertNull(getLastActiveForApp(storage, missingAppId, deletedAppUser));

        stopProcess(process);
    }

    /**
     * The fold must never resurrect a projection row for a user that no longer exists. {@code activity_log}
     * rows are intentionally retained after a user is deleted (the table has no user foreign key), while
     * {@code deleteUserActive_Transaction} removes the user's {@code user_last_active} row. Without the user
     * guard the next fold re-reads the retained events and silently re-inserts the row — a permanent MAU
     * overcount for any user deleted within a fold window of their last activity, and, on a lost-watermark
     * full-retention catch-up, for every user deleted in the retention window. The
     * {@code EXISTS (app_id_to_user_id)} guard confines the fold to still-existing users: a deleted user's
     * retained activity is skipped while a coexisting live user still folds.
     */
    @Test
    public void foldSkipsActivityForDeletedUser() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        if (storage == null) {
            return;
        }
        String log = Config.getConfig(storage).getActivityLogTable();

        long base = System.currentTimeMillis();
        String liveUser = "rollup-user-guard-live";
        String deletedUser = "rollup-user-guard-deleted";
        registerUser(storage, APP_ID, liveUser);
        registerUser(storage, APP_ID, deletedUser);

        insertActivityEvent(storage, log, liveUser, base + 1000);
        insertActivityEvent(storage, log, deletedUser, base + 2000);

        // First pass folds both — both users exist.
        runRollup(storage, base - 10000);
        assertEquals(Long.valueOf(base + 1000), getLastActive(storage, liveUser));
        assertEquals(Long.valueOf(base + 2000), getLastActive(storage, deletedUser));

        // The user is deleted: its user_last_active row and app_id_to_user_id row go away, but its
        // activity_log rows are intentionally retained.
        deleteUser(storage, APP_ID, deletedUser);
        assertNull(getLastActive(storage, deletedUser));

        // Second pass over the same window must NOT resurrect the deleted user from the retained events;
        // the live user is unaffected.
        runRollup(storage, base - 10000);
        assertEquals(Long.valueOf(base + 1000), getLastActive(storage, liveUser));
        assertNull(getLastActive(storage, deletedUser));

        stopProcess(process);
    }

    /**
     * The reconcile is order-insensitive. A recipe user linked, then unlinked, then active again inside
     * one window must keep its post-unlink credit: the {@code account_linking} event matches the user by
     * {@code recipe_user_id}, but its later activity gives the projection a {@code last_active_time} newer
     * than the link, so the ordering guard ({@code al.created_at >= ula.last_active_time}) keeps the row
     * instead of scrubbing it. A recipe user with no activity after the link is still reconciled away.
     */
    @Test
    public void reconcileKeepsRecipeUserActiveAfterLinkInWindow() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        if (storage == null) {
            return;
        }
        String log = Config.getConfig(storage).getActivityLogTable();

        long base = System.currentTimeMillis();
        String activeAgain = "rollup-order-active-again"; // linked, then active again on its own
        String linkedAway = "rollup-order-linked-away";   // linked, no later activity
        String primary = "rollup-order-primary";
        registerUser(storage, APP_ID, activeAgain);
        registerUser(storage, APP_ID, linkedAway);
        registerUser(storage, APP_ID, primary);

        // activeAgain: linked into primary at t1, then (after an unlink) signs in on its own at t3.
        insertAccountLinkingEvent(storage, log, activeAgain, primary, base + 1000);
        insertActivityEvent(storage, log, activeAgain, base + 3000);

        // linkedAway: active before the link at t0, linked at t2, nothing after — the classic reconcile case.
        insertActivityEvent(storage, log, linkedAway, base + 500);
        insertAccountLinkingEvent(storage, log, linkedAway, primary, base + 2000);

        runRollup(storage, base - 10000);

        // activeAgain keeps its post-link credit — the stale earlier link event does not scrub it.
        assertEquals(Long.valueOf(base + 3000), getLastActive(storage, activeAgain));
        // linkedAway had no activity after the link, so it is reconciled away as before.
        assertNull(getLastActive(storage, linkedAway));

        stopProcess(process);
    }

    /**
     * A transactional audit write plus a mutation on one connection, with a failure injected after the
     * write, must roll back together — neither the audit row nor the mutation survives.
     */
    @Test
    public void auditWriteAndMutationRollBackTogetherOnFailure() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        if (storage == null) {
            return;
        }
        String log = Config.getConfig(storage).getActivityLogTable();
        String ula = Config.getConfig(storage).getUserLastActiveTable();

        long base = System.currentTimeMillis();
        String user = "rollup-atomic";

        try {
            storage.startTransaction(con -> {
                Connection sqlCon = (Connection) con.getConnection();
                // A mutation on the projection table...
                String upsert = "INSERT INTO " + ula + " (app_id, user_id, last_active_time)"
                        + " VALUES (?, ?, ?)";
                try (PreparedStatement pst = sqlCon.prepareStatement(upsert)) {
                    pst.setString(1, APP_ID);
                    pst.setString(2, user);
                    pst.setLong(3, base);
                    pst.executeUpdate();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                // ...and its audit entry on the SAME connection.
                storage.createActivityLogEntry_Transaction(con, new TenantIdentifier(null, null, null),
                        new AuditLogEvent(APP_ID, "public", user, user, "user_last_active", "success",
                                null, null, base, null));
                // Injected failure after both writes: startTransaction rolls the connection back.
                throw new RuntimeException("injected failure after audit write");
            });
            fail("expected the injected failure to propagate");
        } catch (Exception e) {
            // expected — the transaction rolled back
        }

        // Both writes are gone.
        assertNull(getLastActive(storage, user));
        assertEquals(0, countActivityLogEventsForUser(storage, log, user));

        stopProcess(process);
    }

    /**
     * The fold's activity source is the semantic event set ({@link RollupEventTypes#FOLD_SET}), not the retired
     * {@code user_last_active} event. Every included type must credit its {@code primary_or_recipe_user_id};
     * every excluded type ({@code user_import}, other lifecycle types, {@code user_last_active} itself) must
     * be ignored. Asserted per type because a typo or drift from the names core emits would silently drop or
     * spuriously credit activity with nothing else failing.
     */
    @Test
    public void foldCreditsIncludedEventTypesAndIgnoresExcluded() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        if (storage == null) {
            return;
        }
        String log = Config.getConfig(storage).getActivityLogTable();

        long base = System.currentTimeMillis();

        // One distinct user per included type, each at a distinct timestamp. The credited user is the
        // event's primary_or_recipe_user_id. For account_linking the recipe_user_id is a distinct throwaway
        // id (a real link credits the primary, not the linked-away recipe user) so the reconcile — which
        // deletes rows whose user_id matches an account_linking recipe_user_id — does not remove the primary.
        String[] included = {"sign_in", "token_refresh", "session_create", "sign_out",
                "oauth_token_exchange", "oauth_authorize", "user_creation", "account_linking"};
        for (int i = 0; i < included.length; i++) {
            String user = "fold-included-" + included[i];
            String recipeUserId = included[i].equals("account_linking") ? "fold-linked-away-" + i : user;
            registerUser(storage, APP_ID, user);
            insertActivityLogRow(storage, log, APP_ID, recipeUserId, user, included[i], base + 1000 + i);
        }

        // Types the fold must ignore: user_import (imported != active), other lifecycle types, and the
        // retired user_last_active synthetic event (no writer remains).
        String[] excluded = {"user_import", "user_deletion", "account_unlinking", "tenant_association",
                "user_last_active"};
        for (int i = 0; i < excluded.length; i++) {
            String user = "fold-excluded-" + excluded[i];
            // Registered too, so the ONLY reason these never fold is the event_type filter, not the user guard.
            registerUser(storage, APP_ID, user);
            insertActivityLogRow(storage, log, APP_ID, user, user, excluded[i], base + 2000 + i);
        }

        runRollup(storage, base - 10000);

        // Each included type credited its user with the event's timestamp.
        for (int i = 0; i < included.length; i++) {
            String user = "fold-included-" + included[i];
            assertEquals("expected " + included[i] + " to fold", Long.valueOf(base + 1000 + i),
                    getLastActive(storage, user));
        }
        // No excluded type produced a projection row.
        for (String type : excluded) {
            assertNull("expected " + type + " to be ignored", getLastActive(storage, "fold-excluded-" + type));
        }

        stopProcess(process);
    }

    /**
     * The rollup cron's gate: {@code hasUnfoldedActivitySince} must return true only when there is
     * rollup-relevant activity (a fold-relevant event, per {@link RollupEventTypes#FOLD_SET})
     * strictly newer than the watermark, and must ignore excluded event types. A regression here (e.g. a typo
     * in the event-type literals, or drift from the names core emits) would silently disable the rollup with
     * nothing else failing, so it is asserted directly.
     */
    @Test
    public void gateSeesOnlyRollupRelevantActivityStrictlyAfterWatermark() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        if (storage == null) {
            return;
        }
        String log = Config.getConfig(storage).getActivityLogTable();

        long base = System.currentTimeMillis();

        // Nothing recorded yet.
        assertEquals(false, storage.hasUnfoldedActivitySince(base));

        // A sign_in event at base+1000 makes the gate open for any watermark below it...
        insertActivityEvent(storage, log, "gate-user-A", base + 1000);
        assertEquals(true, storage.hasUnfoldedActivitySince(base));

        // ...but the predicate is strict (created_at > ?): a watermark exactly on the row's timestamp
        // must not see it, and one above it must not either.
        assertEquals(false, storage.hasUnfoldedActivitySince(base + 1000));
        assertEquals(false, storage.hasUnfoldedActivitySince(base + 2000));

        // account_linking is another rollup-relevant type and must also open the gate.
        insertAccountLinkingEvent(storage, log, "gate-recipe-B", "gate-primary-B", base + 3000);
        assertEquals(true, storage.hasUnfoldedActivitySince(base + 2000));

        // An excluded event type (user_import), even when newer than everything, must not open the gate.
        insertActivityLogRow(storage, log, APP_ID, "gate-user-C", "gate-user-C", "user_import", base + 4000);
        assertEquals(false, storage.hasUnfoldedActivitySince(base + 3000));

        // ...nor the retired user_last_active synthetic event.
        insertActivityLogRow(storage, log, APP_ID, "gate-user-D", "gate-user-D", "user_last_active", base + 5000);
        assertEquals(false, storage.hasUnfoldedActivitySince(base + 3000));

        stopProcess(process);
    }

    // ---- helpers ----

    /**
     * When a concurrent instance already holds the rollup advisory lock, the fold takes the non-blocking
     * lock, loses it, and returns {@code false} without writing anything — so the caller (the cron) knows
     * it folded nothing and must not advance its watermark. Once the lock is released, the same fold runs
     * and returns {@code true}. Models the lock-loser case that the void signature could not express.
     */
    @Test
    public void skippedFoldReturnsFalseAndFoldsNothingWhileTheRollupLockIsHeld() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        if (storage == null) {
            return;
        }
        String log = Config.getConfig(storage).getActivityLogTable();

        long base = System.currentTimeMillis();
        String user = "rollup-lock-skip";
        registerUser(storage, APP_ID, user);
        insertActivityEvent(storage, log, user, base + 1000);

        // Hold the rollup advisory lock in a concurrent, still-open transaction so the fold under test
        // loses the non-blocking lock. The key must match the (private) constant
        // ActiveUsersQueries.LAST_ACTIVE_ROLLUP_LOCK_KEY.
        CountDownLatch lockHeld = new CountDownLatch(1);
        CountDownLatch releaseLock = new CountDownLatch(1);
        AtomicReference<Exception> holderError = new AtomicReference<>();
        Thread holder = new Thread(() -> {
            try {
                storage.startTransaction(con -> {
                    Connection sqlCon = (Connection) con.getConnection();
                    try {
                        io.supertokens.storage.postgresql.queries.Utils.takeAdvisoryLock(sqlCon,
                                "last_active_rollup");
                        lockHeld.countDown();
                        releaseLock.await();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    storage.commitTransaction(con);
                    return null;
                });
            } catch (Exception e) {
                holderError.set(e);
                lockHeld.countDown();
            }
        });
        holder.start();
        assertTrue(lockHeld.await(20, TimeUnit.SECONDS));
        assertNull(holderError.get());

        // The fold loses the lock: returns false and writes nothing.
        boolean folded = storage.startTransaction(con -> {
            boolean r = storage.rollupLastActiveFromActivityLog_Transaction(con, base - 10000);
            storage.commitTransaction(con);
            return r;
        });
        assertFalse(folded);
        assertNull(getLastActive(storage, user));

        // Release the lock; the next fold runs to completion and reports it folded.
        releaseLock.countDown();
        holder.join(20_000);
        assertNull(holderError.get());

        boolean foldedNow = storage.startTransaction(con -> {
            boolean r = storage.rollupLastActiveFromActivityLog_Transaction(con, base - 10000);
            storage.commitTransaction(con);
            return r;
        });
        assertTrue(foldedNow);
        assertEquals(Long.valueOf(base + 1000), getLastActive(storage, user));

        stopProcess(process);
    }

    private TestingProcessManager.TestingProcess startProcess() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            return process;
        }
        return process;
    }

    private void stopProcess(TestingProcessManager.TestingProcess process) throws Exception {
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private void runRollup(Start storage, long windowStartMillis) throws Exception {
        storage.startTransaction(con -> {
            storage.rollupLastActiveFromActivityLog_Transaction(con, windowStartMillis);
            storage.commitTransaction(con);
            return null;
        });
    }

    private Long getLastActive(Start storage, String userId) throws Exception {
        return ActiveUsersQueries.getLastActiveByUserId(storage, new AppIdentifier(null, null), userId);
    }

    private void seedUserLastActive(Start storage, String ulaTable, String userId, long lastActiveTime)
            throws Exception {
        String query = "INSERT INTO " + ulaTable + " (app_id, user_id, last_active_time) VALUES (?, ?, ?)";
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setString(1, APP_ID);
                pst.setString(2, userId);
                pst.setLong(3, lastActiveTime);
                pst.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            storage.commitTransaction(con);
            return null;
        });
    }

    private Long getLastActiveForApp(Start storage, String appId, String userId) throws Exception {
        return ActiveUsersQueries.getLastActiveByUserId(storage, new AppIdentifier(null, appId), userId);
    }

    /**
     * Register {@code userId} as a standalone (self-primary) user in {@code app_id_to_user_id} so it
     * satisfies the fold's user-existence guard. Not deleting this row is how we simulate a still-existing
     * user; omitting the row (or deleting it) simulates a deleted user whose activity_log rows linger.
     */
    private void registerUser(Start storage, String appId, String userId) throws Exception {
        String query = "INSERT INTO " + Config.getConfig(storage).getAppIdToUserIdTable()
                + " (app_id, user_id, recipe_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user,"
                + " time_joined, primary_or_recipe_user_time_joined) VALUES (?, ?, 'emailpassword', ?, false, 0, 0)";
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setString(1, appId);
                pst.setString(2, userId);
                pst.setString(3, userId);
                pst.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            storage.commitTransaction(con);
            return null;
        });
    }

    /**
     * Simulate a user deletion: drop the user's {@code user_last_active} projection row and its
     * {@code app_id_to_user_id} row, while leaving its {@code activity_log} rows in place (they are
     * retained after deletion). This is the exact state that let the fold resurrect the user.
     */
    private void deleteUser(Start storage, String appId, String userId) throws Exception {
        String ula = Config.getConfig(storage).getUserLastActiveTable();
        String appToUser = Config.getConfig(storage).getAppIdToUserIdTable();
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                for (String table : new String[]{ula, appToUser}) {
                    try (PreparedStatement pst = sqlCon.prepareStatement(
                            "DELETE FROM " + table + " WHERE app_id = ? AND user_id = ?")) {
                        pst.setString(1, appId);
                        pst.setString(2, userId);
                        pst.executeUpdate();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            storage.commitTransaction(con);
            return null;
        });
    }

    private void insertActivityEvent(Start storage, String table, String userId, long createdAt)
            throws Exception {
        // A semantic activity event (one of RollupEventTypes.FOLD_SET); the user is its own
        // primary_or_recipe_user_id.
        insertActivityLogRow(storage, table, APP_ID, userId, userId, "sign_in", createdAt);
    }

    private void insertActivityEventForApp(Start storage, String table, String appId, String userId,
                                           long createdAt) throws Exception {
        insertActivityLogRow(storage, table, appId, userId, userId, "sign_in", createdAt);
    }

    private void insertAccountLinkingEvent(Start storage, String table, String recipeUserId,
                                           String primaryUserId, long createdAt) throws Exception {
        insertActivityLogRow(storage, table, APP_ID, recipeUserId, primaryUserId, "account_linking", createdAt);
    }

    private void insertActivityLogRow(Start storage, String table, String appId, String recipeUserId,
                                      String primaryOrRecipeUserId, String eventType, long createdAt)
            throws Exception {
        String query = "INSERT INTO " + table
                + " (app_id, tenant_id, recipe_user_id, primary_or_recipe_user_id, event_type, status, created_at)"
                + " VALUES (?, 'public', ?, ?, ?, 'success', ?)";
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setString(1, appId);
                pst.setString(2, recipeUserId);
                pst.setString(3, primaryOrRecipeUserId);
                pst.setString(4, eventType);
                pst.setLong(5, createdAt);
                pst.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            storage.commitTransaction(con);
            return null;
        });
    }

    private int countActivityLogEventsForUser(Start storage, String table, String userId) throws Exception {
        String query = "SELECT COUNT(*) FROM " + table + " WHERE primary_or_recipe_user_id = ?";
        return storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
                pst.setString(1, userId);
                try (ResultSet rs = pst.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
