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
 */

package io.supertokens.storage.postgresql.queries;

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.useridmapping.LockedUser;
import io.supertokens.pluginInterface.useridmapping.LockedUserPair;
import io.supertokens.pluginInterface.useridmapping.UserNotFoundForLockingException;
import io.supertokens.storage.postgresql.LockedUserImpl;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;

public class UserLockingQueries {

    /**
     * Locks a single user and returns LockedUser.
     * Also locks the primary user if the user is linked.
     */
    public static LockedUser lockUser(Start start, Connection con, AppIdentifier appIdentifier, String userId)
            throws SQLException, StorageQueryException, UserNotFoundForLockingException {
        return lockUsers(start, con, appIdentifier, List.of(userId)).get(0);
    }

    /**
     * Locks multiple users (and their primaries) with a single query.
     * Uses ORDER BY user_id to acquire locks in consistent order, preventing deadlocks.
     */
    public static List<LockedUser> lockUsers(Start start, Connection con, AppIdentifier appIdentifier,
                                              List<String> userIds)
            throws SQLException, StorageQueryException, UserNotFoundForLockingException {

        if (userIds.isEmpty()) {
            return Collections.emptyList();
        }

        String table = Config.getConfig(start).getAppIdToUserIdTable();
        String appId = appIdentifier.getAppId();

        // Two-query approach: first discover primary IDs, then lock everything.
        //
        // The former single-query approach used an OR + IN subquery:
        //     WHERE u.user_id = ANY(?) OR u.user_id IN (SELECT primary_or_recipe_user_id ...)
        // Under PostgreSQL's generic prepared-statement plans the OR forced a
        // BitmapOr / nested-loop semi-join that scanned app_id_to_user_id twice.
        // At 20 VUs on 100k users this was the dominant cost in both make_primary
        // (6.2 ms avg, 154s total) and link_users (11.3 ms avg, 250s total).
        //
        // Splitting into two PK-only queries guarantees each one resolves via a
        // simple Index Scan on (app_id, user_id) regardless of plan caching.
        // The extra round-trip is negligible (same connection, same transaction,
        // sub-microsecond network) and the total wall time drops significantly.

        // --- Round 1: find primary user IDs for any linked input users ----------
        // Pure read, no lock. Uses PK (app_id, user_id) for the WHERE, returns
        // primary_or_recipe_user_id for linked users whose primary differs from
        // themselves. Typical result: 0-2 rows.
        String FIND_PRIMARIES =
                "SELECT primary_or_recipe_user_id FROM " + table
                + " WHERE app_id = ? AND user_id = ANY(?)"
                + "   AND is_linked_or_is_a_primary_user = TRUE"
                + "   AND primary_or_recipe_user_id <> user_id";

        Array userIdsArray = con.createArrayOf("VARCHAR", userIds.toArray(new String[0]));

        Set<String> allIdsToLock = new LinkedHashSet<>(userIds);
        execute(con, FIND_PRIMARIES, pst -> {
            pst.setObject(1, appId, Types.VARCHAR);
            pst.setArray(2, userIdsArray);
        }, rs -> {
            while (rs.next()) {
                allIdsToLock.add(rs.getString("primary_or_recipe_user_id").trim());
            }
            return null;
        });

        // --- Round 2: lock all discovered user IDs ----------------------------
        // Simple ANY on PK — the planner always picks an Index Scan.
        // ORDER BY user_id keeps lock acquisition deterministic across
        // concurrent callers (no deadlock cycles).
        //
        // Post-lock validation: after FOR UPDATE returns the latest committed
        // state, we check whether any locked row's primary_or_recipe_user_id
        // points to a user we didn't lock. This closes the TOCTOU window
        // between round 1 (unlocked read) and round 2 (lock acquisition) —
        // if another transaction changed the linking between the two rounds,
        // we expand the lock set and re-issue FOR UPDATE. Re-locking rows we
        // already hold in the same transaction is a no-op in PostgreSQL.
        // The loop is capped at MAX_LOCK_EXPANSION_ATTEMPTS iterations.
        // In practice it converges in 1-2: primaries don't have their own
        // primaries, so one expansion is always sufficient. The cap is a
        // safety net against pathological concurrent re-linking storms.

        final int MAX_LOCK_EXPANSION_ATTEMPTS = 3;

        String LOCK_QUERY = "SELECT u.user_id, u.primary_or_recipe_user_id, u.is_linked_or_is_a_primary_user, u.recipe_id"
                + " FROM " + table + " u"
                + " WHERE u.app_id = ? AND u.user_id = ANY(?)"
                + " ORDER BY u.user_id"
                + " FOR UPDATE";

        Map<String, LockedUser> lockedByUserId = null;
        for (int attempt = 0; attempt < MAX_LOCK_EXPANSION_ATTEMPTS; attempt++) {
            Array allIdsArray = con.createArrayOf("VARCHAR", allIdsToLock.toArray(new String[0]));

            lockedByUserId = execute(con, LOCK_QUERY, pst -> {
                pst.setObject(1, appId, Types.VARCHAR);
                pst.setArray(2, allIdsArray);
            }, rs -> {
                Map<String, LockedUser> map = new HashMap<>();
                while (rs.next()) {
                    String uid = rs.getString("user_id").trim();
                    String recipeId = rs.getString("recipe_id");
                    boolean isLinkedOrPrimary = rs.getBoolean("is_linked_or_is_a_primary_user");
                    String primaryUid = isLinkedOrPrimary
                            ? rs.getString("primary_or_recipe_user_id").trim() : null;
                    map.put(uid, new LockedUserImpl(uid, recipeId, primaryUid, con));
                }
                return map;
            });

            // Post-lock validation: check if any locked user's primary is
            // outside the lock set. If so, expand and re-lock.
            boolean needsExpansion = false;
            for (LockedUser lu : lockedByUserId.values()) {
                String primary = lu.getPrimaryUserId();
                if (primary != null && !allIdsToLock.contains(primary)) {
                    allIdsToLock.add(primary);
                    needsExpansion = true;
                }
            }
            if (!needsExpansion) {
                break;
            }
            if (attempt == MAX_LOCK_EXPANSION_ATTEMPTS - 1) {
                throw new SQLException(
                        "Failed to stabilise user lock set after " + MAX_LOCK_EXPANSION_ATTEMPTS
                        + " attempts — concurrent re-linking is preventing lock convergence");
            }
            // Loop re-issues FOR UPDATE with the expanded set.
        }

        // Build result list in the same order as requested, verifying all users were found
        List<LockedUser> result = new ArrayList<>(userIds.size());
        for (String userId : userIds) {
            LockedUser locked = lockedByUserId.get(userId);
            if (locked == null) {
                throw new UserNotFoundForLockingException(userId);
            }
            result.add(locked);
        }

        return result;
    }

    /**
     * Convenience method for locking two users for linking operations.
     */
    public static LockedUserPair lockUsersForLinking(Start start, Connection con, AppIdentifier appIdentifier,
                                                      String recipeUserId, String primaryUserId)
            throws SQLException, StorageQueryException, UserNotFoundForLockingException {

        List<LockedUser> locked = lockUsers(start, con, appIdentifier, List.of(recipeUserId, primaryUserId));
        return new LockedUserPair(locked.get(0), locked.get(1));
    }
}
