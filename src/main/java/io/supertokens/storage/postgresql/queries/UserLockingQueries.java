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

        // Locks the requested users AND their primary users (if linked) in one scan.
        //
        // The first branch of the WHERE (u.user_id = ANY(?)) covers every
        // requested user. The second branch adds any primary_or_recipe_user_id
        // for requested users that are linked — so that a linking primary gets
        // locked too, even if the caller didn't name it.
        //
        // This replaces the former pattern of
        //     u.user_id IN (SELECT user_id ... UNION SELECT primary_or_recipe_user_id ...)
        // whose first UNION branch was redundant with the outer scan and forced
        // an extra index access + Sort + Unique + Memoize. The OR form gives the
        // planner a BitmapOr across one index, and when no user is linked (the
        // common case) short-circuits to a single array-probe index scan.
        //
        // ORDER BY user_id keeps lock acquisition deterministic across
        // concurrent callers (no deadlock cycles).
        String QUERY = "SELECT u.user_id, u.primary_or_recipe_user_id, u.is_linked_or_is_a_primary_user, u.recipe_id"
                + " FROM " + table + " u"
                + " WHERE u.app_id = ?"
                + "   AND ("
                + "        u.user_id = ANY(?)"
                + "     OR u.user_id IN ("
                + "          SELECT primary_or_recipe_user_id FROM " + table
                + "           WHERE app_id = ? AND user_id = ANY(?)"
                + "             AND is_linked_or_is_a_primary_user = TRUE"
                + "        )"
                + "   )"
                + " ORDER BY u.user_id"
                + " FOR UPDATE";

        // Build a single VARCHAR[] from the input user_ids and bind it twice
        // (once per array-probe in the query). Using = ANY(?) with an array
        // keeps the prepared-statement plan cache stable regardless of N.
        String appId = appIdentifier.getAppId();
        Array userIdsArray = con.createArrayOf("VARCHAR", userIds.toArray(new String[0]));

        Map<String, LockedUser> lockedByUserId = execute(con, QUERY, pst -> {
            // Explicit VARCHAR binds avoid the (app_id)::text = $N cast that
            // JDBC's default `text` inference produces — that cast breaks
            // index-only scans on the VARCHAR(64) app_id column.
            pst.setObject(1, appId, Types.VARCHAR);
            pst.setArray(2, userIdsArray);
            pst.setObject(3, appId, Types.VARCHAR);
            pst.setArray(4, userIdsArray);
        }, rs -> {
            Map<String, LockedUser> map = new HashMap<>();
            while (rs.next()) {
                String uid = rs.getString("user_id").trim();
                String recipeId = rs.getString("recipe_id");
                boolean isLinkedOrPrimary = rs.getBoolean("is_linked_or_is_a_primary_user");
                String primaryUserId = isLinkedOrPrimary ? rs.getString("primary_or_recipe_user_id") : null;
                map.put(uid, new LockedUserImpl(uid, recipeId, primaryUserId, con));
            }
            return map;
        });

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
