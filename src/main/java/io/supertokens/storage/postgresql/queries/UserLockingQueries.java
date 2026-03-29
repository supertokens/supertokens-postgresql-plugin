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

import io.supertokens.storage.postgresql.utils.Utils;

import java.sql.Connection;
import java.sql.SQLException;
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
        String placeholders = Utils.generateCommaSeperatedQuestionMarks(userIds.size());

        // Single query that locks both the requested users AND their primary users (if linked).
        // The UNION ensures we lock primaries even if they weren't in the original request.
        // ORDER BY ensures consistent lock ordering to prevent deadlocks.
        String QUERY = "SELECT u.user_id, u.primary_or_recipe_user_id, u.is_linked_or_is_a_primary_user, u.recipe_id"
                + " FROM " + table + " u"
                + " WHERE u.app_id = ? AND u.user_id IN ("
                + "   SELECT user_id FROM " + table + " WHERE app_id = ? AND user_id IN (" + placeholders + ")"
                + "   UNION"
                + "   SELECT primary_or_recipe_user_id FROM " + table
                + "     WHERE app_id = ? AND user_id IN (" + placeholders + ")"
                + "     AND is_linked_or_is_a_primary_user = TRUE"
                + " )"
                + " ORDER BY u.user_id"
                + " FOR UPDATE";

        // Build the result map from a single query
        Map<String, LockedUser> lockedByUserId = execute(con, QUERY, pst -> {
            int idx = 1;
            pst.setString(idx++, appIdentifier.getAppId());
            // First subquery params
            pst.setString(idx++, appIdentifier.getAppId());
            for (String uid : userIds) {
                pst.setString(idx++, uid);
            }
            // Second subquery params
            pst.setString(idx++, appIdentifier.getAppId());
            for (String uid : userIds) {
                pst.setString(idx++, uid);
            }
        }, rs -> {
            Map<String, LockedUser> map = new HashMap<>();
            while (rs.next()) {
                String uid = rs.getString("user_id");
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
