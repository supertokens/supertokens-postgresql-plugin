package io.supertokens.storage.postgresql.queries;

import io.supertokens.pluginInterface.auditlog.RollupEventTypes;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;
import org.jetbrains.annotations.TestOnly;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;
import io.supertokens.storage.postgresql.annotations.AtomicAutoCommitWrite;

public class ActiveUsersQueries {
    static String getQueryToCreateUserLastActiveTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();

        return "CREATE TABLE IF NOT EXISTS " + Config.getConfig(start).getUserLastActiveTable() + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "user_id VARCHAR(128),"
                + "last_active_time BIGINT,"
                + "PRIMARY KEY(app_id, user_id),"
                + "CONSTRAINT " +
                Utils.getConstraintName(schema, Config.getConfig(start).getUserLastActiveTable(), "app_id", "fkey")
                + " FOREIGN KEY(app_id)"
                + " REFERENCES " + Config.getConfig(start).getAppsTable() + " (app_id) ON DELETE CASCADE"
                + ");";
    }

    static String getQueryToCreateAppIdIndexForUserLastActiveTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS user_last_active_app_id_index ON "
                + Config.getConfig(start).getUserLastActiveTable() + "(app_id);";
    }

    public static String getQueryToCreateAppIdLastActiveTimeIndexForUserLastActiveTable(Start start) {
        // (app_id, last_active_time): the MAU queries filter on both columns; the time-leading
        // index below cannot serve them, so they degrade to sequential scans.
        return "CREATE INDEX IF NOT EXISTS user_last_active_app_id_last_active_time_index ON "
                + Config.getConfig(start).getUserLastActiveTable() + "(app_id, last_active_time);";
    }

    public static String getQueryToCreateLastActiveTimeIndexForUserLastActiveTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS user_last_active_last_active_time_index ON "
                + Config.getConfig(start).getUserLastActiveTable() + "(last_active_time DESC, app_id DESC);";
    }

    public static Map<Integer, Integer> countUsersActiveSinceGroupedByDay(Start start, AppIdentifier appIdentifier,
                                                                          long sinceTime, long now)
            throws SQLException, StorageQueryException {
        // One bounded pass instead of one COUNT(*) per threshold: bucket users by whole days since
        // last activity. Callers rebuild the cumulative series in memory.
        String QUERY = "SELECT FLOOR((? - last_active_time) / 86400000) AS days_ago, COUNT(*) AS c FROM "
                + Config.getConfig(start).getUserLastActiveTable()
                + " WHERE app_id = ? AND last_active_time >= ? GROUP BY days_ago";

        return execute(start, QUERY, pst -> {
            pst.setLong(1, now);
            pst.setString(2, appIdentifier.getAppId());
            pst.setLong(3, sinceTime);
        }, result -> {
            Map<Integer, Integer> buckets = new HashMap<>();
            while (result.next()) {
                buckets.put(result.getInt("days_ago"), result.getInt("c"));
            }
            return buckets;
        });
    }

    public static int countUsersActiveSince(Start start, AppIdentifier appIdentifier, long sinceTime)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT COUNT(*) as total FROM " + Config.getConfig(start).getUserLastActiveTable()
                + " WHERE app_id = ? AND last_active_time >= ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, sinceTime);
        }, result -> {
            if (result.next()) {
                return result.getInt("total");
            }
            return 0;
        });
    }

    public static int countUsersActiveSinceAndHasMoreThanOneLoginMethod(Start start, AppIdentifier appIdentifier,
                                                                        long sinceTime)
            throws SQLException, StorageQueryException {
        // TODO: Active users are present only on public tenant and MFA users may be present on different storages
        String QUERY = "SELECT count(1) as c FROM ("
                + "  SELECT count(user_id) as num_login_methods, app_id, primary_or_recipe_user_id"
                + "  FROM " + Config.getConfig(start).getAppIdToUserIdTable()
                + "  WHERE primary_or_recipe_user_id IN ("
                + "    SELECT user_id FROM " + Config.getConfig(start).getUserLastActiveTable()
                + "    WHERE app_id = ? AND last_active_time >= ?"
                + "  )"
                + "  GROUP BY app_id, primary_or_recipe_user_id"
                + ") uc WHERE num_login_methods > 1";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, sinceTime);
        }, result -> {
            if (result.next()) {
                return result.getInt("c");
            }
            return 0;
        });
    }

    @AtomicAutoCommitWrite(justification = "idempotent last-active upsert; nothing to be atomic with")
    public static int updateUserLastActive(Start start, AppIdentifier appIdentifier, String userId)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + Config.getConfig(start).getUserLastActiveTable()
                +
                "(app_id, user_id, last_active_time) VALUES(?, ?, ?) ON CONFLICT(app_id, user_id) DO UPDATE SET " +
                "last_active_time = ?";

        long now = System.currentTimeMillis();
        return update(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
            pst.setLong(3, now);
            pst.setLong(4, now);
        });
    }

    @TestOnly
    @AtomicAutoCommitWrite(justification = "idempotent last-active upsert; nothing to be atomic with")
    public static int updateUserLastActive(Start start, AppIdentifier appIdentifier, String userId, long timestamp)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + Config.getConfig(start).getUserLastActiveTable()
                +
                "(app_id, user_id, last_active_time) VALUES(?, ?, ?) ON CONFLICT(app_id, user_id) DO UPDATE SET " +
                "last_active_time = ?";

        return update(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
            pst.setLong(3, timestamp);
            pst.setLong(4, timestamp);
        });
    }

    public static Long getLastActiveByUserId(Start start, AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        String QUERY = "SELECT last_active_time FROM " + Config.getConfig(start).getUserLastActiveTable()
                + " WHERE app_id = ? AND user_id = ?";

        try {
            return execute(start, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, userId);
            }, res -> {
                if (res.next()) {
                    return res.getLong("last_active_time");
                }
                return null;
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static Map<String, Long> getLastActiveByMultipleUserIds(Start start, AppIdentifier appIdentifier, List<String> userIds)
            throws StorageQueryException {
        if(userIds == null || userIds.isEmpty()) {
            return new HashMap<>();
        }
        String QUERY = "SELECT user_id, last_active_time FROM " + Config.getConfig(start).getUserLastActiveTable()
                + " WHERE app_id = ? AND user_id IN ( " + Utils.generateCommaSeperatedQuestionMarks(userIds.size())+ " )";

        try {
            return execute(start, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                for (int i = 0; i < userIds.size(); i++) {
                    pst.setString(2+i, userIds.get(i));
                }
            }, res -> {
                Map<String, Long> lastActiveByUserIds = new HashMap<>();
                while (res.next()) {
                    String userId = res.getString("user_id");
                    lastActiveByUserIds.put(userId, res.getLong("last_active_time"));
                }
                return lastActiveByUserIds;
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    /**
     * Constant key for the transaction-scoped advisory lock that deduplicates concurrent rollup passes.
     * The fold/reconcile below are idempotent, so the lock is purely work-deduplication, not correctness.
     */
    private static final String LAST_ACTIVE_ROLLUP_LOCK_KEY = "last_active_rollup";

    /**
     * Derives {@code user_last_active} from the activity log over {@code [windowStartMillis, now]}, on the
     * caller's transaction connection. Two idempotent statements:
     * <ol>
     *   <li><b>Fold</b> — upsert each user's most recent activity into the projection, monotonically
     *       ({@code GREATEST} never lowers a stored timestamp). The activity source is the semantic
     *       activity/lifecycle event set in {@link RollupEventTypes#FOLD_SET},
     *       credited to the event's {@code primary_or_recipe_user_id}. A single {@code app_id_to_user_id}
     *       residency guard confines the fold to activity for a user whose auth record lives on this
     *       storage. With per-storage routing a user's activity is colocated with their auth record, so
     *       this normally matches; it doubles as insurance for the one tenant-less emit path
     *       (SessionRemoveAPI's app-wide sign-out) and keeps the fold from resurrecting a since-deleted
     *       user's retained {@code activity_log} rows (no user FK on {@code user_last_active}, but
     *       {@code deleteUserActive_Transaction} had removed the projection row). It subsumes the old
     *       {@code apps} existence guard: {@code app_id_to_user_id} cascades on app delete, so a
     *       since-deleted app has no mapping rows here — its retained {@code activity_log} rows are not
     *       folded and cannot violate the {@code user_last_active -> apps} foreign key.</li>
     *   <li><b>Reconcile</b> — delete projection rows for users linked away within the same window
     *       ({@code account_linking} events, matched on {@code app_id} + {@code recipe_user_id}), but
     *       only when the link event is not older than the row's {@code last_active_time}. That ordering
     *       guard is what makes the delete order-insensitive: a recipe user linked, then unlinked, then
     *       active again inside one window is credited its post-unlink activity (a later
     *       {@code last_active_time}) and must not be scrubbed by the stale earlier link event.</li>
     * </ol>
     * As the first statement it takes a non-blocking advisory lock with a constant key; if another
     * instance holds it the pass is skipped (that instance is folding — the work is redundant, not lost)
     * and this returns {@code false} so the caller leaves its watermark untouched; a completed fold
     * returns {@code true}.
     */
    public static boolean rollupLastActiveFromActivityLog_Transaction(Start start, Connection con,
                                                                   long windowStartMillis)
            throws StorageQueryException, SQLException {
        try {
            io.supertokens.storage.postgresql.queries.Utils.takeAdvisoryLock(con, LAST_ACTIVE_ROLLUP_LOCK_KEY);
        } catch (StorageQueryException e) {
            if (e.getCause() instanceof io.supertokens.storage.postgresql.LockFailure) {
                // Another instance is folding this pass; the fold/reconcile are idempotent, so skip.
                return false;
            }
            throw e;
        }

        String userLastActiveTable = Config.getConfig(start).getUserLastActiveTable();
        String activityLogTable = Config.getConfig(start).getActivityLogTable();
        String appIdToUserIdTable = Config.getConfig(start).getAppIdToUserIdTable();

        // A single app_id_to_user_id residency guard confines the fold to activity for a user whose auth
        // record lives on this storage (mirrors the in-memory guard in core#1410). With per-storage
        // routing a user's activity is colocated with their auth record, so this normally matches; it is
        // insurance for the one tenant-less emit path (SessionRemoveAPI's app-wide sign-out) and keeps a
        // deleted user's retained activity_log rows (no user FK on user_last_active, but
        // deleteUserActive_Transaction had removed the projection row) from resurrecting. It subsumes the
        // old apps existence guard: app_id_to_user_id cascades on app delete, so a since-deleted app has
        // no mapping rows here, so its retained activity_log rows are not folded and cannot violate the
        // user_last_active -> apps foreign key.
        String FOLD_QUERY = "INSERT INTO " + userLastActiveTable + " (app_id, user_id, last_active_time)"
                + " SELECT app_id, primary_or_recipe_user_id, MAX(created_at) FROM " + activityLogTable + " al"
                + " WHERE event_type IN (" + RollupEventTypes.sqlInList() + ") AND created_at >= ?"
                + " AND EXISTS (SELECT 1 FROM " + appIdToUserIdTable + " aiu"
                + " WHERE aiu.app_id = al.app_id AND aiu.user_id = al.primary_or_recipe_user_id)"
                + " GROUP BY app_id, primary_or_recipe_user_id"
                + " ON CONFLICT (app_id, user_id) DO UPDATE"
                + " SET last_active_time = GREATEST(" + userLastActiveTable + ".last_active_time,"
                + " EXCLUDED.last_active_time)";
        update(con, FOLD_QUERY, pst -> pst.setLong(1, windowStartMillis));

        // The ordering guard (al.created_at >= ula.last_active_time) makes the reconcile
        // order-insensitive: only delete a linked-away recipe user's projection row when the link event
        // is not older than the row's stored last-active. A user linked, unlinked, then active again in
        // one window keeps the post-unlink credit (its last_active_time is newer than the stale link),
        // instead of being wrongly scrubbed by an earlier account_linking event in the same window.
        String RECONCILE_QUERY = "DELETE FROM " + userLastActiveTable + " ula USING " + activityLogTable + " al"
                + " WHERE al.event_type = 'account_linking' AND al.created_at >= ?"
                + " AND al.app_id = ula.app_id AND al.recipe_user_id = ula.user_id"
                + " AND al.created_at >= ula.last_active_time";
        update(con, RECONCILE_QUERY, pst -> pst.setLong(1, windowStartMillis));
        return true;
    }

    public static void deleteUserActive_Transaction(Connection con, Start start, AppIdentifier appIdentifier,
                                                    String userId)
            throws StorageQueryException, SQLException {
        String QUERY = "DELETE FROM " + Config.getConfig(start).getUserLastActiveTable()
                + " WHERE app_id = ? AND user_id = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        });
    }

    public static int countUsersThatHaveMoreThanOneLoginMethodOrTOTPEnabledAndActiveSince(Start start,
                                                                                          AppIdentifier appIdentifier,
                                                                                          long sinceTime)
            throws SQLException, StorageQueryException {
        // TODO: Active users are present only on public tenant and MFA users may be present on different storages
        String QUERY =
                "SELECT COUNT (DISTINCT user_id) as c FROM ("
                        + "  (" // users with more than one login method
                        + "    SELECT primary_or_recipe_user_id AS user_id FROM ("
                        + "      SELECT COUNT(user_id) as num_login_methods, app_id, primary_or_recipe_user_id"
                        + "      FROM " + getConfig(start).getAppIdToUserIdTable()
                        + "      WHERE app_id = ? AND primary_or_recipe_user_id IN ("
                        + "        SELECT user_id FROM " + getConfig(start).getUserLastActiveTable()
                        + "        WHERE app_id = ? AND last_active_time >= ?"
                        + "      )"
                        + "      GROUP BY (app_id, primary_or_recipe_user_id)"
                        + "    ) AS nloginmethods"
                        + "    WHERE num_login_methods > 1"
                        + "  ) UNION (" // TOTP users
                        + "    SELECT user_id FROM " + getConfig(start).getTotpUsersTable()
                        + "    WHERE app_id = ? AND user_id IN ("
                        + "      SELECT user_id FROM " + getConfig(start).getUserLastActiveTable()
                        + "      WHERE app_id = ? AND last_active_time >= ?"
                        + "    )"
                        + "  )"
                        + ") AS all_users";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, appIdentifier.getAppId());
            pst.setLong(3, sinceTime);
            pst.setString(4, appIdentifier.getAppId());
            pst.setString(5, appIdentifier.getAppId());
            pst.setLong(6, sinceTime);
        }, result -> {
            return result.next() ? result.getInt("c") : 0;
        });
    }
}
