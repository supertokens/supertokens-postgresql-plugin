package io.supertokens.storage.postgresql.queries;

import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.authRecipe.UsersCountSnapshot;
import io.supertokens.pluginInterface.authRecipe.exceptions.UsersCountBackoffException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.PreparedStatementValueSetter;
import io.supertokens.storage.postgresql.ProcessState;
import io.supertokens.storage.postgresql.ResultSetValueExtractor;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

/**
 * User counts that are computed once and shared through the database by every core, instead of each core running
 * its own full count (see {@code UsersCountCacheStorage}).
 * <p>
 * One call: take a session advisory lock on the scope (waiting, without a timeout, for a count another core is
 * running), reuse the stored count if it is fresh enough, back off if a recent count of the scope failed, otherwise
 * count and store. The count runs as many short statements ("chunks") inside one read-only REPEATABLE READ
 * transaction, so it is exact as of one snapshot, no single statement is long, it can be throttled, and it stops
 * cleanly between chunks.
 * <p>
 * Each chunk covers the next {@code postgresql_users_count_chunk_size} keys of an index whose order keeps every user
 * group (a primary user with its linked members, or a standalone user) in one chunk, so the chunk counts add up to
 * the distinct-group count. A chunk is bounded by keys scanned, not by matches, so a sparse recipe filter cannot turn
 * one chunk into a full scan.
 */
public class UsersCountCacheQueries {

    static final String APP_USERS_COUNT_KEY = "APP_USERS_COUNT";
    static final String TENANT_USERS_COUNT_KEY = "TENANT_USERS_COUNT";
    private static final String FAILED_SUFFIX = "~FAILED";

    // the client network timeout of a chunk is the chunk's statement timeout plus this, so the server's cancel wins
    private static final long NETWORK_TIMEOUT_MARGIN_MS = 30_000;

    /**
     * For tests: sees every statement of a count, with its parameters and on the count's connection (so inside its
     * snapshot), just before it runs.
     */
    public interface ChunkObserver {
        void beforeChunk(Connection con, String query, PreparedStatementValueSetter setter) throws SQLException;
    }

    @Nullable
    public static volatile ChunkObserver chunkObserverForTesting = null;

    /**
     * What is counted, and the key_value name of its stored count. An app count is stored on a tenant of the app that
     * exists in this database (key_value references tenants): the public tenant if this user pool has it, otherwise
     * the app's first tenant here.
     */
    public static final class Scope {
        final AppIdentifier app;
        @Nullable
        final String tenantId; // null: the whole app
        @Nullable
        final RECIPE_ID[] recipes; // sorted and de-duplicated; null: no filter
        final String key;
        final String lockKey;

        private Scope(AppIdentifier app, @Nullable String tenantId, @Nullable RECIPE_ID[] recipes) {
            this.app = app;
            this.tenantId = tenantId;
            this.recipes = normalise(recipes);
            String suffix = this.recipes == null ? "" : "~" + String.join(",",
                    Arrays.stream(this.recipes).map(RECIPE_ID::toString).toArray(String[]::new));
            this.key = (tenantId == null ? APP_USERS_COUNT_KEY : TENANT_USERS_COUNT_KEY) + suffix;
            this.lockKey = "users_count~" + app.getAppId() + "~" + (tenantId == null ? "" : tenantId) + suffix;
        }

        public static Scope app(AppIdentifier app, @Nullable RECIPE_ID[] recipes) {
            return new Scope(app, null, recipes);
        }

        public static Scope tenant(TenantIdentifier tenant, @Nullable RECIPE_ID[] recipes) {
            return new Scope(tenant.toAppIdentifier(), tenant.getTenantId(), recipes);
        }

        // the advisory lock key, for tests that hold the lock as "another core"
        public String getLockKeyForTesting(Start start) {
            return lockKeyOf(start, this);
        }

        private static RECIPE_ID[] normalise(@Nullable RECIPE_ID[] recipes) {
            if (recipes == null || recipes.length == 0) {
                return null;
            }
            TreeSet<RECIPE_ID> sorted = new TreeSet<>(Comparator.comparing(RECIPE_ID::toString));
            sorted.addAll(Arrays.asList(recipes));
            return sorted.toArray(new RECIPE_ID[0]);
        }
    }

    public static UsersCountSnapshot getOrCompute(Start start, Scope scope, long maxAgeMs)
            throws StorageQueryException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            return getOrCompute(start, con, scope, maxAgeMs);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    /**
     * The last stored count of the scope, whatever its age, or null. Takes no lock and never counts.
     */
    @Nullable
    public static UsersCountSnapshot getStored(Start start, Scope scope) throws StorageQueryException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            String storedOn = storedOnTenant(start, con, scope);
            if (storedOn == null) {
                return null;
            }
            KeyValueInfo stored = read(start, con, scope.app, storedOn, scope.key);
            return stored == null ? null : new UsersCountSnapshot(Long.parseLong(stored.value), stored.createdAtTime,
                    false);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    // Advisory locks are per database, and user pools can share one (different schemas or table prefixes): the key
    // names this pool's key_value table. Hashed to 64 bits, so unrelated scopes practically never share a lock.
    private static String lockKeyOf(Start start, Scope scope) {
        return getConfig(start).getKeyValueTable() + "~" + scope.lockKey;
    }

    private static final long LOCK_POLL_MS = 500;

    // Waits for the scope's lock as long as another core's count holds it - the wait has no limit, as that count is
    // bounded by its own chunk timeout and budget - but in short polls, each under the pool's socket timeout, so a
    // broken connection is noticed instead of blocking for hours on a half-open socket.
    private static void lock(Connection con, String key) throws SQLException, StorageQueryException {
        while (!execute(con, "SELECT pg_try_advisory_lock(hashtextextended(?, 0))", pst -> pst.setString(1, key),
                r -> r.next() && r.getBoolean(1))) {
            try {
                Thread.sleep(LOCK_POLL_MS / 2 + ThreadLocalRandom.current().nextLong(LOCK_POLL_MS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StorageQueryException(e);
            }
        }
    }

    private static UsersCountSnapshot getOrCompute(Start start, Connection con, Scope scope, long maxAgeMs)
            throws SQLException, StorageQueryException {
        PostgreSQLConfig config = getConfig(start);
        // The max age counts from this call, so the time spent waiting for another core's count does not age out
        // the count this caller waited for.
        long freshAfter = System.currentTimeMillis() - maxAgeMs;

        String lockKey = lockKeyOf(start, scope);
        lock(con, lockKey);
        try {
            String storedOn = storedOnTenant(start, con, scope);
            if (storedOn == null) {
                // no tenant of this app lives in this database, so it holds none of the app's users
                return new UsersCountSnapshot(0, System.currentTimeMillis(), true);
            }

            // the stored count and the failure record, in one round trip
            KeyValueInfo[] rows = readWithFailure(start, con, scope.app, storedOn, scope.key);
            KeyValueInfo stored = rows[0];
            KeyValueInfo failed = rows[1];
            long now = System.currentTimeMillis();
            if (stored != null && stored.createdAtTime <= now && stored.createdAtTime > freshAfter) {
                return new UsersCountSnapshot(Long.parseLong(stored.value), stored.createdAtTime, false);
            }

            if (failed != null && failed.createdAtTime <= now
                    && now - failed.createdAtTime < config.getUsersCountChunkTimeout()) {
                throw new StorageQueryException(new UsersCountBackoffException(failed.createdAtTime));
            }

            TenantIdentifier storedOnTenant = new TenantIdentifier(scope.app.getConnectionUriDomain(),
                    scope.app.getAppId(), storedOn);
            // Stamped before counting: the snapshot the count reads is taken by its first chunk, after this.
            long countStartedAt = System.currentTimeMillis();
            long count;
            try {
                count = countInChunks(start, con, scope, config, countStartedAt);
            } catch (SQLException | StorageQueryException | RuntimeException e) {
                // every core backs off this scope for one chunk timeout instead of starting over straight away
                try {
                    GeneralQueries.setKeyValue_Transaction(start, con, storedOnTenant, scope.key + FAILED_SUFFIX,
                            new KeyValueInfo("failed", System.currentTimeMillis()));
                } catch (SQLException | StorageQueryException recordFailure) {
                    e.addSuppressed(recordFailure); // e.g. the connection broke; report the count's own failure
                }
                throw e;
            }
            GeneralQueries.setKeyValue_Transaction(start, con, storedOnTenant, scope.key,
                    new KeyValueInfo(Long.toString(count), countStartedAt));
            if (failed != null) {
                GeneralQueries.deleteKeyValue_Transaction(start, con, storedOnTenant, scope.key + FAILED_SUFFIX);
            }
            ProcessState.getInstance(start).addState(ProcessState.PROCESS_STATE.USERS_COUNT_COMPUTED, null);
            return new UsersCountSnapshot(count, countStartedAt, true);
        } finally {
            try {
                execute(con, "SELECT pg_advisory_unlock(hashtextextended(?, 0))", pst -> pst.setString(1, lockKey),
                        r -> null);
            } catch (SQLException | StorageQueryException e) {
                // the connection is broken; closing it ends the session, which releases the lock
                con.abort(Runnable::run);
            }
        }
    }

    // The tenant the scope's count is stored on, or null if the app has no tenant in this database.
    @Nullable
    private static String storedOnTenant(Start start, Connection con, Scope scope)
            throws SQLException, StorageQueryException {
        if (scope.tenantId != null) {
            return scope.tenantId;
        }
        return execute(con, "SELECT tenant_id FROM " + getConfig(start).getTenantsTable()
                        + " WHERE app_id = ? ORDER BY (tenant_id <> 'public'), tenant_id LIMIT 1",
                pst -> pst.setString(1, scope.app.getAppId()), r -> r.next() ? r.getString("tenant_id") : null);
    }

    // {stored count, failure record} of the key; either may be null
    private static KeyValueInfo[] readWithFailure(Start start, Connection con, AppIdentifier app, String tenantId,
                                                  String key) throws SQLException, StorageQueryException {
        return execute(con, "SELECT name, value, created_at_time FROM " + getConfig(start).getKeyValueTable()
                + " WHERE app_id = ? AND tenant_id = ? AND name IN (?, ?)", pst -> {
            pst.setString(1, app.getAppId());
            pst.setString(2, tenantId);
            pst.setString(3, key);
            pst.setString(4, key + FAILED_SUFFIX);
        }, r -> {
            KeyValueInfo[] rows = new KeyValueInfo[2];
            while (r.next()) {
                rows[key.equals(r.getString("name")) ? 0 : 1] = new KeyValueInfo(r.getString("value"),
                        r.getLong("created_at_time"));
            }
            return rows;
        });
    }

    @Nullable
    private static KeyValueInfo read(Start start, Connection con, AppIdentifier app, String tenantId, String key)
            throws SQLException, StorageQueryException {
        return execute(con, "SELECT value, created_at_time FROM " + getConfig(start).getKeyValueTable()
                + " WHERE app_id = ? AND tenant_id = ? AND name = ?", pst -> {
            pst.setString(1, app.getAppId());
            pst.setString(2, tenantId);
            pst.setString(3, key);
        }, r -> r.next() ? new KeyValueInfo(r.getString("value"), r.getLong("created_at_time")) : null);
    }

    private static long countInChunks(Start start, Connection con, Scope scope, PostgreSQLConfig config,
                                      long countStartedAt) throws SQLException, StorageQueryException {
        con.setNetworkTimeout(Runnable::run,
                (int) Math.min(Integer.MAX_VALUE, config.getUsersCountChunkTimeout() + NETWORK_TIMEOUT_MARGIN_MS));
        boolean autoCommit = con.getAutoCommit();
        con.setAutoCommit(false);
        try {
            boolean newTables = Config.getConfig(start).getMigrationMode().readsFromNewTables();
            try (Statement st = con.createStatement()) {
                // one round trip: one snapshot for every chunk (so the sum is exact as of the first chunk), the chunk
                // timeout, and for the new-table tenant count a nested loop for the chunk joins: one index probe per
                // key of the chunk. A hash join would read the whole app's app_id_to_user_id per chunk, and a merge
                // join reads it from the app's first user up to the chunk's last key - both quadratic over the chunks.
                st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY; SET LOCAL statement_timeout = "
                        + config.getUsersCountChunkTimeout()
                        + (newTables && scope.tenantId != null
                        ? "; SET LOCAL enable_hashjoin = off; SET LOCAL enable_mergejoin = off" : ""));
            }
            Budget budget = new Budget(config, countStartedAt);
            long count;
            if (scope.tenantId == null) {
                count = newTables ? appCountNew(start, con, scope, budget) : appCountLegacy(start, con, scope, budget);
            } else if (newTables) {
                count = tenantCountNew(start, con, scope, budget);
            } else {
                count = tenantCountLegacy(start, con, scope, budget);
            }
            con.commit();
            return count;
        } catch (SQLException | StorageQueryException | RuntimeException e) {
            con.rollback();
            throw e;
        } finally {
            con.setAutoCommit(autoCommit);
        }
    }

    // ---- chunk queries. Each returns {keys scanned, counted, cursor...}; the loop stops on a short chunk. ----

    // App, new tables: groups are (primary_or_recipe_user_time_joined, primary_or_recipe_user_id), walked in the
    // order of app_id_to_user_id_pagination_index2 (app_id, time ASC, id DESC). The cursor's OR cannot be an index
    // condition, so the redundant `time >= cursor` bound is what makes each chunk seek to its cursor instead of
    // re-reading the app from its first user (as in GeneralQueries.getUsers_new).
    private static long appCountNew(Start start, Connection con, Scope scope, Budget budget)
            throws SQLException, StorageQueryException {
        String table = getConfig(start).getAppIdToUserIdTable();
        String hit = scope.recipes == null ? "TRUE" : "recipe_id IN (" + placeholders(scope.recipes) + ")";
        String query = "WITH g AS (SELECT t, id, bool_or(h) AS hit FROM ("
                + "SELECT primary_or_recipe_user_time_joined AS t, primary_or_recipe_user_id AS id, " + hit + " AS h"
                + " FROM " + table + " WHERE app_id = ? AND primary_or_recipe_user_time_joined >= ?"
                + " AND (primary_or_recipe_user_time_joined > ?"
                + " OR (primary_or_recipe_user_time_joined = ? AND primary_or_recipe_user_id < ?))) x"
                + " GROUP BY t, id ORDER BY t ASC, id DESC LIMIT ?)"
                + " SELECT (SELECT COUNT(*) FROM g) AS keys, (SELECT COUNT(*) FROM g WHERE hit) AS counted,"
                + " (SELECT t FROM g ORDER BY t DESC, id ASC LIMIT 1) AS last_t,"
                + " (SELECT id FROM g ORDER BY t DESC, id ASC LIMIT 1) AS last_id";
        long total = 0;
        long lastT = Long.MIN_VALUE;
        String lastId = "";
        while (true) {
            long cursorT = lastT;
            String cursorId = lastId;
            Chunk chunk = chunk(con, query, pst -> {
                int i = setRecipes(pst, 1, scope.recipes);
                pst.setString(i++, scope.app.getAppId());
                pst.setLong(i++, cursorT);
                pst.setLong(i++, cursorT);
                pst.setLong(i++, cursorT);
                pst.setString(i++, cursorId);
                pst.setInt(i, budget.chunkSize);
            }, r -> {
                r.next();
                return new Chunk(r.getLong("keys"), r.getLong("counted"), r.getLong("last_t"),
                        r.getString("last_id"));
            });
            total += chunk.counted;
            if (chunk.keys < budget.chunkSize) {
                return total;
            }
            lastT = chunk.lastLong;
            lastId = chunk.lastString;
            budget.afterChunk();
        }
    }

    // App, legacy table: all_auth_recipe_users has one row per (tenant, recipe user), so a group spans tenants. Walk
    // all_auth_recipe_users_pagination_index1 (app_id, tenant_id, time DESC, id DESC) one tenant at a time, with the
    // row-comparison cursor tenantCountLegacy uses (an index condition, so each chunk seeks to its cursor), and count
    // each group once, in its first tenant; the lookups use all_auth_recipe_users_primary_user_id_index
    // (primary_or_recipe_user_id, app_id). Walking that index directly would also scan other apps' rows, unbounded per
    // chunk on a shared database. The lookups are LATERAL ... LIMIT 1, one probe per group: as (NOT) EXISTS the
    // planner may hash the whole app's rows instead, once per chunk.
    private static long appCountLegacy(Start start, Connection con, Scope scope, Budget budget)
            throws SQLException, StorageQueryException {
        String table = getConfig(start).getUsersTable();
        String recipeJoin = scope.recipes == null ? "" : " LEFT JOIN LATERAL (SELECT 1 AS hit FROM " + table + " c"
                + " WHERE c.primary_or_recipe_user_id = g.id AND c.app_id = ? AND c.recipe_id IN ("
                + placeholders(scope.recipes) + ") LIMIT 1) h ON TRUE";
        String nextTenantQuery = "SELECT tenant_id FROM " + table
                + " WHERE app_id = ? AND tenant_id > ? ORDER BY tenant_id LIMIT 1";
        String query = "WITH g AS (SELECT primary_or_recipe_user_time_joined AS t, primary_or_recipe_user_id AS id"
                + " FROM " + table + " WHERE app_id = ? AND tenant_id = ?"
                + " AND (primary_or_recipe_user_time_joined, primary_or_recipe_user_id) < (?, ?)"
                + " GROUP BY primary_or_recipe_user_time_joined, primary_or_recipe_user_id"
                + " ORDER BY t DESC, id DESC LIMIT ?)"
                + " SELECT (SELECT COUNT(*) FROM g) AS keys,"
                + " (SELECT COUNT(*) FROM g LEFT JOIN LATERAL (SELECT 1 AS earlier FROM " + table + " b"
                + " WHERE b.primary_or_recipe_user_id = g.id AND b.app_id = ? AND b.tenant_id < ? LIMIT 1) e ON TRUE"
                + recipeJoin + " WHERE e.earlier IS NULL" + (scope.recipes == null ? "" : " AND h.hit IS NOT NULL")
                + ") AS counted,"
                + " (SELECT t FROM g ORDER BY t ASC, id ASC LIMIT 1) AS last_t,"
                + " (SELECT id FROM g ORDER BY t ASC, id ASC LIMIT 1) AS last_id";
        long total = 0;
        int chunks = 0;
        String tenant = "";
        while (true) {
            String previousTenant = tenant;
            tenant = chunk(con, nextTenantQuery, pst -> {
                pst.setString(1, scope.app.getAppId());
                pst.setString(2, previousTenant);
            }, r -> r.next() ? r.getString("tenant_id") : null);
            if (tenant == null) {
                return total;
            }
            String chunkTenant = tenant;
            long lastT = Long.MAX_VALUE;
            String lastId = "";
            while (true) {
                long cursorT = lastT;
                String cursorId = lastId;
                if (chunks++ > 0) {
                    budget.afterChunk(); // between chunks, also across tenants
                }
                Chunk chunk = chunk(con, query, pst -> {
                    int i = 1;
                    pst.setString(i++, scope.app.getAppId());
                    pst.setString(i++, chunkTenant);
                    pst.setLong(i++, cursorT);
                    pst.setString(i++, cursorId);
                    pst.setInt(i++, budget.chunkSize);
                    pst.setString(i++, scope.app.getAppId());
                    pst.setString(i++, chunkTenant);
                    if (scope.recipes != null) {
                        pst.setString(i++, scope.app.getAppId());
                        setRecipes(pst, i, scope.recipes);
                    }
                }, r -> {
                    r.next();
                    return new Chunk(r.getLong("keys"), r.getLong("counted"), r.getLong("last_t"),
                            r.getString("last_id"));
                });
                total += chunk.counted;
                if (chunk.keys < budget.chunkSize) {
                    break;
                }
                lastT = chunk.lastLong;
                lastId = chunk.lastString;
            }
        }
    }

    // Tenant, legacy table: a group's rows in one tenant share (time, id); all_auth_recipe_users_pagination_index1.
    private static long tenantCountLegacy(Start start, Connection con, Scope scope, Budget budget)
            throws SQLException, StorageQueryException {
        String table = getConfig(start).getUsersTable();
        String hit = scope.recipes == null ? "TRUE" : "recipe_id IN (" + placeholders(scope.recipes) + ")";
        String query = "WITH g AS (SELECT t, id, bool_or(h) AS hit FROM ("
                + "SELECT primary_or_recipe_user_time_joined AS t, primary_or_recipe_user_id AS id, " + hit + " AS h"
                + " FROM " + table + " WHERE app_id = ? AND tenant_id = ?"
                + " AND (primary_or_recipe_user_time_joined, primary_or_recipe_user_id) < (?, ?)) x"
                + " GROUP BY t, id ORDER BY t DESC, id DESC LIMIT ?)"
                + " SELECT (SELECT COUNT(*) FROM g) AS keys, (SELECT COUNT(*) FROM g WHERE hit) AS counted,"
                + " (SELECT t FROM g ORDER BY t ASC, id ASC LIMIT 1) AS last_t,"
                + " (SELECT id FROM g ORDER BY t ASC, id ASC LIMIT 1) AS last_id";
        long total = 0;
        long lastT = Long.MAX_VALUE;
        String lastId = "";
        while (true) {
            long cursorT = lastT;
            String cursorId = lastId;
            Chunk chunk = chunk(con, query, pst -> {
                int i = setRecipes(pst, 1, scope.recipes);
                pst.setString(i++, scope.app.getAppId());
                pst.setString(i++, scope.tenantId);
                pst.setLong(i++, cursorT);
                pst.setString(i++, cursorId);
                pst.setInt(i, budget.chunkSize);
            }, r -> {
                r.next();
                return new Chunk(r.getLong("keys"), r.getLong("counted"), r.getLong("last_t"),
                        r.getString("last_id"));
            });
            total += chunk.counted;
            if (chunk.keys < budget.chunkSize) {
                return total;
            }
            lastT = chunk.lastLong;
            lastId = chunk.lastString;
            budget.afterChunk();
        }
    }

    // Tenant, new tables: D - L + G as the standalone count (GeneralQueries.getUsersCount_new), each part walked in
    // key order: D and L over idx_recipe_user_tenants_tenant_recipe_user, G over
    // idx_primary_user_tenants_tenant_primary.
    // With a recipe filter the same parts are restricted to the recipe users of the tenant with one of the recipes (as
    // getUsersCountByRecipe_new): D and L count only those, and G only the groups with at least one of them. So a
    // tenant's count reads that tenant's rows, not the whole app's.
    private static long tenantCountNew(Start start, Connection con, Scope scope, Budget budget)
            throws SQLException, StorageQueryException {
        PostgreSQLConfig config = getConfig(start);
        boolean filtered = scope.recipes != null;
        // Hash and merge joins are off for this transaction (set with the snapshot): the join probes per key.
        // The chunk is read once and aggregated as it streams: a CTE referenced more than once is materialised, and a
        // chunk of recipe user ids does not fit work_mem, so it spilled to disk. The left join keeps every key of the
        // chunk (app_id_to_user_id has one row per (app_id, user_id)), so COUNT(*) is the chunk's key count.
        String dlQuery = "SELECT COUNT(*) AS keys, MAX(r.recipe_user_id) AS last_id,"
                + (filtered
                ? " COUNT(a.user_id) FILTER (WHERE r.hit) AS d,"
                + " COUNT(*) FILTER (WHERE a.is_linked_or_is_a_primary_user AND r.hit) AS l"
                + " FROM (SELECT recipe_user_id, bool_or(recipe_id IN (" + placeholders(scope.recipes) + ")) AS hit"
                + " FROM " + config.getRecipeUserTenantsTable()
                + " WHERE app_id = ? AND tenant_id = ? AND recipe_user_id > ?"
                + " GROUP BY recipe_user_id ORDER BY recipe_user_id LIMIT ?) r"
                : " COUNT(a.user_id) AS d, COUNT(*) FILTER (WHERE a.is_linked_or_is_a_primary_user) AS l"
                + " FROM (SELECT DISTINCT recipe_user_id FROM " + config.getRecipeUserTenantsTable()
                + " WHERE app_id = ? AND tenant_id = ? AND recipe_user_id > ? ORDER BY recipe_user_id LIMIT ?) r")
                + " LEFT JOIN " + config.getAppIdToUserIdTable() + " a"
                + " ON a.app_id = ? AND a.user_id = r.recipe_user_id";
        long dMinusL = 0;
        String last = "";
        while (true) {
            String cursor = last;
            Chunk chunk = chunk(con, dlQuery, pst -> {
                int i = setRecipes(pst, 1, scope.recipes);
                pst.setString(i++, scope.app.getAppId());
                pst.setString(i++, scope.tenantId);
                pst.setString(i++, cursor);
                pst.setInt(i++, budget.chunkSize);
                pst.setString(i, scope.app.getAppId());
            }, r -> {
                r.next();
                return new Chunk(r.getLong("keys"), r.getLong("d") - r.getLong("l"), 0, r.getString("last_id"));
            });
            dMinusL += chunk.counted;
            if (chunk.keys < budget.chunkSize) {
                break;
            }
            last = chunk.lastString;
            budget.afterChunk();
        }

        String groups = "SELECT DISTINCT primary_user_id FROM " + config.getPrimaryUserTenantsTable()
                + " WHERE app_id = ? AND tenant_id = ? AND primary_user_id > ? ORDER BY primary_user_id LIMIT ?";
        String gQuery = filtered
                ? "SELECT COUNT(*) AS keys, MAX(g.primary_user_id) AS last_id, COUNT(h.hit) AS counted"
                + " FROM (" + groups + ") g LEFT JOIN LATERAL (SELECT 1 AS hit FROM "
                + config.getAppIdToUserIdTable() + " a JOIN " + config.getRecipeUserTenantsTable() + " rut"
                + " ON rut.app_id = a.app_id AND rut.tenant_id = ? AND rut.recipe_user_id = a.user_id"
                + " WHERE a.app_id = ? AND a.primary_or_recipe_user_id = g.primary_user_id AND rut.recipe_id IN ("
                + placeholders(scope.recipes) + ") LIMIT 1) h ON TRUE"
                : "WITH g AS (" + groups + ") SELECT COUNT(*) AS keys, MAX(primary_user_id) AS last_id,"
                + " COUNT(*) AS counted FROM g";
        long g = 0;
        last = "";
        while (true) {
            String cursor = last;
            Chunk chunk = chunk(con, gQuery, pst -> {
                pst.setString(1, scope.app.getAppId());
                pst.setString(2, scope.tenantId);
                pst.setString(3, cursor);
                pst.setInt(4, budget.chunkSize);
                if (filtered) {
                    pst.setString(5, scope.tenantId);
                    pst.setString(6, scope.app.getAppId());
                    setRecipes(pst, 7, scope.recipes);
                }
            }, r -> {
                r.next();
                return new Chunk(r.getLong("keys"), r.getLong("counted"), 0, r.getString("last_id"));
            });
            g += chunk.counted;
            if (chunk.keys < budget.chunkSize) {
                return dMinusL + g;
            }
            last = chunk.lastString;
            budget.afterChunk();
        }
    }

    private static <T> T chunk(Connection con, String query, PreparedStatementValueSetter setter,
                               ResultSetValueExtractor<T> mapper) throws SQLException, StorageQueryException {
        ChunkObserver observer = chunkObserverForTesting;
        if (observer != null) {
            observer.beforeChunk(con, query, setter);
        }
        return execute(con, query, setter, mapper);
    }

    private static String placeholders(RECIPE_ID[] recipes) {
        return String.join(",", java.util.Collections.nCopies(recipes.length, "?"));
    }

    // binds the recipe filter from parameter index `from`; returns the next free index
    private static int setRecipes(java.sql.PreparedStatement pst, int from, @Nullable RECIPE_ID[] recipes)
            throws SQLException {
        if (recipes == null) {
            return from;
        }
        for (RECIPE_ID recipe : recipes) {
            pst.setString(from++, recipe.toString());
        }
        return from;
    }

    private static final class Chunk {
        final long keys;
        final long counted;
        final long lastLong;
        final String lastString;

        Chunk(long keys, long counted, long lastLong, String lastString) {
            this.keys = keys;
            this.counted = counted;
            this.lastLong = lastLong;
            this.lastString = lastString;
        }
    }

    // chunk size, the pause between chunks, and the overall budget
    private static final class Budget {
        final int chunkSize;
        final long pauseMs;
        final long maxDurationMs;
        final long startedAt;

        Budget(PostgreSQLConfig config, long startedAt) {
            this.chunkSize = config.getUsersCountChunkSize();
            this.pauseMs = config.getUsersCountChunkPause();
            this.maxDurationMs = config.getUsersCountMaxDuration();
            this.startedAt = startedAt;
        }

        void afterChunk() throws StorageQueryException {
            if (maxDurationMs > 0 && System.currentTimeMillis() - startedAt > maxDurationMs) {
                throw new StorageQueryException(new TimeoutException(
                        "user count exceeded postgresql_users_count_max_duration (" + maxDurationMs + "ms)"));
            }
            if (pauseMs > 0) {
                try {
                    Thread.sleep(pauseMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StorageQueryException(e);
                }
            }
        }
    }
}
