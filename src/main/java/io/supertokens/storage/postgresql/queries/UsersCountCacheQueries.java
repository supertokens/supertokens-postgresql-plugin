package io.supertokens.storage.postgresql.queries;

import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.authRecipe.UsersCountSnapshot;
import io.supertokens.pluginInterface.authRecipe.exceptions.UsersCountBackoffException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.ProcessState;
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
        public String getLockKeyForTesting() {
            return lockKey;
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

    private static UsersCountSnapshot getOrCompute(Start start, Connection con, Scope scope, long maxAgeMs)
            throws SQLException, StorageQueryException {
        PostgreSQLConfig config = getConfig(start);

        // Waiting for the lock lasts as long as another core's count does; that count is bounded by its own chunk
        // timeout and budget, and Postgres releases the lock if its connection dies. So wait without a timeout.
        // Hikari restores the pool's network timeout when the connection is returned.
        con.setNetworkTimeout(Runnable::run, 0);
        execute(con, "SELECT pg_advisory_lock(hashtext(?))", pst -> pst.setString(1, scope.lockKey), r -> null);
        try {
            String storedOn = storedOnTenant(start, con, scope);
            if (storedOn == null) {
                // no tenant of this app lives in this database, so it holds none of the app's users
                return new UsersCountSnapshot(0, System.currentTimeMillis(), true);
            }

            KeyValueInfo stored = read(start, con, scope.app, storedOn, scope.key);
            long now = System.currentTimeMillis();
            if (stored != null && stored.createdAtTime <= now && now - stored.createdAtTime < maxAgeMs) {
                return new UsersCountSnapshot(Long.parseLong(stored.value), stored.createdAtTime, false);
            }

            KeyValueInfo failed = read(start, con, scope.app, storedOn, scope.key + FAILED_SUFFIX);
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
            GeneralQueries.deleteKeyValue_Transaction(start, con, storedOnTenant, scope.key + FAILED_SUFFIX);
            ProcessState.getInstance(start).addState(ProcessState.PROCESS_STATE.USERS_COUNT_COMPUTED, null);
            return new UsersCountSnapshot(count, countStartedAt, true);
        } finally {
            try {
                execute(con, "SELECT pg_advisory_unlock(hashtext(?))", pst -> pst.setString(1, scope.lockKey),
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
            try (Statement st = con.createStatement()) {
                // one snapshot for every chunk, so the sum is exact as of the first chunk
                st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
                st.execute("SET LOCAL statement_timeout = " + config.getUsersCountChunkTimeout());
            }
            Budget budget = new Budget(config, countStartedAt);
            boolean newTables = Config.getConfig(start).getMigrationMode().readsFromNewTables();
            long count;
            if (scope.tenantId == null) {
                count = newTables ? appCountNew(start, con, scope, budget) : appCountLegacy(start, con, scope, budget);
            } else if (newTables) {
                count = scope.recipes == null ? tenantCountNew(start, con, scope, budget)
                        : tenantCountNewFiltered(start, con, scope, budget);
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
    // order of app_id_to_user_id_pagination_index2 (app_id, time ASC, id DESC).
    private static long appCountNew(Start start, Connection con, Scope scope, Budget budget)
            throws SQLException, StorageQueryException {
        String table = getConfig(start).getAppIdToUserIdTable();
        String hit = scope.recipes == null ? "TRUE" : "recipe_id IN (" + placeholders(scope.recipes) + ")";
        String query = "WITH g AS (SELECT t, id, bool_or(h) AS hit FROM ("
                + "SELECT primary_or_recipe_user_time_joined AS t, primary_or_recipe_user_id AS id, " + hit + " AS h"
                + " FROM " + table + " WHERE app_id = ? AND (primary_or_recipe_user_time_joined > ?"
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
            Chunk chunk = execute(con, query, pst -> {
                int i = setRecipes(pst, 1, scope.recipes);
                pst.setString(i++, scope.app.getAppId());
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
    // all_auth_recipe_users_pagination_index1 (app_id, tenant_id, time DESC, id DESC) and count each group once, in
    // its first tenant; the lookups use all_auth_recipe_users_primary_user_id_index (primary_or_recipe_user_id,
    // app_id). Walking that index directly would also scan other apps' rows, unbounded per chunk on a shared database.
    private static long appCountLegacy(Start start, Connection con, Scope scope, Budget budget)
            throws SQLException, StorageQueryException {
        String table = getConfig(start).getUsersTable();
        String recipeFilter = scope.recipes == null ? "" : " AND EXISTS (SELECT 1 FROM " + table + " c"
                + " WHERE c.primary_or_recipe_user_id = g.id AND c.app_id = ? AND c.recipe_id IN ("
                + placeholders(scope.recipes) + "))";
        String query = "WITH g AS (SELECT tenant_id AS tn, primary_or_recipe_user_time_joined AS t,"
                + " primary_or_recipe_user_id AS id FROM " + table
                + " WHERE app_id = ? AND (tenant_id > ? OR (tenant_id = ?"
                + " AND (primary_or_recipe_user_time_joined, primary_or_recipe_user_id) < (?, ?)))"
                + " GROUP BY tenant_id, primary_or_recipe_user_time_joined, primary_or_recipe_user_id"
                + " ORDER BY tn ASC, t DESC, id DESC LIMIT ?)"
                + " SELECT (SELECT COUNT(*) FROM g) AS keys,"
                + " (SELECT COUNT(*) FROM g WHERE NOT EXISTS (SELECT 1 FROM " + table + " b"
                + " WHERE b.primary_or_recipe_user_id = g.id AND b.app_id = ? AND b.tenant_id < g.tn)"
                + recipeFilter + ") AS counted,"
                + " (SELECT tn FROM g ORDER BY tn DESC, t ASC, id ASC LIMIT 1) AS last_tn,"
                + " (SELECT t FROM g ORDER BY tn DESC, t ASC, id ASC LIMIT 1) AS last_t,"
                + " (SELECT id FROM g ORDER BY tn DESC, t ASC, id ASC LIMIT 1) AS last_id";
        long total = 0;
        String lastTenant = "";
        long lastT = Long.MAX_VALUE;
        String lastId = "";
        while (true) {
            String cursorTenant = lastTenant;
            long cursorT = lastT;
            String cursorId = lastId;
            Chunk chunk = execute(con, query, pst -> {
                int i = 1;
                pst.setString(i++, scope.app.getAppId());
                pst.setString(i++, cursorTenant);
                pst.setString(i++, cursorTenant);
                pst.setLong(i++, cursorT);
                pst.setString(i++, cursorId);
                pst.setInt(i++, budget.chunkSize);
                pst.setString(i++, scope.app.getAppId());
                if (scope.recipes != null) {
                    pst.setString(i++, scope.app.getAppId());
                    setRecipes(pst, i, scope.recipes);
                }
            }, r -> {
                r.next();
                Chunk c = new Chunk(r.getLong("keys"), r.getLong("counted"), r.getLong("last_t"),
                        r.getString("last_id"));
                c.lastTenant = r.getString("last_tn");
                return c;
            });
            total += chunk.counted;
            if (chunk.keys < budget.chunkSize) {
                return total;
            }
            lastTenant = chunk.lastTenant;
            lastT = chunk.lastLong;
            lastId = chunk.lastString;
            budget.afterChunk();
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
            Chunk chunk = execute(con, query, pst -> {
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

    // Tenant, new tables, unfiltered: D - L + G as the standalone count (GeneralQueries.getUsersCount_new), each part
    // walked in key order: D and L over idx_recipe_user_tenants_tenant_recipe_user, G over
    // idx_primary_user_tenants_tenant_primary.
    private static long tenantCountNew(Start start, Connection con, Scope scope, Budget budget)
            throws SQLException, StorageQueryException {
        PostgreSQLConfig config = getConfig(start);
        try (Statement st = con.createStatement()) {
            st.execute("SET LOCAL enable_hashjoin = off"); // as the standalone D/L query
        }
        String dlQuery = "WITH r AS (SELECT DISTINCT recipe_user_id FROM " + config.getRecipeUserTenantsTable()
                + " WHERE app_id = ? AND tenant_id = ? AND recipe_user_id > ? ORDER BY recipe_user_id LIMIT ?)"
                + " SELECT (SELECT COUNT(*) FROM r) AS keys, (SELECT MAX(recipe_user_id) FROM r) AS last_id,"
                + " COUNT(a.user_id) AS d, COUNT(*) FILTER (WHERE a.is_linked_or_is_a_primary_user) AS l"
                + " FROM r JOIN " + config.getAppIdToUserIdTable() + " a"
                + " ON a.app_id = ? AND a.user_id = r.recipe_user_id";
        long dMinusL = 0;
        String last = "";
        while (true) {
            String cursor = last;
            Chunk chunk = execute(con, dlQuery, pst -> {
                pst.setString(1, scope.app.getAppId());
                pst.setString(2, scope.tenantId);
                pst.setString(3, cursor);
                pst.setInt(4, budget.chunkSize);
                pst.setString(5, scope.app.getAppId());
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

        String gQuery = "WITH g AS (SELECT DISTINCT primary_user_id FROM " + config.getPrimaryUserTenantsTable()
                + " WHERE app_id = ? AND tenant_id = ? AND primary_user_id > ? ORDER BY primary_user_id LIMIT ?)"
                + " SELECT COUNT(*) AS keys, MAX(primary_user_id) AS last_id FROM g";
        long g = 0;
        last = "";
        while (true) {
            String cursor = last;
            Chunk chunk = execute(con, gQuery, pst -> {
                pst.setString(1, scope.app.getAppId());
                pst.setString(2, scope.tenantId);
                pst.setString(3, cursor);
                pst.setInt(4, budget.chunkSize);
            }, r -> {
                r.next();
                return new Chunk(r.getLong("keys"), r.getLong("keys"), 0, r.getString("last_id"));
            });
            g += chunk.counted;
            if (chunk.keys < budget.chunkSize) {
                return dMinusL + g;
            }
            last = chunk.lastString;
            budget.afterChunk();
        }
    }

    // Tenant, new tables, recipe filter: the app's groups in app_id_to_user_id_pagination_index2 order, a group
    // counting if any of its recipe users is in the tenant with one of the recipes (as getUsersCountByRecipe_new).
    private static long tenantCountNewFiltered(Start start, Connection con, Scope scope, Budget budget)
            throws SQLException, StorageQueryException {
        PostgreSQLConfig config = getConfig(start);
        String query = "WITH g AS (SELECT t, id, bool_or(h) AS hit FROM ("
                + "SELECT a.primary_or_recipe_user_time_joined AS t, a.primary_or_recipe_user_id AS id,"
                + " EXISTS (SELECT 1 FROM " + config.getRecipeUserTenantsTable() + " rut WHERE rut.app_id = a.app_id"
                + " AND rut.recipe_user_id = a.user_id AND rut.tenant_id = ? AND rut.recipe_id IN ("
                + placeholders(scope.recipes) + ")) AS h"
                + " FROM " + config.getAppIdToUserIdTable() + " a WHERE a.app_id = ?"
                + " AND (a.primary_or_recipe_user_time_joined > ? OR (a.primary_or_recipe_user_time_joined = ?"
                + " AND a.primary_or_recipe_user_id < ?))) x"
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
            Chunk chunk = execute(con, query, pst -> {
                pst.setString(1, scope.tenantId);
                int i = setRecipes(pst, 2, scope.recipes);
                pst.setString(i++, scope.app.getAppId());
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
        String lastTenant;

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
