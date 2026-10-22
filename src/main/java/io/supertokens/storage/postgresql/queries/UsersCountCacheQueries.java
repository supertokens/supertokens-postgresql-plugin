package io.supertokens.storage.postgresql.queries;

import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.ProcessState;
import io.supertokens.storage.postgresql.Start;

import java.sql.Connection;
import java.sql.SQLException;

import static io.supertokens.storage.postgresql.PreparedStatementValueSetter.NO_OP_SETTER;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

/**
 * User counts that are computed once and shared through the database by every core, instead of each core running
 * its own full count (see {@code UsersCountCacheStorage}).
 */
public class UsersCountCacheQueries {

    // key_value rows holding a stored count (value) and the time its count started (created_at_time): the app count
    // on the app's public tenant, a tenant count on that tenant.
    static final String APP_USERS_COUNT_KEY = "APP_USERS_COUNT";
    static final String TENANT_USERS_COUNT_KEY = "TENANT_USERS_COUNT";

    // A full count of a large app or tenant can take minutes, well past the pool's 60s socketTimeout, and callers
    // waiting on the lock wait as long. Both are bounded by this instead: statement_timeout on the server, and a
    // slightly longer network timeout on the client so the server's cancel arrives first. Hikari restores the
    // pool's network timeout when the connection is returned.
    static final int STATEMENT_TIMEOUT_MS = 10 * 60 * 1000;
    private static final int NETWORK_TIMEOUT_MS = STATEMENT_TIMEOUT_MS + 30 * 1000;

    public static long getOrComputeAppUsersCount_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                             long maxAgeMs)
            throws SQLException, StorageQueryException {
        return getOrCompute(start, con, appIdentifier.getAsPublicTenantIdentifier(), APP_USERS_COUNT_KEY,
                "app_users_count~" + appIdentifier.getAppId(), maxAgeMs,
                () -> GeneralQueries.getUsersCount_Transaction(start, con, appIdentifier));
    }

    public static long getOrComputeTenantUsersCount_Transaction(Start start, Connection con,
                                                                TenantIdentifier tenantIdentifier, long maxAgeMs)
            throws SQLException, StorageQueryException {
        return getOrCompute(start, con, tenantIdentifier, TENANT_USERS_COUNT_KEY,
                "tenant_users_count~" + tenantIdentifier.getAppId() + "~" + tenantIdentifier.getTenantId(), maxAgeMs,
                () -> GeneralQueries.getUsersCount_Transaction(start, con, tenantIdentifier));
    }

    /**
     * Returns the stored count if it is younger than {@code maxAgeMs}, otherwise counts and stores it. Runs on the
     * caller's transaction connection and first takes a blocking advisory lock on {@code lockKey}, so concurrent
     * callers on any core queue behind one count and then read the value it stored. The lock is released when the
     * transaction ends, including when the connection of a core that died mid-count is closed.
     */
    private static long getOrCompute(Start start, Connection con, TenantIdentifier storedOn, String key,
                                     String lockKey, long maxAgeMs, Count count)
            throws SQLException, StorageQueryException {
        con.setNetworkTimeout(Runnable::run, NETWORK_TIMEOUT_MS);
        update(con, "SET LOCAL statement_timeout = " + STATEMENT_TIMEOUT_MS, NO_OP_SETTER);

        Utils.takeAdvisoryLockBlocking(con, lockKey);

        KeyValueInfo stored = execute(con, "SELECT value, created_at_time FROM " + getConfig(start).getKeyValueTable()
                + " WHERE app_id = ? AND tenant_id = ? AND name = ?", pst -> {
            pst.setString(1, storedOn.getAppId());
            pst.setString(2, storedOn.getTenantId());
            pst.setString(3, key);
        }, result -> result.next()
                ? new KeyValueInfo(result.getString("value"), result.getLong("created_at_time"))
                : null);

        long now = System.currentTimeMillis();
        if (stored != null && stored.createdAtTime <= now && now - stored.createdAtTime < maxAgeMs) {
            return Long.parseLong(stored.value);
        }

        // Stamped before counting, so the stored age never understates how old the count is.
        long countStartedAt = System.currentTimeMillis();
        long counted = count.run();
        GeneralQueries.setKeyValue_Transaction(start, con, storedOn, key,
                new KeyValueInfo(Long.toString(counted), countStartedAt));
        ProcessState.getInstance(start).addState(ProcessState.PROCESS_STATE.USERS_COUNT_COMPUTED, null);
        return counted;
    }

    @FunctionalInterface
    private interface Count {
        long run() throws SQLException, StorageQueryException;
    }
}
