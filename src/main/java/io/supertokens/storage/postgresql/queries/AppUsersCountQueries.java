package io.supertokens.storage.postgresql.queries;

import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.ProcessState;
import io.supertokens.storage.postgresql.Start;

import java.sql.Connection;
import java.sql.SQLException;

import static io.supertokens.storage.postgresql.PreparedStatementValueSetter.NO_OP_SETTER;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

/**
 * An app's user count that is computed once and shared through the database by every core, instead of each core
 * running its own full count (see {@code AppUsersCountCacheStorage}).
 */
public class AppUsersCountQueries {

    // key_value row holding the stored count (value) and the time its count started (created_at_time), on the
    // app's public tenant.
    static final String APP_USERS_COUNT_KEY = "APP_USERS_COUNT";

    // A full count of a large app can take minutes, well past the pool's 60s socketTimeout, and callers waiting on
    // the lock wait as long. Both are bounded by this instead: statement_timeout on the server, and a slightly
    // longer network timeout on the client so the server's cancel arrives first. Hikari restores the pool's
    // network timeout when the connection is returned.
    static final int STATEMENT_TIMEOUT_MS = 10 * 60 * 1000;
    private static final int NETWORK_TIMEOUT_MS = STATEMENT_TIMEOUT_MS + 30 * 1000;

    /**
     * Returns the stored count if it is younger than {@code maxAgeMs}, otherwise counts and stores it. Runs on the
     * caller's transaction connection and first takes a blocking per-app advisory lock, so concurrent callers on
     * any core queue behind one count and then read the value it stored. The lock is released when the
     * transaction ends, including when the connection of a core that died mid-count is closed.
     */
    public static long getOrComputeAppUsersCount_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                             long maxAgeMs)
            throws SQLException, StorageQueryException {
        con.setNetworkTimeout(Runnable::run, NETWORK_TIMEOUT_MS);
        update(con, "SET LOCAL statement_timeout = " + STATEMENT_TIMEOUT_MS, NO_OP_SETTER);

        Utils.takeAdvisoryLockBlocking(con, "app_users_count~" + appIdentifier.getAppId());

        KeyValueInfo stored = execute(con, "SELECT value, created_at_time FROM " + getConfig(start).getKeyValueTable()
                + " WHERE app_id = ? AND tenant_id = ? AND name = ?", pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, appIdentifier.getAsPublicTenantIdentifier().getTenantId());
            pst.setString(3, APP_USERS_COUNT_KEY);
        }, result -> result.next()
                ? new KeyValueInfo(result.getString("value"), result.getLong("created_at_time"))
                : null);

        long now = System.currentTimeMillis();
        if (stored != null && stored.createdAtTime <= now && now - stored.createdAtTime < maxAgeMs) {
            return Long.parseLong(stored.value);
        }

        // Stamped before counting, so the stored age never understates how old the count is.
        long countStartedAt = System.currentTimeMillis();
        long count = GeneralQueries.getUsersCount_Transaction(start, con, appIdentifier);
        GeneralQueries.setKeyValue_Transaction(start, con, appIdentifier.getAsPublicTenantIdentifier(),
                APP_USERS_COUNT_KEY, new KeyValueInfo(Long.toString(count), countStartedAt));
        ProcessState.getInstance(start).addState(ProcessState.PROCESS_STATE.APP_USERS_COUNT_COMPUTED, null);
        return count;
    }
}
