package io.supertokens.storage.postgresql.test;

import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.authRecipe.UsersCountSnapshot;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class UsersCountCacheTest {

    private static final long TEN_MINUTES = 10 * 60 * 1000L;

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

    // A stored count younger than maxAgeMs is returned as is; an older one is replaced by a fresh count.
    @Test
    public void storedCountIsReusedOnlyWhileYoungerThanMaxAge() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Main main = process.getProcess();
        Start storage = (Start) StorageLayer.getStorage(main);
        AppIdentifier app = new AppIdentifier(null, null);

        EmailPassword.signUp(main, "user1@example.com", "password123");
        EmailPassword.signUp(main, "user2@example.com", "password123");

        long before = System.currentTimeMillis();
        UsersCountSnapshot first = storage.getOrComputeAppUsersCount(app, TEN_MINUTES);
        assertEquals(2, first.count);
        assertTrue(first.newlyCounted);
        assertTrue(first.countedAtMs >= before);
        assertNotNull(computed(storage));

        EmailPassword.signUp(main, "user3@example.com", "password123");
        io.supertokens.storage.postgresql.ProcessState.getInstance(storage).clear();

        // Still fresh: the stored 2 is served without counting, with the time it was counted.
        UsersCountSnapshot reused = storage.getOrComputeAppUsersCount(app, TEN_MINUTES);
        assertEquals(2, reused.count);
        assertFalse(reused.newlyCounted);
        assertEquals(first.countedAtMs, reused.countedAtMs);
        assertNull(computed(storage));

        // Too old for a zero max age: counted again.
        assertEquals(3, storage.getOrComputeAppUsersCount(app, 0).count);
        assertNotNull(computed(storage));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // The tenant count is the same value as getUsersCount(tenant), stored on that tenant and reused while fresh.
    @Test
    public void tenantCountMatchesTheExactCountAndIsReusedWhileFresh() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Main main = process.getProcess();
        Start storage = (Start) StorageLayer.getStorage(main);
        TenantIdentifier tenant = new TenantIdentifier(null, null, null);

        EmailPassword.signUp(main, "user1@example.com", "password123");
        EmailPassword.signUp(main, "user2@example.com", "password123");

        long exact = storage.getUsersCount(tenant, null);
        assertEquals(2, exact);
        assertEquals(exact, storage.getOrComputeTenantUsersCount(tenant, TEN_MINUTES).count);
        assertNotNull(computed(storage));
        KeyValueInfo stored = storage.getKeyValue(tenant, "TENANT_USERS_COUNT");
        assertNotNull(stored);
        assertEquals("2", stored.value);

        EmailPassword.signUp(main, "user3@example.com", "password123");
        io.supertokens.storage.postgresql.ProcessState.getInstance(storage).clear();

        assertEquals(2, storage.getOrComputeTenantUsersCount(tenant, TEN_MINUTES).count);
        assertNull(computed(storage));
        assertEquals(3, storage.getOrComputeTenantUsersCount(tenant, 0).count);
        assertNotNull(computed(storage));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // While another core holds the app's lock and stores its count, a caller queues on the lock and then returns
    // that stored count instead of running its own.
    @Test
    public void callerWaitsForTheLockHolderAndReadsItsStoredCount() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Main main = process.getProcess();
        Start storage = (Start) StorageLayer.getStorage(main);
        AppIdentifier app = new AppIdentifier(null, null);
        String table = Config.getConfig(storage).getKeyValueTable();

        // "another core": holds the lock across its count and stores a value no real count here would give
        try (Connection otherCore = DriverManager.getConnection(DatabaseTestHelper.getTestDatabaseUrl(),
                DatabaseTestHelper.getUser(), DatabaseTestHelper.getPassword())) {
            otherCore.setAutoCommit(false);
            try (PreparedStatement lock = otherCore.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?))")) {
                lock.setString(1, "app_users_count~" + app.getAppId());
                lock.execute();
            }
            try (PreparedStatement insert = otherCore.prepareStatement("INSERT INTO " + table
                    + "(app_id, tenant_id, name, value, created_at_time) VALUES(?, ?, ?, ?, ?)")) {
                insert.setString(1, app.getAppId());
                insert.setString(2, "public");
                insert.setString(3, "APP_USERS_COUNT");
                insert.setString(4, "42");
                insert.setLong(5, System.currentTimeMillis());
                insert.execute();
            }

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Long> result = executor.submit(() -> storage.getOrComputeAppUsersCount(app, TEN_MINUTES).count);

                Thread.sleep(1500);
                assertFalse("the caller must queue on the lock while the other core counts", result.isDone());

                otherCore.commit();

                assertEquals(42L, (long) result.get(10, TimeUnit.SECONDS));
                assertNull("the caller must not run its own count", computed(storage));
                assertNull("waiting must not go through the deadlock-retry loop",
                        io.supertokens.storage.postgresql.ProcessState.getInstance(storage).getLastEventByName(
                                io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.DEADLOCK_FOUND));
            } finally {
                executor.shutdownNow();
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private static io.supertokens.storage.postgresql.ProcessState.EventAndException computed(Start storage) {
        return io.supertokens.storage.postgresql.ProcessState.getInstance(storage).getLastEventByName(
                io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.USERS_COUNT_COMPUTED);
    }
}
