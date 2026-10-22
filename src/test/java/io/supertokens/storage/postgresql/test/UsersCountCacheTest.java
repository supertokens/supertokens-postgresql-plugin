package io.supertokens.storage.postgresql.test;

import com.google.gson.JsonObject;
import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.UsersCountSnapshot;
import io.supertokens.pluginInterface.authRecipe.exceptions.UsersCountBackoffException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.queries.UsersCountCacheQueries;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;
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
import java.util.concurrent.TimeoutException;

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
        UsersCountSnapshot first = storage.getOrComputeAppUsersCount(app, null, TEN_MINUTES);
        assertEquals(2, first.count);
        assertTrue(first.newlyCounted);
        assertTrue(first.countedAtMs >= before);
        assertNotNull(computed(storage));

        EmailPassword.signUp(main, "user3@example.com", "password123");
        io.supertokens.storage.postgresql.ProcessState.getInstance(storage).clear();

        // Still fresh: the stored 2 is served without counting, with the time it was counted.
        UsersCountSnapshot reused = storage.getOrComputeAppUsersCount(app, null, TEN_MINUTES);
        assertEquals(2, reused.count);
        assertFalse(reused.newlyCounted);
        assertEquals(first.countedAtMs, reused.countedAtMs);
        assertNull(computed(storage));
        assertEquals(2, storage.getStoredAppUsersCount(app, null).count);

        // Too old for a zero max age: counted again.
        assertEquals(3, storage.getOrComputeAppUsersCount(app, null, 0).count);
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
        assertEquals(exact, storage.getOrComputeTenantUsersCount(tenant, null, TEN_MINUTES).count);
        assertNotNull(computed(storage));
        KeyValueInfo stored = storage.getKeyValue(tenant, "TENANT_USERS_COUNT");
        assertNotNull(stored);
        assertEquals("2", stored.value);

        EmailPassword.signUp(main, "user3@example.com", "password123");
        io.supertokens.storage.postgresql.ProcessState.getInstance(storage).clear();

        assertEquals(2, storage.getOrComputeTenantUsersCount(tenant, null, TEN_MINUTES).count);
        assertNull(computed(storage));
        assertEquals(3, storage.getOrComputeTenantUsersCount(tenant, null, 0).count);
        assertNotNull(computed(storage));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // While another core holds the scope's lock and stores its count, a caller queues on the lock and then returns
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
                lock.setString(1, UsersCountCacheQueries.Scope.app(app, null).getLockKeyForTesting());
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
                Future<Long> result = executor.submit(
                        () -> storage.getOrComputeAppUsersCount(app, null, TEN_MINUTES).count);

                Thread.sleep(1500);
                assertFalse("the caller must queue on the lock while the other core counts", result.isDone());

                otherCore.commit();

                assertEquals(42L, (long) result.get(10, TimeUnit.SECONDS));
                assertNull("the caller must not run its own count", computed(storage));
            } finally {
                executor.shutdownNow();
            }
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // The chunked count equals the exact single-statement count for every scope and recipe filter, in every read
    // mode. Chunks of 2 put linked groups, users in several tenants and recipe filters across chunk boundaries.
    @Test
    public void chunkedCountsMatchTheExactCountsInEveryReadMode() throws Exception {
        Utils.setValueInConfig("postgresql_users_count_chunk_size", "2");
        RECIPE_ID[][] filters = {null, {RECIPE_ID.EMAIL_PASSWORD}, {RECIPE_ID.THIRD_PARTY},
                {RECIPE_ID.THIRD_PARTY, RECIPE_ID.EMAIL_PASSWORD}};

        for (MigrationMode mode : new MigrationMode[]{MigrationMode.LEGACY, MigrationMode.DUAL_WRITE_READ_NEW,
                MigrationMode.MIGRATED}) {
            TestingProcessManager.TestingProcess process = startWithLinkingAndTenants();
            try {
                Main main = process.getProcess();
                if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                    return;
                }
                TenantIdentifier publicTenant = new TenantIdentifier(null, null, null);
                TenantIdentifier t1 = new TenantIdentifier(null, null, "t1");
                Multitenancy.addNewOrUpdateAppOrTenant(main, new TenantConfig(t1, new EmailPasswordConfig(true),
                        new ThirdPartyConfig(true, null), new PasswordlessConfig(true), null, null,
                        new JsonObject()), false);
                Start storage = (Start) StorageLayer.getStorage(main);
                Config.getConfig(storage).setMigrationModeForTesting(mode);
                Storage t1Storage = StorageLayer.getStorage(t1, main);
                String m = mode.name().toLowerCase();

                // public: 5 emailpassword users, two of them linked with a third-party user into groups
                AuthRecipeUserInfo[] ep = new AuthRecipeUserInfo[5];
                for (int i = 0; i < ep.length; i++) {
                    ep[i] = EmailPassword.signUp(main, "ep" + i + "-" + m + "@test.com", "password123");
                    Thread.sleep(5);
                }
                ThirdParty.SignInUpResponse tpLinked = ThirdParty.signInUp(main, "google", "g1-" + m,
                        "tp1-" + m + "@test.com");
                ThirdParty.SignInUpResponse tpAlone = ThirdParty.signInUp(main, "google", "g2-" + m,
                        "tp2-" + m + "@test.com");
                AuthRecipe.createPrimaryUser(main, ep[1].getSupertokensUserId());
                AuthRecipe.linkAccounts(main, tpLinked.user.getSupertokensUserId(), ep[1].getSupertokensUserId());
                AuthRecipe.createPrimaryUser(main, ep[3].getSupertokensUserId());
                AuthRecipe.linkAccounts(main, ep[4].getSupertokensUserId(), ep[3].getSupertokensUserId());

                // t1: a group and a standalone user shared with public, plus a user only in t1
                Multitenancy.addUserIdToTenant(main, t1, t1Storage, ep[1].getSupertokensUserId());
                Multitenancy.addUserIdToTenant(main, t1, t1Storage, tpAlone.user.getSupertokensUserId());
                EmailPassword.signUp(t1, t1Storage, main, "t1only-" + m + "@test.com", "password123");

                AppIdentifier app = publicTenant.toAppIdentifier();
                for (RECIPE_ID[] filter : filters) {
                    String what = mode + " recipes=" + (filter == null ? "all" : java.util.Arrays.toString(filter));
                    assertEquals("app " + what, storage.getUsersCount(app, filter),
                            storage.getOrComputeAppUsersCount(app, filter, 0).count);
                    for (TenantIdentifier tenant : new TenantIdentifier[]{publicTenant, t1}) {
                        assertEquals(tenant.getTenantId() + " " + what, storage.getUsersCount(tenant, filter),
                                storage.getOrComputeTenantUsersCount(tenant, filter, 0).count);
                    }
                }
            } finally {
                process.kill();
                assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
            }
        }
    }

    // A count that fails (here: over its overall budget) backs every caller off the scope instead of counting again
    // straight away, while the last good count stays readable.
    @Test
    public void failedCountBacksOffAndKeepsTheLastGoodCount() throws Exception {
        Utils.setValueInConfig("postgresql_users_count_chunk_size", "1");
        Utils.setValueInConfig("postgresql_users_count_chunk_pause", "20");
        Utils.setValueInConfig("postgresql_users_count_max_duration", "1");
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Main main = process.getProcess();
        Start storage = (Start) StorageLayer.getStorage(main);
        AppIdentifier app = new AppIdentifier(null, null);

        // no users: one short chunk, within the budget
        assertEquals(0, storage.getOrComputeAppUsersCount(app, null, 0).count);

        for (int i = 0; i < 3; i++) {
            EmailPassword.signUp(main, "user" + i + "@example.com", "password123");
        }
        try {
            storage.getOrComputeAppUsersCount(app, null, 0);
            fail("three chunks with 20ms pauses cannot fit in a 1ms budget");
        } catch (StorageQueryException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
        try {
            storage.getOrComputeAppUsersCount(app, null, 0);
            fail("a count of the scope just failed, so this one must back off");
        } catch (StorageQueryException e) {
            assertTrue(e.getCause() instanceof UsersCountBackoffException);
        }
        assertEquals(0, storage.getStoredAppUsersCount(app, null).count);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // All chunks read one snapshot: users added while a (slow, chunked) count is running do not change its result.
    @Test
    public void usersAddedDuringACountDoNotChangeIt() throws Exception {
        Utils.setValueInConfig("postgresql_users_count_chunk_size", "1");
        Utils.setValueInConfig("postgresql_users_count_chunk_pause", "300");
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Main main = process.getProcess();
        Start storage = (Start) StorageLayer.getStorage(main);
        AppIdentifier app = new AppIdentifier(null, null);
        for (int i = 0; i < 3; i++) {
            EmailPassword.signUp(main, "user" + i + "@example.com", "password123");
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Long> counting = executor.submit(() -> storage.getOrComputeAppUsersCount(app, null, 0).count);
            Thread.sleep(150); // the first chunk (and with it the snapshot) has run; the count pauses after it
            EmailPassword.signUp(main, "late1@example.com", "password123");
            EmailPassword.signUp(main, "late2@example.com", "password123");
            assertEquals(3L, (long) counting.get(20, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
        assertEquals(5, storage.getOrComputeAppUsersCount(app, null, 0).count);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private static TestingProcessManager.TestingProcess startWithLinkingAndTenants() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess()).setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES,
                new EE_FEATURES[]{EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        return process;
    }

    private static io.supertokens.storage.postgresql.ProcessState.EventAndException computed(Start storage) {
        return io.supertokens.storage.postgresql.ProcessState.getInstance(storage).getLastEventByName(
                io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.USERS_COUNT_COMPUTED);
    }
}
