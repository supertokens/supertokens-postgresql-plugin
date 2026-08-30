/*
 *    Copyright (c) 2025, VRAI Labs and/or its affiliates. All rights reserved.
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

package io.supertokens.storage.postgresql.test;

import io.supertokens.ProcessState;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.pluginInterface.MigrationMode;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicateEmailException;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicateUserIdException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.passwordless.exception.DuplicatePhoneNumberException;
import io.supertokens.pluginInterface.sqlStorage.TransactionConnection;
import io.supertokens.pluginInterface.thirdparty.exception.DuplicateThirdPartyUserException;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * Exercises the connection-taking sign-up / user-creation / tenant-removal variants added for
 * plugin-interface#216 (PLAN-010). They must be behaviour-preserving refactors of the existing
 * auto-commit methods: the same writes on the caller's connection with no commit, so the caller
 * can commit the mutation together with a lifecycle audit event (or roll both back).
 */
public class TransactionalSignUpAndTenantRemovalTest {

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

    private static final TenantIdentifier PUBLIC = new TenantIdentifier(null, null, null);
    private static final MigrationMode[] ALL_MODES = new MigrationMode[]{
            MigrationMode.LEGACY, MigrationMode.DUAL_WRITE_READ_OLD,
            MigrationMode.DUAL_WRITE_READ_NEW, MigrationMode.MIGRATED};

    private TestingProcessManager.TestingProcess startProcess(MigrationMode mode) throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        Config.getConfig((Start) StorageLayer.getStorage(process.getProcess())).setMigrationModeForTesting(mode);
        return process;
    }

    // ---- helpers ---------------------------------------------------------------------------------

    @FunctionalInterface
    private interface TxnCreate {
        AuthRecipeUserInfo create(TransactionConnection con, String userId) throws Exception;
    }

    @FunctionalInterface
    private interface TxnAction {
        void run(TransactionConnection con) throws Exception;
    }

    // Counts every row a create writes for this userId, across all tables and all migration modes.
    // After a rollback this must be 0; while the creating transaction is still open it is > 0 on that
    // same connection.
    private long countUserRows(Connection sqlCon, Start storage, String userId) throws SQLException {
        PostgreSQLConfig config = Config.getConfig(storage);
        long total = 0;
        total += countWhere(sqlCon, config.getAppIdToUserIdTable(), "user_id", userId);
        total += countWhere(sqlCon, config.getUsersTable(), "user_id", userId);
        total += countWhere(sqlCon, config.getEmailPasswordUsersTable(), "user_id", userId);
        total += countWhere(sqlCon, config.getThirdPartyUsersTable(), "user_id", userId);
        total += countWhere(sqlCon, config.getPasswordlessUsersTable(), "user_id", userId);
        total += countWhere(sqlCon, config.getRecipeUserTenantsTable(), "recipe_user_id", userId);
        total += countWhere(sqlCon, config.getEmailPasswordUserToTenantTable(), "user_id", userId);
        total += countWhere(sqlCon, config.getThirdPartyUserToTenantTable(), "user_id", userId);
        total += countWhere(sqlCon, config.getPasswordlessUserToTenantTable(), "user_id", userId);
        return total;
    }

    private long countWhere(Connection sqlCon, String table, String column, String value) throws SQLException {
        String query = "SELECT count(*) FROM " + table + " WHERE " + column + " = ?";
        try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
            pst.setString(1, value);
            try (ResultSet rs = pst.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private long tenantRowsOnConnection(Connection sqlCon, Start storage, TenantIdentifier tenant, String userId)
            throws SQLException {
        String query = "SELECT count(*) FROM " + Config.getConfig(storage).getUsersTable()
                + " WHERE app_id = ? AND tenant_id = ? AND user_id = ?";
        try (PreparedStatement pst = sqlCon.prepareStatement(query)) {
            pst.setString(1, tenant.getAppId());
            pst.setString(2, tenant.getTenantId());
            pst.setString(3, userId);
            try (ResultSet rs = pst.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private long countUserRowsFresh(Start storage, String userId) throws Exception {
        return (long) storage.startTransaction(con -> {
            try {
                return countUserRows((Connection) con.getConnection(), storage, userId);
            } catch (SQLException e) {
                throw new StorageTransactionLogicException(e);
            }
        });
    }

    private void assertRollbackLeavesNoRows(Start storage, String userId, TxnCreate creator) throws Exception {
        assertEquals("precondition: no rows for userId yet", 0L, countUserRowsFresh(storage, userId));
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                AuthRecipeUserInfo created = creator.create(con, userId);
                assertNotNull(created);
                // the writes are visible to a later statement on the SAME connection, before any commit
                assertTrue("_Transaction writes must be visible on the caller's own connection",
                        countUserRows(sqlCon, storage, userId) > 0);
                sqlCon.rollback();
                return null;
            } catch (SQLException e) {
                throw new StorageTransactionLogicException(e);
            } catch (Exception e) {
                throw new StorageTransactionLogicException(e);
            }
        });
        assertEquals("a rolled-back _Transaction create must leave no row in any table",
                0L, countUserRowsFresh(storage, userId));
    }

    // Asserts the _Transaction variant raises `expected` (same type the auto-commit path raises) and that the
    // connection stays usable for rollback afterwards.
    private void assertThrowsThenRollbackWorks(Start storage, Class<? extends Exception> expected, TxnAction action)
            throws Exception {
        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            Exception caught = null;
            try {
                action.run(con);
            } catch (Exception e) {
                caught = e;
            }
            assertNotNull("expected " + expected.getSimpleName() + " from the _Transaction variant", caught);
            assertTrue("expected " + expected.getSimpleName() + " but got " + caught.getClass().getName(),
                    expected.isInstance(caught));
            // after the aborted statement the connection must still accept a rollback (issue requirement)
            try {
                sqlCon.rollback();
            } catch (SQLException e) {
                throw new StorageTransactionLogicException(e);
            }
            return null;
        });
    }

    // Runs an auto-commit call and returns the class of whatever it throws (null if it didn't throw).
    private Class<?> autoCommitExceptionClass(ThrowingRunnable r) {
        try {
            r.run();
            return null;
        } catch (Exception e) {
            return e.getClass();
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    // ---- rollback leaves no rows (per migration mode) --------------------------------------------

    @Test
    public void emailPasswordSignUpTransactionRollbackLeavesNoRows() throws Exception {
        for (MigrationMode mode : ALL_MODES) {
            TestingProcessManager.TestingProcess process = startProcess(mode);
            Start storage = (Start) StorageLayer.getStorage(process.getProcess());
            long now = System.currentTimeMillis();
            assertRollbackLeavesNoRows(storage, "ep-" + mode, (con, userId) ->
                    storage.signUp_Transaction(PUBLIC, con, userId, userId + "@example.com", "hash", now));
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void thirdPartySignUpTransactionRollbackLeavesNoRows() throws Exception {
        for (MigrationMode mode : ALL_MODES) {
            TestingProcessManager.TestingProcess process = startProcess(mode);
            Start storage = (Start) StorageLayer.getStorage(process.getProcess());
            long now = System.currentTimeMillis();
            assertRollbackLeavesNoRows(storage, "tp-" + mode, (con, userId) ->
                    storage.signUp_Transaction(PUBLIC, con, userId, userId + "@example.com",
                            new LoginMethod.ThirdParty("google", "tpid-" + userId), now));
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void passwordlessCreateUserTransactionRollbackLeavesNoRows() throws Exception {
        for (MigrationMode mode : ALL_MODES) {
            TestingProcessManager.TestingProcess process = startProcess(mode);
            Start storage = (Start) StorageLayer.getStorage(process.getProcess());
            long now = System.currentTimeMillis();
            // with email
            assertRollbackLeavesNoRows(storage, "ple-" + mode, (con, userId) ->
                    storage.createUser_Transaction(PUBLIC, con, userId, userId + "@example.com", null, now));
            // with phone number
            assertRollbackLeavesNoRows(storage, "plp-" + mode, (con, userId) ->
                    storage.createUser_Transaction(PUBLIC, con, userId, null, "+1000" + Math.abs(userId.hashCode()),
                            now));
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    // ---- committed _Transaction create persists a well-formed user -------------------------------

    @Test
    public void signUpTransactionCommitPersists() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess(MigrationMode.DUAL_WRITE_READ_OLD);
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        long now = System.currentTimeMillis();

        AuthRecipeUserInfo created = (AuthRecipeUserInfo) storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                AuthRecipeUserInfo u = storage.signUp_Transaction(PUBLIC, con, "commit-user",
                        "commit@example.com", "hash", now);
                sqlCon.commit();
                return u;
            } catch (SQLException e) {
                throw new StorageTransactionLogicException(e);
            } catch (Exception e) {
                throw new StorageTransactionLogicException(e);
            }
        });

        assertNotNull(created);
        assertEquals("commit-user", created.getSupertokensUserId());
        assertEquals(1, storage.getUsersCount(PUBLIC, new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD}));
        assertTrue(countUserRowsFresh(storage, "commit-user") > 0);

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // ---- duplicate detection matches the auto-commit path ----------------------------------------
    //
    // Run under both DUAL_WRITE_READ_OLD and LEGACY. The two modes reach the duplicate through
    // different constraints, so they cover different translation arms of the new _Transaction
    // variants: with the new tables present (DUAL_WRITE_READ_OLD) a duplicate email/phone/third-party
    // trips the `recipe_user_tenants` primary key first (the `isPrimaryKeyError(recipeUserTenants)`
    // arm), whereas in LEGACY (old tables only, no `recipe_user_tenants`) it trips the unique
    // constraint on the `..._user_to_tenant` table (the `isUniqueConstraintError(...UserToTenantTable,
    // "email"/"phone_number"/"third_party_user_id")` arm). In every mode the mapped exception type
    // must still match the auto-commit path.

    @Test
    public void emailPasswordDuplicateDetectionMatchesAutoCommit() throws Exception {
        assertEmailPasswordDuplicateParity(MigrationMode.DUAL_WRITE_READ_OLD);
    }

    @Test
    public void emailPasswordDuplicateDetectionMatchesAutoCommitLegacy() throws Exception {
        assertEmailPasswordDuplicateParity(MigrationMode.LEGACY);
    }

    private void assertEmailPasswordDuplicateParity(MigrationMode mode) throws Exception {
        TestingProcessManager.TestingProcess process = startProcess(mode);
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        long now = System.currentTimeMillis();
        storage.signUp(PUBLIC, "ep1", "ep1@example.com", "hash", now);

        // duplicate user id
        assertEquals(DuplicateUserIdException.class,
                autoCommitExceptionClass(() -> storage.signUp(PUBLIC, "ep1", "other@example.com", "hash", now)));
        assertThrowsThenRollbackWorks(storage, DuplicateUserIdException.class, con ->
                storage.signUp_Transaction(PUBLIC, con, "ep1", "other@example.com", "hash", now));

        // duplicate email
        assertEquals(DuplicateEmailException.class,
                autoCommitExceptionClass(() -> storage.signUp(PUBLIC, "ep2", "ep1@example.com", "hash", now)));
        assertThrowsThenRollbackWorks(storage, DuplicateEmailException.class, con ->
                storage.signUp_Transaction(PUBLIC, con, "ep2", "ep1@example.com", "hash", now));

        // the failed attempts left nothing behind
        assertEquals(1, storage.getUsersCount(PUBLIC, new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD}));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void thirdPartyDuplicateDetectionMatchesAutoCommit() throws Exception {
        assertThirdPartyDuplicateParity(MigrationMode.DUAL_WRITE_READ_OLD);
    }

    @Test
    public void thirdPartyDuplicateDetectionMatchesAutoCommitLegacy() throws Exception {
        assertThirdPartyDuplicateParity(MigrationMode.LEGACY);
    }

    private void assertThirdPartyDuplicateParity(MigrationMode mode) throws Exception {
        TestingProcessManager.TestingProcess process = startProcess(mode);
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        long now = System.currentTimeMillis();
        LoginMethod.ThirdParty tp = new LoginMethod.ThirdParty("google", "g-1");
        storage.signUp(PUBLIC, "tp1", "tp1@example.com", tp, now);

        // duplicate user id
        assertEquals(io.supertokens.pluginInterface.thirdparty.exception.DuplicateUserIdException.class,
                autoCommitExceptionClass(() -> storage.signUp(PUBLIC, "tp1", "tp1@example.com",
                        new LoginMethod.ThirdParty("google", "g-2"), now)));
        assertThrowsThenRollbackWorks(storage,
                io.supertokens.pluginInterface.thirdparty.exception.DuplicateUserIdException.class, con ->
                        storage.signUp_Transaction(PUBLIC, con, "tp1", "tp1@example.com",
                                new LoginMethod.ThirdParty("google", "g-2"), now));

        // duplicate third party user (same provider + third-party user id, different user id)
        assertEquals(DuplicateThirdPartyUserException.class,
                autoCommitExceptionClass(() -> storage.signUp(PUBLIC, "tp2", "tp1@example.com", tp, now)));
        assertThrowsThenRollbackWorks(storage, DuplicateThirdPartyUserException.class, con ->
                storage.signUp_Transaction(PUBLIC, con, "tp2", "tp1@example.com", tp, now));

        assertEquals(1, storage.getUsersCount(PUBLIC, new RECIPE_ID[]{RECIPE_ID.THIRD_PARTY}));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void passwordlessDuplicateDetectionMatchesAutoCommit() throws Exception {
        assertPasswordlessDuplicateParity(MigrationMode.DUAL_WRITE_READ_OLD);
    }

    @Test
    public void passwordlessDuplicateDetectionMatchesAutoCommitLegacy() throws Exception {
        assertPasswordlessDuplicateParity(MigrationMode.LEGACY);
    }

    private void assertPasswordlessDuplicateParity(MigrationMode mode) throws Exception {
        TestingProcessManager.TestingProcess process = startProcess(mode);
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        long now = System.currentTimeMillis();
        storage.createUser(PUBLIC, "pl1", "pl1@example.com", null, now);
        storage.createUser(PUBLIC, "pl-phone", null, "+10000001", now);

        // duplicate user id
        assertEquals(DuplicateUserIdException.class,
                autoCommitExceptionClass(() -> storage.createUser(PUBLIC, "pl1", "new@example.com", null, now)));
        assertThrowsThenRollbackWorks(storage, DuplicateUserIdException.class, con ->
                storage.createUser_Transaction(PUBLIC, con, "pl1", "new@example.com", null, now));

        // duplicate email
        assertEquals(DuplicateEmailException.class,
                autoCommitExceptionClass(() -> storage.createUser(PUBLIC, "pl2", "pl1@example.com", null, now)));
        assertThrowsThenRollbackWorks(storage, DuplicateEmailException.class, con ->
                storage.createUser_Transaction(PUBLIC, con, "pl2", "pl1@example.com", null, now));

        // duplicate phone number
        assertEquals(DuplicatePhoneNumberException.class,
                autoCommitExceptionClass(() -> storage.createUser(PUBLIC, "pl3", null, "+10000001", now)));
        assertThrowsThenRollbackWorks(storage, DuplicatePhoneNumberException.class, con ->
                storage.createUser_Transaction(PUBLIC, con, "pl3", null, "+10000001", now));

        assertEquals(2, storage.getUsersCount(PUBLIC, new RECIPE_ID[]{RECIPE_ID.PASSWORDLESS}));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    // ---- removeUserIdFromTenant_Transaction ------------------------------------------------------

    @Test
    public void removeUserIdFromTenantTransactionUnknownUserReturnsFalseWithoutWriting() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess(MigrationMode.DUAL_WRITE_READ_OLD);
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());

        long before = storage.getUsersCount(PUBLIC, new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD,
                RECIPE_ID.THIRD_PARTY, RECIPE_ID.PASSWORDLESS});

        boolean removed = (boolean) storage.startTransaction(con -> {
            try {
                boolean r = storage.removeUserIdFromTenant_Transaction(PUBLIC, con, "no-such-user");
                ((Connection) con.getConnection()).commit();
                return r;
            } catch (SQLException e) {
                throw new StorageTransactionLogicException(e);
            }
        });

        assertFalse("unknown user must yield false", removed);
        assertEquals("no write should have happened", before,
                storage.getUsersCount(PUBLIC, new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD,
                        RECIPE_ID.THIRD_PARTY, RECIPE_ID.PASSWORDLESS}));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Test
    public void removeUserIdFromTenantTransactionVisibilityAndIsolation() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess(MigrationMode.DUAL_WRITE_READ_OLD);
        Start storage = (Start) StorageLayer.getStorage(process.getProcess());
        long now = System.currentTimeMillis();
        storage.signUp(PUBLIC, "rm1", "rm1@example.com", "hash", now);
        assertEquals(1, storage.getUsersCount(PUBLIC, new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD}));

        storage.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                boolean removed = storage.removeUserIdFromTenant_Transaction(PUBLIC, con, "rm1");
                assertTrue(removed);

                // visible to a later statement on the SAME connection
                assertEquals("removal must be visible on the caller's own connection", 0L,
                        tenantRowsOnConnection(sqlCon, storage, PUBLIC, "rm1"));

                // NOT visible to another connection until commit
                long fromAnotherConnection = (long) storage.startTransaction(innerCon -> {
                    try {
                        return tenantRowsOnConnection((Connection) innerCon.getConnection(), storage, PUBLIC, "rm1");
                    } catch (SQLException e) {
                        throw new StorageTransactionLogicException(e);
                    }
                });
                assertEquals("uncommitted removal must not be visible to another connection", 1L,
                        fromAnotherConnection);

                sqlCon.commit();
                return null;
            } catch (SQLException e) {
                throw new StorageTransactionLogicException(e);
            }
        });

        // after commit it is gone everywhere
        assertEquals(0, storage.getUsersCount(PUBLIC, new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD}));

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
