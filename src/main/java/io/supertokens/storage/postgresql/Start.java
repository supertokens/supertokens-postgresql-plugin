/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
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
 *
 */

package io.supertokens.storage.postgresql;

import ch.qos.logback.classic.Logger;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.zaxxer.hikari.pool.HikariPool;
import io.supertokens.pluginInterface.*;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.authRecipe.sqlStorage.AuthRecipeSQLStorage;
import io.supertokens.pluginInterface.dashboard.DashboardSearchTags;
import io.supertokens.pluginInterface.dashboard.DashboardSessionInfo;
import io.supertokens.pluginInterface.dashboard.DashboardUser;
import io.supertokens.pluginInterface.dashboard.exceptions.UserIdNotFoundException;
import io.supertokens.pluginInterface.dashboard.sqlStorage.DashboardSQLStorage;
import io.supertokens.pluginInterface.emailpassword.PasswordResetTokenInfo;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicateEmailException;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicatePasswordResetTokenException;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicateUserIdException;
import io.supertokens.pluginInterface.emailpassword.exceptions.UnknownUserIdException;
import io.supertokens.pluginInterface.emailpassword.sqlStorage.EmailPasswordSQLStorage;
import io.supertokens.pluginInterface.emailverification.EmailVerificationStorage;
import io.supertokens.pluginInterface.emailverification.EmailVerificationTokenInfo;
import io.supertokens.pluginInterface.emailverification.exception.DuplicateEmailVerificationTokenException;
import io.supertokens.pluginInterface.emailverification.sqlStorage.EmailVerificationSQLStorage;
import io.supertokens.pluginInterface.exceptions.DbInitException;
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.jwt.JWTRecipeStorage;
import io.supertokens.pluginInterface.jwt.JWTSigningKeyInfo;
import io.supertokens.pluginInterface.jwt.exceptions.DuplicateKeyIdException;
import io.supertokens.pluginInterface.jwt.sqlstorage.JWTRecipeSQLStorage;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.pluginInterface.multitenancy.exceptions.DuplicateClientTypeException;
import io.supertokens.pluginInterface.multitenancy.exceptions.DuplicateTenantException;
import io.supertokens.pluginInterface.multitenancy.exceptions.DuplicateThirdPartyIdException;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.pluginInterface.multitenancy.sqlStorage.MultitenancySQLStorage;
import io.supertokens.pluginInterface.passwordless.PasswordlessCode;
import io.supertokens.pluginInterface.passwordless.PasswordlessDevice;
import io.supertokens.pluginInterface.passwordless.exception.*;
import io.supertokens.pluginInterface.passwordless.sqlStorage.PasswordlessSQLStorage;
import io.supertokens.pluginInterface.session.SessionInfo;
import io.supertokens.pluginInterface.session.SessionStorage;
import io.supertokens.pluginInterface.session.sqlStorage.SessionSQLStorage;
import io.supertokens.pluginInterface.sqlStorage.TransactionConnection;
import io.supertokens.pluginInterface.thirdparty.exception.DuplicateThirdPartyUserException;
import io.supertokens.pluginInterface.thirdparty.sqlStorage.ThirdPartySQLStorage;
import io.supertokens.pluginInterface.totp.TOTPDevice;
import io.supertokens.pluginInterface.totp.TOTPStorage;
import io.supertokens.pluginInterface.totp.TOTPUsedCode;
import io.supertokens.pluginInterface.totp.exception.DeviceAlreadyExistsException;
import io.supertokens.pluginInterface.totp.exception.UnknownDeviceException;
import io.supertokens.pluginInterface.totp.exception.UnknownTotpUserIdException;
import io.supertokens.pluginInterface.totp.exception.UsedCodeAlreadyExistsException;
import io.supertokens.pluginInterface.totp.sqlStorage.TOTPSQLStorage;
import io.supertokens.pluginInterface.useridmapping.UserIdMapping;
import io.supertokens.pluginInterface.useridmapping.UserIdMappingStorage;
import io.supertokens.pluginInterface.useridmapping.exception.UnknownSuperTokensUserIdException;
import io.supertokens.pluginInterface.useridmapping.exception.UserIdMappingAlreadyExistsException;
import io.supertokens.pluginInterface.useridmapping.sqlStorage.UserIdMappingSQLStorage;
import io.supertokens.pluginInterface.usermetadata.UserMetadataStorage;
import io.supertokens.pluginInterface.usermetadata.sqlStorage.UserMetadataSQLStorage;
import io.supertokens.pluginInterface.userroles.UserRolesStorage;
import io.supertokens.pluginInterface.userroles.exception.DuplicateUserRoleMappingException;
import io.supertokens.pluginInterface.userroles.exception.UnknownRoleException;
import io.supertokens.pluginInterface.userroles.sqlStorage.UserRolesSQLStorage;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storage.postgresql.output.Logging;
import io.supertokens.storage.postgresql.queries.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class Start
        implements SessionSQLStorage, EmailPasswordSQLStorage, EmailVerificationSQLStorage, ThirdPartySQLStorage,
        JWTRecipeSQLStorage, PasswordlessSQLStorage, UserMetadataSQLStorage, UserRolesSQLStorage, UserIdMappingStorage,
        UserIdMappingSQLStorage, MultitenancyStorage, MultitenancySQLStorage, DashboardSQLStorage, TOTPSQLStorage,
        ActiveUsersStorage, ActiveUsersSQLStorage, AuthRecipeSQLStorage {

    // these configs are protected from being modified / viewed by the dev using the SuperTokens
    // SaaS. If the core is not running in SuperTokens SaaS, this array has no effect.
    private static String[] PROTECTED_DB_CONFIG = new String[]{"postgresql_connection_pool_size",
            "postgresql_connection_uri", "postgresql_host", "postgresql_port", "postgresql_user", "postgresql_password",
            "postgresql_database_name", "postgresql_table_schema"};
    private static final Object appenderLock = new Object();
    public static boolean silent = false;
    private ResourceDistributor resourceDistributor = new ResourceDistributor();
    private String processId;
    private HikariLoggingAppender appender;
    private static final String APP_ID_KEY_NAME = "app_id";
    private static final String ACCESS_TOKEN_SIGNING_KEY_NAME = "access_token_signing_key";
    private static final String REFRESH_TOKEN_KEY_NAME = "refresh_token_key";
    public static boolean isTesting = false;
    boolean enabled = true;
    static Thread mainThread = Thread.currentThread();
    private Thread shutdownHook;

    private boolean isBaseTenant = false;

    public ResourceDistributor getResourceDistributor() {
        return resourceDistributor;
    }

    public String getProcessId() {
        return this.processId;
    }

    @Override
    public void constructor(String processId, boolean silent, boolean isTesting) {
        this.processId = processId;
        Start.silent = silent;
        Start.isTesting = isTesting;
    }

    @Override
    public STORAGE_TYPE getType() {
        return STORAGE_TYPE.SQL;
    }

    @Override
    public void loadConfig(JsonObject configJson, Set<LOG_LEVEL> logLevels, TenantIdentifier tenantIdentifier)
            throws InvalidConfigException {
        Config.loadConfig(this, configJson, logLevels, tenantIdentifier);
    }

    @Override
    public String getUserPoolId() {
        return Config.getUserPoolId(this);
    }

    @Override
    public String getConnectionPoolId() {
        return Config.getConnectionPoolId(this);
    }

    @Override
    public void assertThatConfigFromSameUserPoolIsNotConflicting(JsonObject otherConfig) throws InvalidConfigException {
        Config.assertThatConfigFromSameUserPoolIsNotConflicting(this, otherConfig);
    }

    @Override
    public void initFileLogging(String infoLogPath, String errorLogPath) {
        if (Logging.isAlreadyInitialised(this)) {
            return;
        }

        synchronized (appenderLock) {
            Logging.initFileLogging(this, infoLogPath, errorLogPath);

            /*
             * NOTE: The log this produces is only accurate in production or development.
             *
             * For testing, it may happen that multiple processes are running at the same
             * time which can lead to one of them being the winner and its start instance
             * being attached to logger class. This would yield inaccurate processIds during
             * logging.
             *
             * Finally, during testing, the winner's logger might be removed, in which case
             * nothing will be handling logging and hikari's logs would not be outputed
             * anywhere.
             */
            final Logger infoLog = (Logger) LoggerFactory.getLogger("com.zaxxer.hikari");
            appender = new HikariLoggingAppender(this);
            if (infoLog.getAppender(HikariLoggingAppender.NAME) == null) {
                infoLog.setAdditive(false);
                infoLog.addAppender(appender);
            }
        }
    }

    @Override
    public void stopLogging() {
        if (isBaseTenant) {
            synchronized (appenderLock) {
                Logging.stopLogging(this);

                final Logger infoLog = (Logger) LoggerFactory.getLogger("com.zaxxer.hikari");
                if (infoLog.getAppender(HikariLoggingAppender.NAME) != null) {
                    infoLog.detachAppender(HikariLoggingAppender.NAME);
                }
            }
        }
    }

    @Override
    public void initStorage(boolean shouldWait) throws DbInitException {
        if (ConnectionPool.isAlreadyInitialised(this)) {
            return;
        }
        this.isBaseTenant = shouldWait;
        if (isBaseTenant) {
            // We are doing this so that the tests don't hang on to the first main thread
            mainThread = Thread.currentThread();
        }
        try {
            ConnectionPool.initPool(this, shouldWait);
            GeneralQueries.createTablesIfNotExists(this);
        } catch (Exception e) {
            throw new DbInitException(e);
        }
    }

    @Override
    public <T> T startTransaction(TransactionLogic<T> logic)
            throws StorageTransactionLogicException, StorageQueryException {
        return startTransaction(logic, TransactionIsolationLevel.SERIALIZABLE);
    }

    @Override
    public <T> T startTransaction(TransactionLogic<T> logic, TransactionIsolationLevel isolationLevel)
            throws StorageTransactionLogicException, StorageQueryException {
        final int NUM_TRIES = 50;
        int tries = 0;
        while (true) {
            tries++;
            try {
                return startTransactionHelper(logic, isolationLevel);
            } catch (SQLException | StorageQueryException | StorageTransactionLogicException e) {
                Throwable actualException = e;
                if (e instanceof StorageQueryException) {
                    actualException = e.getCause();
                } else if (e instanceof StorageTransactionLogicException) {
                    actualException = ((StorageTransactionLogicException) e).actualException;
                }
                String exceptionMessage = actualException.getMessage();
                if (exceptionMessage == null) {
                    exceptionMessage = "";
                }

                // see: https://github.com/supertokens/supertokens-postgresql-plugin/pull/3

                // We set this variable to the current (or cause) exception casted to
                // PSQLException if we can safely cast it
                PSQLException psqlException = actualException instanceof PSQLException ? (PSQLException) actualException
                        : null;

                // PSQL error class 40 is transaction rollback. See:
                // https://www.postgresql.org/docs/12/errcodes-appendix.html
                boolean isPSQLRollbackException = psqlException != null
                        && psqlException.getServerErrorMessage().getSQLState().startsWith("40");

                // We keep the old exception detection logic to ensure backwards compatibility.
                // We could get here if the new logic hits a false negative,
                // e.g., in case someone renamed constraints/tables
                boolean isDeadlockException = actualException instanceof SQLTransactionRollbackException
                        || exceptionMessage.toLowerCase().contains("concurrent update")
                        || exceptionMessage.toLowerCase().contains("concurrent delete")
                        || exceptionMessage.toLowerCase().contains("the transaction might succeed if retried") ||

                        // we have deadlock as well due to the DeadlockTest.java
                        exceptionMessage.toLowerCase().contains("deadlock");

                if ((isPSQLRollbackException || isDeadlockException) && tries < NUM_TRIES) {
                    try {
                        Thread.sleep((long) (10 + (250 + Math.min(Math.pow(2, tries), 3000)) * Math.random()));
                    } catch (InterruptedException ignored) {
                    }
                    ProcessState.getInstance(this).addState(ProcessState.PROCESS_STATE.DEADLOCK_FOUND, e);
                    // this because deadlocks are not necessarily a result of faulty logic. They can
                    // happen
                    continue;
                }

                if ((isPSQLRollbackException || isDeadlockException) && tries == NUM_TRIES) {
                    ProcessState.getInstance(this).addState(ProcessState.PROCESS_STATE.DEADLOCK_NOT_RESOLVED, e);
                }
                if (e instanceof StorageQueryException) {
                    throw (StorageQueryException) e;
                } else if (e instanceof StorageTransactionLogicException) {
                    throw (StorageTransactionLogicException) e;
                }
                throw new StorageQueryException(e);
            }
        }
    }

    private <T> T startTransactionHelper(TransactionLogic<T> logic, TransactionIsolationLevel isolationLevel)
            throws StorageQueryException, StorageTransactionLogicException, SQLException {
        Connection con = null;
        Integer defaultTransactionIsolation = null;
        try {
            con = ConnectionPool.getConnection(this);
            defaultTransactionIsolation = con.getTransactionIsolation();
            int libIsolationLevel = Connection.TRANSACTION_SERIALIZABLE;
            switch (isolationLevel) {
                case SERIALIZABLE:
                    libIsolationLevel = Connection.TRANSACTION_SERIALIZABLE;
                    break;
                case REPEATABLE_READ:
                    libIsolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
                    break;
                case READ_COMMITTED:
                    libIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
                    break;
                case READ_UNCOMMITTED:
                    libIsolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
                    break;
                case NONE:
                    libIsolationLevel = Connection.TRANSACTION_NONE;
                    break;
            }
            con.setTransactionIsolation(libIsolationLevel);
            con.setAutoCommit(false);
            return logic.mainLogicAndCommit(new TransactionConnection(con));
        } catch (Exception e) {
            if (con != null) {
                con.rollback();
            }
            throw e;
        } finally {
            if (con != null) {
                con.setAutoCommit(true);
                if (defaultTransactionIsolation != null) {
                    con.setTransactionIsolation(defaultTransactionIsolation);
                }
                con.close();
            }
        }
    }

    @Override
    public void commitTransaction(TransactionConnection con) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            sqlCon.commit();
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }

    }

    @Override
    public KeyValueInfo getLegacyAccessTokenSigningKey_Transaction(AppIdentifier appIdentifier,
                                                                   TransactionConnection con)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return GeneralQueries.getKeyValue_Transaction(this, sqlCon,
                    appIdentifier.getAsPublicTenantIdentifier(), ACCESS_TOKEN_SIGNING_KEY_NAME);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void removeLegacyAccessTokenSigningKey_Transaction(AppIdentifier appIdentifier,
                                                              TransactionConnection con) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            GeneralQueries.deleteKeyValue_Transaction(this, sqlCon,
                    appIdentifier.getAsPublicTenantIdentifier(), ACCESS_TOKEN_SIGNING_KEY_NAME);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public KeyValueInfo[] getAccessTokenSigningKeys_Transaction(AppIdentifier appIdentifier,
                                                                TransactionConnection con)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return SessionQueries.getAccessTokenSigningKeys_Transaction(this, sqlCon, appIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addAccessTokenSigningKey_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                     KeyValueInfo info)
            throws StorageQueryException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            SessionQueries.addAccessTokenSigningKey_Transaction(this, sqlCon, appIdentifier, info.createdAtTime,
                    info.value);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isForeignKeyConstraintError(serverMessage, config.getAccessTokenSigningKeysTable(), "app_id")) {
                    throw new TenantOrAppNotFoundException(appIdentifier);
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void removeAccessTokenSigningKeysBefore(AppIdentifier appIdentifier, long time)
            throws StorageQueryException {
        try {
            SessionQueries.removeAccessTokenSigningKeysBefore(this, appIdentifier, time);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public KeyValueInfo getRefreshTokenSigningKey_Transaction(AppIdentifier appIdentifier,
                                                              TransactionConnection con) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return GeneralQueries.getKeyValue_Transaction(this, sqlCon,
                    appIdentifier.getAsPublicTenantIdentifier(), REFRESH_TOKEN_KEY_NAME);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void setRefreshTokenSigningKey_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                      KeyValueInfo info)
            throws StorageQueryException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            GeneralQueries.setKeyValue_Transaction(this, sqlCon,
                    appIdentifier.getAsPublicTenantIdentifier(), REFRESH_TOKEN_KEY_NAME, info);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @TestOnly
    @Override
    public void deleteAllInformation() throws StorageQueryException {
        if (!isTesting) {
            throw new UnsupportedOperationException("This method is only for testing");
        }
        ProcessState.getInstance(this).clear();
        try {
            initStorage(false);
            enabled = true; // Allow get connection to work, to delete the data
            GeneralQueries.deleteAllTables(this);

            // had initStorage with false, so stop logging needs to be forced here
            isBaseTenant = true;
            stopLogging();
            close();
        } catch (SQLException e) {
            if (e.getCause() instanceof HikariPool.PoolInitializationException) {
                // this can happen if the db being connected to is not actually present.
                // So we ignore this since there are tests in which we are adding a non existent db for a tenant,
                // and we want to not throw errors in the next test wherein this function is called.
            } else {
                throw new StorageQueryException(e);
            }
        } catch (DbInitException e) {
            // ignore
        }
    }

    @Override
    public void close() {
        ConnectionPool.close(this);
    }

    @Override
    public void createNewSession(TenantIdentifier tenantIdentifier, String sessionHandle, String userId,
                                 String refreshTokenHash2,
                                 JsonObject userDataInDatabase, long expiry, JsonObject userDataInJWT,
                                 long createdAtTime, boolean useStaticKey)
            throws StorageQueryException, TenantOrAppNotFoundException {
        try {
            SessionQueries.createNewSession(this, tenantIdentifier, sessionHandle, userId, refreshTokenHash2,
                    userDataInDatabase, expiry, userDataInJWT, createdAtTime, useStaticKey);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isForeignKeyConstraintError(serverMessage, config.getSessionInfoTable(), "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteSessionsOfUser(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            SessionQueries.deleteSessionsOfUser(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean deleteSessionsOfUser(TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException {
        try {
            return SessionQueries.deleteSessionsOfUser(this, tenantIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int getNumberOfSessions(TenantIdentifier tenantIdentifier) throws StorageQueryException {
        try {
            return SessionQueries.getNumberOfSessions(this, tenantIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int deleteSession(TenantIdentifier tenantIdentifier, String[] sessionHandles) throws StorageQueryException {
        try {
            return SessionQueries.deleteSession(this, tenantIdentifier, sessionHandles);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String[] getAllNonExpiredSessionHandlesForUser(TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException {
        try {
            return SessionQueries.getAllNonExpiredSessionHandlesForUser(this, tenantIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    private String[] getAllNonExpiredSessionHandlesForUser(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            return SessionQueries.getAllNonExpiredSessionHandlesForUser(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteAllExpiredSessions() throws StorageQueryException {
        try {
            SessionQueries.deleteAllExpiredSessions(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public KeyValueInfo getKeyValue(TenantIdentifier tenantIdentifier, String key) throws StorageQueryException {
        try {
            return GeneralQueries.getKeyValue(this, tenantIdentifier, key);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void setKeyValue(TenantIdentifier tenantIdentifier, String key, KeyValueInfo info)
            throws StorageQueryException, TenantOrAppNotFoundException {
        try {
            GeneralQueries.setKeyValue(this, tenantIdentifier, key, info);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isForeignKeyConstraintError(serverMessage, config.getKeyValueTable(), "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void setStorageLayerEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public SessionInfo getSession(TenantIdentifier tenantIdentifier, String sessionHandle)
            throws StorageQueryException {
        try {
            return SessionQueries.getSession(this, tenantIdentifier, sessionHandle);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int updateSession(TenantIdentifier tenantIdentifier, String sessionHandle, JsonObject sessionData,
                             JsonObject jwtPayload)
            throws StorageQueryException {
        try {
            return SessionQueries.updateSession(this, tenantIdentifier, sessionHandle, sessionData, jwtPayload);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public SessionInfo getSessionInfo_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                  String sessionHandle)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return SessionQueries.getSessionInfo_Transaction(this, sqlCon, tenantIdentifier, sessionHandle);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateSessionInfo_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                              String sessionHandle, String refreshTokenHash2,
                                              long expiry) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            SessionQueries.updateSessionInfo_Transaction(this, sqlCon, tenantIdentifier, sessionHandle,
                    refreshTokenHash2, expiry);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteSessionsOfUser_Transaction(TransactionConnection con, AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            SessionQueries.deleteSessionsOfUser_Transaction(sqlCon, this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void setKeyValue_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con, String key,
                                        KeyValueInfo info)
            throws StorageQueryException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            GeneralQueries.setKeyValue_Transaction(this, sqlCon, tenantIdentifier, key, info);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isForeignKeyConstraintError(serverMessage, config.getKeyValueTable(), "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public KeyValueInfo getKeyValue_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                String key) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return GeneralQueries.getKeyValue_Transaction(this, sqlCon, tenantIdentifier, key);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    void removeShutdownHook() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
                shutdownHook = null;
            } catch (IllegalStateException ignored) {
            }
        }
    }

    void handleKillSignalForWhenItHappens() {
        if (shutdownHook != null) {
            return;
        }
        shutdownHook = new Thread(() -> {
            mainThread.interrupt();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    @Override
    public boolean canBeUsed(JsonObject configJson) throws InvalidConfigException {
        return Config.canBeUsed(configJson);
    }

    @Override
    public boolean isUserIdBeingUsedInNonAuthRecipe(AppIdentifier appIdentifier, String className, String userId)
            throws StorageQueryException {
        // check if the input userId is being used in nonAuthRecipes.
        if (className.equals(SessionStorage.class.getName())) {
            String[] sessionHandlesForUser = getAllNonExpiredSessionHandlesForUser(appIdentifier, userId);
            return sessionHandlesForUser.length > 0;
        } else if (className.equals(UserRolesStorage.class.getName())) {
            String[] roles = getRolesForUser(appIdentifier, userId);
            return roles.length > 0;
        } else if (className.equals(UserMetadataStorage.class.getName())) {
            JsonObject userMetadata = getUserMetadata(appIdentifier, userId);
            return userMetadata != null;
        } else if (className.equals(EmailVerificationStorage.class.getName())) {
            try {
                return EmailVerificationQueries.isUserIdBeingUsedForEmailVerification(this, appIdentifier, userId);
            } catch (SQLException e) {
                throw new StorageQueryException(e);
            }
        } else if (className.equals(TOTPStorage.class.getName())) {
            try {
                TOTPDevice[] devices = TOTPQueries.getDevices(this, appIdentifier, userId);
                return devices.length > 0;
            } catch (SQLException e) {
                throw new StorageQueryException(e);
            }
        } else if (className.equals(TOTPStorage.class.getName())) {
            try {
                TOTPDevice[] devices = TOTPQueries.getDevices(this, appIdentifier, userId);
                return devices.length > 0;
            } catch (SQLException e) {
                throw new StorageQueryException(e);
            }
        } else if (className.equals(JWTRecipeStorage.class.getName())) {
            return false;
        } else if (className.equals(ActiveUsersStorage.class.getName())) {
            return ActiveUsersQueries.getLastActiveByUserId(this, appIdentifier, userId) != null;
        } else {
            throw new IllegalStateException("ClassName: " + className + " is not part of NonAuthRecipeStorage");
        }
    }

    @TestOnly
    @Override
    public void addInfoToNonAuthRecipesBasedOnUserId(TenantIdentifier tenantIdentifier, String className, String userId)
            throws StorageQueryException {
        if (!isTesting) {
            throw new UnsupportedOperationException("This method is only for testing");
        }
        // add entries to nonAuthRecipe tables with input userId
        if (className.equals(SessionStorage.class.getName())) {
            try {
                createNewSession(tenantIdentifier, "sessionHandle", userId, "refreshTokenHash",
                        new JsonObject(),
                        System.currentTimeMillis() + 1000000, new JsonObject(), System.currentTimeMillis(), false);
            } catch (Exception e) {
                throw new StorageQueryException(e);
            }
        } else if (className.equals(UserRolesStorage.class.getName())) {
            try {
                String role = "testRole";
                this.startTransaction(con -> {
                    try {
                        createNewRoleOrDoNothingIfExists_Transaction(tenantIdentifier.toAppIdentifier(), con, role);
                    } catch (TenantOrAppNotFoundException e) {
                        throw new IllegalStateException(e);
                    }
                    return null;
                });
                try {
                    addRoleToUser(tenantIdentifier, userId, role);
                } catch (Exception e) {
                    throw new StorageTransactionLogicException(e);
                }
            } catch (StorageTransactionLogicException e) {
                throw new StorageQueryException(e.actualException);
            }
        } else if (className.equals(EmailVerificationStorage.class.getName())) {
            try {
                EmailVerificationTokenInfo info = new EmailVerificationTokenInfo(userId, "someToken", 10000,
                        "test123@example.com");
                addEmailVerificationToken(tenantIdentifier, info);

            } catch (DuplicateEmailVerificationTokenException e) {
                throw new StorageQueryException(e);
            } catch (TenantOrAppNotFoundException e) {
                throw new IllegalStateException(e);
            }
        } else if (className.equals(UserMetadataStorage.class.getName())) {
            JsonObject data = new JsonObject();
            data.addProperty("test", "testData");
            try {
                this.startTransaction(con -> {
                    try {
                        setUserMetadata_Transaction(tenantIdentifier.toAppIdentifier(), con, userId, data);
                    } catch (TenantOrAppNotFoundException e) {
                        throw new StorageTransactionLogicException(e);
                    }
                    return null;
                });
            } catch (StorageTransactionLogicException e) {
                if (e.actualException instanceof TenantOrAppNotFoundException) {
                    throw new IllegalStateException(e);
                }
                throw new StorageQueryException(e);
            }
        } else if (className.equals(TOTPStorage.class.getName())) {
            try {
                TOTPDevice device = new TOTPDevice(userId, "testDevice", "secret", 0, 30, false, System.currentTimeMillis());
                this.startTransaction(con -> {
                    try {
                        long now = System.currentTimeMillis();
                        Connection sqlCon = (Connection) con.getConnection();
                        TOTPQueries.createDevice_Transaction(this, sqlCon, tenantIdentifier.toAppIdentifier(), device);
                        TOTPQueries.insertUsedCode_Transaction(this,
                                sqlCon, tenantIdentifier,
                                new TOTPUsedCode(userId, "123456", true, 1000 + now, now));
                    } catch (SQLException e) {
                        throw new StorageTransactionLogicException(e);
                    }
                    return null;
                });

            } catch (StorageTransactionLogicException e) {
                throw new StorageQueryException(e.actualException);
            }
        } else if (className.equals(JWTRecipeStorage.class.getName())) {
            /* Since JWT recipe tables do not store userId we do not add any data to them */
        } else if (className.equals(ActiveUsersStorage.class.getName())) {
            try {
                ActiveUsersQueries.updateUserLastActive(this, tenantIdentifier.toAppIdentifier(), userId);
            } catch (SQLException e) {
                throw new StorageQueryException(e);
            }
        } else {
            throw new IllegalStateException("ClassName: " + className + " is not part of NonAuthRecipeStorage");
        }
    }

    @Override
    public void modifyConfigToAddANewUserPoolForTesting(JsonObject config, int poolNumber) {
        config.add("postgresql_database_name", new JsonPrimitive("st" + poolNumber));
    }

    @Override
    public String[] getProtectedConfigsFromSuperTokensSaaSUsers() {
        return PROTECTED_DB_CONFIG;
    }

    @Override
    public AuthRecipeUserInfo signUp(TenantIdentifier tenantIdentifier, String id, String email, String passwordHash,
                           long timeJoined)
            throws StorageQueryException, DuplicateUserIdException, DuplicateEmailException,
            TenantOrAppNotFoundException {
        try {
            return EmailPasswordQueries.signUp(this, tenantIdentifier, id, email, passwordHash, timeJoined);
        } catch (StorageTransactionLogicException eTemp) {
            Exception e = eTemp.actualException;
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isUniqueConstraintError(serverMessage, config.getEmailPasswordUserToTenantTable(), "email")) {
                    throw new DuplicateEmailException();
                } else if (isPrimaryKeyError(serverMessage, config.getEmailPasswordUsersTable())
                        || isPrimaryKeyError(serverMessage, config.getUsersTable())
                        || isPrimaryKeyError(serverMessage, config.getEmailPasswordUserToTenantTable())
                        || isPrimaryKeyError(serverMessage, config.getAppIdToUserIdTable())) {
                    throw new DuplicateUserIdException();
                } else if (isForeignKeyConstraintError(serverMessage, config.getEmailPasswordUsersTable(), "user_id")) {
                    // This should never happen because we add the user to app_id_to_user_id table first
                    throw new IllegalStateException("should never come here");
                } else if (isForeignKeyConstraintError(serverMessage, config.getAppIdToUserIdTable(), "app_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier.toAppIdentifier());
                } else if (isForeignKeyConstraintError(serverMessage, config.getUsersTable(), "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);
                }
            }

            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteEmailPasswordUser_Transaction(TransactionConnection con, AppIdentifier appIdentifier,
                                                    String userId, boolean deleteUserIdMappingToo)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            EmailPasswordQueries.deleteUser_Transaction(sqlCon, this, appIdentifier, userId, deleteUserIdMappingToo);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addPasswordResetToken(AppIdentifier appIdentifier, PasswordResetTokenInfo passwordResetTokenInfo)
            throws StorageQueryException, UnknownUserIdException, DuplicatePasswordResetTokenException {
        try {
            EmailPasswordQueries.addPasswordResetToken(this, appIdentifier, passwordResetTokenInfo.userId,
                    passwordResetTokenInfo.token, passwordResetTokenInfo.tokenExpiry, passwordResetTokenInfo.email);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isPrimaryKeyError(serverMessage, Config.getConfig(this).getPasswordResetTokensTable())) {
                    throw new DuplicatePasswordResetTokenException();
                } else if (isForeignKeyConstraintError(serverMessage,
                        Config.getConfig(this).getPasswordResetTokensTable(), "user_id")) {
                    throw new UnknownUserIdException();
                }
            }

            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordResetTokenInfo getPasswordResetTokenInfo(AppIdentifier appIdentifier, String token)
            throws StorageQueryException {
        try {
            return EmailPasswordQueries.getPasswordResetTokenInfo(this, appIdentifier, token);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordResetTokenInfo[] getAllPasswordResetTokenInfoForUser(AppIdentifier appIdentifier,
                                                                        String userId) throws StorageQueryException {
        try {
            return EmailPasswordQueries.getAllPasswordResetTokenInfoForUser(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordResetTokenInfo[] getAllPasswordResetTokenInfoForUser_Transaction(AppIdentifier appIdentifier,
                                                                                    TransactionConnection con,
                                                                                    String userId)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return EmailPasswordQueries.getAllPasswordResetTokenInfoForUser_Transaction(this, sqlCon, appIdentifier,
                    userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteAllPasswordResetTokensForUser_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                                String userId)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            EmailPasswordQueries.deleteAllPasswordResetTokensForUser_Transaction(this, sqlCon, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateUsersPassword_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId,
                                                String newPassword)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            EmailPasswordQueries.updateUsersPassword_Transaction(this, sqlCon, appIdentifier, userId, newPassword);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateUsersEmail_Transaction(AppIdentifier appIdentifier, TransactionConnection conn, String userId,
                                             String email)
            throws StorageQueryException, DuplicateEmailException {
        Connection sqlCon = (Connection) conn.getConnection();
        try {
            EmailPasswordQueries.updateUsersEmail_Transaction(this, sqlCon, appIdentifier, userId, email);
        } catch (SQLException e) {
            if (e instanceof PSQLException && isUniqueConstraintError(((PSQLException) e).getServerErrorMessage(),
                    Config.getConfig(this).getEmailPasswordUserToTenantTable(), "email")) {
                throw new DuplicateEmailException();
            }

            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteExpiredEmailVerificationTokens() throws StorageQueryException {
        try {
            EmailVerificationQueries.deleteExpiredEmailVerificationTokens(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public EmailVerificationTokenInfo[] getAllEmailVerificationTokenInfoForUser_Transaction(
            TenantIdentifier tenantIdentifier,
            TransactionConnection con,
            String userId, String email)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return EmailVerificationQueries.getAllEmailVerificationTokenInfoForUser_Transaction(this, sqlCon,
                    tenantIdentifier, userId, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteAllEmailVerificationTokensForUser_Transaction(TenantIdentifier tenantIdentifier,
                                                                    TransactionConnection con, String userId,
                                                                    String email) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            EmailVerificationQueries.deleteAllEmailVerificationTokensForUser_Transaction(this, sqlCon, tenantIdentifier,
                    userId, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateIsEmailVerified_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId,
                                                  String email,
                                                  boolean isEmailVerified)
            throws StorageQueryException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            EmailVerificationQueries.updateUsersIsEmailVerified_Transaction(this, sqlCon, appIdentifier, userId,
                    email,
                    isEmailVerified);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isForeignKeyConstraintError(serverMessage, config.getEmailVerificationTable(), "app_id")) {
                    throw new TenantOrAppNotFoundException(appIdentifier);
                }
            }

            boolean isPSQLPrimKeyError = e instanceof PSQLException && isPrimaryKeyError(
                    ((PSQLException) e).getServerErrorMessage(),
                    Config.getConfig(this).getEmailVerificationTable());

            if (!isEmailVerified || !isPSQLPrimKeyError) {
                throw new StorageQueryException(e);
            }
            // we do not throw an error since the email is already verified
        }
    }

    @Override
    public void deleteEmailVerificationUserInfo_Transaction(TransactionConnection con, AppIdentifier appIdentifier,
                                                            String userId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            EmailVerificationQueries.deleteUserInfo_Transaction(sqlCon, this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean deleteEmailVerificationUserInfo(TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException {
        try {
            return EmailVerificationQueries.deleteUserInfo(this, tenantIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addEmailVerificationToken(TenantIdentifier tenantIdentifier,
                                          EmailVerificationTokenInfo emailVerificationInfo)
            throws StorageQueryException, DuplicateEmailVerificationTokenException, TenantOrAppNotFoundException {
        try {
            EmailVerificationQueries.addEmailVerificationToken(this, tenantIdentifier, emailVerificationInfo.userId,
                    emailVerificationInfo.token, emailVerificationInfo.tokenExpiry, emailVerificationInfo.email);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isPrimaryKeyError(serverMessage, config.getEmailVerificationTokensTable())) {
                    throw new DuplicateEmailVerificationTokenException();
                }

                if (isForeignKeyConstraintError(serverMessage, config.getEmailVerificationTokensTable(),
                        "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);
                }
            }

            throw new StorageQueryException(e);
        }
    }

    @Override
    public EmailVerificationTokenInfo getEmailVerificationTokenInfo(TenantIdentifier tenantIdentifier, String token)
            throws StorageQueryException {
        try {
            return EmailVerificationQueries.getEmailVerificationTokenInfo(this, tenantIdentifier, token);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void revokeAllTokens(TenantIdentifier tenantIdentifier, String userId, String email) throws
            StorageQueryException {
        try {
            EmailVerificationQueries.revokeAllTokens(this, tenantIdentifier, userId, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void unverifyEmail(AppIdentifier appIdentifier, String userId, String email) throws StorageQueryException {
        try {
            EmailVerificationQueries.unverifyEmail(this, appIdentifier, userId, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public EmailVerificationTokenInfo[] getAllEmailVerificationTokenInfoForUser(TenantIdentifier
                                                                                        tenantIdentifier,
                                                                                String userId, String email)
            throws StorageQueryException {
        try {
            return EmailVerificationQueries.getAllEmailVerificationTokenInfoForUser(this, tenantIdentifier, userId,
                    email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean isEmailVerified(AppIdentifier appIdentifier, String userId, String email)
            throws StorageQueryException {
        try {
            return EmailVerificationQueries.isEmailVerified(this, appIdentifier, userId, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateIsEmailVerifiedToExternalUserId(AppIdentifier appIdentifier, String supertokensUserId, String externalUserId) throws StorageQueryException {
        EmailVerificationQueries.updateIsEmailVerifiedToExternalUserId(this, appIdentifier, supertokensUserId, externalUserId);
    }

    @Override
    public void deleteExpiredPasswordResetTokens() throws StorageQueryException {
        try {
            EmailPasswordQueries.deleteExpiredPasswordResetTokens(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateUserEmail_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                            String thirdPartyId, String thirdPartyUserId,
                                            String newEmail) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            ThirdPartyQueries.updateUserEmail_Transaction(this, sqlCon, appIdentifier, thirdPartyId,
                    thirdPartyUserId, newEmail);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo signUp(
            TenantIdentifier tenantIdentifier, String id, String email,
            LoginMethod.ThirdParty thirdParty, long timeJoined)
            throws StorageQueryException, io.supertokens.pluginInterface.thirdparty.exception.DuplicateUserIdException,
            DuplicateThirdPartyUserException, TenantOrAppNotFoundException {
        try {
            return ThirdPartyQueries.signUp(this, tenantIdentifier, id, email, thirdParty, timeJoined);
        } catch (StorageTransactionLogicException eTemp) {
            Exception e = eTemp.actualException;
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isUniqueConstraintError(serverMessage, config.getThirdPartyUserToTenantTable(),
                        "third_party_user_id")) {
                    throw new DuplicateThirdPartyUserException();

                } else if (isPrimaryKeyError(serverMessage, config.getThirdPartyUsersTable())
                        || isPrimaryKeyError(serverMessage, config.getUsersTable())
                        || isPrimaryKeyError(serverMessage, config.getThirdPartyUserToTenantTable())
                        || isPrimaryKeyError(serverMessage, config.getAppIdToUserIdTable())) {
                    throw new io.supertokens.pluginInterface.thirdparty.exception.DuplicateUserIdException();

                } else if (isForeignKeyConstraintError(serverMessage, config.getThirdPartyUsersTable(), "user_id")) {
                    // This should never happen because we add the user to app_id_to_user_id table first
                    throw new IllegalStateException("should never come here");

                } else if (isForeignKeyConstraintError(serverMessage, config.getAppIdToUserIdTable(), "app_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier.toAppIdentifier());

                } else if (isForeignKeyConstraintError(serverMessage, config.getUsersTable(), "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);

                }


            }

            throw new StorageQueryException(eTemp.actualException);
        }
    }

    @Override
    public void deleteThirdPartyUser_Transaction(TransactionConnection con, AppIdentifier appIdentifier, String userId,
                                                 boolean deleteUserIdMappingToo)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            ThirdPartyQueries.deleteUser_Transaction(sqlCon, this, appIdentifier, userId, deleteUserIdMappingToo);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public long getUsersCount(TenantIdentifier tenantIdentifier, RECIPE_ID[] includeRecipeIds)
            throws StorageQueryException {
        try {
            return GeneralQueries.getUsersCount(this, tenantIdentifier, includeRecipeIds);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public long getUsersCount(AppIdentifier appIdentifier, RECIPE_ID[] includeRecipeIds)
            throws StorageQueryException {
        try {
            return GeneralQueries.getUsersCount(this, appIdentifier, includeRecipeIds);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo[] getUsers(TenantIdentifier tenantIdentifier, @NotNull Integer limit,
                                         @NotNull String timeJoinedOrder,
                                         @Nullable RECIPE_ID[] includeRecipeIds, @Nullable String userId,
                                         @Nullable Long timeJoined, @Nullable DashboardSearchTags dashboardSearchTags)
            throws StorageQueryException {
        try {
            return GeneralQueries.getUsers(this, tenantIdentifier, limit, timeJoinedOrder, includeRecipeIds, userId,
                    timeJoined, dashboardSearchTags);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean doesUserIdExist(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        try {
            return GeneralQueries.doesUserIdExist(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateLastActive(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        try {
            ActiveUsersQueries.updateUserLastActive(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int countUsersActiveSince(AppIdentifier appIdentifier, long time) throws StorageQueryException {
        try {
            return ActiveUsersQueries.countUsersActiveSince(this, appIdentifier, time);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteUserActive_Transaction(TransactionConnection con, AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            ActiveUsersQueries.deleteUserActive_Transaction(sqlCon, this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean doesUserIdExist(TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException {
        try {
            return GeneralQueries.doesUserIdExist(this, tenantIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo getPrimaryUserById(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            return GeneralQueries.getPrimaryUserInfoForUserId(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String getPrimaryUserIdStrForUserId(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            return GeneralQueries.getPrimaryUserIdStrForUserId(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo[] listPrimaryUsersByEmail(TenantIdentifier tenantIdentifier, String email)
            throws StorageQueryException {
        try {
            return GeneralQueries.listPrimaryUsersByEmail(this, tenantIdentifier, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo[] listPrimaryUsersByPhoneNumber(TenantIdentifier tenantIdentifier, String phoneNumber)
            throws StorageQueryException {
        try {
            return GeneralQueries.listPrimaryUsersByPhoneNumber(this, tenantIdentifier, phoneNumber);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo getPrimaryUserByThirdPartyInfo(TenantIdentifier tenantIdentifier, String thirdPartyId,
                                                             String thirdPartyUserId) throws StorageQueryException {
        try {
            return GeneralQueries.getPrimaryUserByThirdPartyInfo(this, tenantIdentifier, thirdPartyId,
                    thirdPartyUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public List<JWTSigningKeyInfo> getJWTSigningKeys_Transaction(AppIdentifier
                                                                         appIdentifier, TransactionConnection con)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return JWTSigningQueries.getJWTSigningKeys_Transaction(this, sqlCon, appIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void setJWTSigningKey_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                             JWTSigningKeyInfo info)
            throws StorageQueryException, DuplicateKeyIdException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            JWTSigningQueries.setJWTSigningKeyInfo_Transaction(this, sqlCon, appIdentifier, info);
        } catch (SQLException e) {

            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isPrimaryKeyError(serverMessage, config.getJWTSigningKeysTable())) {
                    throw new DuplicateKeyIdException();
                }

                if (isForeignKeyConstraintError(serverMessage, config.getJWTSigningKeysTable(), "app_id")) {
                    throw new TenantOrAppNotFoundException(appIdentifier);
                }
            }

            throw new StorageQueryException(e);
        }
    }

    private boolean isUniqueConstraintError(ServerErrorMessage serverMessage, String tableName, String
            columnName) {
        String[] tableNameParts = tableName.split("\\.");
        tableName = tableNameParts[tableNameParts.length - 1];

        return serverMessage.getSQLState().equals("23505") && serverMessage.getConstraint() != null
                && serverMessage.getConstraint().equals(tableName + "_" + columnName + "_key");
    }

    private boolean isForeignKeyConstraintError(ServerErrorMessage serverMessage, String tableName, String
            columnName) {
        String[] tableNameParts = tableName.split("\\.");
        tableName = tableNameParts[tableNameParts.length - 1];
        return serverMessage.getSQLState().equals("23503") && serverMessage.getConstraint() != null
                && serverMessage.getConstraint().equals(tableName + "_" + columnName + "_fkey");
    }

    private boolean isPrimaryKeyError(ServerErrorMessage serverMessage, String tableName) {
        String[] tableNameParts = tableName.split("\\.");
        tableName = tableNameParts[tableNameParts.length - 1];
        return serverMessage.getSQLState().equals("23505") && serverMessage.getConstraint() != null
                && serverMessage.getConstraint().equals(tableName + "_pkey");
    }

    @Override
    public PasswordlessDevice getDevice_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection
            con,
                                                    String deviceIdHash)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return PasswordlessQueries.getDevice_Transaction(this, sqlCon, tenantIdentifier, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void incrementDeviceFailedAttemptCount_Transaction(TenantIdentifier tenantIdentifier,
                                                              TransactionConnection con, String deviceIdHash)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.incrementDeviceFailedAttemptCount_Transaction(this, sqlCon, tenantIdentifier,
                    deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }

    }

    @Override
    public PasswordlessCode[] getCodesOfDevice_Transaction(TenantIdentifier
                                                                   tenantIdentifier, TransactionConnection con,
                                                           String deviceIdHash)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return PasswordlessQueries.getCodesOfDevice_Transaction(this, sqlCon, tenantIdentifier, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteDevice_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                         String deviceIdHash) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteDevice_Transaction(this, sqlCon, tenantIdentifier, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }

    }

    @Override
    public void deleteDevicesByPhoneNumber_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection
            con,
                                                       @Nonnull String phoneNumber)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteDevicesByPhoneNumber_Transaction(this, sqlCon, tenantIdentifier, phoneNumber);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteDevicesByEmail_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                 @Nonnull String email)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteDevicesByEmail_Transaction(this, sqlCon, tenantIdentifier, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteDevicesByPhoneNumber_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                       String phoneNumber, String userId) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteDevicesByPhoneNumber_Transaction(this, sqlCon, appIdentifier, phoneNumber,
                    userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteDevicesByEmail_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String
            email,
                                                 String userId) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteDevicesByEmail_Transaction(this, sqlCon, appIdentifier, email, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessCode getCodeByLinkCodeHash_Transaction(TenantIdentifier tenantIdentifier,
                                                              TransactionConnection con, String linkCodeHash)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return PasswordlessQueries.getCodeByLinkCodeHash_Transaction(this, sqlCon, tenantIdentifier,
                    linkCodeHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteCode_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                       String deviceIdHash) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteCode_Transaction(this, sqlCon, tenantIdentifier, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateUserEmail_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId,
                                            String email)
            throws StorageQueryException, UnknownUserIdException, DuplicateEmailException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            int updated_rows = PasswordlessQueries.updateUserEmail_Transaction(this, sqlCon, appIdentifier, userId,
                    email);
            if (updated_rows != 1) {
                throw new UnknownUserIdException();
            }
        } catch (SQLException e) {

            if (e instanceof PSQLException) {
                if (isUniqueConstraintError(((PSQLException) e).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessUserToTenantTable(), "email")) {
                    throw new DuplicateEmailException();

                }
            }
            throw new StorageQueryException(e);

        }
    }

    @Override
    public void updateUserPhoneNumber_Transaction(AppIdentifier appIdentifier, TransactionConnection
            con, String userId, String phoneNumber)
            throws StorageQueryException, UnknownUserIdException, DuplicatePhoneNumberException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            int updated_rows = PasswordlessQueries.updateUserPhoneNumber_Transaction(this, sqlCon, appIdentifier,
                    userId,
                    phoneNumber);

            if (updated_rows != 1) {
                throw new UnknownUserIdException();
            }

        } catch (SQLException e) {

            if (e instanceof PSQLException) {
                if (isUniqueConstraintError(((PSQLException) e).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessUserToTenantTable(), "phone_number")) {
                    throw new DuplicatePhoneNumberException();

                }
            }

            throw new StorageQueryException(e);
        }
    }

    @Override
    public void createDeviceWithCode(TenantIdentifier tenantIdentifier, @Nullable String email,
                                     @Nullable String phoneNumber, @NotNull String linkCodeSalt,
                                     PasswordlessCode code)
            throws StorageQueryException, DuplicateDeviceIdHashException,
            DuplicateCodeIdException, DuplicateLinkCodeHashException, TenantOrAppNotFoundException {
        if (email == null && phoneNumber == null) {
            throw new IllegalArgumentException("Both email and phoneNumber can't be null");
        }
        try {
            PasswordlessQueries.createDeviceWithCode(this, tenantIdentifier, email, phoneNumber, linkCodeSalt,
                    code);
        } catch (StorageTransactionLogicException e) {
            Exception actualException = e.actualException;

            if (actualException instanceof PSQLException) {
                if (isPrimaryKeyError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessDevicesTable())) {
                    throw new DuplicateDeviceIdHashException();
                }
                if (isPrimaryKeyError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessCodesTable())) {
                    throw new DuplicateCodeIdException();
                }
                if (isUniqueConstraintError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessCodesTable(), "link_code_hash")) {
                    throw new DuplicateLinkCodeHashException();
                }
                if (isForeignKeyConstraintError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessDevicesTable(), "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);
                }
            }

            throw new StorageQueryException(e);
        }
    }

    @Override
    public void createCode(TenantIdentifier tenantIdentifier, PasswordlessCode code)
            throws StorageQueryException, UnknownDeviceIdHash,
            DuplicateCodeIdException, DuplicateLinkCodeHashException {
        try {
            PasswordlessQueries.createCode(this, tenantIdentifier, code);
        } catch (StorageTransactionLogicException e) {

            Exception actualException = e.actualException;

            if (actualException instanceof PSQLException) {
                if (isForeignKeyConstraintError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessCodesTable(), "device_id_hash")) {
                    throw new UnknownDeviceIdHash();
                }
                if (isPrimaryKeyError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessCodesTable())) {
                    throw new DuplicateCodeIdException();
                }
                if (isUniqueConstraintError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessCodesTable(), "link_code_hash")) {
                    throw new DuplicateLinkCodeHashException();
                }
            }
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public AuthRecipeUserInfo createUser(TenantIdentifier tenantIdentifier,
                                                                           String id,
                                                                           @javax.annotation.Nullable String email,
                                                                           @javax.annotation.Nullable
                                                                           String phoneNumber, long timeJoined)
            throws StorageQueryException,
            DuplicateEmailException, DuplicatePhoneNumberException, DuplicateUserIdException,
            TenantOrAppNotFoundException {
        if (email == null && phoneNumber == null) {
            throw new IllegalArgumentException("Both email and phoneNumber cannot be null");
        }

        try {
            return PasswordlessQueries.createUser(this, tenantIdentifier, id, email, phoneNumber, timeJoined);
        } catch (StorageTransactionLogicException e) {

            Exception actualException = e.actualException;

            if (actualException instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) actualException).getServerErrorMessage();

                if (isPrimaryKeyError(serverMessage, config.getPasswordlessUsersTable())
                        || isPrimaryKeyError(serverMessage, config.getUsersTable())
                        || isPrimaryKeyError(serverMessage, config.getPasswordlessUserToTenantTable())
                        || isPrimaryKeyError(serverMessage, config.getAppIdToUserIdTable())) {
                    throw new DuplicateUserIdException();
                }

                if (isUniqueConstraintError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessUserToTenantTable(), "email")) {
                    throw new DuplicateEmailException();
                }

                if (isUniqueConstraintError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessUserToTenantTable(), "phone_number")) {
                    throw new DuplicatePhoneNumberException();
                }

                if (isForeignKeyConstraintError(serverMessage, config.getPasswordlessUsersTable(), "user_id")) {
                    // This should never happen because we add the user to app_id_to_user_id table first
                    throw new IllegalStateException("should never come here");
                }

                if (isForeignKeyConstraintError(serverMessage, config.getAppIdToUserIdTable(), "app_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier.toAppIdentifier());
                }

                if (isForeignKeyConstraintError(serverMessage, config.getUsersTable(), "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);
                }

            }
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public void deletePasswordlessUser_Transaction(TransactionConnection con, AppIdentifier appIdentifier,
                                                   String userId, boolean deleteUserIdMappingToo) throws
            StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            PasswordlessQueries.deleteUser_Transaction(sqlCon, this, appIdentifier, userId, deleteUserIdMappingToo);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessDevice getDevice(TenantIdentifier tenantIdentifier, String deviceIdHash)
            throws StorageQueryException {
        try {
            return PasswordlessQueries.getDevice(this, tenantIdentifier, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessDevice[] getDevicesByEmail(TenantIdentifier tenantIdentifier, String email)
            throws StorageQueryException {
        try {
            return PasswordlessQueries.getDevicesByEmail(this, tenantIdentifier, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessDevice[] getDevicesByPhoneNumber(TenantIdentifier tenantIdentifier, String
            phoneNumber)
            throws StorageQueryException {
        try {
            return PasswordlessQueries.getDevicesByPhoneNumber(this, tenantIdentifier, phoneNumber);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessCode[] getCodesOfDevice(TenantIdentifier tenantIdentifier, String deviceIdHash)
            throws StorageQueryException {
        try {
            return PasswordlessQueries.getCodesOfDevice(this, tenantIdentifier, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessCode[] getCodesBefore(TenantIdentifier tenantIdentifier, long time)
            throws StorageQueryException {
        try {
            return PasswordlessQueries.getCodesBefore(this, tenantIdentifier, time);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessCode getCode(TenantIdentifier tenantIdentifier, String codeId) throws
            StorageQueryException {
        try {
            return PasswordlessQueries.getCode(this, tenantIdentifier, codeId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessCode getCodeByLinkCodeHash(TenantIdentifier tenantIdentifier, String linkCodeHash)
            throws StorageQueryException {
        try {
            return PasswordlessQueries.getCodeByLinkCodeHash(this, tenantIdentifier, linkCodeHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public JsonObject getUserMetadata(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        try {
            return UserMetadataQueries.getUserMetadata(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public JsonObject getUserMetadata_Transaction(AppIdentifier appIdentifier, TransactionConnection
            con, String userId)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserMetadataQueries.getUserMetadata_Transaction(this, sqlCon, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int setUserMetadata_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId,
                                           JsonObject metadata)
            throws StorageQueryException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserMetadataQueries.setUserMetadata_Transaction(this, sqlCon, appIdentifier, userId,
                    metadata);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isForeignKeyConstraintError(serverMessage, config.getUserMetadataTable(), "app_id")) {
                    throw new TenantOrAppNotFoundException(appIdentifier);
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int deleteUserMetadata_Transaction(TransactionConnection con, AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return UserMetadataQueries.deleteUserMetadata_Transaction(sqlCon, this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int deleteUserMetadata(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        try {
            return UserMetadataQueries.deleteUserMetadata(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addRoleToUser(TenantIdentifier tenantIdentifier, String userId, String role)
            throws StorageQueryException, UnknownRoleException, DuplicateUserRoleMappingException,
            TenantOrAppNotFoundException {
        try {
            UserRolesQueries.addRoleToUser(this, tenantIdentifier, userId, role);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverErrorMessage = ((PSQLException) e).getServerErrorMessage();
                if (isForeignKeyConstraintError(serverErrorMessage, config.getUserRolesTable(), "role")) {
                    throw new UnknownRoleException();
                }
                if (isPrimaryKeyError(serverErrorMessage, config.getUserRolesTable())) {
                    throw new DuplicateUserRoleMappingException();
                }
                if (isForeignKeyConstraintError(serverErrorMessage, config.getUserRolesTable(),
                        "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);
                }
            }
            throw new StorageQueryException(e);
        }

    }

    @Override
    public String[] getRolesForUser(TenantIdentifier tenantIdentifier, String userId) throws
            StorageQueryException {
        try {
            return UserRolesQueries.getRolesForUser(this, tenantIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    private String[] getRolesForUser(AppIdentifier appIdentifier, String userId) throws
            StorageQueryException {
        try {
            return UserRolesQueries.getRolesForUser(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String[] getUsersForRole(TenantIdentifier tenantIdentifier, String role) throws
            StorageQueryException {
        try {
            return UserRolesQueries.getUsersForRole(this, tenantIdentifier, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String[] getPermissionsForRole(AppIdentifier appIdentifier, String role) throws
            StorageQueryException {
        try {
            return UserRolesQueries.getPermissionsForRole(this, appIdentifier, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String[] getRolesThatHavePermission(AppIdentifier appIdentifier, String permission)
            throws StorageQueryException {
        try {
            return UserRolesQueries.getRolesThatHavePermission(this, appIdentifier, permission);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean deleteRole(AppIdentifier appIdentifier, String role) throws StorageQueryException {
        try {
            return UserRolesQueries.deleteRole(this, appIdentifier, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String[] getRoles(AppIdentifier appIdentifier) throws StorageQueryException {
        try {
            return UserRolesQueries.getRoles(this, appIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean doesRoleExist(AppIdentifier appIdentifier, String role) throws StorageQueryException {
        try {
            return UserRolesQueries.doesRoleExist(this, appIdentifier, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int deleteAllRolesForUser(TenantIdentifier tenantIdentifier, String userId) throws
            StorageQueryException {
        try {
            return UserRolesQueries.deleteAllRolesForUser(this, tenantIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteAllRolesForUser_Transaction(TransactionConnection con, AppIdentifier appIdentifier, String userId)
            throws
            StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            UserRolesQueries.deleteAllRolesForUser_Transaction(sqlCon, this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean deleteRoleForUser_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                 String userId, String role)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();

        try {
            return UserRolesQueries.deleteRoleForUser_Transaction(this, sqlCon, tenantIdentifier, userId,
                    role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean createNewRoleOrDoNothingIfExists_Transaction(AppIdentifier appIdentifier,
                                                                TransactionConnection con, String role)
            throws StorageQueryException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();

        try {
            return UserRolesQueries.createNewRoleOrDoNothingIfExists_Transaction(
                    this, sqlCon, appIdentifier, role);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isForeignKeyConstraintError(serverMessage, config.getRolesTable(), "app_id")) {
                    throw new TenantOrAppNotFoundException(appIdentifier);
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addPermissionToRoleOrDoNothingIfExists_Transaction(AppIdentifier appIdentifier,
                                                                   TransactionConnection con, String role,
                                                                   String permission)
            throws StorageQueryException, UnknownRoleException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            UserRolesQueries.addPermissionToRoleOrDoNothingIfExists_Transaction(this, sqlCon, appIdentifier,
                    role, permission);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverErrorMessage = ((PSQLException) e).getServerErrorMessage();
                if (isForeignKeyConstraintError(serverErrorMessage, config.getUserRolesPermissionsTable(),
                        "role")) {
                    throw new UnknownRoleException();
                }
            }

            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean deletePermissionForRole_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                       String role, String permission)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserRolesQueries.deletePermissionForRole_Transaction(this, sqlCon, appIdentifier, role,
                    permission);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int deleteAllPermissionsForRole_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                       String role)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserRolesQueries.deleteAllPermissionsForRole_Transaction(this, sqlCon, appIdentifier,
                    role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean doesRoleExist_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String role)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserRolesQueries.doesRoleExist_transaction(this, sqlCon, appIdentifier, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void createUserIdMapping(AppIdentifier appIdentifier, String superTokensUserId, String externalUserId,
                                    @org.jetbrains.annotations.Nullable String externalUserIdInfo)
            throws StorageQueryException, UnknownSuperTokensUserIdException, UserIdMappingAlreadyExistsException {
        try {
            UserIdMappingQueries.createUserIdMapping(this, appIdentifier, superTokensUserId, externalUserId,
                    externalUserIdInfo);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverErrorMessage = ((PSQLException) e).getServerErrorMessage();
                if (isForeignKeyConstraintError(serverErrorMessage, config.getUserIdMappingTable(),
                        "supertokens_user_id")) {
                    throw new UnknownSuperTokensUserIdException();
                }

                if (isPrimaryKeyError(serverErrorMessage, config.getUserIdMappingTable())) {
                    throw new UserIdMappingAlreadyExistsException(true, true);
                }

                if (isUniqueConstraintError(serverErrorMessage, config.getUserIdMappingTable(),
                        "supertokens_user_id")) {
                    throw new UserIdMappingAlreadyExistsException(true, false);
                }

                if (isUniqueConstraintError(serverErrorMessage, config.getUserIdMappingTable(),
                        "external_user_id")) {
                    throw new UserIdMappingAlreadyExistsException(false, true);
                }
            }
            throw new StorageQueryException(e);
        }

    }

    @Override
    public boolean deleteUserIdMapping(AppIdentifier appIdentifier, String userId, boolean isSuperTokensUserId)
            throws StorageQueryException {
        try {
            if (isSuperTokensUserId) {
                return UserIdMappingQueries.deleteUserIdMappingWithSuperTokensUserId(this, appIdentifier,
                        userId);
            }

            return UserIdMappingQueries.deleteUserIdMappingWithExternalUserId(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public UserIdMapping getUserIdMapping(AppIdentifier appIdentifier, String userId, boolean isSuperTokensUserId)
            throws StorageQueryException {
        try {
            if (isSuperTokensUserId) {
                return UserIdMappingQueries.getuseraIdMappingWithSuperTokensUserId(this, appIdentifier,
                        userId);
            }

            return UserIdMappingQueries.getUserIdMappingWithExternalUserId(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public UserIdMapping[] getUserIdMapping(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            return UserIdMappingQueries.getUserIdMappingWithEitherSuperTokensUserIdOrExternalUserId(this,
                    appIdentifier,
                    userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean updateOrDeleteExternalUserIdInfo(AppIdentifier appIdentifier, String userId,
                                                    boolean isSuperTokensUserId,
                                                    @Nullable String externalUserIdInfo) throws StorageQueryException {

        try {
            if (isSuperTokensUserId) {
                return UserIdMappingQueries.updateOrDeleteExternalUserIdInfoWithSuperTokensUserId(this,
                        appIdentifier, userId, externalUserIdInfo);
            }

            return UserIdMappingQueries.updateOrDeleteExternalUserIdInfoWithExternalUserId(this,
                    appIdentifier, userId, externalUserIdInfo);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public HashMap<String, String> getUserIdMappingForSuperTokensIds(ArrayList<String> userIds)
            throws StorageQueryException {
        try {
            return UserIdMappingQueries.getUserIdMappingWithUserIds(this, userIds);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void createTenant(TenantConfig tenantConfig)
            throws DuplicateTenantException, StorageQueryException, DuplicateThirdPartyIdException,
            DuplicateClientTypeException {
        try {
            MultitenancyQueries.createTenantConfig(this, tenantConfig);
        } catch (StorageTransactionLogicException e) {
            if (e.actualException instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                if (isPrimaryKeyError(((PSQLException) e.actualException).getServerErrorMessage(),
                        config.getTenantConfigsTable())) {
                    throw new DuplicateTenantException();
                }
                if (isPrimaryKeyError(((PSQLException) e.actualException).getServerErrorMessage(),
                        config.getTenantThirdPartyProvidersTable())) {
                    throw new DuplicateThirdPartyIdException();
                }
                if (isPrimaryKeyError(((PSQLException) e.actualException).getServerErrorMessage(),
                        config.getTenantThirdPartyProviderClientsTable())) {
                    throw new DuplicateClientTypeException();
                }
            }
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public void addTenantIdInTargetStorage(TenantIdentifier tenantIdentifier)
            throws DuplicateTenantException, StorageQueryException {
        try {
            MultitenancyQueries.addTenantIdInTargetStorage(this, tenantIdentifier);
        } catch (StorageTransactionLogicException e) {
            if (e.actualException instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                if (isPrimaryKeyError(((PSQLException) e.actualException).getServerErrorMessage(),
                        config.getTenantsTable())) {
                    throw new DuplicateTenantException();
                }
            }
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public void overwriteTenantConfig(TenantConfig tenantConfig)
            throws TenantOrAppNotFoundException, StorageQueryException, DuplicateThirdPartyIdException,
            DuplicateClientTypeException {
        try {
            MultitenancyQueries.overwriteTenantConfig(this, tenantConfig);
        } catch (StorageTransactionLogicException e) {
            if (e.actualException instanceof TenantOrAppNotFoundException) {
                throw (TenantOrAppNotFoundException) e.actualException;
            }
            if (e.actualException instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                if (isPrimaryKeyError(((PSQLException) e.actualException).getServerErrorMessage(),
                        config.getTenantThirdPartyProvidersTable())) {
                    throw new DuplicateThirdPartyIdException();
                }
                if (isPrimaryKeyError(((PSQLException) e.actualException).getServerErrorMessage(),
                        config.getTenantThirdPartyProviderClientsTable())) {
                    throw new DuplicateClientTypeException();
                }
            }
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public void deleteTenantIdInTargetStorage(TenantIdentifier tenantIdentifier) throws StorageQueryException {
        MultitenancyQueries.deleteTenantIdInTargetStorage(this, tenantIdentifier);
    }

    @Override
    public boolean deleteTenantInfoInBaseStorage(TenantIdentifier tenantIdentifier) throws StorageQueryException {
        return MultitenancyQueries.deleteTenantConfig(this, tenantIdentifier);
    }

    @Override
    public boolean deleteAppInfoInBaseStorage(AppIdentifier appIdentifier) throws StorageQueryException {
        return deleteTenantInfoInBaseStorage(appIdentifier.getAsPublicTenantIdentifier());
    }

    @Override
    public boolean deleteConnectionUriDomainInfoInBaseStorage(String connectionUriDomain) throws StorageQueryException {
        return deleteTenantInfoInBaseStorage(new TenantIdentifier(connectionUriDomain, null, null));
    }

    @Override
    public TenantConfig[] getAllTenants() throws StorageQueryException {
        return MultitenancyQueries.getAllTenants(this);
    }

    @Override
    public boolean addUserIdToTenant_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con, String userId)
            throws TenantOrAppNotFoundException, UnknownUserIdException, StorageQueryException,
            DuplicateEmailException, DuplicateThirdPartyUserException, DuplicatePhoneNumberException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            String recipeId = GeneralQueries.getRecipeIdForUser_Transaction(this, sqlCon, tenantIdentifier,
                    userId);

            if (recipeId == null) {
                throw new UnknownUserIdException();
            }

            boolean added;
            if (recipeId.equals("emailpassword")) {
                added = EmailPasswordQueries.addUserIdToTenant_Transaction(this, sqlCon, tenantIdentifier,
                        userId);
            } else if (recipeId.equals("thirdparty")) {
                added = ThirdPartyQueries.addUserIdToTenant_Transaction(this, sqlCon, tenantIdentifier, userId);
            } else if (recipeId.equals("passwordless")) {
                added = PasswordlessQueries.addUserIdToTenant_Transaction(this, sqlCon, tenantIdentifier,
                        userId);
            } else {
                throw new IllegalStateException("Should never come here!");
            }

            sqlCon.commit();
            return added;
        } catch (SQLException throwables) {
            PostgreSQLConfig config = Config.getConfig(this);
            ServerErrorMessage serverErrorMessage = ((PSQLException) throwables).getServerErrorMessage();

            if (isForeignKeyConstraintError(serverErrorMessage, config.getUsersTable(), "tenant_id")) {
                throw new TenantOrAppNotFoundException(tenantIdentifier);
            }
            if (isUniqueConstraintError(serverErrorMessage, config.getEmailPasswordUserToTenantTable(), "email")) {
                throw new DuplicateEmailException();
            }
            if (isUniqueConstraintError(serverErrorMessage, config.getThirdPartyUserToTenantTable(),
                    "third_party_user_id")) {
                throw new DuplicateThirdPartyUserException();
            }
            if (isUniqueConstraintError(serverErrorMessage,
                    Config.getConfig(this).getPasswordlessUserToTenantTable(), "phone_number")) {
                throw new DuplicatePhoneNumberException();
            }
            if (isUniqueConstraintError(serverErrorMessage,
                    Config.getConfig(this).getPasswordlessUserToTenantTable(), "email")) {
                throw new DuplicateEmailException();
            }

            throw new StorageQueryException(throwables);
        }
    }

    @Override
    public boolean removeUserIdFromTenant(TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException {
        try {
            return this.startTransaction(con -> {
                Connection sqlCon = (Connection) con.getConnection();
                try {
                    String recipeId = GeneralQueries.getRecipeIdForUser_Transaction(this, sqlCon, tenantIdentifier,
                            userId);

                    if (recipeId == null) {
                        sqlCon.commit();
                        return false; // No auth user to remove
                    }

                    boolean removed;
                    if (recipeId.equals("emailpassword")) {
                        removed = EmailPasswordQueries.removeUserIdFromTenant_Transaction(this, sqlCon,
                                tenantIdentifier, userId);
                    } else if (recipeId.equals("thirdparty")) {
                        removed = ThirdPartyQueries.removeUserIdFromTenant_Transaction(this, sqlCon, tenantIdentifier,
                                userId);
                    } else if (recipeId.equals("passwordless")) {
                        removed = PasswordlessQueries.removeUserIdFromTenant_Transaction(this, sqlCon, tenantIdentifier,
                                userId);
                    } else {
                        throw new IllegalStateException("Should never come here!");
                    }

                    sqlCon.commit();
                    return removed;
                } catch (SQLException throwables) {
                    throw new StorageTransactionLogicException(throwables);
                }
            });
        } catch (StorageTransactionLogicException e) {
            if (e.actualException instanceof SQLException) {
                throw new StorageQueryException(e.actualException);
            } else if (e.actualException instanceof StorageQueryException) {
                throw (StorageQueryException) e.actualException;
            }
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public boolean deleteDashboardUserWithUserId(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            return DashboardQueries.deleteDashboardUserWithUserId(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void createNewDashboardUserSession(AppIdentifier appIdentifier, String userId, String sessionId,
                                              long timeCreated, long expiry)
            throws StorageQueryException, UserIdNotFoundException {
        try {
            DashboardQueries.createDashboardSession(this, appIdentifier, userId, sessionId, timeCreated,
                    expiry);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                ServerErrorMessage serverMessage =
                        ((PSQLException) e).getServerErrorMessage();

                if (isForeignKeyConstraintError(serverMessage,
                        Config.getConfig(this).getDashboardSessionsTable(), "user_id")) {
                    throw new UserIdNotFoundException();
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public DashboardSessionInfo[] getAllSessionsForUserId(AppIdentifier appIdentifier, String userId) throws
            StorageQueryException {
        try {
            return DashboardQueries.getAllSessionsForUserId(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public DashboardSessionInfo getSessionInfoWithSessionId(AppIdentifier appIdentifier, String sessionId) throws
            StorageQueryException {
        try {
            return DashboardQueries.getSessionInfoWithSessionId(this, appIdentifier, sessionId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean revokeSessionWithSessionId(AppIdentifier appIdentifier, String sessionId)
            throws StorageQueryException {
        try {
            return DashboardQueries.deleteDashboardUserSessionWithSessionId(this, appIdentifier,
                    sessionId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateDashboardUsersEmailWithUserId_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                                String userId,
                                                                String newEmail) throws StorageQueryException,
            io.supertokens.pluginInterface.dashboard.exceptions.DuplicateEmailException, UserIdNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            if (!DashboardQueries.updateDashboardUsersEmailWithUserId_Transaction(this,
                    sqlCon, appIdentifier, userId, newEmail)) {
                throw new UserIdNotFoundException();
            }
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverErrorMessage = ((PSQLException) e).getServerErrorMessage();

                if (isUniqueConstraintError(serverErrorMessage,
                        config.getDashboardUsersTable(),
                        "email")) {
                    throw new io.supertokens.pluginInterface.dashboard.exceptions.DuplicateEmailException();
                }
            }
            throw new StorageQueryException(e);
        }

    }

    @Override
    public void updateDashboardUsersPasswordWithUserId_Transaction(AppIdentifier appIdentifier,
                                                                   TransactionConnection con, String userId,
                                                                   String newPassword)
            throws StorageQueryException, UserIdNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            if (!DashboardQueries.updateDashboardUsersPasswordWithUserId_Transaction(this,
                    sqlCon, appIdentifier, userId, newPassword)) {
                throw new UserIdNotFoundException();
            }
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public DashboardUser[] getAllDashboardUsers(AppIdentifier appIdentifier) throws StorageQueryException {
        try {
            return DashboardQueries.getAllDashBoardUsers(this, appIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public DashboardUser getDashboardUserByUserId(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            return DashboardQueries.getDashboardUserByUserId(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void createNewDashboardUser(AppIdentifier appIdentifier, DashboardUser userInfo)
            throws StorageQueryException, io.supertokens.pluginInterface.dashboard.exceptions.DuplicateUserIdException,
            io.supertokens.pluginInterface.dashboard.exceptions.DuplicateEmailException, TenantOrAppNotFoundException {
        try {
            DashboardQueries.createDashboardUser(this, appIdentifier, userInfo.userId, userInfo.email,
                    userInfo.passwordHash,
                    userInfo.timeJoined);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverErrorMessage = ((PSQLException) e).getServerErrorMessage();

                if (isPrimaryKeyError(serverErrorMessage, config.getDashboardUsersTable())) {
                    throw new io.supertokens.pluginInterface.dashboard.exceptions.DuplicateUserIdException();
                }
                if (isUniqueConstraintError(serverErrorMessage, config.getDashboardUsersTable(),
                        "email")) {
                    throw new io.supertokens.pluginInterface.dashboard.exceptions.DuplicateEmailException();
                }
                if (isForeignKeyConstraintError(serverErrorMessage, config.getDashboardUsersTable(), "app_id")) {
                    throw new TenantOrAppNotFoundException(appIdentifier);
                }

            }
            throw new StorageQueryException(e);
        }

    }

    @Override
    public DashboardUser getDashboardUserByEmail(AppIdentifier appIdentifier, String email)
            throws StorageQueryException {
        try {
            return DashboardQueries.getDashboardUserByEmail(this, appIdentifier, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void revokeExpiredSessions() throws StorageQueryException {
        try {
            DashboardQueries.deleteExpiredSessions(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }

    }

    // TOTP recipe:
    @TestOnly
    @Override
    public void createDevice(AppIdentifier appIdentifier, TOTPDevice device)
            throws DeviceAlreadyExistsException, TenantOrAppNotFoundException, StorageQueryException {
        try {
            startTransaction(con -> {
                try {
                    createDevice_Transaction(con, new AppIdentifier(null, null), device);
                } catch (DeviceAlreadyExistsException | TenantOrAppNotFoundException e) {
                    throw new StorageTransactionLogicException(e);
                }
                return null;
            });
        } catch (StorageTransactionLogicException e) {
            if (e.actualException instanceof DeviceAlreadyExistsException) {
                throw (DeviceAlreadyExistsException) e.actualException;
            } else if (e.actualException instanceof TenantOrAppNotFoundException) {
                throw (TenantOrAppNotFoundException) e.actualException;
            } else if (e.actualException instanceof StorageQueryException) {
                throw (StorageQueryException) e.actualException;
            }
        }
    }

    @Override
    public TOTPDevice createDevice_Transaction(TransactionConnection con, AppIdentifier appIdentifier, TOTPDevice device)
            throws StorageQueryException, DeviceAlreadyExistsException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            TOTPQueries.createDevice_Transaction(this, sqlCon, appIdentifier, device);
            return device;
        } catch (SQLException e) {
            Exception actualException = e;

            if (actualException instanceof PSQLException) {
                ServerErrorMessage errMsg = ((PSQLException) actualException).getServerErrorMessage();

                if (isPrimaryKeyError(errMsg, Config.getConfig(this).getTotpUserDevicesTable())) {
                   throw new DeviceAlreadyExistsException();
                } else if (isForeignKeyConstraintError(errMsg, Config.getConfig(this).getTotpUsersTable(), "app_id")) {
                   throw new TenantOrAppNotFoundException(appIdentifier);
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public TOTPDevice getDeviceByName_Transaction(TransactionConnection con, AppIdentifier appIdentifier, String userId, String deviceName) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return TOTPQueries.getDeviceByName_Transaction(this, sqlCon, appIdentifier, userId, deviceName);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void markDeviceAsVerified(AppIdentifier appIdentifier, String userId, String deviceName)
            throws StorageQueryException, UnknownDeviceException {
        try {
            int matchedCount = TOTPQueries.markDeviceAsVerified(this, appIdentifier, userId, deviceName);
            if (matchedCount == 0) {
                // Note matchedCount != updatedCount
                throw new UnknownDeviceException();
            }
            return; // Device was marked as verified
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int deleteDevice_Transaction(TransactionConnection con, AppIdentifier appIdentifier, String userId,
                                        String deviceName)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return TOTPQueries.deleteDevice_Transaction(this, sqlCon, appIdentifier, userId, deviceName);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void removeUser_Transaction(TransactionConnection con, AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            TOTPQueries.removeUser_Transaction(this, sqlCon, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean removeUser(TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException {
        try {
            return TOTPQueries.removeUser(this, tenantIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateDeviceName(AppIdentifier appIdentifier, String userId, String oldDeviceName, String newDeviceName)
            throws StorageQueryException,
            UnknownDeviceException, DeviceAlreadyExistsException {
        try {
            int updatedCount = TOTPQueries.updateDeviceName(this, appIdentifier, userId, oldDeviceName, newDeviceName);
            if (updatedCount == 0) {
                throw new UnknownDeviceException();
            }
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                ServerErrorMessage errMsg = ((PSQLException) e).getServerErrorMessage();
                if (isPrimaryKeyError(errMsg, Config.getConfig(this).getTotpUserDevicesTable())) {
                   throw new DeviceAlreadyExistsException();
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public TOTPDevice[] getDevices(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            return TOTPQueries.getDevices(this, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public TOTPDevice[] getDevices_Transaction(TransactionConnection con, AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return TOTPQueries.getDevices_Transaction(this, sqlCon, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void insertUsedCode_Transaction(TransactionConnection con, TenantIdentifier tenantIdentifier,
                                           TOTPUsedCode usedCodeObj)
            throws StorageQueryException, UnknownTotpUserIdException, UsedCodeAlreadyExistsException,
            TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            TOTPQueries.insertUsedCode_Transaction(this, sqlCon, tenantIdentifier, usedCodeObj);
        } catch (SQLException e) {
            ServerErrorMessage err = ((PSQLException) e).getServerErrorMessage();

            if (isPrimaryKeyError(err, Config.getConfig(this).getTotpUsedCodesTable())) {
                throw new UsedCodeAlreadyExistsException();
            } else if (isForeignKeyConstraintError(err, Config.getConfig(this).getTotpUsedCodesTable(),
                    "user_id")) {
                throw new UnknownTotpUserIdException();
            } else if (isForeignKeyConstraintError(err, Config.getConfig(this).getTotpUsedCodesTable(), "tenant_id")) {
                throw new TenantOrAppNotFoundException(tenantIdentifier);
            }

            throw new StorageQueryException(e);
        }
    }

    @Override
    public TOTPUsedCode[] getAllUsedCodesDescOrder_Transaction(TransactionConnection con,
                                                               TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return TOTPQueries.getAllUsedCodesDescOrder_Transaction(this, sqlCon, tenantIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int removeExpiredCodes(TenantIdentifier tenantIdentifier, long expiredBefore)
            throws StorageQueryException {
        try {
            return TOTPQueries.removeExpiredCodes(this, tenantIdentifier, expiredBefore);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public Set<String> getValidFieldsInConfig() {
        return PostgreSQLConfig.getValidFields();
    }

    @Override
    public void setLogLevels(Set<LOG_LEVEL> logLevels) {
        Config.setLogLevels(this, logLevels);
    }

    @TestOnly
    @Override
    public String[] getAllTablesInTheDatabase() throws StorageQueryException {
        if (!isTesting) {
            throw new UnsupportedOperationException();
        }
        try {
            return GeneralQueries.getAllTablesInTheDatabase(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @TestOnly
    @Override
    public String[] getAllTablesInTheDatabaseThatHasDataForAppId(String appId) throws StorageQueryException {
        if (!isTesting) {
            throw new UnsupportedOperationException();
        }
        try {
            return GeneralQueries.getAllTablesInTheDatabaseThatHasDataForAppId(this, appId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo getPrimaryUserById_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                             String userId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return GeneralQueries.getPrimaryUserInfoForUserId_Transaction(this, sqlCon, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo[] listPrimaryUsersByEmail_Transaction(AppIdentifier appIdentifier,
                                                                    TransactionConnection con, String email)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return GeneralQueries.listPrimaryUsersByEmail_Transaction(this, sqlCon, appIdentifier, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo[] listPrimaryUsersByPhoneNumber_Transaction(AppIdentifier appIdentifier,
                                                                          TransactionConnection con,
                                                                          String phoneNumber)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return GeneralQueries.listPrimaryUsersByPhoneNumber_Transaction(this, sqlCon, appIdentifier,
                    phoneNumber);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo[] listPrimaryUsersByThirdPartyInfo(AppIdentifier appIdentifier,
                                                                 String thirdPartyId,
                                                                 String thirdPartyUserId)
            throws StorageQueryException {
        try {
            return GeneralQueries.listPrimaryUsersByThirdPartyInfo(this, appIdentifier,
                    thirdPartyId, thirdPartyUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo[] listPrimaryUsersByThirdPartyInfo_Transaction(AppIdentifier appIdentifier,
                                                                             TransactionConnection con,
                                                                             String thirdPartyId,
                                                                             String thirdPartyUserId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return GeneralQueries.listPrimaryUsersByThirdPartyInfo_Transaction(this, sqlCon, appIdentifier,
                    thirdPartyId, thirdPartyUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void makePrimaryUser_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            // we do not bother returning if a row was updated here or not, cause it's happening
            // in a transaction anyway.
            GeneralQueries.makePrimaryUser_Transaction(this, sqlCon, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void linkAccounts_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String recipeUserId,
                                         String primaryUserId) throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            // we do not bother returning if a row was updated here or not, cause it's happening
            // in a transaction anyway.
            GeneralQueries.linkAccounts_Transaction(this, sqlCon, appIdentifier, recipeUserId, primaryUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void unlinkAccounts_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String primaryUserId, String recipeUserId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            // we do not bother returning if a row was updated here or not, cause it's happening
            // in a transaction anyway.
            GeneralQueries.unlinkAccounts_Transaction(this, sqlCon, appIdentifier, primaryUserId, recipeUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean doesUserIdExist_Transaction(TransactionConnection con, AppIdentifier appIdentifier,
                                               String externalUserId) throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return GeneralQueries.doesUserIdExist_Transaction(this, sqlCon, appIdentifier, externalUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean checkIfUsesAccountLinking(AppIdentifier appIdentifier) throws StorageQueryException {
        try {
            return GeneralQueries.checkIfUsesAccountLinking(this, appIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int countUsersThatHaveMoreThanOneLoginMethodAndActiveSince(AppIdentifier appIdentifier, long sinceTime) throws StorageQueryException {
        try {
            return ActiveUsersQueries.countUsersActiveSinceAndHasMoreThanOneLoginMethod(this, appIdentifier, sinceTime);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int getUsersCountWithMoreThanOneLoginMethod(AppIdentifier appIdentifier) throws StorageQueryException {
        try {
            return GeneralQueries.getUsersCountWithMoreThanOneLoginMethod(this, appIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @TestOnly
    public Thread getMainThread() {
        return mainThread;
    }

    @Override
    public UserIdMapping getUserIdMapping_Transaction(TransactionConnection con, AppIdentifier appIdentifier,
                                                      String userId, boolean isSuperTokensUserId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            if (isSuperTokensUserId) {
                return UserIdMappingQueries.getuseraIdMappingWithSuperTokensUserId_Transaction(this, sqlCon, appIdentifier,
                        userId);
            }

            return UserIdMappingQueries.getUserIdMappingWithExternalUserId_Transaction(this, sqlCon, appIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public UserIdMapping[] getUserIdMapping_Transaction(TransactionConnection con, AppIdentifier appIdentifier,
                                                        String userId) throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return UserIdMappingQueries.getUserIdMappingWithEitherSuperTokensUserIdOrExternalUserId_Transaction(this,
                    sqlCon,
                    appIdentifier,
                    userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int getUsersCountWithMoreThanOneLoginMethodOrTOTPEnabled(AppIdentifier appIdentifier) throws StorageQueryException {
        try {
            return GeneralQueries.getUsersCountWithMoreThanOneLoginMethodOrTOTPEnabled(this, appIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int countUsersThatHaveMoreThanOneLoginMethodOrTOTPEnabledAndActiveSince(AppIdentifier appIdentifier, long sinceTime) throws StorageQueryException {
        try {
            return ActiveUsersQueries.countUsersThatHaveMoreThanOneLoginMethodOrTOTPEnabledAndActiveSince(this, appIdentifier, sinceTime);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }
}
