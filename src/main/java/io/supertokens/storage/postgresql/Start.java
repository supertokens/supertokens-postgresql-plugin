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

import java.lang.reflect.Field;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.zaxxer.hikari.pool.HikariPool;

import ch.qos.logback.classic.Logger;
import io.supertokens.pluginInterface.ActiveUsersSQLStorage;
import io.supertokens.pluginInterface.ActiveUsersStorage;
import io.supertokens.pluginInterface.ConfigFieldInfo;
import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.LOG_LEVEL;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.Storage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.authRecipe.sqlStorage.AuthRecipeSQLStorage;
import io.supertokens.pluginInterface.bulkimport.BulkImportStorage;
import io.supertokens.pluginInterface.bulkimport.BulkImportUser;
import io.supertokens.pluginInterface.bulkimport.exceptions.BulkImportBatchInsertException;
import io.supertokens.pluginInterface.bulkimport.exceptions.BulkImportTransactionRolledBackException;
import io.supertokens.pluginInterface.bulkimport.sqlStorage.BulkImportSQLStorage;
import io.supertokens.pluginInterface.dashboard.DashboardSearchTags;
import io.supertokens.pluginInterface.dashboard.DashboardSessionInfo;
import io.supertokens.pluginInterface.dashboard.DashboardUser;
import io.supertokens.pluginInterface.dashboard.exceptions.UserIdNotFoundException;
import io.supertokens.pluginInterface.dashboard.sqlStorage.DashboardSQLStorage;
import io.supertokens.pluginInterface.emailpassword.EmailPasswordImportUser;
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
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.MultitenancyStorage;
import io.supertokens.pluginInterface.multitenancy.TenantConfig;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.exceptions.DuplicateClientTypeException;
import io.supertokens.pluginInterface.multitenancy.exceptions.DuplicateTenantException;
import io.supertokens.pluginInterface.multitenancy.exceptions.DuplicateThirdPartyIdException;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.pluginInterface.multitenancy.sqlStorage.MultitenancySQLStorage;
import io.supertokens.pluginInterface.oauth.OAuthClient;
import io.supertokens.pluginInterface.oauth.OAuthLogoutChallenge;
import io.supertokens.pluginInterface.oauth.OAuthStorage;
import io.supertokens.pluginInterface.oauth.exception.DuplicateOAuthLogoutChallengeException;
import io.supertokens.pluginInterface.oauth.exception.OAuthClientNotFoundException;
import io.supertokens.pluginInterface.opentelemetry.OtelProvider;
import io.supertokens.pluginInterface.opentelemetry.WithinOtelSpan;
import io.supertokens.pluginInterface.passwordless.PasswordlessCode;
import io.supertokens.pluginInterface.passwordless.PasswordlessDevice;
import io.supertokens.pluginInterface.passwordless.PasswordlessImportUser;
import io.supertokens.pluginInterface.passwordless.exception.DuplicateCodeIdException;
import io.supertokens.pluginInterface.passwordless.exception.DuplicateDeviceIdHashException;
import io.supertokens.pluginInterface.passwordless.exception.DuplicateLinkCodeHashException;
import io.supertokens.pluginInterface.passwordless.exception.DuplicatePhoneNumberException;
import io.supertokens.pluginInterface.passwordless.exception.UnknownDeviceIdHash;
import io.supertokens.pluginInterface.passwordless.sqlStorage.PasswordlessSQLStorage;
import io.supertokens.pluginInterface.saml.SAMLClaimsInfo;
import io.supertokens.pluginInterface.saml.SAMLClient;
import io.supertokens.pluginInterface.saml.SAMLRelayStateInfo;
import io.supertokens.pluginInterface.saml.SAMLStorage;
import io.supertokens.pluginInterface.session.SessionInfo;
import io.supertokens.pluginInterface.session.SessionStorage;
import io.supertokens.pluginInterface.session.sqlStorage.SessionSQLStorage;
import io.supertokens.pluginInterface.sqlStorage.TransactionConnection;
import io.supertokens.pluginInterface.thirdparty.ThirdPartyImportUser;
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
import io.supertokens.pluginInterface.webauthn.AccountRecoveryTokenInfo;
import io.supertokens.pluginInterface.webauthn.WebAuthNOptions;
import io.supertokens.pluginInterface.webauthn.WebAuthNStoredCredential;
import io.supertokens.pluginInterface.webauthn.exceptions.DuplicateOptionsIdException;
import io.supertokens.pluginInterface.webauthn.exceptions.DuplicateRecoverAccountTokenException;
import io.supertokens.pluginInterface.webauthn.exceptions.DuplicateUserEmailException;
import io.supertokens.pluginInterface.webauthn.exceptions.WebauthNCredentialNotExistsException;
import io.supertokens.pluginInterface.webauthn.exceptions.WebauthNOptionsNotExistsException;
import io.supertokens.pluginInterface.webauthn.slqStorage.WebAuthNSQLStorage;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import io.supertokens.storage.postgresql.annotations.EnvName;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storage.postgresql.output.Logging;
import io.supertokens.storage.postgresql.queries.ActiveUsersQueries;
import io.supertokens.storage.postgresql.queries.BulkImportQueries;
import io.supertokens.storage.postgresql.queries.DashboardQueries;
import io.supertokens.storage.postgresql.queries.EmailPasswordQueries;
import io.supertokens.storage.postgresql.queries.EmailVerificationQueries;
import io.supertokens.storage.postgresql.queries.GeneralQueries;
import io.supertokens.storage.postgresql.queries.JWTSigningQueries;
import io.supertokens.storage.postgresql.queries.MultitenancyQueries;
import io.supertokens.storage.postgresql.queries.OAuthQueries;
import io.supertokens.storage.postgresql.queries.PasswordlessQueries;
import io.supertokens.storage.postgresql.queries.SAMLQueries;
import io.supertokens.storage.postgresql.queries.SessionQueries;
import io.supertokens.storage.postgresql.queries.TOTPQueries;
import io.supertokens.storage.postgresql.queries.ThirdPartyQueries;
import io.supertokens.storage.postgresql.queries.UserIdMappingQueries;
import io.supertokens.storage.postgresql.queries.UserMetadataQueries;
import io.supertokens.storage.postgresql.queries.UserRolesQueries;
import io.supertokens.storage.postgresql.queries.WebAuthNQueries;

@WithinOtelSpan
public class Start
        implements SessionSQLStorage, EmailPasswordSQLStorage, EmailVerificationSQLStorage, ThirdPartySQLStorage,
        JWTRecipeSQLStorage, PasswordlessSQLStorage, UserMetadataSQLStorage, UserRolesSQLStorage, UserIdMappingStorage,
        UserIdMappingSQLStorage, MultitenancyStorage, MultitenancySQLStorage, DashboardSQLStorage, TOTPSQLStorage,
        ActiveUsersStorage, ActiveUsersSQLStorage, AuthRecipeSQLStorage, OAuthStorage, BulkImportSQLStorage,
        WebAuthNSQLStorage, SAMLStorage {

    // these configs are protected from being modified / viewed by the dev using the SuperTokens
    // SaaS. If the core is not running in SuperTokens SaaS, this array has no effect.
    private static String[] PROTECTED_DB_CONFIG = new String[]{"postgresql_connection_pool_size",
            "postgresql_connection_uri", "postgresql_host", "postgresql_port", "postgresql_user", "postgresql_password",
            "postgresql_database_name", "postgresql_table_schema", "postgresql_idle_connection_timeout",
            "postgresql_minimum_idle_connections", "postgresql_connection_attributes", "postgresql_connection_scheme",
            "postgresql_table_names_prefix"
    };
    private static final Object appenderLock = new Object();
    public static boolean silent = false;
    private ResourceDistributor resourceDistributor = new ResourceDistributor();
    private String processId;
    private HikariLoggingAppender appender;
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
    public Storage createBulkImportProxyStorageInstance() {
        return new BulkImportProxyStorage();
    }

    @Override
    public void closeConnectionForBulkImportProxyStorage() throws StorageQueryException {
        throw new UnsupportedOperationException(
                "closeConnectionForBulkImportProxyStorage should only be called from BulkImportProxyStorage");
    }

    @Override
    public void commitTransactionForBulkImportProxyStorage() throws StorageQueryException {
        throw new UnsupportedOperationException(
                "commitTransactionForBulkImportProxyStorage should only be called from BulkImportProxyStorage");
    }

    @Override
    public void rollbackTransactionForBulkImportProxyStorage() throws StorageQueryException {
        throw new UnsupportedOperationException(
                "rollbackTransactionForBulkImportProxyStorage should only be called from BulkImportProxyStorage");
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
    public void initFileLogging(String infoLogPath, String errorLogPath, OtelProvider otelProvider) {
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
            appender.setOtelProvider(otelProvider);
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
    public void initStorage(boolean shouldWait, List<TenantIdentifier> tenantIdentifiers) throws DbInitException {
        if (ConnectionPool.isAlreadyInitialised(this)) {
            return;
        }
        this.isBaseTenant = shouldWait;
        if (isBaseTenant) {
            // We are doing this so that the tests don't hang on to the first main thread
            mainThread = Thread.currentThread();
        }
        try {
            ConnectionPool.initPool(this, shouldWait, (con) -> {
                try {
                    GeneralQueries.createTablesIfNotExists(this, con);
                } catch (SQLException e) {
                    throw new StorageQueryException(e);
                }
                for (TenantIdentifier tenantIdentifier : tenantIdentifiers) {
                    try {
                        this.addTenantIdInTargetStorage_Transaction(con, tenantIdentifier);
                    } catch (DuplicateTenantException e) {
                        // ignore
                    }
                }
            });
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
            } catch (SQLException | StorageQueryException | StorageTransactionLogicException |
                     TenantOrAppNotFoundException e) {
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
                        && psqlException.getServerErrorMessage() != null
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
                        if(this instanceof BulkImportProxyStorage){
                            throw new StorageTransactionLogicException(new BulkImportTransactionRolledBackException(e));
                            // if the current instance is of BulkImportProxyStorage, that means we are doing a bulk import
                            // which uses nested transactions. With MySQL this retry logic doesn't going to work, we have
                            // to retry the whole "big" transaction, not just the innermost, current one.
                            // @see BulkImportTransactionRolledBackException for more explanation.
                        }
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
                } else if  (e instanceof TenantOrAppNotFoundException) {
                    throw new StorageTransactionLogicException(e);
                }
                throw new StorageQueryException(e);
            }
        }
    }

    protected <T> T startTransactionHelper(TransactionLogic<T> logic, TransactionIsolationLevel isolationLevel)
            throws StorageQueryException, StorageTransactionLogicException, SQLException, TenantOrAppNotFoundException {
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
            initStorage(false, new ArrayList<>());
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
                                              long expiry, boolean useStaticKey) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            SessionQueries.updateSessionInfo_Transaction(this, sqlCon, tenantIdentifier, sessionHandle,
                    refreshTokenHash2, expiry, useStaticKey);
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
    public void updateConfigJsonFromEnv(JsonObject configJson) {
        Map<String, String> env = System.getenv();

        for (Field field : PostgreSQLConfig.class.getDeclaredFields()) {
            if (field.isAnnotationPresent(EnvName.class)) {
                String envName = field.getAnnotation(EnvName.class).value();
                String stringValue = env.get(envName);

                if (stringValue == null || stringValue.isEmpty()) {
                    continue;
                }

                if (stringValue.startsWith("\"") && stringValue.endsWith("\"")) {
                    stringValue = stringValue.substring(1, stringValue.length() - 1);
                    stringValue = stringValue
                            .replace("\\n", "\n")
                            .replace("\\t", "\t")
                            .replace("\\r", "\r")
                            .replace("\\\"", "\"")
                            .replace("\\'", "'")
                            .replace("\\\\", "\\");
                }

                if (field.getType().equals(String.class)) {
                    configJson.addProperty(field.getName(), stringValue);
                } else if (field.getType().equals(int.class)) {
                    configJson.addProperty(field.getName(), Integer.parseInt(stringValue));
                } else if (field.getType().equals(long.class)) {
                    configJson.addProperty(field.getName(), Long.parseLong(stringValue));
                } else if (field.getType().equals(boolean.class)) {
                    configJson.addProperty(field.getName(), Boolean.parseBoolean(stringValue));
                } else if (field.getType().equals(float.class)) {
                    configJson.addProperty(field.getName(), Float.parseFloat(stringValue));
                } else if (field.getType().equals(double.class)) {
                    configJson.addProperty(field.getName(), Double.parseDouble(stringValue));
                }
            }
        }
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
        } else if (className.equals(JWTRecipeStorage.class.getName())) {
            return false;
        } else if (className.equals(ActiveUsersStorage.class.getName())) {
            return ActiveUsersQueries.getLastActiveByUserId(this, appIdentifier, userId) != null;
        } else {
            throw new IllegalStateException("ClassName: " + className + " is not part of NonAuthRecipeStorage");
        }
    }

    @Override
    public Map<String, List<String>> findNonAuthRecipesWhereForUserIdsUsed(AppIdentifier appIdentifier,
                                                                           List<String> userIds)
            throws StorageQueryException {
        try {
            Map<String, List<String>> sessionHandlesByUserId = SessionQueries.getAllNonExpiredSessionHandlesForUsers(this, appIdentifier, userIds);
            Map<String, List<String>> rolesByUserIds = UserRolesQueries.getRolesForUsers(this, appIdentifier, userIds);
            Map<String, JsonObject> userMetadatasByIds = UserMetadataQueries.getMultipleUserMetadatas(this, appIdentifier, userIds);
            Set<String> userIdsUsedInEmailVerification = EmailVerificationQueries.findUserIdsBeingUsedForEmailVerification(this, appIdentifier, userIds);
            Map<String, List<TOTPDevice>> devicesByUserIds = TOTPQueries.getDevicesForMultipleUsers(this, appIdentifier, userIds);
            Map<String, Long> lastActivesByUserIds = ActiveUsersQueries.getLastActiveByMultipleUserIds(this, appIdentifier, userIds);

            Map<String, List<String>> nonAuthRecipeClassnamesByUserIds = new HashMap<>();
            //session recipe
            for(String userId: sessionHandlesByUserId.keySet()){
                if(!nonAuthRecipeClassnamesByUserIds.containsKey(userId)){
                    nonAuthRecipeClassnamesByUserIds.put(userId, new ArrayList<>());
                }
                nonAuthRecipeClassnamesByUserIds.get(userId).add(SessionStorage.class.getName());
            }

            //role recipe
            for(String userId: rolesByUserIds.keySet()){
                if(!nonAuthRecipeClassnamesByUserIds.containsKey(userId)){
                    nonAuthRecipeClassnamesByUserIds.put(userId, new ArrayList<>());
                }
                nonAuthRecipeClassnamesByUserIds.get(userId).add(UserRolesStorage.class.getName());
            }

            //usermetadata recipe
            for(String userId: userMetadatasByIds.keySet()){
                if(!nonAuthRecipeClassnamesByUserIds.containsKey(userId)){
                    nonAuthRecipeClassnamesByUserIds.put(userId, new ArrayList<>());
                }
                nonAuthRecipeClassnamesByUserIds.get(userId).add(UserMetadataStorage.class.getName());
            }

            //emailverification recipe
            for(String userId: userIdsUsedInEmailVerification){
                if(!nonAuthRecipeClassnamesByUserIds.containsKey(userId)){
                    nonAuthRecipeClassnamesByUserIds.put(userId, new ArrayList<>());
                }
                nonAuthRecipeClassnamesByUserIds.get(userId).add(EmailVerificationStorage.class.getName());
            }

            //totp recipe
            for(String userId: devicesByUserIds.keySet()){
                if(!nonAuthRecipeClassnamesByUserIds.containsKey(userId)){
                    nonAuthRecipeClassnamesByUserIds.put(userId, new ArrayList<>());
                }
                nonAuthRecipeClassnamesByUserIds.get(userId).add(TOTPStorage.class.getName());
            }

            //active users
            for(String userId: lastActivesByUserIds.keySet()){
                if(!nonAuthRecipeClassnamesByUserIds.containsKey(userId)){
                    nonAuthRecipeClassnamesByUserIds.put(userId, new ArrayList<>());
                }
                nonAuthRecipeClassnamesByUserIds.get(userId).add(ActiveUsersStorage.class.getName());
            }

            return nonAuthRecipeClassnamesByUserIds;
        } catch (SQLException | StorageTransactionLogicException exc) {
            throw new StorageQueryException(exc);
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
                TOTPDevice device = new TOTPDevice(userId, "testDevice", "secret", 0, 30, false,
                        System.currentTimeMillis());
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

        } else if (className.equals(BulkImportStorage.class.getName())){
            //ignore
        } else if (className.equals(OAuthStorage.class.getName())) {
            /* Since OAuth recipe tables do not store userId we do not add any data to them */
        } else if (className.equals(SAMLStorage.class.getName())) {
            //ignore
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
    public void signUpMultipleViaBulkImport_Transaction(TransactionConnection connection, List<EmailPasswordImportUser> users)
            throws StorageQueryException, StorageTransactionLogicException {
        try {
            Connection sqlConnection = (Connection) connection.getConnection();
            EmailPasswordQueries.signUpMultipleForBulkImport_Transaction(this, sqlConnection, users);
        } catch (StorageQueryException | StorageTransactionLogicException e) {
            Throwable actual = e.getCause();
            if (actual instanceof BatchUpdateException batchUpdateException) {
                Map<String, Exception> errorByPosition = new HashMap<>();
                SQLException nextException = batchUpdateException.getNextException();
                while (nextException != null) {

                    if (nextException instanceof PSQLException) {
                        PostgreSQLConfig config = Config.getConfig(this);
                        ServerErrorMessage serverMessage = ((PSQLException) nextException).getServerErrorMessage();

                        int position = getErroneousEntryPosition(batchUpdateException);
                        if (isUniqueConstraintError(serverMessage, config.getEmailPasswordUserToTenantTable(),
                                "email")) {

                            errorByPosition.put(users.get(position).userId, new DuplicateEmailException());

                        } else if (isPrimaryKeyError(serverMessage, config.getThirdPartyUsersTable())
                                || isPrimaryKeyError(serverMessage, config.getUsersTable())
                                || isPrimaryKeyError(serverMessage, config.getThirdPartyUserToTenantTable())
                                || isPrimaryKeyError(serverMessage, config.getAppIdToUserIdTable())) {
                            errorByPosition.put(users.get(position).userId,
                                    new io.supertokens.pluginInterface.thirdparty.exception.DuplicateUserIdException());
                        } else if (isForeignKeyConstraintError(serverMessage, config.getAppIdToUserIdTable(), "app_id")) {
                            errorByPosition.put(users.get(position).userId, new TenantOrAppNotFoundException(users.get(position).tenantIdentifier.toAppIdentifier()));
                        } else if (isForeignKeyConstraintError(serverMessage, config.getUsersTable(), "tenant_id")) {
                            errorByPosition.put(users.get(position).userId,new TenantOrAppNotFoundException(users.get(position).tenantIdentifier));
                        }

                    }
                    nextException = nextException.getNextException();
                }
                throw new StorageTransactionLogicException(new BulkImportBatchInsertException("emailpassword errors", errorByPosition));
            }
            throw new StorageQueryException(e);
        }
    }

    private static int getErroneousEntryPosition(BatchUpdateException batchUpdateException) {
        String errorMessage = batchUpdateException.getMessage();
        String searchFor = "Batch entry ";
        int searchForIndex = errorMessage.indexOf("Batch entry ");
        String entryIndex = errorMessage.substring(searchForIndex + searchFor.length(), errorMessage.indexOf(" ", searchForIndex + searchFor.length()));
        int position = Integer.parseInt(entryIndex);
        return position;
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

    /**
     * Update the isEmailVerified column for multiple users in the email verification table. This method is used in the
     * Bulk Migration process.
     * Important note: this method expects a Map of email to userId, but if there is an error in the batch processing,
     * it will throw a BulkImportBatchInsertException with a map of userid to Exception, based on the position of the
     * erroneous item in the batch.
     * This means, that the underlying map implementation must be one that preserves iteration order (LinkedHashMap
     * is a good choice) and this is the responsibility of the caller to ensure that the passed map is such.
     */
    @Override
    public void updateMultipleIsEmailVerified_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                          Map<String, String> emailToUserId, boolean isEmailVerified)
            throws StorageQueryException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            EmailVerificationQueries.updateMultipleUsersIsEmailVerified_Transaction(this, sqlCon, appIdentifier,
                    emailToUserId, isEmailVerified);
        } catch (SQLException e) {
            if (e instanceof BatchUpdateException batchUpdateException) {
                SQLException nextException = batchUpdateException.getNextException();
                Map<String, Exception> errorByPosition = new HashMap<>();
                    while (nextException != null) {

                        if (nextException instanceof PSQLException) {
                            PostgreSQLConfig config = Config.getConfig(this);
                            ServerErrorMessage serverMessage = ((PSQLException) nextException).getServerErrorMessage();

                            int position = getErroneousEntryPosition(batchUpdateException);
                            String userid = ((Map.Entry<String, String>) emailToUserId.entrySet().toArray()[position]).getKey();
                            if (isNullConstraintError(serverMessage, config.getEmailVerificationTable(), "email")) {
                                errorByPosition.put(userid, new NullPointerException("email is null"));
                            } else if (isPrimaryKeyError(serverMessage, config.getEmailVerificationTable())) {
                                errorByPosition.put(userid,
                                        new DuplicateEmailException());
                            }
                            if (isForeignKeyConstraintError(serverMessage, config.getEmailVerificationTable(),
                                    "app_id")) {
                                throw new TenantOrAppNotFoundException(appIdentifier);
                            }
                        }

                        nextException = nextException.getNextException();
                    }
                    throw new StorageQueryException(
                            new BulkImportBatchInsertException("emailverification errors", errorByPosition));
                }
                throw new StorageQueryException(e);
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
    public void updateIsEmailVerifiedToExternalUserId(AppIdentifier appIdentifier, String supertokensUserId,
                                                      String externalUserId) throws StorageQueryException {
        EmailVerificationQueries.updateIsEmailVerifiedToExternalUserId(this, appIdentifier, supertokensUserId,
                externalUserId);
    }

    @Override
    public void updateMultipleIsEmailVerifiedToExternalUserIds(AppIdentifier appIdentifier,
                                                               Map<String, String> supertokensUserIdToExternalUserId)
            throws StorageQueryException {
        EmailVerificationQueries.updateMultipleIsEmailVerifiedToExternalUserIds(this, appIdentifier, supertokensUserIdToExternalUserId);
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
    public void importThirdPartyUsers_Transaction(TransactionConnection con,
                                                  List<ThirdPartyImportUser> usersToImport)
            throws StorageQueryException, StorageTransactionLogicException, TenantOrAppNotFoundException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            ThirdPartyQueries.importUser_Transaction(this, sqlCon, usersToImport);
        } catch (SQLException e) {
                if (e instanceof BatchUpdateException batchUpdateException) {
                    Map<String, Exception> errorByPosition = new HashMap<>();
                    SQLException nextException = batchUpdateException.getNextException();
                    while (nextException != null) {

                        if (nextException instanceof PSQLException) {
                            PostgreSQLConfig config = Config.getConfig(this);
                            ServerErrorMessage serverMessage = ((PSQLException) nextException).getServerErrorMessage();

                            int position = getErroneousEntryPosition(batchUpdateException);
                            if (isUniqueConstraintError(serverMessage, config.getEmailPasswordUserToTenantTable(),
                                    "third_party_user_id")) {

                                errorByPosition.put(usersToImport.get(position).userId, new DuplicateThirdPartyUserException());

                            } else if (isPrimaryKeyError(serverMessage, config.getThirdPartyUsersTable())
                                    || isPrimaryKeyError(serverMessage, config.getUsersTable())
                                    || isPrimaryKeyError(serverMessage, config.getThirdPartyUserToTenantTable())
                                    || isPrimaryKeyError(serverMessage, config.getAppIdToUserIdTable())) {
                                errorByPosition.put(usersToImport.get(position).userId,
                                        new io.supertokens.pluginInterface.thirdparty.exception.DuplicateUserIdException());
                            }
                            else if (isForeignKeyConstraintError(serverMessage, config.getAppIdToUserIdTable(), "app_id")) {
                                throw new TenantOrAppNotFoundException(usersToImport.get(position).tenantIdentifier.toAppIdentifier());

                            } else if (isForeignKeyConstraintError(serverMessage, config.getUsersTable(), "tenant_id")) {
                                throw new TenantOrAppNotFoundException(usersToImport.get(position).tenantIdentifier);
                            }
                        }
                        nextException = nextException.getNextException();
                    }
                    throw new StorageTransactionLogicException(new BulkImportBatchInsertException("thirdparty errors", errorByPosition));
                }
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

    @TestOnly
    public void updateLastActive(AppIdentifier appIdentifier, String userId, long timestamp) throws StorageQueryException {
        try {
            ActiveUsersQueries.updateUserLastActive(this, appIdentifier, userId, timestamp);
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
    public List<String> findExistingUserIds(AppIdentifier appIdentifier, List<String> userIds)
            throws StorageQueryException {
        try {
            return GeneralQueries.findUserIdsThatExist(this, appIdentifier, userIds);
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
    public AuthRecipeUserInfo getPrimaryUserByWebauthNCredentialId(TenantIdentifier tenantIdentifier,
                                                                   String webauthNCredentialId)
            throws StorageQueryException {
        try {
            return GeneralQueries.getPrimaryUserByWebauthNCredentialId(this, tenantIdentifier, webauthNCredentialId);
        } catch (SQLException | StorageTransactionLogicException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo getPrimaryUserByWebauthNCredentialId_Transaction(TenantIdentifier tenantIdentifier,
                                                                               TransactionConnection con,
                                                                               String credentialId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return GeneralQueries.getPrimaryUserByWebauthNCredentialId_Transaction(this, sqlCon, tenantIdentifier, credentialId);
        } catch (SQLException | StorageTransactionLogicException e) {
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

    private boolean isNullConstraintError(ServerErrorMessage serverMessage, String tableName, String columnName) {
        String[] tableNameParts = tableName.split("\\.");
        tableName = tableNameParts[tableNameParts.length - 1];
        return serverMessage.getSQLState().equals("23502")
                && serverMessage.getMessage().contains("null value in column \"" + columnName + "\" of relation \"" + tableName + "\" violates not-null constraint");
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
    public void importPasswordlessUsers_Transaction(TransactionConnection con,
                                                    List<PasswordlessImportUser> users)
            throws StorageQueryException, TenantOrAppNotFoundException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            PasswordlessQueries.importUsers_Transaction(sqlCon, this, users);
        } catch (SQLException e) {
            if (e instanceof BatchUpdateException batchUpdateException) {
                Map<String, Exception> errorByPosition = new HashMap<>();
                SQLException nextException = batchUpdateException.getNextException();
                while (nextException != null) {

                    if (nextException instanceof PSQLException) {
                        PostgreSQLConfig config = Config.getConfig(this);
                        ServerErrorMessage serverMessage = ((PSQLException) nextException).getServerErrorMessage();

                        int position = getErroneousEntryPosition(batchUpdateException);

                        if (isPrimaryKeyError(serverMessage, config.getPasswordlessUsersTable())
                                || isPrimaryKeyError(serverMessage, config.getUsersTable())
                                || isPrimaryKeyError(serverMessage, config.getPasswordlessUserToTenantTable())
                                || isPrimaryKeyError(serverMessage, config.getAppIdToUserIdTable())) {
                            errorByPosition.put(users.get(position).userId, new DuplicateUserIdException());
                        }
                        if (isUniqueConstraintError(serverMessage, config.getPasswordlessUserToTenantTable(),
                                "email")) {
                            errorByPosition.put(users.get(position).userId, new DuplicateEmailException());

                        } else if (isUniqueConstraintError(serverMessage, config.getPasswordlessUserToTenantTable(),
                                "phone_number")) {
                            errorByPosition.put(users.get(position).userId, new DuplicatePhoneNumberException());

                        } else if (isForeignKeyConstraintError(serverMessage, config.getAppIdToUserIdTable(),
                                "app_id")) {
                            throw new TenantOrAppNotFoundException(users.get(position).tenantIdentifier.toAppIdentifier());

                        } else if (isForeignKeyConstraintError(serverMessage, config.getUsersTable(),
                                "tenant_id")) {
                            throw new TenantOrAppNotFoundException(users.get(position).tenantIdentifier.toAppIdentifier());
                        }
                    }
                    nextException = nextException.getNextException();
                }
                throw new StorageQueryException(
                        new BulkImportBatchInsertException("passwordless errors", errorByPosition));
            }
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
    public Map<String, JsonObject> getMultipleUsersMetadatas_Transaction(AppIdentifier appIdentifier, TransactionConnection
            con, List<String> userIds)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserMetadataQueries.getMultipleUsersMetadatas_Transaction(this, sqlCon, appIdentifier, userIds);
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
    public void setMultipleUsersMetadatas_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                      Map<String, JsonObject> metadataByUserId)
            throws StorageQueryException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            UserMetadataQueries.setMultipleUsersMetadatas_Transaction(this, sqlCon, appIdentifier, metadataByUserId);
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
            throws StorageQueryException, DuplicateUserRoleMappingException,
            TenantOrAppNotFoundException {
        try {
            UserRolesQueries.addRoleToUser(this, tenantIdentifier, userId, role);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverErrorMessage = ((PSQLException) e).getServerErrorMessage();
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
    public void addRolesToUsers_Transaction(TransactionConnection connection,
                                            Map<TenantIdentifier, Map<String, List<String>>> rolesToUserByTenants)
            throws StorageQueryException {
        try {
            UserRolesQueries.addRolesToUsers_Transaction(this, (Connection) connection.getConnection(), rolesToUserByTenants);
        } catch (SQLException e) {
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
    public boolean deleteAllUserRoleAssociationsForRole(AppIdentifier appIdentifier, String role)
            throws StorageQueryException {
        try {
            return UserRolesQueries.deleteAllUserRoleAssociationsForRole(this, appIdentifier, role);
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
    public List<String> doesMultipleRoleExist_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                           List<String> roles) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserRolesQueries.doesMultipleRoleExist_transaction(this, sqlCon, appIdentifier, roles);
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
    public void createBulkUserIdMapping(AppIdentifier appIdentifier,
                                        Map<String, String> superTokensUserIdToExternalUserId)
            throws StorageQueryException {
        try {

            UserIdMappingQueries.createBulkUserIdMapping(this, appIdentifier, superTokensUserIdToExternalUserId);
        } catch (SQLException e) {
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
    public HashMap<String, String> getUserIdMappingForSuperTokensIds(AppIdentifier appIdentifier,
                                                                     ArrayList<String> userIds)
            throws StorageQueryException {
        try {
            return UserIdMappingQueries.getUserIdMappingWithUserIds(this, appIdentifier, userIds);
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

    public void addTenantIdInTargetStorage_Transaction(Connection con, TenantIdentifier tenantIdentifier)
            throws DuplicateTenantException, StorageQueryException {
        try {
            MultitenancyQueries.addTenantIdInTargetStorage_Transaction(this, con, tenantIdentifier);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                if (isPrimaryKeyError(((PSQLException) e).getServerErrorMessage(),
                        config.getTenantsTable())) {
                    throw new DuplicateTenantException();
                }
            }
            throw new StorageQueryException(e);
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
    public boolean addUserIdToTenant_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                 String userId)
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
                    createDevice_Transaction(con, appIdentifier, device);
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
    public TOTPDevice createDevice_Transaction(TransactionConnection con, AppIdentifier appIdentifier,
                                               TOTPDevice device)
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
    public void createDevices_Transaction(TransactionConnection con, AppIdentifier appIdentifier,
                                          List<TOTPDevice> devices)
            throws StorageQueryException, TenantOrAppNotFoundException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            TOTPQueries.createDevices_Transaction(this, sqlCon, appIdentifier, devices);
        } catch (SQLException e) {
            Exception actualException = e;

            if (actualException instanceof PSQLException) {
                ServerErrorMessage errMsg = ((PSQLException) actualException).getServerErrorMessage();
                if (isForeignKeyConstraintError(errMsg, Config.getConfig(this).getTotpUsersTable(), "app_id")) {
                    throw new TenantOrAppNotFoundException(appIdentifier);
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public TOTPDevice getDeviceByName_Transaction(TransactionConnection con, AppIdentifier appIdentifier, String userId,
                                                  String deviceName) throws StorageQueryException {
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
    public List<ConfigFieldInfo> getPluginConfigFieldsInfo() {
        return PostgreSQLConfig.getConfigFieldsInfoForDashboard(this);
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
    public List<AuthRecipeUserInfo> getPrimaryUsersByIds_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                             List<String> userIds)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return GeneralQueries.getPrimaryUserInfosForUserIds_Transaction(this, sqlCon, appIdentifier, userIds);
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
    public AuthRecipeUserInfo[] listPrimaryUsersByMultipleEmailsOrPhoneNumbersOrThirdparty_Transaction(
            AppIdentifier appIdentifier, TransactionConnection con, List<String> emails, List<String> phones,
            Map<String, String> thirdpartyIdToThirdpartyUserId) throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return GeneralQueries.listPrimaryUsersByMultipleEmailsOrPhonesOrThirdParty_Transaction(this, sqlCon,
                    appIdentifier, emails, phones, thirdpartyIdToThirdpartyUserId);
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
    public void makePrimaryUsers_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                             List<String> userIds) throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            GeneralQueries.makePrimaryUsers_Transaction(this, sqlCon, appIdentifier, userIds);
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
    public void linkMultipleAccounts_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                 Map<String, String> recipeUserIdByPrimaryUserId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            GeneralQueries.linkMultipleAccounts_Transaction(this, sqlCon, appIdentifier, recipeUserIdByPrimaryUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }

    }

    @Override
    public void unlinkAccounts_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String primaryUserId,
                                           String recipeUserId)
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
    public int countUsersThatHaveMoreThanOneLoginMethodAndActiveSince(AppIdentifier appIdentifier, long sinceTime)
            throws StorageQueryException {
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
                return UserIdMappingQueries.getuseraIdMappingWithSuperTokensUserId_Transaction(this, sqlCon,
                        appIdentifier,
                        userId);
            }

            return UserIdMappingQueries.getUserIdMappingWithExternalUserId_Transaction(this, sqlCon, appIdentifier,
                    userId);
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
    public List<UserIdMapping> getMultipleUserIdMapping_Transaction(TransactionConnection connection,
                                                            AppIdentifier appIdentifier, List<String> userIds,
                                                            boolean isSupertokensIds)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) connection.getConnection();
            List<UserIdMapping> result;
            if(isSupertokensIds){
                result = UserIdMappingQueries.getMultipleUserIdMappingWithSupertokensUserId_Transaction(this,
                        sqlCon, appIdentifier, userIds);
            } else {
                result = UserIdMappingQueries.getMultipleUserIdMappingWithExternalUserId_Transaction(this,
                        sqlCon, appIdentifier, userIds);
            }
            return result;
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int getUsersCountWithMoreThanOneLoginMethodOrTOTPEnabled(AppIdentifier appIdentifier)
            throws StorageQueryException {
        try {
            return GeneralQueries.getUsersCountWithMoreThanOneLoginMethodOrTOTPEnabled(this, appIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int countUsersThatHaveMoreThanOneLoginMethodOrTOTPEnabledAndActiveSince(AppIdentifier appIdentifier,
                                                                                   long sinceTime)
            throws StorageQueryException {
        try {
            return ActiveUsersQueries.countUsersThatHaveMoreThanOneLoginMethodOrTOTPEnabledAndActiveSince(this,
                    appIdentifier, sinceTime);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }


    @Override
    public boolean deleteOAuthClient(AppIdentifier appIdentifier, String clientId) throws StorageQueryException {
        try {
            return OAuthQueries.deleteOAuthClient(this, clientId, appIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }



    @TestOnly
    public int getDbActivityCount(String dbname) throws SQLException, StorageQueryException {
        String QUERY = "SELECT COUNT(*) as c FROM pg_stat_activity WHERE datname = ?;";
        return execute(this, QUERY, pst -> {
            pst.setString(1, dbname);
        }, result -> {
            if (result.next()) {
                return result.getInt("c");
            }
            return -1;
        });
    }

    @Override
    public void addBulkImportUsers(AppIdentifier appIdentifier, List<BulkImportUser> users)
            throws StorageQueryException,
            TenantOrAppNotFoundException {
        try {
            this.startTransaction(con -> {
                try {
                    BulkImportQueries.insertBulkImportUsers_Transaction(this, (Connection) con.getConnection(), appIdentifier, users);
                } catch (SQLException e) {
                    if (e instanceof BatchUpdateException batchUpdateException) {
                        SQLException nextException = batchUpdateException.getNextException();
                        if(nextException instanceof PSQLException){
                            ServerErrorMessage serverErrorMessage = ((PSQLException) nextException).getServerErrorMessage();
                            if (isPrimaryKeyError(serverErrorMessage, Config.getConfig(this).getBulkImportUsersTable())) {
                                throw new StorageTransactionLogicException(new io.supertokens.pluginInterface.bulkimport.exceptions.DuplicateUserIdException());
                            }
                            if (isForeignKeyConstraintError(serverErrorMessage, Config.getConfig(this).getBulkImportUsersTable(),
                                    "app_id")) {
                                throw new TenantOrAppNotFoundException(appIdentifier);
                            }
                        }

                    }
                }
                return null;
            });
        } catch (StorageTransactionLogicException e) {
            if(e.actualException instanceof StorageQueryException) {
                throw (StorageQueryException) e.actualException;
            } else {
                throw new StorageQueryException(e.actualException);
            }
        }

    }

    @Override
    public OAuthClient getOAuthClientById(AppIdentifier appIdentifier, String clientId)
            throws StorageQueryException, OAuthClientNotFoundException {
        try {
            OAuthClient client =  OAuthQueries.getOAuthClientById(this, clientId, appIdentifier);
            if (client == null) {
                throw new OAuthClientNotFoundException();
            }
            return client;
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addOrUpdateOauthClient(AppIdentifier appIdentifier, String clientId, String clientSecret, boolean isClientCredentialsOnly, boolean enableRefreshTokenRotation)
            throws StorageQueryException, TenantOrAppNotFoundException {
        try {
            OAuthQueries.addOrUpdateOauthClient(this, appIdentifier, clientId, clientSecret, isClientCredentialsOnly, enableRefreshTokenRotation);
        } catch (SQLException e) {
            ServerErrorMessage errorMessage = ((PSQLException) e).getServerErrorMessage();
            PostgreSQLConfig config = Config.getConfig(this);

            if (isForeignKeyConstraintError(
                    errorMessage,
                    config.getOAuthClientsTable(),
                    "app_id")) {
                throw new TenantOrAppNotFoundException(appIdentifier);
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public List<BulkImportUser> getBulkImportUsers(AppIdentifier appIdentifier, @Nonnull Integer limit, @Nullable BULK_IMPORT_USER_STATUS status,
            @Nullable String bulkImportUserId, @Nullable Long createdAt) throws StorageQueryException {
        try {
            return BulkImportQueries.getBulkImportUsers(this, appIdentifier, limit, status, bulkImportUserId, createdAt);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public List<OAuthClient> getOAuthClients(AppIdentifier appIdentifier, List<String> clientIds) throws StorageQueryException {
        try {
            return OAuthQueries.getOAuthClients(this, appIdentifier, clientIds);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateBulkImportUserStatus_Transaction(AppIdentifier appIdentifier, TransactionConnection con, @Nonnull String bulkImportUserId, @Nonnull BULK_IMPORT_USER_STATUS status, @Nullable String errorMessage)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            BulkImportQueries.updateBulkImportUserStatus_Transaction(this, sqlCon, appIdentifier, bulkImportUserId, status, errorMessage);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateMultipleBulkImportUsersStatusToError_Transaction(AppIdentifier appIdentifier,
                                                                       TransactionConnection con,
                                                                       @NotNull Map<String, String> bulkImportUserIdToErrorMessage)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            BulkImportQueries.updateMultipleBulkImportUsersStatusToError_Transaction(this, sqlCon, appIdentifier,
                    bulkImportUserIdToErrorMessage);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean revokeOAuthTokenByGID(AppIdentifier appIdentifier, String gid) throws StorageQueryException {
        try {
            return OAuthQueries.deleteOAuthSessionByGID(this, appIdentifier, gid);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean revokeOAuthTokenByClientId(AppIdentifier appIdentifier, String clientId)
            throws StorageQueryException {
        try {
            return OAuthQueries.deleteOAuthSessionByClientId(this, appIdentifier, clientId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public List<String> deleteBulkImportUsers(AppIdentifier appIdentifier, @Nonnull String[] bulkImportUserIds) throws StorageQueryException {
        try {
            return BulkImportQueries.deleteBulkImportUsers(this, appIdentifier, bulkImportUserIds);
        } catch (SQLException e) {
            Logging.error(this, "Error deleting bulk import users", true, e);
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean revokeOAuthTokenByJTI(AppIdentifier appIdentifier, String gid, String jti)
            throws StorageQueryException {
        try {
            return OAuthQueries.deleteJTIFromOAuthSession(this, appIdentifier, gid, jti);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public List<BulkImportUser> getBulkImportUsersAndChangeStatusToProcessing(AppIdentifier appIdentifier, @Nonnull Integer limit) throws StorageQueryException {
        try {
            return BulkImportQueries.getBulkImportUsersAndChangeStatusToProcessing(this, appIdentifier, limit);
        } catch (StorageTransactionLogicException e) {
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public void updateBulkImportUserPrimaryUserId(AppIdentifier appIdentifier, @Nonnull String bulkImportUserId, @Nonnull String primaryUserId) throws StorageQueryException {
        try {
            BulkImportQueries.updateBulkImportUserPrimaryUserId(this, appIdentifier, bulkImportUserId, primaryUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean revokeOAuthTokenBySessionHandle(AppIdentifier appIdentifier, String sessionHandle)
            throws StorageQueryException {
        try {
            return OAuthQueries.deleteOAuthSessionBySessionHandle(this, appIdentifier, sessionHandle);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public long getBulkImportUsersCount(AppIdentifier appIdentifier, @Nullable BULK_IMPORT_USER_STATUS status) throws StorageQueryException {
        try {
            return BulkImportQueries.getBulkImportUsersCount(this, appIdentifier, status);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addOAuthM2MTokenForStats(AppIdentifier appIdentifier, String clientId, long iat, long exp)
            throws StorageQueryException, OAuthClientNotFoundException {
        try {
            OAuthQueries.addOAuthM2MTokenForStats(this, appIdentifier, clientId, iat, exp);
        } catch (SQLException e) {
            ServerErrorMessage errorMessage = ((PSQLException) e).getServerErrorMessage();
            PostgreSQLConfig config = Config.getConfig(this);

            if (isForeignKeyConstraintError(
                    errorMessage,
                    config.getOAuthM2MTokensTable(),
                    "client_id")) {
                throw new OAuthClientNotFoundException();
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteExpiredOAuthM2MTokens(long exp) throws StorageQueryException {
        try {
            OAuthQueries.deleteExpiredOAuthM2MTokens(this, exp);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addOAuthLogoutChallenge(AppIdentifier appIdentifier, String challenge, String clientId,
                                        String postLogoutRedirectionUri, String sessionHandle, String state, long timeCreated)
            throws StorageQueryException, DuplicateOAuthLogoutChallengeException, OAuthClientNotFoundException {
        try {
            OAuthQueries.addOAuthLogoutChallenge(this, appIdentifier, challenge, clientId, postLogoutRedirectionUri, sessionHandle, state, timeCreated);
        } catch (SQLException e) {
            ServerErrorMessage errorMessage = ((PSQLException) e).getServerErrorMessage();
            PostgreSQLConfig config = Config.getConfig(this);

            if (isPrimaryKeyError(errorMessage, config.getOAuthLogoutChallengesTable())) {
                throw new DuplicateOAuthLogoutChallengeException();
            } else if (isForeignKeyConstraintError(
                    errorMessage,
                    config.getOAuthLogoutChallengesTable(),
                    "client_id")) {
                throw new OAuthClientNotFoundException();
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public OAuthLogoutChallenge getOAuthLogoutChallenge(AppIdentifier appIdentifier, String challenge) throws StorageQueryException {
        try {
            return OAuthQueries.getOAuthLogoutChallenge(this, appIdentifier, challenge);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteOAuthLogoutChallenge(AppIdentifier appIdentifier, String challenge) throws StorageQueryException {
        try {
            OAuthQueries.deleteOAuthLogoutChallenge(this, appIdentifier, challenge);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteOAuthLogoutChallengesBefore(long time) throws StorageQueryException {
        try {
            OAuthQueries.deleteOAuthLogoutChallengesBefore(this, time);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void createOrUpdateOAuthSession(AppIdentifier appIdentifier, String gid, String clientId,
                                           String externalRefreshToken, String internalRefreshToken,
                                           String sessionHandle, String jti, long exp)
            throws StorageQueryException, OAuthClientNotFoundException {
        try {
            OAuthQueries.createOrUpdateOAuthSession(this, appIdentifier, gid, clientId, externalRefreshToken,
                    internalRefreshToken, sessionHandle, jti, exp);
        } catch (SQLException e) {
            ServerErrorMessage errorMessage = ((PSQLException) e).getServerErrorMessage();
            PostgreSQLConfig config = Config.getConfig(this);

            if (isForeignKeyConstraintError(
                    errorMessage,
                    config.getOAuthSessionsTable(),
                    "client_id")) {
                throw new OAuthClientNotFoundException();
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String getRefreshTokenMapping(AppIdentifier appIdentifier, String externalRefreshToken)
            throws StorageQueryException {
        try {
            return OAuthQueries.getRefreshTokenMapping(this, appIdentifier, externalRefreshToken);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteExpiredOAuthSessions(long exp) throws StorageQueryException {
        try {
            OAuthQueries.deleteExpiredOAuthSessions(this, exp);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int countTotalNumberOfOAuthClients(AppIdentifier appIdentifier) throws StorageQueryException {
        try {
            return OAuthQueries.countTotalNumberOfClients(this, appIdentifier, false);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int countTotalNumberOfClientCredentialsOnlyOAuthClients(AppIdentifier appIdentifier)
            throws StorageQueryException {
        try {
            return OAuthQueries.countTotalNumberOfClients(this, appIdentifier, true);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int countTotalNumberOfOAuthM2MTokensCreatedSince(AppIdentifier appIdentifier, long since)
            throws StorageQueryException {
        try {
            return OAuthQueries.countTotalNumberOfOAuthM2MTokensCreatedSince(this, appIdentifier, since);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int countTotalNumberOfOAuthM2MTokensAlive(AppIdentifier appIdentifier) throws StorageQueryException {
        try {
            return OAuthQueries.countTotalNumberOfOAuthM2MTokensAlive(this, appIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean isOAuthTokenRevokedByGID(AppIdentifier appIdentifier, String gid) throws StorageQueryException {
        try {
            return !OAuthQueries.isOAuthSessionExistsByGID(this, appIdentifier, gid);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean isOAuthTokenRevokedByJTI(AppIdentifier appIdentifier, String gid, String jti)
            throws StorageQueryException {
        try {
            return !OAuthQueries.isOAuthSessionExistsByJTI(this, appIdentifier, gid, jti);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public WebAuthNStoredCredential saveCredentials(TenantIdentifier tenantIdentifier, WebAuthNStoredCredential credential)
            throws StorageQueryException,
            io.supertokens.pluginInterface.webauthn.exceptions.DuplicateCredentialException,
            TenantOrAppNotFoundException, io.supertokens.pluginInterface.webauthn.exceptions.UserIdNotFoundException {
        try {
            return WebAuthNQueries.saveCredential(this, tenantIdentifier, credential);
        } catch (SQLException e) {
            ServerErrorMessage errorMessage = ((PSQLException) e).getServerErrorMessage();
            PostgreSQLConfig config = Config.getConfig(this);

            if (isPrimaryKeyError(errorMessage, config.getWebAuthNCredentialsTable())) {
                throw new io.supertokens.pluginInterface.webauthn.exceptions.DuplicateCredentialException();
            } else if (isForeignKeyConstraintError(
                    errorMessage,
                    config.getWebAuthNCredentialsTable(),
                    "user_id")) {
                throw new io.supertokens.pluginInterface.webauthn.exceptions.UserIdNotFoundException();
            } else if (isForeignKeyConstraintError(
                    errorMessage,
                    config.getAppsTable(),
                    "app_id")) {
                throw new TenantOrAppNotFoundException(tenantIdentifier.toAppIdentifier());
            } else if (isForeignKeyConstraintError(
                    errorMessage,
                    config.getTenantsTable(),
                    "tenant_id")) {
                throw new TenantOrAppNotFoundException(tenantIdentifier);
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public WebAuthNOptions saveGeneratedOptions(TenantIdentifier tenantIdentifier, WebAuthNOptions optionsToSave)
            throws StorageQueryException, DuplicateOptionsIdException, TenantOrAppNotFoundException {
        try {
            return WebAuthNQueries.saveOptions(this, tenantIdentifier, optionsToSave);
        } catch (SQLException e) {
            ServerErrorMessage errorMessage = ((PSQLException) e).getServerErrorMessage();
            PostgreSQLConfig config = Config.getConfig(this);

            if (isPrimaryKeyError(errorMessage, config.getWebAuthNGeneratedOptionsTable())) {
                throw new DuplicateOptionsIdException();
            } else if (isForeignKeyConstraintError(
                    errorMessage,
                    config.getAppsTable(),
                    "app_id")) {
                throw new TenantOrAppNotFoundException(tenantIdentifier.toAppIdentifier());
            } else if (isForeignKeyConstraintError(
                    errorMessage,
                    config.getTenantsTable(),
                    "tenant_id")) {
                throw new TenantOrAppNotFoundException(tenantIdentifier);
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public WebAuthNOptions loadOptionsById(TenantIdentifier tenantIdentifier, String optionsId)
            throws StorageQueryException {
        try {
            return WebAuthNQueries.loadOptionsById(this, tenantIdentifier, optionsId);
        } catch (SQLException e){
            throw new StorageQueryException(e);
        }
    }

    @Override
    public WebAuthNStoredCredential loadCredentialByIdForUser(TenantIdentifier tenantIdentifier, String credentialId, String recipeUserId)
            throws StorageQueryException {
        try {
            return WebAuthNQueries.loadCredentialByIdForUser(this, tenantIdentifier, credentialId, recipeUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }


    @Override
    public WebAuthNOptions loadOptionsById_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                       String optionsId) throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return WebAuthNQueries.loadOptionsById_Transaction(this, sqlCon, tenantIdentifier, optionsId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public WebAuthNStoredCredential loadCredentialById_Transaction(TenantIdentifier tenantIdentifier,
                                                                   TransactionConnection con, String credentialId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return WebAuthNQueries.loadCredentialById_Transaction(this, sqlCon, tenantIdentifier, credentialId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo signUpWithCredentialsRegister_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                                        String userId, String email, String relyingPartyId, WebAuthNStoredCredential credential)
            throws StorageQueryException, io.supertokens.pluginInterface.webauthn.exceptions.DuplicateUserIdException, TenantOrAppNotFoundException,
            DuplicateUserEmailException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return WebAuthNQueries.signUpWithCredentialRegister_Transaction(this, sqlCon, tenantIdentifier, userId, email, relyingPartyId, credential);
        } catch (StorageTransactionLogicException stle) {
            if (stle.actualException instanceof SQLException) {
                ServerErrorMessage errorMessage = ((PSQLException) stle.actualException).getServerErrorMessage();
                PostgreSQLConfig config = Config.getConfig(this);

                if (isUniqueConstraintError(errorMessage, config.getWebAuthNUserToTenantTable(),"email")) {
                    Logging.error(this, errorMessage.getMessage(), true);
                    Logging.error(this, email, true);
                    throw new DuplicateUserEmailException();
                } else if (isPrimaryKeyError(errorMessage, config.getWebAuthNUsersTable())
                        || isPrimaryKeyError(errorMessage, config.getUsersTable())
                        || isPrimaryKeyError(errorMessage, config.getWebAuthNUserToTenantTable())
                        || isPrimaryKeyError(errorMessage, config.getAppIdToUserIdTable())) {
                    throw new io.supertokens.pluginInterface.webauthn.exceptions.DuplicateUserIdException();
                } else if (isForeignKeyConstraintError(
                        errorMessage,
                        config.getAppsTable(),
                        "app_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier.toAppIdentifier());
                } else if (isForeignKeyConstraintError(
                        errorMessage,
                        config.getTenantsTable(),
                        "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);
                }
            }

            throw new StorageQueryException(stle.actualException);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo signUp_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                 String userId, String email, String relyingPartyId)
            throws StorageQueryException, TenantOrAppNotFoundException, DuplicateUserEmailException,
            io.supertokens.pluginInterface.webauthn.exceptions.DuplicateUserIdException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return WebAuthNQueries.signUp_Transaction(this, sqlCon, tenantIdentifier, userId, email, relyingPartyId);
        } catch (StorageTransactionLogicException stle) {
            if (stle.actualException instanceof SQLException) {
                ServerErrorMessage errorMessage = ((PSQLException) stle.actualException).getServerErrorMessage();
                PostgreSQLConfig config = Config.getConfig(this);

                if (isUniqueConstraintError(errorMessage, config.getWebAuthNUserToTenantTable(),"email")) {
                    throw new DuplicateUserEmailException();
                } else if (isPrimaryKeyError(errorMessage, config.getWebAuthNUsersTable())
                        || isPrimaryKeyError(errorMessage, config.getUsersTable())
                        || isPrimaryKeyError(errorMessage, config.getWebAuthNUserToTenantTable())
                        || isPrimaryKeyError(errorMessage, config.getAppIdToUserIdTable())) {
                    throw new io.supertokens.pluginInterface.webauthn.exceptions.DuplicateUserIdException();
                } else if (isForeignKeyConstraintError(
                        errorMessage,
                        config.getAppsTable(),
                        "app_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier.toAppIdentifier());
                } else if (isForeignKeyConstraintError(
                        errorMessage,
                        config.getTenantsTable(),
                        "tenant_id")) {
                    throw new TenantOrAppNotFoundException(tenantIdentifier);
                }
            }

            throw new StorageQueryException(stle.actualException);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo getUserInfoByCredentialId_Transaction(TenantIdentifier tenantIdentifier,
                                                                    TransactionConnection con, String credentialId)
            throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            return WebAuthNQueries.getUserInfoByCredentialId_Transaction(this, sqlCon, tenantIdentifier, credentialId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateCounter_Transaction(TenantIdentifier tenantIdentifier,
                                          TransactionConnection con, String credentialId,
                                          long counter) throws StorageQueryException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            WebAuthNQueries.updateCounter_Transaction(this, sqlCon, tenantIdentifier, credentialId, counter);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addRecoverAccountToken(TenantIdentifier tenantIdentifier, AccountRecoveryTokenInfo accountRecoveryTokenInfo)
            throws DuplicateRecoverAccountTokenException, StorageQueryException {
        try {
            WebAuthNQueries.addRecoverAccountToken(this, tenantIdentifier, accountRecoveryTokenInfo);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void removeCredential(TenantIdentifier tenantIdentifier, String userId, String credentialId)
            throws StorageQueryException, WebauthNCredentialNotExistsException {
        try {
            int rowsUpdated = WebAuthNQueries.removeCredential(this, tenantIdentifier, userId, credentialId);
            if(rowsUpdated < 1) {
                throw new WebauthNCredentialNotExistsException();
            }
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void removeOptions(TenantIdentifier tenantIdentifier, String optionsId)
            throws StorageQueryException, WebauthNOptionsNotExistsException {
        try {
            int rowsUpdated = WebAuthNQueries.removeOptions(this, tenantIdentifier, optionsId);
            if(rowsUpdated < 1) {
                throw new WebauthNOptionsNotExistsException();
            }
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public List<WebAuthNStoredCredential> listCredentialsForUser(TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException {
        try {
            return WebAuthNQueries.listCredentials(this, tenantIdentifier, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateUserEmail(TenantIdentifier tenantIdentifier, String userId, String newEmail)
            throws StorageQueryException, io.supertokens.pluginInterface.webauthn.exceptions.UserIdNotFoundException,
            DuplicateUserEmailException {
        try {
            WebAuthNQueries.updateUserEmail(this, tenantIdentifier, userId, newEmail);
        } catch (StorageQueryException e) {
            if (e.getCause() instanceof SQLException){
                ServerErrorMessage errorMessage = ((PSQLException) e.getCause()).getServerErrorMessage();
                PostgreSQLConfig config = Config.getConfig(this);

                if (isUniqueConstraintError(errorMessage, config.getWebAuthNUserToTenantTable(),
                        "email")) {
                    throw new DuplicateUserEmailException();
                } else if (isForeignKeyConstraintError(errorMessage,config.getWebAuthNUserToTenantTable(),"user_id")) {
                    throw new io.supertokens.pluginInterface.webauthn.exceptions.UserIdNotFoundException();
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateUserEmail_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con, String userId,
                                            String newEmail)
            throws StorageQueryException, io.supertokens.pluginInterface.webauthn.exceptions.UserIdNotFoundException,
            DuplicateUserEmailException {
        try {
            Connection sqlCon = (Connection) con.getConnection();
            WebAuthNQueries.updateUserEmail_Transaction(this, sqlCon, tenantIdentifier, userId, newEmail);
        } catch (StorageQueryException e) {
            if (e.getCause() instanceof SQLException){
                ServerErrorMessage errorMessage = ((PSQLException) e.getCause()).getServerErrorMessage();
                PostgreSQLConfig config = Config.getConfig(this);

                if (isUniqueConstraintError(errorMessage, config.getWebAuthNUserToTenantTable(),
                        "email")) {
                    throw new DuplicateUserEmailException();
                } else if (isForeignKeyConstraintError(errorMessage,config.getWebAuthNUserToTenantTable(),"user_id")) {
                    throw new io.supertokens.pluginInterface.webauthn.exceptions.UserIdNotFoundException();
                }
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AccountRecoveryTokenInfo getAccountRecoveryTokenInfoByToken_Transaction(TenantIdentifier tenantIdentifier,
                                                                                   TransactionConnection con,
                                                                                   String token)
            throws StorageQueryException {

        Connection sqlCon = (Connection) con.getConnection();
        try {
            return WebAuthNQueries.getAccountRecoveryTokenInfoByToken_Transaction(this, tenantIdentifier, sqlCon, token);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteAccountRecoveryTokenByEmail_Transaction(TenantIdentifier tenantIdentifier,
                                                              TransactionConnection con, String email)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            WebAuthNQueries.deleteAccountRecoveryTokenByEmail_Transaction(this, sqlCon, tenantIdentifier, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteExpiredAccountRecoveryTokens() throws StorageQueryException {
        try {
            WebAuthNQueries.deleteExpiredAccountRecoveryTokens(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteExpiredGeneratedOptions() throws StorageQueryException {
        try {
            WebAuthNQueries.deleteExpiredGeneratedOptions(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public SAMLClient createOrUpdateSAMLClient(TenantIdentifier tenantIdentifier, SAMLClient samlClient)
            throws StorageQueryException {
        try {
            return SAMLQueries.createOrUpdateSAMLClient(this, tenantIdentifier, samlClient.clientId, samlClient.clientSecret,
                    samlClient.ssoLoginURL, samlClient.redirectURIs.toString(), samlClient.defaultRedirectURI,
                    samlClient.idpEntityId, samlClient.idpSigningCertificate,
                    samlClient.allowIDPInitiatedLogin, samlClient.enableRequestSigning);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean removeSAMLClient(TenantIdentifier tenantIdentifier, String clientId) throws StorageQueryException {
        try {
            return SAMLQueries.removeSAMLClient(this, tenantIdentifier, clientId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public SAMLClient getSAMLClient(TenantIdentifier tenantIdentifier, String clientId) throws StorageQueryException {
        try {
            return SAMLQueries.getSAMLClient(this, tenantIdentifier, clientId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public SAMLClient getSAMLClientByIDPEntityId(TenantIdentifier tenantIdentifier, String idpEntityId) throws StorageQueryException {
        try {
            return SAMLQueries.getSAMLClientByIDPEntityId(this, tenantIdentifier, idpEntityId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public List<SAMLClient> getSAMLClients(TenantIdentifier tenantIdentifier) throws StorageQueryException {
        try {
            return SAMLQueries.getSAMLClients(this, tenantIdentifier);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void saveRelayStateInfo(TenantIdentifier tenantIdentifier, SAMLRelayStateInfo relayStateInfo) throws StorageQueryException {
        try {
            SAMLQueries.saveRelayStateInfo(this, tenantIdentifier, relayStateInfo.relayState, relayStateInfo.clientId, relayStateInfo.state, relayStateInfo.redirectURI);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public SAMLRelayStateInfo getRelayStateInfo(TenantIdentifier tenantIdentifier, String relayState) throws StorageQueryException {
        try {
            return SAMLQueries.getRelayStateInfo(this, tenantIdentifier, relayState);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void saveSAMLClaims(TenantIdentifier tenantIdentifier, String clientId, String code, JsonObject claims) throws StorageQueryException {
        try {
            SAMLQueries.saveSAMLClaims(this, tenantIdentifier, clientId, code, claims.toString());
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public SAMLClaimsInfo getSAMLClaimsAndRemoveCode(TenantIdentifier tenantIdentifier, String code) throws StorageQueryException {
        try {
            return SAMLQueries.getSAMLClaimsAndRemoveCode(this, tenantIdentifier, code);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void removeExpiredSAMLCodesAndRelayStates() throws StorageQueryException {
        try {
            SAMLQueries.removeExpiredSAMLCodesAndRelayStates(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }
}
