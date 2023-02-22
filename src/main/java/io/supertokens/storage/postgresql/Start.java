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
import io.supertokens.pluginInterface.KeyValueInfo;
import io.supertokens.pluginInterface.LOG_LEVEL;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.emailpassword.PasswordResetTokenInfo;
import io.supertokens.pluginInterface.emailpassword.UserInfo;
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
import io.supertokens.pluginInterface.multitenancy.exceptions.DuplicateTenantException;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
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
import io.supertokens.pluginInterface.useridmapping.UserIdMapping;
import io.supertokens.pluginInterface.useridmapping.UserIdMappingStorage;
import io.supertokens.pluginInterface.useridmapping.exception.UnknownSuperTokensUserIdException;
import io.supertokens.pluginInterface.useridmapping.exception.UserIdMappingAlreadyExistsException;
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
        MultitenancyStorage {

    private static final Object appenderLock = new Object();
    public static boolean silent = false;
    private ResourceDistributor resourceDistributor = new ResourceDistributor();
    private String processId;
    private HikariLoggingAppender appender = new HikariLoggingAppender(this);
    private static final String APP_ID_KEY_NAME = "app_id";
    private static final String ACCESS_TOKEN_SIGNING_KEY_NAME = "access_token_signing_key";
    private static final String REFRESH_TOKEN_KEY_NAME = "refresh_token_key";
    public static boolean isTesting = false;
    boolean enabled = true;
    Thread mainThread = Thread.currentThread();
    private Thread shutdownHook;

    public ResourceDistributor getResourceDistributor() {
        return resourceDistributor;
    }

    public String getProcessId() {
        return this.processId;
    }

    @Override
    public void constructor(String processId, boolean silent) {
        this.processId = processId;
        Start.silent = silent;
    }

    @Override
    public STORAGE_TYPE getType() {
        return STORAGE_TYPE.SQL;
    }

    @Override
    public void loadConfig(JsonObject configJson, Set<LOG_LEVEL> logLevels) throws InvalidConfigException {
        Config.loadConfig(this, configJson, logLevels);
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
        synchronized (appenderLock) {
            final Logger infoLog = (Logger) LoggerFactory.getLogger("com.zaxxer.hikari");
            if (infoLog.getAppender(HikariLoggingAppender.NAME) == null) {
                infoLog.setAdditive(false);
                infoLog.addAppender(appender);
            }
        }

    }

    @Override
    public void stopLogging() {
        Logging.stopLogging(this);

        synchronized (appenderLock) {
            final Logger infoLog = (Logger) LoggerFactory.getLogger("com.zaxxer.hikari");
            if (infoLog.getAppender(HikariLoggingAppender.NAME) != null) {
                infoLog.detachAppender(HikariLoggingAppender.NAME);
            }
        }
    }

    @Override
    public void initStorage() throws DbInitException {
        if (ConnectionPool.isAlreadyInitialised(this)) {
            return;
        }
        try {
            ConnectionPool.initPool(this);
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
                        || exceptionMessage.toLowerCase().contains("the transaction might succeed if retried") ||

                        // we have deadlock as well due to the DeadlockTest.java
                        exceptionMessage.toLowerCase().contains("deadlock");

                if ((isPSQLRollbackException || isDeadlockException) && tries < 3) {
                    try {
                        Thread.sleep((long) (10 + (Math.random() * 20)));
                    } catch (InterruptedException ignored) {
                    }
                    ProcessState.getInstance(this).addState(ProcessState.PROCESS_STATE.DEADLOCK_FOUND, e);
                    // this because deadlocks are not necessarily a result of faulty logic. They can happen
                    continue;
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
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return GeneralQueries.getKeyValue_Transaction(this, sqlCon, ACCESS_TOKEN_SIGNING_KEY_NAME);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void removeLegacyAccessTokenSigningKey_Transaction(AppIdentifier appIdentifier,
                                                              TransactionConnection con) throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            GeneralQueries.deleteKeyValue_Transaction(this, sqlCon, ACCESS_TOKEN_SIGNING_KEY_NAME);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public KeyValueInfo[] getAccessTokenSigningKeys_Transaction(AppIdentifier appIdentifier,
                                                                TransactionConnection con)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return SessionQueries.getAccessTokenSigningKeys_Transaction(this, sqlCon);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addAccessTokenSigningKey_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                     KeyValueInfo info)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            SessionQueries.addAccessTokenSigningKey_Transaction(this, sqlCon, info.createdAtTime, info.value);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void removeAccessTokenSigningKeysBefore(AppIdentifier appIdentifier, long time)
            throws StorageQueryException {
        try {
            // TODO..
            SessionQueries.removeAccessTokenSigningKeysBefore(this, time);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public KeyValueInfo getRefreshTokenSigningKey_Transaction(AppIdentifier appIdentifier,
                                                              TransactionConnection con) throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return GeneralQueries.getKeyValue_Transaction(this, sqlCon, REFRESH_TOKEN_KEY_NAME);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void setRefreshTokenSigningKey_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                      KeyValueInfo info)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            GeneralQueries.setKeyValue_Transaction(this, sqlCon, REFRESH_TOKEN_KEY_NAME, info);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @TestOnly
    @Override
    public void deleteAllInformation() throws StorageQueryException {
        try {
            GeneralQueries.deleteAllTables(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
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
                                 long createdAtTime)
            throws StorageQueryException {
        // TODO..
        try {
            SessionQueries.createNewSession(this, sessionHandle, userId, refreshTokenHash2, userDataInDatabase, expiry,
                    userDataInJWT, createdAtTime);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteSessionsOfUser(AppIdentifier appIdentifierIdentifier, String userId)
            throws StorageQueryException {
        try {
            // TODO..
            SessionQueries.deleteSessionsOfUser(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int getNumberOfSessions(TenantIdentifier tenantIdentifier) throws StorageQueryException {
        try {
            // TODO..
            return SessionQueries.getNumberOfSessions(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int deleteSession(TenantIdentifier tenantIdentifier, String[] sessionHandles) throws StorageQueryException {
        try {
            // TODO..
            return SessionQueries.deleteSession(this, sessionHandles);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String[] getAllNonExpiredSessionHandlesForUser(TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException {
        try {
            // TODO..
            return SessionQueries.getAllNonExpiredSessionHandlesForUser(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    private String[] getAllNonExpiredSessionHandlesForUser(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            // TODO..
            return SessionQueries.getAllNonExpiredSessionHandlesForUser(this, userId);
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
        // TODO..
        try {
            return GeneralQueries.getKeyValue(this, key);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void setKeyValue(TenantIdentifier tenantIdentifier, String key, KeyValueInfo info)
            throws StorageQueryException {
        // TODO..
        try {
            GeneralQueries.setKeyValue(this, key, info);
        } catch (SQLException e) {
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
            // TODO..
            return SessionQueries.getSession(this, sessionHandle);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int updateSession(TenantIdentifier tenantIdentifier, String sessionHandle, JsonObject sessionData,
                             JsonObject jwtPayload)
            throws StorageQueryException {
        try {
            // TODO..
            return SessionQueries.updateSession(this, sessionHandle, sessionData, jwtPayload);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public SessionInfo getSessionInfo_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                  String sessionHandle)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return SessionQueries.getSessionInfo_Transaction(this, sqlCon, sessionHandle);
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
            // TODO..
            SessionQueries.updateSessionInfo_Transaction(this, sqlCon, sessionHandle, refreshTokenHash2, expiry);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void setKeyValue_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con, String key,
                                        KeyValueInfo info)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            GeneralQueries.setKeyValue_Transaction(this, sqlCon, key, info);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public KeyValueInfo getKeyValue_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                String key) throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return GeneralQueries.getKeyValue_Transaction(this, sqlCon, key);
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
    public boolean canBeUsed(JsonObject configJson) {
        return Config.canBeUsed(configJson);
    }

    @Override
    public boolean isUserIdBeingUsedInNonAuthRecipe(AppIdentifier appIdentifier, String className, String userId)
            throws StorageQueryException {
        // TODO..
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
                return EmailVerificationQueries.isUserIdBeingUsedForEmailVerification(this, userId);
            } catch (SQLException e) {
                throw new StorageQueryException(e);
            }
        } else if (className.equals(JWTRecipeStorage.class.getName())) {
            return false;
        } else {
            throw new IllegalStateException("ClassName: " + className + " is not part of NonAuthRecipeStorage");
        }
    }

    @TestOnly
    @Override
    public void addInfoToNonAuthRecipesBasedOnUserId(String className, String userId) throws StorageQueryException {
        // add entries to nonAuthRecipe tables with input userId
        if (className.equals(SessionStorage.class.getName())) {
            try {
                createNewSession(new TenantIdentifier(null, null, null), "sessionHandle", userId, "refreshTokenHash",
                        new JsonObject(),
                        System.currentTimeMillis() + 1000000, new JsonObject(), System.currentTimeMillis());
            } catch (Exception e) {
                throw new StorageQueryException(e);
            }
        } else if (className.equals(UserRolesStorage.class.getName())) {
            try {
                String role = "testRole";
                this.startTransaction(con -> {
                    createNewRoleOrDoNothingIfExists_Transaction(new TenantIdentifier(null, null, null), con, role);
                    return null;
                });
                try {
                    addRoleToUser(new TenantIdentifier(null, null, null), userId, role);
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
                addEmailVerificationToken(new AppIdentifier(null, null), info);

            } catch (DuplicateEmailVerificationTokenException e) {
                throw new StorageQueryException(e);
            }
        } else if (className.equals(UserMetadataStorage.class.getName())) {
            JsonObject data = new JsonObject();
            data.addProperty("test", "testData");
            try {
                this.startTransaction(con -> {
                    setUserMetadata_Transaction(new AppIdentifier(null, null), con, userId, data);
                    return null;
                });
            } catch (StorageTransactionLogicException e) {
                throw new StorageQueryException(e);
            }
        } else if (className.equals(JWTRecipeStorage.class.getName())) {
            /* Since JWT recipe tables do not store userId we do not add any data to them */
        } else {
            throw new IllegalStateException("ClassName: " + className + " is not part of NonAuthRecipeStorage");
        }
    }

    @Override
    public void modifyConfigToAddANewUserPoolForTesting(JsonObject config, int poolNumber) {
        config.add("postgresql_database_name", new JsonPrimitive("st" + poolNumber));
    }

    @Override
    public void signUp(TenantIdentifier tenantIdentifier, UserInfo userInfo)
            throws StorageQueryException, DuplicateUserIdException, DuplicateEmailException {
        // TODO..
        try {
            EmailPasswordQueries.signUp(this, userInfo.id, userInfo.email, userInfo.passwordHash, userInfo.timeJoined);
        } catch (StorageTransactionLogicException eTemp) {
            Exception e = eTemp.actualException;
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();

                if (isUniqueConstraintError(serverMessage, config.getEmailPasswordUsersTable(), "email")) {
                    throw new DuplicateEmailException();
                } else if (isPrimaryKeyError(serverMessage, config.getEmailPasswordUsersTable())
                        || isPrimaryKeyError(serverMessage, config.getUsersTable())) {
                    throw new DuplicateUserIdException();
                }
            }

            // We keep the old exception detection logic to ensure backwards compatibility.
            // We could get here if the new logic hits a false negative,
            // e.g., in case someone renamed constraints/tables
            if (e.getMessage().contains("ERROR: duplicate key") && e.getMessage().contains("Key (email)")) {
                throw new DuplicateEmailException();
            } else if (e.getMessage().contains("ERROR: duplicate key") && e.getMessage().contains("Key (user_id)")) {
                throw new DuplicateUserIdException();
            }

            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteEmailPasswordUser(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        // TODO..
        try {
            EmailPasswordQueries.deleteUser(this, userId);
        } catch (StorageTransactionLogicException e) {
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public UserInfo getUserInfoUsingId(AppIdentifier appIdentifier, String id) throws StorageQueryException {
        // TODO..
        try {
            return EmailPasswordQueries.getUserInfoUsingId(this, id);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public UserInfo getUserInfoUsingEmail(TenantIdentifier tenantIdentifier, String email)
            throws StorageQueryException {
        // TODO..
        try {
            return EmailPasswordQueries.getUserInfoUsingEmail(this, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addPasswordResetToken(AppIdentifier appIdentifier, PasswordResetTokenInfo passwordResetTokenInfo)
            throws StorageQueryException, UnknownUserIdException, DuplicatePasswordResetTokenException {
        // TODO..
        try {
            EmailPasswordQueries.addPasswordResetToken(this, passwordResetTokenInfo.userId,
                    passwordResetTokenInfo.token, passwordResetTokenInfo.tokenExpiry);
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

            // We keep the old exception detection logic to ensure backwards compatibility.
            // We could get here if the new logic hits a false negative,
            // e.g., in case someone renamed constraints/tables
            if (e.getMessage().contains("ERROR: duplicate key") && e.getMessage().contains("Key (user_id, token)")) {
                throw new DuplicatePasswordResetTokenException();
            } else if (e.getMessage().contains("foreign key") && e.getMessage().contains("user_id")) {
                throw new UnknownUserIdException();
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordResetTokenInfo getPasswordResetTokenInfo(AppIdentifier appIdentifier, String token)
            throws StorageQueryException {
        // TODO..
        try {
            return EmailPasswordQueries.getPasswordResetTokenInfo(this, token);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordResetTokenInfo[] getAllPasswordResetTokenInfoForUser(AppIdentifier appIdentifier,
                                                                        String userId) throws StorageQueryException {
        // TODO..
        try {
            return EmailPasswordQueries.getAllPasswordResetTokenInfoForUser(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordResetTokenInfo[] getAllPasswordResetTokenInfoForUser_Transaction(AppIdentifier appIdentifier,
                                                                                    TransactionConnection con,
                                                                                    String userId)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return EmailPasswordQueries.getAllPasswordResetTokenInfoForUser_Transaction(this, sqlCon, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteAllPasswordResetTokensForUser_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                                String userId)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            EmailPasswordQueries.deleteAllPasswordResetTokensForUser_Transaction(this, sqlCon, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateUsersPassword_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId,
                                                String newPassword)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            EmailPasswordQueries.updateUsersPassword_Transaction(this, sqlCon, userId, newPassword);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateUsersEmail_Transaction(AppIdentifier appIdentifier, TransactionConnection conn, String userId,
                                             String email)
            throws StorageQueryException, DuplicateEmailException {
        // TODO...
        Connection sqlCon = (Connection) conn.getConnection();
        try {
            EmailPasswordQueries.updateUsersEmail_Transaction(this, sqlCon, userId, email);
        } catch (SQLException e) {
            if (e instanceof PSQLException && isUniqueConstraintError(((PSQLException) e).getServerErrorMessage(),
                    Config.getConfig(this).getEmailPasswordUsersTable(), "email")) {
                throw new DuplicateEmailException();
            }

            // We keep the old exception detection logic to ensure backwards compatibility.
            // We could get here if the new logic hits a false negative,
            // e.g., in case someone renamed constraints/tables
            if (e.getMessage().contains("ERROR: duplicate key") && e.getMessage().contains("Key (email)")) {
                throw new DuplicateEmailException();
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public UserInfo getUserInfoUsingId_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                   String userId)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return EmailPasswordQueries.getUserInfoUsingId_Transaction(this, sqlCon, userId);
        } catch (SQLException e) {
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
    public EmailVerificationTokenInfo[] getAllEmailVerificationTokenInfoForUser_Transaction(AppIdentifier appIdentifier,
                                                                                            TransactionConnection con,
                                                                                            String userId, String email)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return EmailVerificationQueries.getAllEmailVerificationTokenInfoForUser_Transaction(this, sqlCon, userId,
                    email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteAllEmailVerificationTokensForUser_Transaction(AppIdentifier appIdentifier,
                                                                    TransactionConnection con, String userId,
                                                                    String email) throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            EmailVerificationQueries.deleteAllEmailVerificationTokensForUser_Transaction(this, sqlCon, userId, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateIsEmailVerified_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId,
                                                  String email,
                                                  boolean isEmailVerified) throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            EmailVerificationQueries.updateUsersIsEmailVerified_Transaction(this, sqlCon, userId, email,
                    isEmailVerified);
        } catch (SQLException e) {
            boolean isPSQLPrimKeyError = e instanceof PSQLException && isPrimaryKeyError(
                    ((PSQLException) e).getServerErrorMessage(), Config.getConfig(this).getEmailVerificationTable());

            // We keep the old exception detection logic to ensure backwards compatibility.
            // We could get here if the new logic hits a false negative,
            // e.g., in case someone renamed constraints/tables
            boolean isDuplicateKeyError = e.getMessage().contains("ERROR: duplicate key")
                    && e.getMessage().contains("Key (user_id, email)");

            if (!isEmailVerified || (!isPSQLPrimKeyError && !isDuplicateKeyError)) {
                throw new StorageQueryException(e);
            }
            // we do not throw an error since the email is already verified
        }
    }

    @Override
    public void deleteEmailVerificationUserInfo(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        try {
            // TODO..
            EmailVerificationQueries.deleteUserInfo(this, userId);
        } catch (StorageTransactionLogicException e) {
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public void addEmailVerificationToken(AppIdentifier appIdentifier, EmailVerificationTokenInfo emailVerificationInfo)
            throws StorageQueryException, DuplicateEmailVerificationTokenException {
        try {
            // TODO..
            EmailVerificationQueries.addEmailVerificationToken(this, emailVerificationInfo.userId,
                    emailVerificationInfo.token, emailVerificationInfo.tokenExpiry, emailVerificationInfo.email);
        } catch (SQLException e) {
            if (e instanceof PSQLException && isPrimaryKeyError(((PSQLException) e).getServerErrorMessage(),
                    Config.getConfig(this).getEmailVerificationTokensTable())) {
                throw new DuplicateEmailVerificationTokenException();
            }

            // We keep the old exception detection logic to ensure backwards compatibility.
            // We could get here if the new logic hits a false negative,
            // e.g., in case someone renamed constraints/tables
            if (e.getMessage().contains("ERROR: duplicate key")
                    && e.getMessage().contains("Key (user_id, email, token)")) {
                throw new DuplicateEmailVerificationTokenException();
            }
            throw new StorageQueryException(e);
        }
    }

    @Override
    public EmailVerificationTokenInfo getEmailVerificationTokenInfo(AppIdentifier appIdentifier, String token)
            throws StorageQueryException {
        try {
            // TODO..
            return EmailVerificationQueries.getEmailVerificationTokenInfo(this, token);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void revokeAllTokens(AppIdentifier appIdentifier, String userId, String email) throws StorageQueryException {
        try {
            // TODO..
            EmailVerificationQueries.revokeAllTokens(this, userId, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void unverifyEmail(AppIdentifier appIdentifier, String userId, String email) throws StorageQueryException {
        try {
            // TODO..
            EmailVerificationQueries.unverifyEmail(this, userId, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public EmailVerificationTokenInfo[] getAllEmailVerificationTokenInfoForUser(AppIdentifier appIdentifier,
                                                                                String userId, String email)
            throws StorageQueryException {
        // TODO..
        try {
            return EmailVerificationQueries.getAllEmailVerificationTokenInfoForUser(this, userId, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean isEmailVerified(AppIdentifier appIdentifier, String userId, String email)
            throws StorageQueryException {
        try {
            // TODO..
            return EmailVerificationQueries.isEmailVerified(this, userId, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
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
    public io.supertokens.pluginInterface.thirdparty.UserInfo getUserInfoUsingId_Transaction(
            TenantIdentifier tenantIdentifier, TransactionConnection con,
            String thirdPartyId,
            String thirdPartyUserId)
            throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        try {
            // TODO..
            return ThirdPartyQueries.getUserInfoUsingId_Transaction(this, sqlCon, thirdPartyId, thirdPartyUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateUserEmail_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                            String thirdPartyId, String thirdPartyUserId,
                                            String newEmail) throws StorageQueryException {
        Connection sqlCon = (Connection) con.getConnection();
        // TODO..
        try {
            ThirdPartyQueries.updateUserEmail_Transaction(this, sqlCon, thirdPartyId, thirdPartyUserId, newEmail);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void signUp(TenantIdentifier tenantIdentifier, io.supertokens.pluginInterface.thirdparty.UserInfo userInfo)
            throws StorageQueryException, io.supertokens.pluginInterface.thirdparty.exception.DuplicateUserIdException,
            DuplicateThirdPartyUserException {
        // TODO..
        try {
            ThirdPartyQueries.signUp(this, userInfo);
        } catch (StorageTransactionLogicException eTemp) {
            Exception e = eTemp.actualException;
            if (e instanceof PSQLException) {
                ServerErrorMessage serverMessage = ((PSQLException) e).getServerErrorMessage();
                if (isPrimaryKeyError(serverMessage, Config.getConfig(this).getThirdPartyUsersTable())) {
                    throw new DuplicateThirdPartyUserException();
                } else if (isPrimaryKeyError(serverMessage, Config.getConfig(this).getUsersTable())) {
                    throw new io.supertokens.pluginInterface.thirdparty.exception.DuplicateUserIdException();
                }
            }

            // We keep the old exception detection logic to ensure backwards compatibility.
            // We could get here if the new logic hits a false negative,
            // e.g., in case someone renamed constraints/tables
            if (e.getMessage().contains("ERROR: duplicate key")
                    && e.getMessage().contains("Key (third_party_id, third_party_user_id)")) {
                throw new DuplicateThirdPartyUserException();
            } else if (e.getMessage().contains("ERROR: duplicate key") && e.getMessage().contains("Key (user_id)")) {
                throw new io.supertokens.pluginInterface.thirdparty.exception.DuplicateUserIdException();
            }

            throw new StorageQueryException(eTemp.actualException);
        }
    }

    @Override
    public void deleteThirdPartyUser(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        try {
            // TODO..
            ThirdPartyQueries.deleteUser(this, userId);
        } catch (StorageTransactionLogicException e) {
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public io.supertokens.pluginInterface.thirdparty.UserInfo getThirdPartyUserInfoUsingId(
            TenantIdentifier tenantIdentifier, String thirdPartyId,
            String thirdPartyUserId)
            throws StorageQueryException {
        // TODO..
        try {
            return ThirdPartyQueries.getThirdPartyUserInfoUsingId(this, thirdPartyId, thirdPartyUserId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public io.supertokens.pluginInterface.thirdparty.UserInfo getThirdPartyUserInfoUsingId(AppIdentifier appIdentifier,
                                                                                           String id)
            throws StorageQueryException {
        // TODO..
        try {
            return ThirdPartyQueries.getThirdPartyUserInfoUsingId(this, id);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public io.supertokens.pluginInterface.thirdparty.UserInfo[] getThirdPartyUsersByEmail(
            TenantIdentifier tenantIdentifier, @NotNull String email)
            throws StorageQueryException {
        // TODO..
        try {
            return ThirdPartyQueries.getThirdPartyUsersByEmail(this, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public long getUsersCount(TenantIdentifier tenantIdentifier, RECIPE_ID[] includeRecipeIds)
            throws StorageQueryException {
        // TODO..
        try {
            return GeneralQueries.getUsersCount(this, includeRecipeIds);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public AuthRecipeUserInfo[] getUsers(TenantIdentifier tenantIdentifier, @NotNull Integer limit,
                                         @NotNull String timeJoinedOrder,
                                         @Nullable RECIPE_ID[] includeRecipeIds, @Nullable String userId,
                                         @Nullable Long timeJoined)
            throws StorageQueryException {
        // TODO..
        try {
            return GeneralQueries.getUsers(this, limit, timeJoinedOrder, includeRecipeIds, userId, timeJoined);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean doesUserIdExist(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        // TODO..
        try {
            return GeneralQueries.doesUserIdExist(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean doesUserIdExist(TenantIdentifier tenantIdentifierIdentifier, String userId)
            throws StorageQueryException {
        // TODO:...
        try {
            return GeneralQueries.doesUserIdExist(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public List<JWTSigningKeyInfo> getJWTSigningKeys_Transaction(AppIdentifier appIdentifier, TransactionConnection con)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return JWTSigningQueries.getJWTSigningKeys_Transaction(this, sqlCon);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void setJWTSigningKey_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                             JWTSigningKeyInfo info)
            throws StorageQueryException, DuplicateKeyIdException {
        // TODO...
        Connection sqlCon = (Connection) con.getConnection();
        try {
            JWTSigningQueries.setJWTSigningKeyInfo_Transaction(this, sqlCon, info);
        } catch (SQLException e) {
            if (e instanceof PSQLException && isPrimaryKeyError(((PSQLException) e).getServerErrorMessage(),
                    Config.getConfig(this).getJWTSigningKeysTable())) {
                throw new DuplicateKeyIdException();
            }

            // We keep the old exception detection logic to ensure backwards compatibility.
            // We could get here if the new logic hits a false negative,
            // e.g., in case someone renamed constraints/tables
            if (e.getMessage().contains("ERROR: duplicate key") && e.getMessage().contains("Key (key_id)")) {
                throw new DuplicateKeyIdException();
            }

            throw new StorageQueryException(e);
        }
    }

    private boolean isUniqueConstraintError(ServerErrorMessage serverMessage, String tableName, String columnName) {
        return serverMessage.getSQLState().equals("23505") && serverMessage.getConstraint() != null
                && serverMessage.getConstraint().equals(tableName + "_" + columnName + "_key");
    }

    private boolean isForeignKeyConstraintError(ServerErrorMessage serverMessage, String tableName, String columnName) {
        return serverMessage.getSQLState().equals("23503") && serverMessage.getConstraint() != null
                && serverMessage.getConstraint().equals(tableName + "_" + columnName + "_fkey");
    }

    private boolean isPrimaryKeyError(ServerErrorMessage serverMessage, String tableName) {
        return serverMessage.getSQLState().equals("23505") && serverMessage.getConstraint() != null
                && serverMessage.getConstraint().equals(tableName + "_pkey");
    }

    @Override
    public PasswordlessDevice getDevice_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                    String deviceIdHash)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return PasswordlessQueries.getDevice_Transaction(this, sqlCon, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void incrementDeviceFailedAttemptCount_Transaction(TenantIdentifier tenantIdentifier,
                                                              TransactionConnection con, String deviceIdHash)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.incrementDeviceFailedAttemptCount_Transaction(this, sqlCon, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }

    }

    @Override
    public PasswordlessCode[] getCodesOfDevice_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                           String deviceIdHash)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return PasswordlessQueries.getCodesOfDevice_Transaction(this, sqlCon, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteDevice_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                         String deviceIdHash) throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteDevice_Transaction(this, sqlCon, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }

    }

    @Override
    public void deleteDevicesByPhoneNumber_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                       @Nonnull String phoneNumber)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteDevicesByPhoneNumber_Transaction(this, sqlCon, phoneNumber);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteDevicesByEmail_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                 @Nonnull String email)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteDevicesByEmail_Transaction(this, sqlCon, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteDevicesByPhoneNumber_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                       String phoneNumber, String userId) throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteDevicesByPhoneNumber_Transaction(this, sqlCon, phoneNumber);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteDevicesByEmail_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String email,
                                                 String userId) throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteDevicesByEmail_Transaction(this, sqlCon, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessCode getCodeByLinkCodeHash_Transaction(TenantIdentifier tenantIdentifier,
                                                              TransactionConnection con, String linkCodeHash)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return PasswordlessQueries.getCodeByLinkCodeHash_Transaction(this, sqlCon, linkCodeHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteCode_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                       String deviceIdHash) throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            PasswordlessQueries.deleteCode_Transaction(this, sqlCon, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void updateUserEmail_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId,
                                            String email)
            throws StorageQueryException, UnknownUserIdException, DuplicateEmailException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            int updated_rows = PasswordlessQueries.updateUserEmail_Transaction(this, sqlCon, userId, email);
            if (updated_rows != 1) {
                throw new UnknownUserIdException();
            }
        } catch (SQLException e) {

            if (e instanceof PSQLException) {
                if (isUniqueConstraintError(((PSQLException) e).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessUsersTable(), "email")) {
                    throw new DuplicateEmailException();

                }
            }
            throw new StorageQueryException(e);

        }
    }

    @Override
    public void updateUserPhoneNumber_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId,
                                                  String phoneNumber)
            throws StorageQueryException, UnknownUserIdException, DuplicatePhoneNumberException {
        // TODO...
        Connection sqlCon = (Connection) con.getConnection();
        try {
            int updated_rows = PasswordlessQueries.updateUserPhoneNumber_Transaction(this, sqlCon, userId, phoneNumber);

            if (updated_rows != 1) {
                throw new UnknownUserIdException();
            }

        } catch (SQLException e) {

            if (e instanceof PSQLException) {
                if (isUniqueConstraintError(((PSQLException) e).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessUsersTable(), "phone_number")) {
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
            DuplicateCodeIdException, DuplicateLinkCodeHashException {
        // TODO..
        if (email == null && phoneNumber == null) {
            throw new IllegalArgumentException("Both email and phoneNumber can't be null");
        }
        try {
            PasswordlessQueries.createDeviceWithCode(this, email, phoneNumber, linkCodeSalt, code);
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
            }

            throw new StorageQueryException(e);
        }
    }

    @Override
    public void createCode(TenantIdentifier tenantIdentifier, PasswordlessCode code)
            throws StorageQueryException, UnknownDeviceIdHash,
            DuplicateCodeIdException, DuplicateLinkCodeHashException {
        // TODO..
        try {
            PasswordlessQueries.createCode(this, code);
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
    public void createUser(TenantIdentifier tenantIdentifier, io.supertokens.pluginInterface.passwordless.UserInfo user)
            throws StorageQueryException,
            DuplicateEmailException, DuplicatePhoneNumberException, DuplicateUserIdException {
        try {
            // TODO..
            PasswordlessQueries.createUser(this, user);
        } catch (StorageTransactionLogicException e) {

            String message = e.actualException.getMessage();
            Exception actualException = e.actualException;

            if (actualException instanceof PSQLException) {
                if (isPrimaryKeyError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessUsersTable())
                        || isPrimaryKeyError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getUsersTable())) {
                    throw new DuplicateUserIdException();
                }

                if (isUniqueConstraintError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessUsersTable(), "email")) {
                    throw new DuplicateEmailException();
                }

                if (isUniqueConstraintError(((PSQLException) actualException).getServerErrorMessage(),
                        Config.getConfig(this).getPasswordlessUsersTable(), "phone_number")) {
                    throw new DuplicatePhoneNumberException();
                }

            }
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public void deletePasswordlessUser(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        try {
            // TODO..
            PasswordlessQueries.deleteUser(this, userId);
        } catch (StorageTransactionLogicException e) {
            throw new StorageQueryException(e.actualException);
        }
    }

    @Override
    public PasswordlessDevice getDevice(TenantIdentifier tenantIdentifier, String deviceIdHash)
            throws StorageQueryException {
        try {
            // TODO..
            return PasswordlessQueries.getDevice(this, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessDevice[] getDevicesByEmail(TenantIdentifier tenantIdentifier, String email)
            throws StorageQueryException {
        try {
            // TODO..
            return PasswordlessQueries.getDevicesByEmail(this, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessDevice[] getDevicesByPhoneNumber(TenantIdentifier tenantIdentifier, String phoneNumber)
            throws StorageQueryException {
        try {
            // TODO..
            return PasswordlessQueries.getDevicesByPhoneNumber(this, phoneNumber);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessCode[] getCodesOfDevice(TenantIdentifier tenantIdentifier, String deviceIdHash)
            throws StorageQueryException {
        try {
            // TODO..
            return PasswordlessQueries.getCodesOfDevice(this, deviceIdHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessCode[] getCodesBefore(TenantIdentifier tenantIdentifier, long time)
            throws StorageQueryException {
        try {
            // TODO..
            return PasswordlessQueries.getCodesBefore(this, time);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessCode getCode(TenantIdentifier tenantIdentifier, String codeId) throws StorageQueryException {
        try {
            // TODO..
            return PasswordlessQueries.getCode(this, codeId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public PasswordlessCode getCodeByLinkCodeHash(TenantIdentifier tenantIdentifier, String linkCodeHash)
            throws StorageQueryException {
        try {
            // TODO..
            return PasswordlessQueries.getCodeByLinkCodeHash(this, linkCodeHash);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public io.supertokens.pluginInterface.passwordless.UserInfo getUserById(AppIdentifier appIdentifier,
                                                                            String userId)
            throws StorageQueryException {
        // TODO..
        try {
            return PasswordlessQueries.getUserById(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public io.supertokens.pluginInterface.passwordless.UserInfo getUserByEmail(TenantIdentifier tenantIdentifier,
                                                                               String email)
            throws StorageQueryException {
        // TODO..
        try {
            return PasswordlessQueries.getUserByEmail(this, email);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public io.supertokens.pluginInterface.passwordless.UserInfo getUserByPhoneNumber(TenantIdentifier tenantIdentifier,
                                                                                     String phoneNumber)
            throws StorageQueryException {
        // TODO...
        try {
            return PasswordlessQueries.getUserByPhoneNumber(this, phoneNumber);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public JsonObject getUserMetadata(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        try {
            // TODO..
            return UserMetadataQueries.getUserMetadata(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public JsonObject getUserMetadata_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserMetadataQueries.getUserMetadata_Transaction(this, sqlCon, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int setUserMetadata_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String userId,
                                           JsonObject metadata)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserMetadataQueries.setUserMetadata_Transaction(this, sqlCon, userId, metadata);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int deleteUserMetadata(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        try {
            // TODO..
            return UserMetadataQueries.deleteUserMetadata(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addRoleToUser(TenantIdentifier tenantIdentifier, String userId, String role)
            throws StorageQueryException, UnknownRoleException, DuplicateUserRoleMappingException {
        // TODO...
        try {
            UserRolesQueries.addRoleToUser(this, userId, role);
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
            }
            throw new StorageQueryException(e);
        }

    }

    @Override
    public String[] getRolesForUser(TenantIdentifier tenantIdentifier, String userId) throws StorageQueryException {
        try {
            // TODO..
            return UserRolesQueries.getRolesForUser(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    private String[] getRolesForUser(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        try {
            // TODO..
            return UserRolesQueries.getRolesForUser(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String[] getUsersForRole(TenantIdentifier tenantIdentifier, String role) throws StorageQueryException {
        try {
            // TODO..
            return UserRolesQueries.getUsersForRole(this, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String[] getPermissionsForRole(AppIdentifier appIdentifier, String role) throws StorageQueryException {
        try {
            // TODO..
            return UserRolesQueries.getPermissionsForRole(this, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String[] getRolesThatHavePermission(AppIdentifier appIdentifier, String permission)
            throws StorageQueryException {
        try {
            // TODO..
            return UserRolesQueries.getRolesThatHavePermission(this, permission);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean deleteRole(AppIdentifier appIdentifier, String role) throws StorageQueryException {
        try {
            // TODO..
            return UserRolesQueries.deleteRole(this, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public String[] getRoles(AppIdentifier appIdentifier) throws StorageQueryException {
        try {
            // TODO..
            return UserRolesQueries.getRoles(this);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean doesRoleExist(AppIdentifier appIdentifier, String role) throws StorageQueryException {
        try {
            // TODO..
            return UserRolesQueries.doesRoleExist(this, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int deleteAllRolesForUser(TenantIdentifier tenantIdentifier, String userId) throws StorageQueryException {
        try {
            // TODO..
            return UserRolesQueries.deleteAllRolesForUser(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void deleteAllRolesForUser(AppIdentifier appIdentifier, String userId) throws StorageQueryException {
        try {
            // TODO..
            UserRolesQueries.deleteAllRolesForUser(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean deleteRoleForUser_Transaction(TenantIdentifier tenantIdentifier, TransactionConnection con,
                                                 String userId, String role)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();

        try {
            return UserRolesQueries.deleteRoleForUser_Transaction(this, sqlCon, userId, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean createNewRoleOrDoNothingIfExists_Transaction(TenantIdentifier tenantIdentifier,
                                                                TransactionConnection con, String role)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();

        try {
            return UserRolesQueries.createNewRoleOrDoNothingIfExists_Transaction(this, sqlCon, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void addPermissionToRoleOrDoNothingIfExists_Transaction(AppIdentifier appIdentifier,
                                                                   TransactionConnection con, String role,
                                                                   String permission)
            throws StorageQueryException, UnknownRoleException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            UserRolesQueries.addPermissionToRoleOrDoNothingIfExists_Transaction(this, sqlCon, role, permission);
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                PostgreSQLConfig config = Config.getConfig(this);
                ServerErrorMessage serverErrorMessage = ((PSQLException) e).getServerErrorMessage();
                if (isForeignKeyConstraintError(serverErrorMessage, config.getUserRolesPermissionsTable(), "role")) {
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
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserRolesQueries.deletePermissionForRole_Transaction(this, sqlCon, role, permission);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public int deleteAllPermissionsForRole_Transaction(AppIdentifier appIdentifier, TransactionConnection con,
                                                       String role)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserRolesQueries.deleteAllPermissionsForRole_Transaction(this, sqlCon, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean doesRoleExist_Transaction(AppIdentifier appIdentifier, TransactionConnection con, String role)
            throws StorageQueryException {
        // TODO..
        Connection sqlCon = (Connection) con.getConnection();
        try {
            return UserRolesQueries.doesRoleExist_transaction(this, sqlCon, role);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void createUserIdMapping(AppIdentifier appIdentifier, String superTokensUserId, String externalUserId,
                                    @Nullable String externalUserIdInfo)
            throws StorageQueryException, UnknownSuperTokensUserIdException, UserIdMappingAlreadyExistsException {
        // TODO..
        try {
            UserIdMappingQueries.createUserIdMapping(this, superTokensUserId, externalUserId, externalUserIdInfo);
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

                if (isUniqueConstraintError(serverErrorMessage, config.getUserIdMappingTable(), "external_user_id")) {
                    throw new UserIdMappingAlreadyExistsException(false, true);
                }
            }
            throw new StorageQueryException(e);
        }

    }

    @Override
    public boolean deleteUserIdMapping(AppIdentifier appIdentifier, String userId, boolean isSuperTokensUserId)
            throws StorageQueryException {
        // TODO..
        try {
            if (isSuperTokensUserId) {
                return UserIdMappingQueries.deleteUserIdMappingWithSuperTokensUserId(this, userId);
            }

            return UserIdMappingQueries.deleteUserIdMappingWithExternalUserId(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public UserIdMapping getUserIdMapping(AppIdentifier appIdentifier, String userId, boolean isSuperTokensUserId)
            throws StorageQueryException {
        // TODO..
        try {
            if (isSuperTokensUserId) {
                return UserIdMappingQueries.getuseraIdMappingWithSuperTokensUserId(this, userId);
            }

            return UserIdMappingQueries.getUserIdMappingWithExternalUserId(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public UserIdMapping[] getUserIdMapping(AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        // TODO..
        try {
            return UserIdMappingQueries.getUserIdMappingWithEitherSuperTokensUserIdOrExternalUserId(this, userId);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public boolean updateOrDeleteExternalUserIdInfo(AppIdentifier appIdentifier, String userId,
                                                    boolean isSuperTokensUserId,
                                                    @Nullable String externalUserIdInfo) throws StorageQueryException {

        // TODO..
        try {
            if (isSuperTokensUserId) {
                return UserIdMappingQueries.updateOrDeleteExternalUserIdInfoWithSuperTokensUserId(this, userId,
                        externalUserIdInfo);
            }

            return UserIdMappingQueries.updateOrDeleteExternalUserIdInfoWithExternalUserId(this, userId,
                    externalUserIdInfo);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public HashMap<String, String> getUserIdMappingForSuperTokensIds(AppIdentifier appIdentifier,
                                                                     ArrayList<String> userIds)
            throws StorageQueryException {
        // TODO..
        try {
            return UserIdMappingQueries.getUserIdMappingWithUserIds(this, userIds);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    @Override
    public void createTenant(TenantConfig config) throws DuplicateTenantException {
        try {
            MultitenancyQueries.createTenant(this, config);
        } catch (SQLException e) {
            if (e.getMessage().contains("duplicate key")) {
                throw new DuplicateTenantException();
            }
            // TODO: what about other error?
        } catch (StorageQueryException e) {
            // TODO: what about other error?
        }
    }

    @Override
    public void addTenantIdInUserPool(TenantIdentifier tenantIdentifier) throws DuplicateTenantException {
        try {
            MultitenancyQueries.addTenantIdInUserPool(this, tenantIdentifier);
        } catch (StorageTransactionLogicException e) {
            if (e.getMessage().contains("duplicate key")) {
                throw new DuplicateTenantException();
            }
            // TODO: what about other error?
        } catch (StorageQueryException e) {
            // TODO: what about other error?
        }
    }

    @Override
    public void deleteTenantIdInUserPool(TenantIdentifier tenantIdentifier) throws TenantOrAppNotFoundException {
        // TODO:
    }

    @Override
    public void overwriteTenantConfig(TenantConfig config) throws TenantOrAppNotFoundException {
        try {
            MultitenancyQueries.updateTenant(this, config);
        } catch (SQLException e) {
            // TODO: handle error??
        } catch (StorageQueryException e) {
            // TODO: handle error??
        }
    }

    @Override
    public void deleteTenant(TenantIdentifier tenantIdentifier) throws TenantOrAppNotFoundException {
        // TODO:
    }

    @Override
    public void deleteApp(TenantIdentifier tenantIdentifier) throws TenantOrAppNotFoundException {
        // TODO:
    }

    @Override
    public void deleteConnectionUriDomainMapping(TenantIdentifier tenantIdentifier) throws
            TenantOrAppNotFoundException {
        // TODO:
    }

    @Override
    public TenantConfig[] getAllTenants() {
        // TODO:
        try {
            return MultitenancyQueries.getAllTenants(this);
        } catch (SQLException e) {
            // TODO:
        } catch (StorageQueryException e) {
            // TODO:
        }
        return new TenantConfig[]{};
    }

    @Override
    public void addUserIdToTenant(TenantIdentifier tenantIdentifier, String userId)
            throws TenantOrAppNotFoundException, UnknownUserIdException {
        // TODO:
    }

    @Override
    public void addRoleToTenant(TenantIdentifier tenantIdentifier, String role)
            throws TenantOrAppNotFoundException, UnknownRoleException {
        // TODO:
    }

    @Override
    public void deleteAppId(String appId) throws TenantOrAppNotFoundException {
        // TODO:
    }

    @Override
    public void deleteConnectionUriDomain(String connectionUriDomain) throws TenantOrAppNotFoundException {
        // TODO:
    }
}
