/*
 *    Copyright (c) 2021, VRAI Labs and/or its affiliates. All rights reserved.
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

package io.supertokens.storage.postgresql.queries;

import io.supertokens.pluginInterface.RowMapper;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.emailpassword.exceptions.UnknownUserIdException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.passwordless.PasswordlessCode;
import io.supertokens.pluginInterface.passwordless.PasswordlessDevice;
import io.supertokens.pluginInterface.passwordless.PasswordlessImportUser;
import io.supertokens.pluginInterface.sqlStorage.SQLStorage.TransactionIsolationLevel;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.PreparedStatementValueSetter;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static io.supertokens.pluginInterface.RECIPE_ID.PASSWORDLESS;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.*;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class PasswordlessQueries {
    public static String getQueryToCreateUsersTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String usersTable = Config.getConfig(start).getPasswordlessUsersTable();

        return "CREATE TABLE IF NOT EXISTS " + usersTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "user_id CHAR(36) NOT NULL,"
                + "email VARCHAR(256),"
                + "phone_number VARCHAR(256),"
                + "time_joined BIGINT NOT NULL, "
                + "CONSTRAINT " + Utils.getConstraintName(schema, usersTable, "user_id", "fkey")
                + " FOREIGN KEY(app_id, user_id)"
                + " REFERENCES " + Config.getConfig(start).getAppIdToUserIdTable() +
                " (app_id, user_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, usersTable, null, "pkey")
                + " PRIMARY KEY (app_id, user_id)"
                + ");";
    }

    static String getQueryToCreatePasswordlessUsersEmailIndex(Start start) {
        return "CREATE INDEX passwordless_users_email_index ON "
                + Config.getConfig(start).getPasswordlessUsersTable() + "(app_id, email);";
    }

    static String getQueryToCreatePasswordlessUsersPhoneNumberIndex(Start start) {
        return "CREATE INDEX passwordless_users_phone_number_index ON "
                + Config.getConfig(start).getPasswordlessUsersTable() + "(app_id, phone_number);";
    }

    static String getQueryToCreatePasswordlessUserToTenantTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String passwordlessUserToTenantTable = Config.getConfig(start).getPasswordlessUserToTenantTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + passwordlessUserToTenantTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "user_id CHAR(36) NOT NULL,"
                + "email VARCHAR(256),"
                + "phone_number VARCHAR(256),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUserToTenantTable, "email", "key")
                + " UNIQUE (app_id, tenant_id, email),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUserToTenantTable, "phone_number", "key")
                + " UNIQUE (app_id, tenant_id, phone_number),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUserToTenantTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, user_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, passwordlessUserToTenantTable, "user_id", "fkey")
                + " FOREIGN KEY (app_id, tenant_id, user_id)"
                + " REFERENCES " + Config.getConfig(start).getUsersTable() +
                "(app_id, tenant_id, user_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    static String getQueryToCreatePasswordlessUserToTenantEmailIndex(Start start) {
        return "CREATE INDEX passwordless_user_to_tenant_email_index ON "
                + Config.getConfig(start).getPasswordlessUserToTenantTable() + "(app_id, tenant_id, email);";
    }

    static String getQueryToCreatePasswordlessUserToTenantPhoneNumberIndex(Start start) {
        return "CREATE INDEX passwordless_user_to_tenant_phone_number_index ON "
                + Config.getConfig(start).getPasswordlessUserToTenantTable() + "(app_id, tenant_id, phone_number);";
    }

    public static String getQueryToCreateDevicesTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String devicesTable = Config.getConfig(start).getPasswordlessDevicesTable();

        return "CREATE TABLE IF NOT EXISTS " + devicesTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "device_id_hash CHAR(44) NOT NULL,"
                + "email VARCHAR(256), "
                + "phone_number VARCHAR(256),"
                + "link_code_salt CHAR(44) NOT NULL,"
                + "failed_attempts INT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, devicesTable, "tenant_id", "fkey")
                + " FOREIGN KEY(app_id, tenant_id)"
                + " REFERENCES " + Config.getConfig(start).getTenantsTable() + " (app_id, tenant_id) ON DELETE CASCADE,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, devicesTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, device_id_hash)"
                + ");";
    }

    public static String getQueryToCreateTenantIdIndexForDevicesTable(Start start) {
        return "CREATE INDEX passwordless_devices_tenant_id_index ON "
                + Config.getConfig(start).getPasswordlessDevicesTable() + "(app_id, tenant_id);";
    }

    public static String getQueryToCreateCodesTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String codesTable = Config.getConfig(start).getPasswordlessCodesTable();

        return "CREATE TABLE IF NOT EXISTS " + codesTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "tenant_id VARCHAR(64) DEFAULT 'public',"
                + "code_id CHAR(36) NOT NULL,"
                + "device_id_hash CHAR(44) NOT NULL,"
                + "link_code_hash CHAR(44) NOT NULL,"
                + "created_at BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, codesTable, "link_code_hash", "key")
                + " UNIQUE (app_id, tenant_id, link_code_hash),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, codesTable, null, "pkey")
                + " PRIMARY KEY (app_id, tenant_id, code_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, codesTable, "device_id_hash", "fkey")
                + " FOREIGN KEY (app_id, tenant_id, device_id_hash)"
                + " REFERENCES " + Config.getConfig(start).getPasswordlessDevicesTable() +
                "(app_id, tenant_id, device_id_hash)"
                + " ON DELETE CASCADE ON UPDATE CASCADE"
                + ");";
    }

    public static String getQueryToCreateDeviceEmailIndex(Start start) {
        return "CREATE INDEX passwordless_devices_email_index ON "
                + Config.getConfig(start).getPasswordlessDevicesTable() + " (app_id, tenant_id, email);"; // USING hash
    }

    public static String getQueryToCreateDevicePhoneNumberIndex(Start start) {
        return "CREATE INDEX passwordless_devices_phone_number_index ON "
                + Config.getConfig(start).getPasswordlessDevicesTable() +
                " (app_id, tenant_id, phone_number);"; // USING hash
    }

    public static String getQueryToCreateCodeDeviceIdHashIndex(Start start) {
        return "CREATE INDEX IF NOT EXISTS passwordless_codes_device_id_hash_index ON "
                + Config.getConfig(start).getPasswordlessCodesTable() + "(app_id, tenant_id, device_id_hash);";
    }

    public static String getQueryToCreateCodeCreatedAtIndex(Start start) {
        return "CREATE INDEX passwordless_codes_created_at_index ON "
                + Config.getConfig(start).getPasswordlessCodesTable() + "(app_id, tenant_id, created_at);";
    }

    public static void createDeviceWithCode(Start start, TenantIdentifier tenantIdentifier, String email,
                                            String phoneNumber, String linkCodeSalt,
                                            PasswordlessCode code)
            throws StorageTransactionLogicException, StorageQueryException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                String QUERY = "INSERT INTO " + getConfig(start).getPasswordlessDevicesTable()
                        + "(app_id, tenant_id, device_id_hash, email, phone_number, link_code_salt, failed_attempts)"
                        + " VALUES(?, ?, ?, ?, ?, ?, 0)";
                update(sqlCon, QUERY, pst -> {
                    pst.setString(1, tenantIdentifier.getAppId());
                    pst.setString(2, tenantIdentifier.getTenantId());
                    pst.setString(3, code.deviceIdHash);
                    pst.setString(4, email);
                    pst.setString(5, phoneNumber);
                    pst.setString(6, linkCodeSalt);
                });

                createCode_Transaction(start, sqlCon, tenantIdentifier, code);
                sqlCon.commit();
            } catch (SQLException throwables) {
                throw new StorageTransactionLogicException(throwables);
            }
            return null;
        }, TransactionIsolationLevel.REPEATABLE_READ);
    }

    public static PasswordlessDevice getDevice_Transaction(Start start, Connection con,
                                                           TenantIdentifier tenantIdentifier, String deviceIdHash)
            throws StorageQueryException, SQLException {

        String QUERY = "SELECT device_id_hash, email, phone_number, link_code_salt, failed_attempts FROM "
                + getConfig(start).getPasswordlessDevicesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND device_id_hash = ? FOR UPDATE";
        return execute(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, deviceIdHash);
        }, result -> {
            if (result.next()) {
                return PasswordlessDeviceRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static void incrementDeviceFailedAttemptCount_Transaction(Start start, Connection con,
                                                                     TenantIdentifier tenantIdentifier,
                                                                     String deviceIdHash)
            throws SQLException, StorageQueryException {
        String QUERY = "UPDATE " + getConfig(start).getPasswordlessDevicesTable()
                + " SET failed_attempts = failed_attempts + 1"
                + " WHERE app_id = ? AND tenant_id = ? AND device_id_hash = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, deviceIdHash);
        });
    }

    public static void deleteDevice_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier,
                                                String deviceIdHash)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessDevicesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND device_id_hash = ?";
        update(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, deviceIdHash);
        });
    }

    public static void deleteDevicesByPhoneNumber_Transaction(Start start, Connection con,
                                                              TenantIdentifier tenantIdentifier,
                                                              @Nonnull String phoneNumber)
            throws SQLException, StorageQueryException {

        String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessDevicesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND phone_number = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, phoneNumber);
        });
    }

    public static void deleteDevicesByPhoneNumber_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                              @Nonnull String phoneNumber, String userId)
            throws SQLException, StorageQueryException {

        String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessDevicesTable()
                + " WHERE app_id = ? AND phone_number = ? AND tenant_id IN ("
                + "    SELECT tenant_id FROM " + getConfig(start).getPasswordlessUserToTenantTable()
                + "    WHERE app_id = ? AND user_id = ?"
                + ")";

        update(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, phoneNumber);
            pst.setString(3, appIdentifier.getAppId());
            pst.setString(4, userId);
        });
    }

    public static void deleteDevicesByEmail_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier,
                                                        @Nonnull String email)
            throws SQLException, StorageQueryException {

        String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessDevicesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND email = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, email);
        });
    }

    public static void deleteDevicesByEmail_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                        @Nonnull String email, String userId)
            throws SQLException, StorageQueryException {

        String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessDevicesTable()
                + " WHERE app_id = ? AND email = ? AND tenant_id IN ("
                + "    SELECT tenant_id FROM " + getConfig(start).getPasswordlessUserToTenantTable()
                + "    WHERE app_id = ? AND user_id = ?"
                + ")";

        update(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, email);
            pst.setString(3, appIdentifier.getAppId());
            pst.setString(4, userId);
        });
    }

    private static void createCode_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier,
                                               PasswordlessCode code)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getPasswordlessCodesTable()
                + "(app_id, tenant_id, code_id, device_id_hash, link_code_hash, created_at)"
                + " VALUES(?, ?, ?, ?, ?, ?)";
        update(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, code.id);
            pst.setString(4, code.deviceIdHash);
            pst.setString(5, code.linkCodeHash);
            pst.setLong(6, code.createdAt);
        });
    }

    public static void createCode(Start start, TenantIdentifier tenantIdentifier, PasswordlessCode code)
            throws StorageTransactionLogicException, StorageQueryException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();

            try {
                PasswordlessQueries.createCode_Transaction(start, sqlCon, tenantIdentifier, code);
                sqlCon.commit();
            } catch (SQLException e) {
                throw new StorageTransactionLogicException(e);
            }
            return null;
        });
    }

    public static PasswordlessCode[] getCodesOfDevice_Transaction(Start start, Connection con,
                                                                  TenantIdentifier tenantIdentifier,
                                                                  String deviceIdHash)
            throws StorageQueryException, SQLException {
        // We do not lock here, since the device is already locked earlier in the transaction.
        String QUERY = "SELECT code_id, device_id_hash, link_code_hash, created_at FROM "
                + getConfig(start).getPasswordlessCodesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND device_id_hash = ?";

        return execute(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, deviceIdHash);
        }, result -> {
            List<PasswordlessCode> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(PasswordlessCodeRowMapper.getInstance().mapOrThrow(result));
            }
            PasswordlessCode[] finalResult = new PasswordlessCode[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        });
    }

    public static PasswordlessCode getCodeByLinkCodeHash_Transaction(Start start, Connection con,
                                                                     TenantIdentifier tenantIdentifier,
                                                                     String linkCodeHash)
            throws StorageQueryException, SQLException {
        // We do not lock here, since the device is already locked earlier in the transaction.
        String QUERY = "SELECT code_id, device_id_hash, link_code_hash, created_at FROM "
                + getConfig(start).getPasswordlessCodesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND link_code_hash = ?";

        return execute(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, linkCodeHash);
        }, result -> {
            if (result.next()) {
                return PasswordlessCodeRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static void deleteCode_Transaction(Start start, Connection con, TenantIdentifier tenantIdentifier,
                                              String codeId)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessCodesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND code_id = ?";

        update(con, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, codeId);
        });
    }

    public static AuthRecipeUserInfo createUser(Start start, TenantIdentifier tenantIdentifier, String id,
                                                @Nullable String email,
                                                @Nullable String phoneNumber, long timeJoined)
            throws StorageTransactionLogicException, StorageQueryException {
        return start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                { // app_id_to_user_id
                    String QUERY = "INSERT INTO " + getConfig(start).getAppIdToUserIdTable()
                            + "(app_id, user_id, primary_or_recipe_user_id, recipe_id)" + " VALUES(?, ?, ?, ?)";
                    update(sqlCon, QUERY, pst -> {
                        pst.setString(1, tenantIdentifier.getAppId());
                        pst.setString(2, id);
                        pst.setString(3, id);
                        pst.setString(4, PASSWORDLESS.toString());
                    });
                }

                { // all_auth_recipe_users
                    String QUERY = "INSERT INTO " + getConfig(start).getUsersTable()
                            +
                            "(app_id, tenant_id, user_id, primary_or_recipe_user_id, recipe_id, time_joined, " +
                            "primary_or_recipe_user_time_joined)" +
                            " VALUES(?, ?, ?, ?, ?, ?, ?)";
                    update(sqlCon, QUERY, pst -> {
                        pst.setString(1, tenantIdentifier.getAppId());
                        pst.setString(2, tenantIdentifier.getTenantId());
                        pst.setString(3, id);
                        pst.setString(4, id);
                        pst.setString(5, PASSWORDLESS.toString());
                        pst.setLong(6, timeJoined);
                        pst.setLong(7, timeJoined);
                    });
                }

                { // passwordless_users
                    String QUERY = "INSERT INTO " + getConfig(start).getPasswordlessUsersTable()
                            + "(app_id, user_id, email, phone_number, time_joined)" + " VALUES(?, ?, ?, ?, ?)";
                    update(sqlCon, QUERY, pst -> {
                        pst.setString(1, tenantIdentifier.getAppId());
                        pst.setString(2, id);
                        pst.setString(3, email);
                        pst.setString(4, phoneNumber);
                        pst.setLong(5, timeJoined);
                    });
                }

                { // passwordless_user_to_tenant
                    String QUERY = "INSERT INTO " + getConfig(start).getPasswordlessUserToTenantTable()
                            + "(app_id, tenant_id, user_id, email, phone_number)" + " VALUES(?, ?, ?, ?, ?)";

                    update(sqlCon, QUERY, pst -> {
                        pst.setString(1, tenantIdentifier.getAppId());
                        pst.setString(2, tenantIdentifier.getTenantId());
                        pst.setString(3, id);
                        pst.setString(4, email);
                        pst.setString(5, phoneNumber);
                    });
                }
                UserInfoPartial userInfo = new UserInfoPartial(id, email, phoneNumber, timeJoined);
                fillUserInfoWithTenantIds_transaction(start, sqlCon, tenantIdentifier.toAppIdentifier(), userInfo);
                fillUserInfoWithVerified_transaction(start, sqlCon, tenantIdentifier.toAppIdentifier(), userInfo);
                sqlCon.commit();
                return AuthRecipeUserInfo.create(id, false,
                        userInfo.toLoginMethod());
            } catch (SQLException throwables) {
                throw new StorageTransactionLogicException(throwables);
            }
        });
    }

    private static UserInfoWithTenantId[] getUserInfosWithTenant_Transaction(Start start, Connection con,
                                                                             AppIdentifier appIdentifier, String userId)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT pl_users.user_id as user_id, pl_users.email as email, "
                + "pl_users.phone_number as phone_number, pl_users_to_tenant.tenant_id as tenant_id "
                + "FROM " + getConfig(start).getPasswordlessUsersTable() + " AS pl_users "
                + "JOIN " + getConfig(start).getPasswordlessUserToTenantTable() + " AS pl_users_to_tenant "
                + "ON pl_users.app_id = pl_users_to_tenant.app_id AND pl_users.user_id = pl_users_to_tenant.user_id "
                + "WHERE pl_users_to_tenant.app_id = ? AND pl_users_to_tenant.user_id = ?";
        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        }, result -> {
            List<UserInfoWithTenantId> userInfos = new ArrayList<>();

            while (result.next()) {
                userInfos.add(new UserInfoWithTenantId(
                        result.getString("user_id"),
                        result.getString("tenant_id"),
                        result.getString("email"),
                        result.getString("phoneNumber")
                ));
                PasswordlessDeviceRowMapper.getInstance().mapOrThrow(result);
            }
            return userInfos.toArray(new UserInfoWithTenantId[0]);
        });
    }

    public static void deleteUser_Transaction(Connection sqlCon, Start start, AppIdentifier appIdentifier,
                                              String userId, boolean deleteUserIdMappingToo)
            throws StorageQueryException, SQLException {
        UserInfoWithTenantId[] userInfos = getUserInfosWithTenant_Transaction(start, sqlCon, appIdentifier, userId);

        if (deleteUserIdMappingToo) {
            String QUERY = "DELETE FROM " + getConfig(start).getAppIdToUserIdTable()
                    + " WHERE app_id = ? AND user_id = ?";

            update(sqlCon, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, userId);
            });
        } else {
            {
                String QUERY = "DELETE FROM " + getConfig(start).getUsersTable()
                        + " WHERE app_id = ? AND user_id = ?";
                update(sqlCon, QUERY, pst -> {
                    pst.setString(1, appIdentifier.getAppId());
                    pst.setString(2, userId);
                });
            }

            {
                String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessUsersTable()
                        + " WHERE app_id = ? AND user_id = ?";
                update(sqlCon, QUERY, pst -> {
                    pst.setString(1, appIdentifier.getAppId());
                    pst.setString(2, userId);
                });
            }
        }

        for (UserInfoWithTenantId userInfo : userInfos) {
            if (userInfo.email != null) {
                deleteDevicesByEmail_Transaction(start, sqlCon,
                        new TenantIdentifier(
                                appIdentifier.getConnectionUriDomain(), appIdentifier.getAppId(),
                                userInfo.tenantId),
                        userInfo.email);
            }
            if (userInfo.phoneNumber != null) {
                deleteDevicesByPhoneNumber_Transaction(start, sqlCon,
                        new TenantIdentifier(
                                appIdentifier.getConnectionUriDomain(), appIdentifier.getAppId(),
                                userInfo.tenantId),
                        userInfo.phoneNumber);
            }
        }
    }

    public static int updateUserEmail_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                  String userId, String email)
            throws SQLException, StorageQueryException {
        {
            String QUERY = "UPDATE " + Config.getConfig(start).getPasswordlessUserToTenantTable()
                    + " SET email = ? WHERE app_id = ? AND user_id = ?";

            update(con, QUERY, pst -> {
                pst.setString(1, email);
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, userId);
            });
        }
        {
            String QUERY = "UPDATE " + Config.getConfig(start).getPasswordlessUsersTable()
                    + " SET email = ? WHERE app_id = ? AND user_id = ?";

            return update(con, QUERY, pst -> {
                pst.setString(1, email);
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, userId);
            });
        }
    }

    public static int updateUserPhoneNumber_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                        String userId, String phoneNumber)
            throws SQLException, StorageQueryException {
        {
            String QUERY = "UPDATE " + Config.getConfig(start).getPasswordlessUserToTenantTable()
                    + " SET phone_number = ? WHERE app_id = ? AND user_id = ?";

            update(con, QUERY, pst -> {
                pst.setString(1, phoneNumber);
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, userId);
            });
        }
        {
            String QUERY = "UPDATE " + Config.getConfig(start).getPasswordlessUsersTable()
                    + " SET phone_number = ? WHERE app_id = ? AND user_id = ?";

            return update(con, QUERY, pst -> {
                pst.setString(1, phoneNumber);
                pst.setString(2, appIdentifier.getAppId());
                pst.setString(3, userId);
            });
        }
    }

    public static PasswordlessDevice getDevice(Start start, TenantIdentifier tenantIdentifier, String deviceIdHash)
            throws StorageQueryException, SQLException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            String QUERY = "SELECT device_id_hash, email, phone_number, link_code_salt, failed_attempts FROM "
                    + getConfig(start).getPasswordlessDevicesTable()
                    + " WHERE app_id = ? AND tenant_id = ? AND device_id_hash = ?";
            return execute(con, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, deviceIdHash);
            }, result -> {
                if (result.next()) {
                    return PasswordlessDeviceRowMapper.getInstance().mapOrThrow(result);
                }
                return null;
            });
        }
    }

    public static PasswordlessDevice[] getDevicesByEmail(Start start, TenantIdentifier tenantIdentifier,
                                                         @Nonnull String email)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT device_id_hash, email, phone_number, link_code_salt, failed_attempts FROM "
                + getConfig(start).getPasswordlessDevicesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND email = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, email);
        }, result -> {
            List<PasswordlessDevice> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(PasswordlessDeviceRowMapper.getInstance().mapOrThrow(result));
            }
            PasswordlessDevice[] finalResult = new PasswordlessDevice[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        });
    }

    public static PasswordlessDevice[] getDevicesByPhoneNumber(Start start, TenantIdentifier tenantIdentifier,
                                                               @Nonnull String phoneNumber)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT device_id_hash, email, phone_number, link_code_salt, failed_attempts FROM "
                + getConfig(start).getPasswordlessDevicesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND phone_number = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, phoneNumber);
        }, result -> {
            List<PasswordlessDevice> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(PasswordlessDeviceRowMapper.getInstance().mapOrThrow(result));
            }
            PasswordlessDevice[] finalResult = new PasswordlessDevice[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        });
    }

    public static PasswordlessCode[] getCodesOfDevice(Start start, TenantIdentifier tenantIdentifier,
                                                      String deviceIdHash)
            throws StorageQueryException, SQLException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            // We can call the transaction version here because it doesn't lock anything.
            return PasswordlessQueries.getCodesOfDevice_Transaction(start, con, tenantIdentifier, deviceIdHash);
        }
    }

    public static PasswordlessCode[] getCodesBefore(Start start, TenantIdentifier tenantIdentifier, long time)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT code_id, device_id_hash, link_code_hash, created_at FROM "
                + getConfig(start).getPasswordlessCodesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND created_at < ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setLong(3, time);
        }, result -> {
            List<PasswordlessCode> temp = new ArrayList<>();
            while (result.next()) {
                temp.add(PasswordlessCodeRowMapper.getInstance().mapOrThrow(result));
            }
            PasswordlessCode[] finalResult = new PasswordlessCode[temp.size()];
            for (int i = 0; i < temp.size(); i++) {
                finalResult[i] = temp.get(i);
            }
            return finalResult;
        });
    }

    public static PasswordlessCode getCode(Start start, TenantIdentifier tenantIdentifier, String codeId)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT code_id, device_id_hash, link_code_hash, created_at FROM "
                + getConfig(start).getPasswordlessCodesTable()
                + " WHERE app_id = ? AND tenant_id = ? AND code_id = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, codeId);
        }, result -> {
            if (result.next()) {
                return PasswordlessCodeRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static PasswordlessCode getCodeByLinkCodeHash(Start start, TenantIdentifier tenantIdentifier,
                                                         String linkCodeHash)
            throws StorageQueryException, SQLException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            // We can call the transaction version here because it doesn't lock anything.
            return PasswordlessQueries.getCodeByLinkCodeHash_Transaction(start, con, tenantIdentifier, linkCodeHash);
        }
    }

    public static List<LoginMethod> getUsersInfoUsingIdList(Start start, Set<String> ids,
                                                            AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        if (ids != null && !ids.isEmpty()) {
            // No need to filter based on tenantId because the id list is already filtered for a tenant
            String QUERY = "SELECT user_id, email, phone_number, time_joined "
                    + "FROM " + getConfig(start).getPasswordlessUsersTable() + " WHERE user_id IN (" +
                    Utils.generateCommaSeperatedQuestionMarks(ids.size()) + ") AND app_id = ?";

            List<UserInfoPartial> userInfos = execute(start, QUERY, pst -> {
                int index = 1;
                for (String id : ids) {
                    pst.setString(index, id);
                    index++;
                }
                pst.setString(index, appIdentifier.getAppId());
            }, result -> {
                List<UserInfoPartial> finalResult = new ArrayList<>();
                while (result.next()) {
                    finalResult.add(UserInfoRowMapper.getInstance().mapOrThrow(result));
                }
                return finalResult;
            });
            fillUserInfoWithTenantIds(start, appIdentifier, userInfos);
            fillUserInfoWithVerified(start, appIdentifier, userInfos);
            return userInfos.stream().map(UserInfoPartial::toLoginMethod).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public static List<LoginMethod> getUsersInfoUsingIdList_Transaction(Start start, Connection con, Set<String> ids,
                                                                        AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        if (ids != null && !ids.isEmpty()) {
            // No need to filter based on tenantId because the id list is already filtered for a tenant
            String QUERY = "SELECT user_id, email, phone_number, time_joined "
                    + "FROM " + getConfig(start).getPasswordlessUsersTable() + " WHERE user_id IN (" +
                    Utils.generateCommaSeperatedQuestionMarks(ids.size()) + ") AND app_id = ?";

            List<UserInfoPartial> userInfos = execute(con, QUERY, pst -> {
                int index = 1;
                for (String id : ids) {
                    pst.setString(index, id);
                    index++;
                }
                pst.setString(index, appIdentifier.getAppId());
            }, result -> {
                List<UserInfoPartial> finalResult = new ArrayList<>();
                while (result.next()) {
                    finalResult.add(UserInfoRowMapper.getInstance().mapOrThrow(result));
                }
                return finalResult;
            });
            fillUserInfoWithTenantIds_transaction(start, con, appIdentifier, userInfos);
            fillUserInfoWithVerified_transaction(start, con, appIdentifier, userInfos);
            return userInfos.stream().map(UserInfoPartial::toLoginMethod).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private static UserInfoPartial getUserById_Transaction(Start start, Connection sqlCon, AppIdentifier appIdentifier,
                                                           String userId)
            throws StorageQueryException, SQLException {
        // we don't need a LOCK here because this is already part of a transaction, and locked on app_id_to_user_id
        // table
        String QUERY = "SELECT user_id, email, phone_number, time_joined FROM "
                + getConfig(start).getPasswordlessUsersTable() + " WHERE app_id = ? AND user_id = ?";

        return execute(sqlCon, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        }, result -> {
            if (result.next()) {
                return UserInfoRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static List<String> lockEmail_Transaction(Start start, Connection con, AppIdentifier appIdentifier,
                                                     String email) throws StorageQueryException, SQLException {
        // we don't need a FOR UPDATE here because this is already part of a transaction, and locked on
        // app_id_to_user_id table
        String QUERY = "SELECT user_id FROM " + getConfig(start).getPasswordlessUsersTable() +
                " WHERE app_id = ? AND email = ? FOR UPDATE";

        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, email);
        }, result -> {
            List<String> userIds = new ArrayList<>();
            while (result.next()) {
                userIds.add(result.getString("user_id"));
            }
            return userIds;
        });
    }

    public static List<String> lockEmail_Transaction(Start start, Connection con,
                                                     AppIdentifier appIdentifier,
                                                     List<String> emails)
            throws StorageQueryException, SQLException {
        if(emails == null || emails.isEmpty()){
            return new ArrayList<>();
        }
        String QUERY = "SELECT user_id FROM " + getConfig(start).getPasswordlessUsersTable() +
                " WHERE app_id = ? AND email IN (" + Utils.generateCommaSeperatedQuestionMarks(emails.size()) + ") FOR UPDATE";

        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            for (int i = 0; i < emails.size(); i++) {
                pst.setString(2 + i, emails.get(i));
            }
        }, result -> {
            List<String> results = new ArrayList<>();
            while (result.next()) {
                results.add(result.getString("user_id"));
            }
            return results;
        });
    }

    public static List<String> lockPhoneAndTenant_Transaction(Start start, Connection con,
                                                              AppIdentifier appIdentifier,
                                                              String phoneNumber)
            throws SQLException, StorageQueryException {

        String QUERY = "SELECT user_id FROM " + getConfig(start).getPasswordlessUsersTable() +
                " WHERE app_id = ? AND phone_number = ? FOR UPDATE";
        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, phoneNumber);
        }, result -> {
            List<String> userIds = new ArrayList<>();
            while (result.next()) {
                userIds.add(result.getString("user_id"));
            }
            return userIds;
        });
    }

    public static List<String> lockPhoneAndTenant_Transaction(Start start, Connection con,
                                                     AppIdentifier appIdentifier,
                                                     List<String> phones)
            throws StorageQueryException, SQLException {
        if(phones == null || phones.isEmpty()){
            return new ArrayList<>();
        }
        String QUERY = "SELECT user_id FROM " + getConfig(start).getPasswordlessUsersTable() +
                " WHERE app_id = ? AND phone_number IN (" + Utils.generateCommaSeperatedQuestionMarks(phones.size()) + ") FOR UPDATE";

        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            for (int i = 0; i < phones.size(); i++) {
                pst.setString(2 + i, phones.get(i));
            }
        }, result -> {
            List<String> results = new ArrayList<>();
            while (result.next()) {
                results.add(result.getString("user_id"));
            }
            return results;
        });
    }

    public static String getPrimaryUserIdUsingEmail(Start start, TenantIdentifier tenantIdentifier,
                                                    String email)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT DISTINCT all_users.primary_or_recipe_user_id AS user_id "
                + "FROM " + getConfig(start).getPasswordlessUserToTenantTable() + " AS pless" +
                " JOIN " + getConfig(start).getUsersTable() + " AS all_users" +
                " ON pless.app_id = all_users.app_id AND pless.user_id = all_users.user_id" +
                " WHERE pless.app_id = ? AND pless.tenant_id = ? AND pless.email = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, email);
        }, result -> {
            if (result.next()) {
                return result.getString("user_id");
            }
            return null;
        });
    }

    public static List<String> getPrimaryUserIdsUsingEmail_Transaction(Start start, Connection con,
                                                                       AppIdentifier appIdentifier,
                                                                       String email)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT DISTINCT all_users.primary_or_recipe_user_id AS user_id "
                + "FROM " + getConfig(start).getPasswordlessUsersTable() + " AS pless" +
                " JOIN " + getConfig(start).getAppIdToUserIdTable() + " AS all_users" +
                " ON pless.app_id = all_users.app_id AND pless.user_id = all_users.user_id" +
                " WHERE pless.app_id = ? AND pless.email = ?";

        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, email);
        }, result -> {
            List<String> userIds = new ArrayList<>();
            while (result.next()) {
                userIds.add(result.getString("user_id"));
            }
            return userIds;
        });
    }

    public static List<String> getPrimaryUserIdsUsingMultipleEmails_Transaction(Start start, Connection con,
                                                                                AppIdentifier appIdentifier,
                                                                                List<String> emails)
            throws StorageQueryException, SQLException {
        if(emails == null || emails.isEmpty()){
            return new ArrayList<>();
        }
        String QUERY = "SELECT DISTINCT all_users.primary_or_recipe_user_id AS user_id "
                + "FROM " + getConfig(start).getPasswordlessUsersTable() + " AS ep" +
                " JOIN " + getConfig(start).getAppIdToUserIdTable() + " AS all_users" +
                " ON ep.app_id = all_users.app_id AND ep.user_id = all_users.user_id" +
                " WHERE ep.app_id = ? AND ep.email IN ( " + Utils.generateCommaSeperatedQuestionMarks(emails.size()) + " )";

        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            for (int i = 0; i < emails.size(); i++) {
                pst.setString(2+i, emails.get(i));
            }
        }, result -> {
            List<String> userIds = new ArrayList<>();
            while (result.next()) {
                userIds.add(result.getString("user_id"));
            }
            return userIds;
        });
    }

    public static String getPrimaryUserByPhoneNumber(Start start, TenantIdentifier tenantIdentifier,
                                                     @Nonnull String phoneNumber)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT DISTINCT all_users.primary_or_recipe_user_id AS user_id "
                + "FROM " + getConfig(start).getPasswordlessUserToTenantTable() + " AS pless" +
                " JOIN " + getConfig(start).getUsersTable() + " AS all_users" +
                " ON pless.app_id = all_users.app_id AND pless.user_id = all_users.user_id" +
                " WHERE pless.app_id = ? AND pless.tenant_id = ? AND pless.phone_number = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenantIdentifier.getAppId());
            pst.setString(2, tenantIdentifier.getTenantId());
            pst.setString(3, phoneNumber);
        }, result -> {
            if (result.next()) {
                return result.getString("user_id");
            }
            return null;
        });
    }

    public static List<String> listUserIdsByPhoneNumber_Transaction(Start start, Connection con,
                                                                    AppIdentifier appIdentifier,
                                                                    @Nonnull String phoneNumber)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT DISTINCT all_users.primary_or_recipe_user_id AS user_id "
                + "FROM " + getConfig(start).getPasswordlessUsersTable() + " AS pless" +
                " JOIN " + getConfig(start).getUsersTable() + " AS all_users" +
                " ON pless.app_id = all_users.app_id AND pless.user_id = all_users.user_id" +
                " WHERE pless.app_id = ? AND pless.phone_number = ?";

        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, phoneNumber);
        }, result -> {
            List<String> userIds = new ArrayList<>();
            while (result.next()) {
                userIds.add(result.getString("user_id"));
            }
            return userIds;
        });
    }

    public static List<String> listUserIdsByMultiplePhoneNumber_Transaction(Start start, Connection con,
                                                                    AppIdentifier appIdentifier,
                                                                    @Nonnull List<String> phoneNumbers)
            throws StorageQueryException, SQLException {
        if(phoneNumbers == null || phoneNumbers.isEmpty()){
            return new ArrayList<>();
        }
        String QUERY = "SELECT DISTINCT all_users.primary_or_recipe_user_id AS user_id "
                + "FROM " + getConfig(start).getPasswordlessUsersTable() + " AS pless" +
                " JOIN " + getConfig(start).getUsersTable() + " AS all_users" +
                " ON pless.app_id = all_users.app_id AND pless.user_id = all_users.user_id" +
                " WHERE pless.app_id = ? AND pless.phone_number IN ( "+ Utils.generateCommaSeperatedQuestionMarks(phoneNumbers.size()) +" )";

        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            for (int i = 0; i < phoneNumbers.size(); i++) {
                pst.setString(2 + i, phoneNumbers.get(i));
            }
        }, result -> {
            List<String> userIds = new ArrayList<>();
            while (result.next()) {
                userIds.add(result.getString("user_id"));
            }
            return userIds;
        });
    }

    public static boolean addUserIdToTenant_Transaction(Start start, Connection sqlCon,
                                                        TenantIdentifier tenantIdentifier, String userId)
            throws StorageQueryException, SQLException, UnknownUserIdException {
        UserInfoPartial userInfo = PasswordlessQueries.getUserById_Transaction(start, sqlCon,
                tenantIdentifier.toAppIdentifier(), userId);

        if (userInfo == null) {
            throw new UnknownUserIdException();
        }

        GeneralQueries.AccountLinkingInfo accountLinkingInfo = GeneralQueries.getAccountLinkingInfo_Transaction(start,
                sqlCon, tenantIdentifier.toAppIdentifier(), userId);

        { // all_auth_recipe_users
            String QUERY = "INSERT INTO " + getConfig(start).getUsersTable()
                    +
                    "(app_id, tenant_id, user_id, primary_or_recipe_user_id, is_linked_or_is_a_primary_user, " +
                    "recipe_id, time_joined, primary_or_recipe_user_time_joined)"
                    + " VALUES(?, ?, ?, ?, ?, ?, ?, ?)" + " ON CONFLICT DO NOTHING";
            update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, userInfo.id);
                pst.setString(4, accountLinkingInfo.primaryUserId);
                pst.setBoolean(5, accountLinkingInfo.isLinked);
                pst.setString(6, PASSWORDLESS.toString());
                pst.setLong(7, userInfo.timeJoined);
                pst.setLong(8, userInfo.timeJoined);
            });

            GeneralQueries.updateTimeJoinedForPrimaryUser_Transaction(start, sqlCon, tenantIdentifier.toAppIdentifier(),
                    accountLinkingInfo.primaryUserId);
        }

        { // passwordless_user_to_tenant
            String QUERY = "INSERT INTO " + getConfig(start).getPasswordlessUserToTenantTable()
                    + "(app_id, tenant_id, user_id, email, phone_number)"
                    + " VALUES(?, ?, ?, ?, ?)" + " ON CONFLICT ON CONSTRAINT "
                    + Utils.getConstraintName(Config.getConfig(start).getTableSchema(),
                    getConfig(start).getPasswordlessUserToTenantTable(), null, "pkey")
                    + " DO NOTHING";

            int numRows = update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, userInfo.id);
                pst.setString(4, userInfo.email);
                pst.setString(5, userInfo.phoneNumber);
            });

            return numRows > 0;
        }
    }

    public static boolean removeUserIdFromTenant_Transaction(Start start, Connection sqlCon,
                                                             TenantIdentifier tenantIdentifier, String userId)
            throws SQLException, StorageQueryException {
        { // all_auth_recipe_users
            String QUERY = "DELETE FROM " + getConfig(start).getUsersTable()
                    + " WHERE app_id = ? AND tenant_id = ? and user_id = ? and recipe_id = ?";
            int numRows = update(sqlCon, QUERY, pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, userId);
                pst.setString(4, PASSWORDLESS.toString());
            });

            return numRows > 0;
        }

        // automatically deleted from passwordless_user_to_tenant because of foreign key constraint
    }

    private static UserInfoPartial fillUserInfoWithVerified_transaction(Start start,
                                                                        Connection sqlCon,
                                                                        AppIdentifier appIdentifier,
                                                                        UserInfoPartial userInfo)
            throws SQLException, StorageQueryException {
        if (userInfo == null) return null;
        return fillUserInfoWithVerified_transaction(start, sqlCon, appIdentifier, List.of(userInfo)).get(0);
    }

    private static List<UserInfoPartial> fillUserInfoWithVerified_transaction(Start start,
                                                                              Connection sqlCon,
                                                                              AppIdentifier appIdentifier,
                                                                              List<UserInfoPartial> userInfos)
            throws SQLException, StorageQueryException {
        List<EmailVerificationQueries.UserIdAndEmail> userIdsAndEmails = new ArrayList<>();
        for (UserInfoPartial userInfo : userInfos) {
            if (userInfo.email == null) {
                // phone number, so we mark it as verified
                userInfo.verified = true;
            } else {
                userIdsAndEmails.add(new EmailVerificationQueries.UserIdAndEmail(userInfo.id, userInfo.email));
            }
        }
        List<String> userIdsThatAreVerified = EmailVerificationQueries.isEmailVerified_transaction(start, sqlCon,
                appIdentifier,
                userIdsAndEmails);
        Set<String> verifiedUserIdsSet = new HashSet<>(userIdsThatAreVerified);
        for (UserInfoPartial userInfo : userInfos) {
            if (userInfo.verified != null) {
                // this means phone number
                assert (userInfo.email == null);
                continue;
            }
            if (verifiedUserIdsSet.contains(userInfo.id)) {
                userInfo.verified = true;
            } else {
                userInfo.verified = false;
            }
        }
        return userInfos;
    }

    private static List<UserInfoPartial> fillUserInfoWithVerified(Start start,
                                                                  AppIdentifier appIdentifier,
                                                                  List<UserInfoPartial> userInfos)
            throws SQLException, StorageQueryException {
        List<EmailVerificationQueries.UserIdAndEmail> userIdsAndEmails = new ArrayList<>();
        for (UserInfoPartial userInfo : userInfos) {
            if (userInfo.email == null) {
                // phone number, so we mark it as verified
                userInfo.verified = true;
            } else {
                userIdsAndEmails.add(new EmailVerificationQueries.UserIdAndEmail(userInfo.id, userInfo.email));
            }
        }
        List<String> userIdsThatAreVerified = EmailVerificationQueries.isEmailVerified(start,
                appIdentifier,
                userIdsAndEmails);
        Set<String> verifiedUserIdsSet = new HashSet<>(userIdsThatAreVerified);
        for (UserInfoPartial userInfo : userInfos) {
            if (userInfo.verified != null) {
                // this means phone number
                assert (userInfo.email == null);
                continue;
            }
            if (verifiedUserIdsSet.contains(userInfo.id)) {
                userInfo.verified = true;
            } else {
                userInfo.verified = false;
            }
        }
        return userInfos;
    }

    private static UserInfoPartial fillUserInfoWithTenantIds_transaction(Start start, Connection sqlCon,
                                                                         AppIdentifier appIdentifier,
                                                                         UserInfoPartial userInfo)
            throws SQLException, StorageQueryException {
        if (userInfo == null) return null;
        return fillUserInfoWithTenantIds_transaction(start, sqlCon, appIdentifier, List.of(userInfo)).get(0);
    }

    private static List<UserInfoPartial> fillUserInfoWithTenantIds_transaction(Start start, Connection sqlCon,
                                                                               AppIdentifier appIdentifier,
                                                                               List<UserInfoPartial> userInfos)
            throws SQLException, StorageQueryException {
        String[] userIds = new String[userInfos.size()];
        for (int i = 0; i < userInfos.size(); i++) {
            userIds[i] = userInfos.get(i).id;
        }

        Map<String, List<String>> tenantIdsForUserIds = GeneralQueries.getTenantIdsForUserIds_transaction(start, sqlCon,
                appIdentifier,
                userIds);
        List<AuthRecipeUserInfo> result = new ArrayList<>();
        for (UserInfoPartial userInfo : userInfos) {
            userInfo.tenantIds = tenantIdsForUserIds.get(userInfo.id).toArray(new String[0]);
        }
        return userInfos;
    }

    private static List<UserInfoPartial> fillUserInfoWithTenantIds(Start start,
                                                                   AppIdentifier appIdentifier,
                                                                   List<UserInfoPartial> userInfos)
            throws SQLException, StorageQueryException {
        String[] userIds = new String[userInfos.size()];
        for (int i = 0; i < userInfos.size(); i++) {
            userIds[i] = userInfos.get(i).id;
        }

        Map<String, List<String>> tenantIdsForUserIds = GeneralQueries.getTenantIdsForUserIds(start,
                appIdentifier,
                userIds);
        List<AuthRecipeUserInfo> result = new ArrayList<>();
        for (UserInfoPartial userInfo : userInfos) {
            userInfo.tenantIds = tenantIdsForUserIds.get(userInfo.id).toArray(new String[0]);
        }
        return userInfos;
    }

    public static void importUsers_Transaction(Connection sqlCon, Start start,
                                               Collection<PasswordlessImportUser> users)
            throws SQLException, StorageQueryException {

        String app_id_to_user_id_QUERY = "INSERT INTO " + getConfig(start).getAppIdToUserIdTable()
                + "(app_id, user_id, primary_or_recipe_user_id, recipe_id)" + " VALUES(?, ?, ?, ?)";

        String all_auth_recipe_users_QUERY = "INSERT INTO " + getConfig(start).getUsersTable() +
                "(app_id, tenant_id, user_id, primary_or_recipe_user_id, recipe_id, time_joined, " +
                "primary_or_recipe_user_time_joined)" +
                " VALUES(?, ?, ?, ?, ?, ?, ?)";

        String passwordless_users_QUERY = "INSERT INTO " + getConfig(start).getPasswordlessUsersTable()
                + "(app_id, user_id, email, phone_number, time_joined)" + " VALUES(?, ?, ?, ?, ?)";

        String passwordless_user_to_tenant_QUERY = "INSERT INTO " + getConfig(start).getPasswordlessUserToTenantTable()
                + "(app_id, tenant_id, user_id, email, phone_number)" + " VALUES(?, ?, ?, ?, ?)";

        List<PreparedStatementValueSetter> appIdToUserIdBatch = new ArrayList<>();
        List<PreparedStatementValueSetter> allAuthRecipeUsersBatch = new ArrayList<>();
        List<PreparedStatementValueSetter> passwordlessUsersBatch = new ArrayList<>();
        List<PreparedStatementValueSetter> passwordlessUserToTenantBatch = new ArrayList<>();

        for (PasswordlessImportUser user: users){
            TenantIdentifier tenantIdentifier = user.tenantIdentifier;
            appIdToUserIdBatch.add(pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, user.userId);
                pst.setString(3, user.userId);
                pst.setString(4, PASSWORDLESS.toString());
            });

            allAuthRecipeUsersBatch.add(pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, user.userId);
                pst.setString(4, user.userId);
                pst.setString(5, PASSWORDLESS.toString());
                pst.setLong(6, user.timeJoinedMSSinceEpoch);
                pst.setLong(7, user.timeJoinedMSSinceEpoch);
            });

            passwordlessUsersBatch.add(pst -> {
                       pst.setString(1, tenantIdentifier.getAppId());
                       pst.setString(2, user.userId);
                       pst.setString(3, user.email);
                       pst.setString(4, user.phoneNumber);
                       pst.setLong(5, user.timeJoinedMSSinceEpoch);
            });

            passwordlessUserToTenantBatch.add(pst -> {
                pst.setString(1, tenantIdentifier.getAppId());
                pst.setString(2, tenantIdentifier.getTenantId());
                pst.setString(3, user.userId);
                pst.setString(4, user.email);
                pst.setString(5, user.phoneNumber);
            });

        }

        executeBatch(sqlCon, app_id_to_user_id_QUERY, appIdToUserIdBatch);
        executeBatch(sqlCon, all_auth_recipe_users_QUERY, allAuthRecipeUsersBatch);
        executeBatch(sqlCon, passwordless_users_QUERY, passwordlessUsersBatch);
        executeBatch(sqlCon, passwordless_user_to_tenant_QUERY, passwordlessUserToTenantBatch);
    }

    private static class PasswordlessDeviceRowMapper implements RowMapper<PasswordlessDevice, ResultSet> {
        private static final PasswordlessDeviceRowMapper INSTANCE = new PasswordlessDeviceRowMapper();

        private PasswordlessDeviceRowMapper() {
        }

        private static PasswordlessDeviceRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public PasswordlessDevice map(ResultSet result) throws Exception {
            return new PasswordlessDevice(result.getString("device_id_hash").trim(), result.getString("email"),
                    result.getString("phone_number"), result.getString("link_code_salt"),
                    result.getInt("failed_attempts"));
        }
    }

    private static class PasswordlessCodeRowMapper implements RowMapper<PasswordlessCode, ResultSet> {
        private static final PasswordlessCodeRowMapper INSTANCE = new PasswordlessCodeRowMapper();

        private PasswordlessCodeRowMapper() {
        }

        private static PasswordlessCodeRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public PasswordlessCode map(ResultSet result) throws Exception {
            return new PasswordlessCode(result.getString("code_id"), result.getString("device_id_hash").trim(),
                    result.getString("link_code_hash"), result.getLong("created_at"));
        }
    }

    private static class UserInfoPartial {
        public final String id;
        public final long timeJoined;
        public final String email;
        public final String phoneNumber;
        public String[] tenantIds;
        public Boolean verified;
        public Boolean isPrimary;

        UserInfoPartial(String id, @Nullable String email, @Nullable String phoneNumber, long timeJoined) {
            this.id = id.trim();
            this.timeJoined = timeJoined;

            if (email == null && phoneNumber == null) {
                throw new IllegalArgumentException("Both email and phoneNumber cannot be null");
            }

            this.email = email;
            this.phoneNumber = phoneNumber;
        }

        public LoginMethod toLoginMethod() {
            assert (tenantIds != null);
            assert (verified != null);
            return new LoginMethod(id, timeJoined, verified, new LoginMethod.PasswordlessInfo(email, phoneNumber),
                    tenantIds);
        }
    }

    private static class UserInfoRowMapper implements RowMapper<UserInfoPartial, ResultSet> {
        private static final UserInfoRowMapper INSTANCE = new UserInfoRowMapper();

        private UserInfoRowMapper() {
        }

        private static UserInfoRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public UserInfoPartial map(ResultSet result) throws Exception {
            return new UserInfoPartial(result.getString("user_id"), result.getString("email"),
                    result.getString("phone_number"), result.getLong("time_joined"));
        }
    }

    private static class UserInfoWithTenantId {
        public final String userId;
        public final String tenantId;
        public final String email;
        public final String phoneNumber;

        public UserInfoWithTenantId(String userId, String tenantId, String email, String phoneNumber) {
            this.userId = userId;
            this.tenantId = tenantId;
            this.email = email;
            this.phoneNumber = phoneNumber;
        }
    }
}
