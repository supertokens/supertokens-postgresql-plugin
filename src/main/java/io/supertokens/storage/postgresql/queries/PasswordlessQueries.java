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
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.passwordless.PasswordlessCode;
import io.supertokens.pluginInterface.passwordless.PasswordlessDevice;
import io.supertokens.pluginInterface.passwordless.UserInfo;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.supertokens.pluginInterface.RECIPE_ID.PASSWORDLESS;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class PasswordlessQueries {
    public static String getQueryToCreateUsersTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String usersTable = Config.getConfig(start).getPasswordlessUsersTable();

        return "CREATE TABLE IF NOT EXISTS " + usersTable + " (" + "user_id CHAR(36) NOT NULL,"
                + "email VARCHAR(256) CONSTRAINT " + Utils.getConstraintName(schema, usersTable, "email", "key")
                + " UNIQUE," + "phone_number VARCHAR(256) CONSTRAINT "
                + Utils.getConstraintName(schema, usersTable, "phone_number", "key") + " UNIQUE,"
                + "time_joined BIGINT NOT NULL, " + "CONSTRAINT "
                + Utils.getConstraintName(schema, usersTable, null, "pkey") + " PRIMARY KEY (user_id)" + ");";
    }

    public static String getQueryToCreateDevicesTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String devicesTable = Config.getConfig(start).getPasswordlessDevicesTable();

        return "CREATE TABLE IF NOT EXISTS " + devicesTable + " (" + "device_id_hash CHAR(44) NOT NULL,"
                + "email VARCHAR(256), " + "phone_number VARCHAR(256)," + "link_code_salt CHAR(44) NOT NULL,"
                + "failed_attempts INT NOT NULL," + "CONSTRAINT "
                + Utils.getConstraintName(schema, devicesTable, null, "pkey") + " PRIMARY KEY (device_id_hash));";
    }

    public static String getQueryToCreateCodesTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String codesTable = Config.getConfig(start).getPasswordlessCodesTable();

        return "CREATE TABLE IF NOT EXISTS " + codesTable + " (" + "code_id CHAR(36) NOT NULL,"
                + "device_id_hash CHAR(44) NOT NULL," + "link_code_hash CHAR(44) NOT NULL CONSTRAINT "
                + Utils.getConstraintName(schema, codesTable, "link_code_hash", "key") + " UNIQUE,"
                + "created_at BIGINT NOT NULL," + "CONSTRAINT "
                + Utils.getConstraintName(schema, codesTable, null, "pkey") + " PRIMARY KEY (code_id)," + "CONSTRAINT "
                + Utils.getConstraintName(schema, codesTable, "device_id_hash", "fkey")
                + " FOREIGN KEY (device_id_hash) " + "REFERENCES "
                + Config.getConfig(start).getPasswordlessDevicesTable()
                + "(device_id_hash) ON DELETE CASCADE ON UPDATE CASCADE);";
    }

    public static String getQueryToCreateDeviceEmailIndex(Start start) {
        return "CREATE INDEX passwordless_devices_email_index ON "
                + Config.getConfig(start).getPasswordlessDevicesTable() + " (email);"; // USING hash
    }

    public static String getQueryToCreateDevicePhoneNumberIndex(Start start) {
        return "CREATE INDEX passwordless_devices_phone_number_index ON "
                + Config.getConfig(start).getPasswordlessDevicesTable() + " (phone_number);"; // USING hash
    }

    public static String getQueryToCreateCodeCreatedAtIndex(Start start) {
        return "CREATE INDEX passwordless_codes_created_at_index ON "
                + Config.getConfig(start).getPasswordlessCodesTable() + "(created_at);";
    }

    public static void createDeviceWithCode(Start start, String email, String phoneNumber, String linkCodeSalt,
            PasswordlessCode code) throws StorageTransactionLogicException, StorageQueryException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                String QUERY = "INSERT INTO " + getConfig(start).getPasswordlessDevicesTable()
                        + "(device_id_hash, email, phone_number, link_code_salt, failed_attempts)"
                        + " VALUES(?, ?, ?, ?, 0)";
                update(start, QUERY, pst -> {
                    pst.setString(1, code.deviceIdHash);
                    pst.setString(2, email);
                    pst.setString(3, phoneNumber);
                    pst.setString(4, linkCodeSalt);
                });

                createCode_Transaction(start, sqlCon, code);
                sqlCon.commit();
            } catch (SQLException throwables) {
                throw new StorageTransactionLogicException(throwables);
            }
            return null;
        });
    }

    public static PasswordlessDevice getDevice_Transaction(Start start, Connection con, String deviceIdHash)
            throws StorageQueryException, SQLException {

        String QUERY = "SELECT device_id_hash, email, phone_number, link_code_salt, failed_attempts FROM "
                + getConfig(start).getPasswordlessDevicesTable() + " WHERE device_id_hash = ? FOR UPDATE";
        return execute(con, QUERY, pst -> {
            pst.setString(1, deviceIdHash);
        }, result -> {
            if (result.next()) {
                return PasswordlessDeviceRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static void incrementDeviceFailedAttemptCount_Transaction(Start start, Connection con, String deviceIdHash)
            throws SQLException, StorageQueryException {
        String QUERY = "UPDATE " + getConfig(start).getPasswordlessDevicesTable()
                + " SET failed_attempts = failed_attempts + 1 WHERE device_id_hash = ?";

        update(start, QUERY, pst -> {
            pst.setString(1, deviceIdHash);
        });
    }

    public static void deleteDevice_Transaction(Start start, Connection con, String deviceIdHash)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessDevicesTable() + " WHERE device_id_hash = ?";
        update(start, QUERY, pst -> {
            pst.setString(1, deviceIdHash);
        });
    }

    public static void deleteDevicesByPhoneNumber_Transaction(Start start, Connection con, @Nonnull String phoneNumber)
            throws SQLException, StorageQueryException {

        String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessDevicesTable() + " WHERE phone_number = ?";

        update(start, QUERY, pst -> {
            pst.setString(1, phoneNumber);
        });
    }

    public static void deleteDevicesByEmail_Transaction(Start start, Connection con, @Nonnull String email)
            throws SQLException, StorageQueryException {

        String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessDevicesTable() + " WHERE email = ?";

        update(start, QUERY, pst -> {
            pst.setString(1, email);
        });
    }

    private static void createCode_Transaction(Start start, Connection con, PasswordlessCode code)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getPasswordlessCodesTable()
                + "(code_id, device_id_hash, link_code_hash, created_at)" + " VALUES(?, ?, ?, ?)";
        update(start, QUERY, pst -> {
            pst.setString(1, code.id);
            pst.setString(2, code.deviceIdHash);
            pst.setString(3, code.linkCodeHash);
            pst.setLong(4, code.createdAt);
        });
    }

    public static void createCode(Start start, PasswordlessCode code)
            throws StorageTransactionLogicException, StorageQueryException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();

            try {
                PasswordlessQueries.createCode_Transaction(start, sqlCon, code);
                sqlCon.commit();
            } catch (SQLException e) {
                throw new StorageTransactionLogicException(e);
            }
            return null;
        });
    }

    public static PasswordlessCode[] getCodesOfDevice_Transaction(Start start, Connection con, String deviceIdHash)
            throws StorageQueryException, SQLException {
        // We do not lock here, since the device is already locked earlier in the transaction.
        String QUERY = "SELECT code_id, device_id_hash, link_code_hash, created_at FROM "
                + getConfig(start).getPasswordlessCodesTable() + " WHERE device_id_hash = ?";

        return execute(con, QUERY, pst -> {
            pst.setString(1, deviceIdHash);
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

    public static PasswordlessCode getCodeByLinkCodeHash_Transaction(Start start, Connection con, String linkCodeHash)
            throws StorageQueryException, SQLException {
        // We do not lock here, since the device is already locked earlier in the transaction.
        String QUERY = "SELECT code_id, device_id_hash, link_code_hash, created_at FROM "
                + getConfig(start).getPasswordlessCodesTable() + " WHERE link_code_hash = ?";

        return execute(con, QUERY, pst -> {
            pst.setString(1, linkCodeHash);
        }, result -> {
            if (result.next()) {
                return PasswordlessCodeRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static void deleteCode_Transaction(Start start, Connection con, String codeId)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getPasswordlessCodesTable() + " WHERE code_id = ?";

        update(start, QUERY, pst -> {
            pst.setString(1, codeId);
        });
    }

    public static void createUser(Start start, UserInfo user)
            throws StorageTransactionLogicException, StorageQueryException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                {
                    String QUERY = "INSERT INTO " + getConfig(start).getUsersTable()
                            + "(user_id, recipe_id, time_joined)" + " VALUES(?, ?, ?)";
                    update(start, QUERY, pst -> {
                        pst.setString(1, user.id);
                        pst.setString(2, PASSWORDLESS.toString());
                        pst.setLong(3, user.timeJoined);
                    });
                }

                {
                    String QUERY = "INSERT INTO " + getConfig(start).getPasswordlessUsersTable()
                            + "(user_id, email, phone_number, time_joined)" + " VALUES(?, ?, ?, ?)";
                    update(start, QUERY, pst -> {
                        pst.setString(1, user.id);
                        pst.setString(2, user.email);
                        pst.setString(3, user.phoneNumber);
                        pst.setLong(4, user.timeJoined);
                    });
                }
                sqlCon.commit();
            } catch (SQLException throwables) {
                throw new StorageTransactionLogicException(throwables);
            }
            return null;
        });
    }

    public static void deleteUser(Start start, String userId)
            throws StorageQueryException, StorageTransactionLogicException {
        start.startTransaction(con -> {
            Connection sqlCon = (Connection) con.getConnection();
            try {
                {
                    String QUERY = "DELETE FROM " + getConfig(start).getUsersTable()
                            + " WHERE user_id = ? AND recipe_id = ?";

                    update(start, QUERY, pst -> {
                        pst.setString(1, userId);
                        pst.setString(2, PASSWORDLESS.toString());
                    });
                }

                UserInfo user;
                {
                    String QUERY = "DELETE FROM " + Config.getConfig(start).getPasswordlessUsersTable()
                            + " WHERE user_id = ? RETURNING user_id, email, phone_number, time_joined";

                    user = execute(sqlCon, QUERY, pst -> pst.setString(1, userId),
                            result -> UserInfoRowMapper.getInstance().mapOrThrow(result));
                }

                if (user != null) {
                    if (user.email != null) {
                        deleteDevicesByEmail_Transaction(start, sqlCon, user.email);
                    }
                    if (user.phoneNumber != null) {
                        deleteDevicesByPhoneNumber_Transaction(start, sqlCon, user.phoneNumber);
                    }
                }

                sqlCon.commit();
            } catch (SQLException throwables) {
                throw new StorageTransactionLogicException(throwables);
            }
            return null;
        });
    }

    public static int updateUserEmail_Transaction(Start start, Connection con, String userId, String email)
            throws SQLException, StorageQueryException {
        String QUERY = "UPDATE " + Config.getConfig(start).getPasswordlessUsersTable()
                + " SET email = ? WHERE user_id = ?";

        return update(con, QUERY, pst -> {
            pst.setString(1, email);
            pst.setString(2, userId);
        });
    }

    public static int updateUserPhoneNumber_Transaction(Start start, Connection con, String userId, String phoneNumber)
            throws SQLException, StorageQueryException {
        String QUERY = "UPDATE " + Config.getConfig(start).getPasswordlessUsersTable()
                + " SET phone_number = ? WHERE user_id = ?";

        return update(con, QUERY, pst -> {
            pst.setString(1, phoneNumber);
            pst.setString(2, userId);
        });
    }

    public static PasswordlessDevice getDevice(Start start, String deviceIdHash)
            throws StorageQueryException, SQLException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            String QUERY = "SELECT device_id_hash, email, phone_number, link_code_salt, failed_attempts FROM "
                    + getConfig(start).getPasswordlessDevicesTable() + " WHERE device_id_hash = ?";
            return execute(con, QUERY, pst -> {
                pst.setString(1, deviceIdHash);
            }, result -> {
                if (result.next()) {
                    return PasswordlessDeviceRowMapper.getInstance().mapOrThrow(result);
                }
                return null;
            });
        }
    }

    public static PasswordlessDevice[] getDevicesByEmail(Start start, @Nonnull String email)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT device_id_hash, email, phone_number, link_code_salt, failed_attempts FROM "
                + getConfig(start).getPasswordlessDevicesTable() + " WHERE email = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, email);
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

    public static PasswordlessDevice[] getDevicesByPhoneNumber(Start start, @Nonnull String phoneNumber)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT device_id_hash, email, phone_number, link_code_salt, failed_attempts FROM "
                + getConfig(start).getPasswordlessDevicesTable() + " WHERE phone_number = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, phoneNumber);
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

    public static PasswordlessCode[] getCodesOfDevice(Start start, String deviceIdHash)
            throws StorageQueryException, SQLException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            // We can call the transaction version here because it doesn't lock anything.
            return PasswordlessQueries.getCodesOfDevice_Transaction(start, con, deviceIdHash);
        }
    }

    public static PasswordlessCode[] getCodesBefore(Start start, long time) throws StorageQueryException, SQLException {
        String QUERY = "SELECT code_id, device_id_hash, link_code_hash, created_at FROM "
                + getConfig(start).getPasswordlessCodesTable() + " WHERE created_at < ?";

        return execute(start, QUERY, pst -> {
            pst.setLong(1, time);
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

    public static PasswordlessCode getCode(Start start, String codeId) throws StorageQueryException, SQLException {
        String QUERY = "SELECT code_id, device_id_hash, link_code_hash, created_at FROM "
                + getConfig(start).getPasswordlessCodesTable() + " WHERE code_id = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, codeId);
        }, result -> {
            if (result.next()) {
                return PasswordlessCodeRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static PasswordlessCode getCodeByLinkCodeHash(Start start, String linkCodeHash)
            throws StorageQueryException, SQLException {
        try (Connection con = ConnectionPool.getConnection(start)) {
            // We can call the transaction version here because it doesn't lock anything.
            return PasswordlessQueries.getCodeByLinkCodeHash_Transaction(start, con, linkCodeHash);
        }
    }

    public static List<UserInfo> getUsersByIdList(Start start, List<String> ids)
            throws SQLException, StorageQueryException {
        if (ids.size() > 0) {
            StringBuilder QUERY = new StringBuilder("SELECT user_id, email, phone_number, time_joined FROM "
                    + getConfig(start).getPasswordlessUsersTable());
            QUERY.append(" WHERE user_id IN (");
            for (int i = 0; i < ids.size(); i++) {

                QUERY.append("?");
                if (i != ids.size() - 1) {
                    // not the last element
                    QUERY.append(",");
                }
            }
            QUERY.append(")");

            return execute(start, QUERY.toString(), pst -> {
                for (int i = 0; i < ids.size(); i++) {
                    // i+1 cause this starts with 1 and not 0
                    pst.setString(i + 1, ids.get(i));
                }
            }, result -> {
                List<UserInfo> finalResult = new ArrayList<>();
                while (result.next()) {
                    finalResult.add(UserInfoRowMapper.getInstance().mapOrThrow(result));
                }
                return finalResult;
            });
        }
        return Collections.emptyList();
    }

    public static UserInfo getUserById(Start start, String userId) throws StorageQueryException, SQLException {
        List<String> input = new ArrayList<>();
        input.add(userId);
        List<UserInfo> result = getUsersByIdList(start, input);
        if (result.size() == 1) {
            return result.get(0);
        }
        return null;
    }

    public static UserInfo getUserByEmail(Start start, @Nonnull String email)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT user_id, email, phone_number, time_joined FROM "
                + getConfig(start).getPasswordlessUsersTable() + " WHERE email = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, email);
        }, result -> {
            if (result.next()) {
                return UserInfoRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
    }

    public static UserInfo getUserByPhoneNumber(Start start, @Nonnull String phoneNumber)
            throws StorageQueryException, SQLException {
        String QUERY = "SELECT user_id, email, phone_number, time_joined FROM "
                + getConfig(start).getPasswordlessUsersTable() + " WHERE phone_number = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, phoneNumber);
        }, result -> {
            if (result.next()) {
                return UserInfoRowMapper.getInstance().mapOrThrow(result);
            }
            return null;
        });
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
            return new PasswordlessDevice(result.getString("device_id_hash"), result.getString("email"),
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
            return new PasswordlessCode(result.getString("code_id"), result.getString("device_id_hash"),
                    result.getString("link_code_hash"), result.getLong("created_at"));
        }
    }

    private static class UserInfoRowMapper implements RowMapper<UserInfo, ResultSet> {
        private static final UserInfoRowMapper INSTANCE = new UserInfoRowMapper();

        private UserInfoRowMapper() {
        }

        private static UserInfoRowMapper getInstance() {
            return INSTANCE;
        }

        @Override
        public UserInfo map(ResultSet result) throws Exception {
            return new UserInfo(result.getString("user_id"), result.getString("email"),
                    result.getString("phone_number"), result.getLong("time_joined"));
        }
    }
}
