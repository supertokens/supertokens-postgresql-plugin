/*
 *    Copyright (c) 2022, VRAI Labs and/or its affiliates. All rights reserved.
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

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.storage.postgresql.PreparedStatementValueSetter;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class UserRolesQueries {
    public static String getQueryToCreateRolesTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = getConfig(start).getRolesTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "role VARCHAR(255) NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey") + " PRIMARY KEY(role)" + " );";

        // @formatter:on
    }

    public static String getQueryToCreateRolePermissionsTable(Start start) {
        String tableName = getConfig(start).getUserRolesPermissionsTable();
        String schema = Config.getConfig(start).getTableSchema();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "role VARCHAR(255) NOT NULL,"
                + "permission VARCHAR(255) NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey") + " PRIMARY KEY(role, permission),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "role", "fkey") + " FOREIGN KEY(role)"
                + " REFERENCES " + getConfig(start).getRolesTable()
                +"(role) ON DELETE CASCADE );";

        // @formatter:on
    }

    static String getQueryToCreateRolePermissionsPermissionIndex(Start start) {
        return "CREATE INDEX role_permissions_permission_index ON " + getConfig(start).getUserRolesPermissionsTable()
                + "(permission);";
    }

    public static String getQueryToCreateUserRolesTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String tableName = getConfig(start).getUserRolesTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                + "user_id VARCHAR(128) NOT NULL,"
                + "role VARCHAR(255) NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "pkey") + " PRIMARY KEY(user_id, role),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, "role", "fkey") + " FOREIGN KEY(role)"
                + " REFERENCES " + getConfig(start).getRolesTable()
                +"(role) ON DELETE CASCADE );";

        // @formatter:on
    }

    public static String getQueryToCreateUserRolesRoleIndex(Start start) {
        return "CREATE INDEX user_roles_role_index ON " + getConfig(start).getUserRolesTable() + "(role);";
    }

    public static boolean createNewRoleOrDoNothingIfExists_Transaction(Start start, Connection con, String role)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getRolesTable() + " VALUES(?) ON CONFLICT DO NOTHING;";
        int rowsUpdated = update(con, QUERY, pst -> pst.setString(1, role));
        return rowsUpdated > 0;
    }

    public static void addPermissionToRoleOrDoNothingIfExists_Transaction(Start start, Connection con, String role,
            String permission) throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getUserRolesPermissionsTable()
                + " (role, permission) VALUES(?, ?) ON CONFLICT DO NOTHING";

        update(con, QUERY, pst -> {
            pst.setString(1, role);
            pst.setString(2, permission);
        });
    }

    public static boolean deleteRole(Start start, String role) throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getRolesTable() + " WHERE role = ? ;";
        return update(start, QUERY, pst -> {
            pst.setString(1, role);
        }) == 1;
    }

    public static boolean doesRoleExist(Start start, String role) throws SQLException, StorageQueryException {
        String QUERY = "SELECT 1 FROM " + getConfig(start).getRolesTable() + " WHERE role = ?";
        return execute(start, QUERY, pst -> pst.setString(1, role), ResultSet::next);
    }

    public static String[] getPermissionsForRole(Start start, String role) throws SQLException, StorageQueryException {
        String QUERY = "SELECT permission FROM " + Config.getConfig(start).getUserRolesPermissionsTable()
                + " WHERE role = ?;";
        return execute(start, QUERY, pst -> pst.setString(1, role), result -> {
            ArrayList<String> permissions = new ArrayList<>();
            while (result.next()) {
                permissions.add(result.getString("permission"));
            }
            return permissions.toArray(String[]::new);
        });
    }

    public static String[] getRoles(Start start) throws SQLException, StorageQueryException {
        String QUERY = "SELECT role FROM " + getConfig(start).getRolesTable();
        return execute(start, QUERY, PreparedStatementValueSetter.NO_OP_SETTER, result -> {
            ArrayList<String> roles = new ArrayList<>();
            while (result.next()) {
                roles.add(result.getString("role"));
            }
            return roles.toArray(String[]::new);
        });
    }

    public static int addRoleToUser(Start start, String userId, String role)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getUserRolesTable() + "(user_id, role) VALUES(?, ?);";
        return update(start, QUERY, pst -> {
            pst.setString(1, userId);
            pst.setString(2, role);
        });
    }

    public static String[] getRolesForUser(Start start, String userId) throws SQLException, StorageQueryException {
        String QUERY = "SELECT role FROM " + getConfig(start).getUserRolesTable() + " WHERE user_id = ? ;";

        return execute(start, QUERY, pst -> pst.setString(1, userId), result -> {
            ArrayList<String> roles = new ArrayList<>();
            while (result.next()) {
                roles.add(result.getString("role"));
            }
            return roles.toArray(String[]::new);
        });
    }

    public static boolean deleteRoleForUser_Transaction(Start start, Connection con, String userId, String role)
            throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getUserRolesTable() + " WHERE user_id = ? AND role = ? ;";

        // store the number of rows updated
        int rowUpdatedCount = update(con, QUERY, pst -> {
            pst.setString(1, userId);
            pst.setString(2, role);
        });
        return rowUpdatedCount > 0;
    }

    public static boolean doesRoleExist_transaction(Start start, Connection con, String role)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT 1 FROM " + getConfig(start).getRolesTable() + " WHERE role = ? FOR UPDATE";
        return execute(con, QUERY, pst -> pst.setString(1, role), ResultSet::next);
    }

    public static String[] getUsersForRole(Start start, String role) throws SQLException, StorageQueryException {
        String QUERY = "SELECT user_id FROM " + getConfig(start).getUserRolesTable() + " WHERE role = ? ";
        return execute(start, QUERY, pst -> pst.setString(1, role), result -> {
            ArrayList<String> userIds = new ArrayList<>();
            while (result.next()) {
                userIds.add(result.getString("user_id"));
            }
            return userIds.toArray(String[]::new);
        });
    }

    public static boolean deletePermissionForRole_Transaction(Start start, Connection con, String role,
            String permission) throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getUserRolesPermissionsTable()
                + " WHERE role = ? AND permission = ? ";

        // store the number of rows updated
        int rowUpdatedCount = update(con, QUERY, pst -> {
            pst.setString(1, role);
            pst.setString(2, permission);
        });

        return rowUpdatedCount > 0;
    }

    public static int deleteAllPermissionsForRole_Transaction(Start start, Connection con, String role)
            throws SQLException, StorageQueryException {

        String QUERY = "DELETE FROM " + getConfig(start).getUserRolesPermissionsTable() + " WHERE role = ? ";
        // return the number of rows updated
        return update(con, QUERY, pst -> {
            pst.setString(1, role);
        });

    }

    public static String[] getRolesThatHavePermission(Start start, String permission)
            throws SQLException, StorageQueryException {

        String QUERY = "SELECT role FROM " + getConfig(start).getUserRolesPermissionsTable() + " WHERE permission = ? ";

        return execute(start, QUERY, pst -> pst.setString(1, permission), result -> {
            ArrayList<String> roles = new ArrayList<>();

            while (result.next()) {
                roles.add(result.getString("role"));
            }

            return roles.toArray(String[]::new);
        });
    }

    public static int deleteAllRolesForUser(Start start, String userId) throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + getConfig(start).getUserRolesTable() + " WHERE user_id = ?";
        return update(start, QUERY, pst -> pst.setString(1, userId));
    }

}
