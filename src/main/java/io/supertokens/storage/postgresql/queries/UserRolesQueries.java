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
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "fkey") + " FOREIGN KEY(role)"
                + " REFERENCES " + getConfig(start).getRolesTable()
                +"(role) ON DELETE CASCADE );";

        // @formatter:on
    }

    // CONSTRAINT role_permissions_pkey PRIMARY KEY (role, permission),
    // CONSTRAINT role_permissions_role_fkey FOREIGN KEY (role) REFERENCES roles(role) ON DELETE CASCADE

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
                + "CONSTRAINT " + Utils.getConstraintName(schema, tableName, null, "fkey") + " FOREIGN KEY(role)"
                + " REFERENCES " + getConfig(start).getRolesTable()
                +"(role) ON DELETE CASCADE );";

        // @formatter:on
    }

    public static String getQueryToCreateUserRolesRoleIndex(Start start) {
        return "CREATE INDEX user_roles_role_index ON " + getConfig(start).getUserRolesTable() + "(role);";
    }

    public static void createNewRole_Transaction(Start start, Connection con, String role)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getRolesTable() + " VALUES(?);";
        update(con, QUERY, pst -> pst.setString(1, role));
    }

    public static void addPermissionToRole_Transaction(Start start, Connection con, String role, String permission)
            throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + getConfig(start).getUserRolesPermissionsTable()
                + " (role, permission) VALUES(?, ?)";

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
}
