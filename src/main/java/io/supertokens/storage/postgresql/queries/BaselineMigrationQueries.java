/*
 *    Copyright (c) 2023, VRAI Labs and/or its affiliates. All rights reserved.
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
import org.postgresql.util.PSQLException;
import java.sql.SQLException;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.config.Config.getConfig;

public class BaselineMigrationQueries {
    /* To verify if a migration was done, we are verifying for creation of INDEXES, COLUMNS and TABLES. We do this by
      counting. If the result is "1" => the migration is done. "0" => migration not done
     */
    private static final String MIGRATION_EXISTS = "1";
    // This means Flyway is free to migrate starting with first migration.
    private static final String NO_BASELINE_FOUND = "0";
    public static final int LAST_MIGRATION = 5;
    private static final int FIRST_MIGRATION = 1;
    /*
    We are dealing with existing users who have already performed manual database migration, so it's important to
    handle the situation carefully to avoid issues and data inconsistencies. To accomplish this, we need to ascertain
    the migration version that the user has manually executed. This will serve as the baseline for our subsequent Flyway migration.
    */
    public static String getBaselineMigrationVersion(Start start)
            throws SQLException, StorageQueryException {
        for (int migrationVersion = LAST_MIGRATION; migrationVersion >= FIRST_MIGRATION; migrationVersion--) {
            try {
                if (checkMigration(start, migrationVersion).equals(MIGRATION_EXISTS)) {
                    return String.valueOf(migrationVersion);
                }
            } catch (PSQLException e) {
                // If the database is empty some checkMigration script will throw an exception because if table does not
                // exist, the column can't exist either.
            }
        }
       return NO_BASELINE_FOUND;
    }

    private static String checkMigration(Start start, int migrationVersion) throws SQLException, StorageQueryException {
        return execute(start, getQueryToCheckForMigration(start, migrationVersion), null, result -> {
            result.next();
            return result.getString("MIGRATION_EXISTS");
        });
    }

    private static String getQueryToCheckForMigration(Start start, int migrationVersion) {
        switch (migrationVersion) {
            case 5:
                return getQueryToCheckForMigrationV5(start);
            case 4:
                return getQueryToCheckForMigrationV4(start);
            case 3:
                return getQueryToCheckForMigrationV3(start);
            case 2:
                return getQueryToCheckForMigrationV2(start);
            case 1:
                return getQueryToCheckForMigrationV1(start);
            default:
                throw new IllegalArgumentException("Unknown migration version: " + migrationVersion);
        }
    }

    private static String getQueryToCheckForMigrationV1(Start start) {
        return "SELECT COUNT(*) AS MIGRATION_EXISTS FROM information_schema.tables" +
                " WHERE table_name =" + getConfig(start).getKeyValueTable();
    }

    private static String getQueryToCheckForMigrationV2(Start start) {
        return "SELECT COUNT(*) AS MIGRATION_EXISTS FROM information_schema.columns" +
                " WHERE table_name = " + Config.getConfig(start).getSessionInfoTable() +
                " AND column_name = 'use_static_key';";
    }

    private static String getQueryToCheckForMigrationV3(Start start) {
        return "SELECT COUNT(*) AS MIGRATION_EXISTS FROM information_schema.tables" +
                " WHERE table_name =" +Config.getConfig(start).getTenantsTable();
    }

    private static String getQueryToCheckForMigrationV4(Start start) {
        return "SELECT COUNT(*) AS MIGRATION_EXISTS FROM information_schema.columns" +
                " WHERE table_name =" + Config.getConfig(start).getPasswordResetTokensTable() +
                " AND column_name = 'email';";
    }

    private static String getQueryToCheckForMigrationV5(Start start) {
        return "SELECT COUNT(*) AS MIGRATION_EXISTS FROM pg_indexes WHERE indexname = " +
                "'app_id_to_user_id_primary_user_id_index'";
    }
}
