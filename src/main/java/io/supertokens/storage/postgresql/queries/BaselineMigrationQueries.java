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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.select;

public class BaselineMigrationQueries {

    /*
    We are dealing with existing users who have already performed manual database migration, so it's important to
    handle the situation carefully to avoid issues and data inconsistencies. To accomplish this, we need to ascertain
    the migration version that the user has manually executed. This will serve as the baseline for our subsequent Flyway migration.
    */
    public static String getBaselineMigrationVersion(Start start, Connection con)
            throws SQLException, StorageQueryException {
        /* TODO: Some migration include multiple SQL queries. Will have to define what a successful manual migration
            looks like.
            This is a bare implementation of the idea of checking if the latest migration is present in database. If
            the answer is yes, then we assume the all the migration until that were done.
         */
        String resultSet = select(con, getQueryToCheckForMigrationV3(start));
        if (resultSet.equals("1")) return "3";

        resultSet = select(con, getQueryToCheckForMigrationV2(start));
        if (resultSet.equals("1")) return "2";

        resultSet = select(con, getQueryToCheckForMigrationV1(start));
        if (resultSet.equals("1")) return "1";

        return "0";
    }

    private static String getQueryToCheckForMigrationV1(Start start) {
        return "SELECT COUNT(*) AS table_exists\n" +
                "FROM information_schema.tables\n" +
                "WHERE table_catalog = 'supertokens'\n" +
                "  AND table_name = 'something';";
    }

    private static String getQueryToCheckForMigrationV2(Start start) {
        return "SELECT COUNT(*) AS table_exists\n" +
                "FROM information_schema.tables\n" +
                "WHERE table_catalog = 'supertokens'\n" +
                "  AND table_name = 'something_else';";
    }

    private static String getQueryToCheckForMigrationV3(Start start) {
        return "SELECT COUNT(*) AS table_exists\n" +
                "FROM information_schema.tables\n" +
                "WHERE table_catalog = 'supertokens'\n" +
                "  AND table_name = 'something_else_but_nothing';";
    }
}
