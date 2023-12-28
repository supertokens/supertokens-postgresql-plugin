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

package io.supertokens.storage.postgresql;

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.storage.postgresql.output.Logging;
import io.supertokens.storage.postgresql.queries.BaselineMigrationQueries;
import org.flywaydb.core.Flyway;
import java.io.IOException;
import java.io.InputStream;
import java.net.CookieHandler;
import java.net.URL;
import java.sql.SQLException;
import java.util.Enumeration;

public final class FlywayMigration {

    private FlywayMigration() {}

    public static void startMigration(Start start) {
        Logging.info(start, "Setting up Flyway.", true);

        try {
            /*TODO:
               Only perform getBaselineVersion once and only if flyway migration history table does not contain all
               the migration. After that, check how to store the baselineversion in database. Flyway might have
               something.
            */
            String baselineVersion = getBaselineVersion(start);

            Flyway flyway = Flyway.configure()
                    .dataSource(ConnectionPool.getHikariDataSource(start))
                    .baselineOnMigrate(true)
                    .baselineVersion(baselineVersion)
                    .load();
            flyway.migrate();
        } catch (Exception e) {
            Logging.error(start, "Error Setting up Flyway.", true);
           // TODO: Find all possible exception
        }
    }


    private static String getBaselineVersion(Start start) throws IOException, SQLException, StorageQueryException {
        return BaselineMigrationQueries.getBaselineMigrationVersion(start, ConnectionPool.getConnection(start));

        /*
         TODO: Remove this if current solution if validated.
        Logging.info(start, "Starting baseline.", true);
        ClassLoader classLoader = FlywayMigration.class.getClassLoader();
        String migrationPath = "classpath:db/baseline";
        Enumeration<URL> resources = classLoader.getResources(migrationPath);

        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            try (InputStream inputStream = resource.openStream()) {
                // Print the files at the specified path
                Logging.info(start,"Files at " + resource + ":", true);
                // For simplicity, just print the file names
                // In a real-world scenario, you may want to parse the URLs and extract meaningful information
                while (inputStream.read() != -1) {
                    Logging.info(start,"  " + inputStream, true);
                }
            }
        }
         */
    }

}
