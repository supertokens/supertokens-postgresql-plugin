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
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.output.Logging;
import io.supertokens.storage.postgresql.queries.BaselineMigrationQueries;
import org.flywaydb.core.Flyway;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public final class FlywayMigration {

    private FlywayMigration() {}

    public static void startMigration(Start start) throws SQLException, StorageQueryException {
        Logging.info(start, "Starting migration.", true);
        MigrationContextManager.putContext(start.getProcessId(), start);

        try {
            Flyway flyway = Flyway.configure()
                    .dataSource(ConnectionPool.getHikariDataSource(start))
                    .baselineOnMigrate(true)
                    .baselineVersion(BaselineMigrationQueries.getBaselineMigrationVersion(start))
                    .table(Config.getConfig(start).getFlywaySchemaHistory())
                    .locations("classpath:/io/supertokens/storage/postgresql/migrations")
                    .placeholders(getPlaceholders(start))
                    .load();
            flyway.clean();
            flyway.migrate();
        } finally {
            MigrationContextManager.removeContext(start.getProcessId());
        }
    }

    private static Map<String, String> getPlaceholders(Start start) {
        Map<String, String> ph = new HashMap<>();
        ph.put("process_id", start.getProcessId());
        ph.put("access_token_signing_key_dynamic", String.valueOf( Config.getConfig(start).getAccessTokenSigningKeyDynamic()));
        return ph;
    }
}
