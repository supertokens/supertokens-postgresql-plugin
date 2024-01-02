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

package io.supertokens.storage.postgresql.migrations;

import io.supertokens.storage.postgresql.MigrationContextManager;
import io.supertokens.storage.postgresql.Start;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Statement;
import java.util.Map;

import static io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.STARTING_MIGRATION;
import static io.supertokens.storage.postgresql.ProcessState.getInstance;

public class V2__plugin_version_3_0_0 extends BaseJavaMigration {

    @Override
    public void migrate(Context context) throws Exception {
        Map<String, String> ph = context.getConfiguration().getPlaceholders();
        Start start = MigrationContextManager.getContext(ph.get("process_id"));
        getInstance(start).addState(STARTING_MIGRATION, null);

        try (Statement statement = context.getConnection().createStatement()) {
            // Add a new column with a default value
            statement.execute("ALTER TABLE " + ph.get("session_info_table") + " ADD COLUMN IF NOT EXISTS use_static_key BOOLEAN NOT NULL DEFAULT" +
                    "(" + !Boolean.parseBoolean(ph.get("access_token_signing_key_dynamic")) + ")");
            // Alter the column to drop the default value
            statement.execute("ALTER TABLE " + ph.get("session_info_table") + " ALTER COLUMN " +
                    "use_static_key DROP DEFAULT");

            // Insert data into jwt_signing_keys from session_access_token_signing_keys
            statement.execute("INSERT INTO " + ph.get("jwt_signing_keys_table") + " (key_id, key_string, algorithm, " +
                    "created_at) " +
                    "SELECT CONCAT('s-', created_at_time) as key_id, value as key_string, 'RS256' as algorithm, created_at_time as created_at " +
                    "FROM " + ph.get("session_access_token_signing_keys_table"));
        }
    }
}
