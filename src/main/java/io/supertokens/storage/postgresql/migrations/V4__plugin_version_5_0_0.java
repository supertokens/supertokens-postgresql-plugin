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
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Statement;
import java.util.Map;

public class V4__plugin_version_5_0_0 extends BaseJavaMigration {

    @Override
    public void migrate(Context context) throws Exception {
        Map<String, String> ph = context.getConfiguration().getPlaceholders();
        Start start = MigrationContextManager.getContext(ph.get("process_id"));
        String usersTable = Config.getConfig(start).getUsersTable();
        String schema = Config.getConfig(start).getTableSchema();
        String appIdToUserIdTable = Config.getConfig(start).getAppIdToUserIdTable();
        String passwordResetTokensTable = Config.getConfig(start).getPasswordResetTokensTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + usersTable +
                    " ADD COLUMN IF NOT EXISTS primary_or_recipe_user_id CHAR(36) NOT NULL DEFAULT '0';");

            statement.execute("ALTER TABLE " + usersTable +
                    " ADD COLUMN IF NOT EXISTS is_linked_or_is_a_primary_user BOOLEAN NOT NULL DEFAULT FALSE;");

            statement.execute("ALTER TABLE " + usersTable +
                    " ADD COLUMN IF NOT EXISTS primary_or_recipe_user_time_joined BIGINT NOT NULL DEFAULT 0;");

            statement.execute("UPDATE " + usersTable +
                    " SET primary_or_recipe_user_id = user_id WHERE primary_or_recipe_user_id = '0';");

            statement.execute("UPDATE " + usersTable +
                    " SET primary_or_recipe_user_time_joined = time_joined WHERE primary_or_recipe_user_time_joined = 0;");

            statement.execute("ALTER TABLE " + usersTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    usersTable, "primary_or_recipe_user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + usersTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    usersTable, "primary_or_recipe_user_id", "fkey") + " FOREIGN KEY " +
                            "(app_id,primary_or_recipe_user_id) " +
                    "REFERENCES " + appIdToUserIdTable + " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + usersTable +
                    " ALTER primary_or_recipe_user_id DROP DEFAULT;");

            statement.execute("ALTER TABLE " + appIdToUserIdTable +
                    " ADD COLUMN IF NOT EXISTS primary_or_recipe_user_id CHAR(36) NOT NULL DEFAULT '0';");

            statement.execute("ALTER TABLE " + appIdToUserIdTable +
                    " ADD COLUMN IF NOT EXISTS is_linked_or_is_a_primary_user BOOLEAN NOT NULL DEFAULT FALSE;");

            statement.execute("UPDATE " + appIdToUserIdTable +
                    " SET primary_or_recipe_user_id = user_id WHERE primary_or_recipe_user_id = '0';");

            statement.execute("ALTER TABLE " + appIdToUserIdTable +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    appIdToUserIdTable , "primary_or_recipe_user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + appIdToUserIdTable +
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                            appIdToUserIdTable, "primary_or_recipe_user_id", "fkey") + " FOREIGN KEY " +
                            "(app_id,primary_or_recipe_user_id) " +
                    "REFERENCES " + appIdToUserIdTable + " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + appIdToUserIdTable +
                    " ALTER primary_or_recipe_user_id DROP DEFAULT;");

            statement.execute("DROP INDEX IF EXISTS all_auth_recipe_users_pagination_index;");

            statement.execute(
                    "CREATE INDEX IF NOT EXISTS all_auth_recipe_users_pagination_index1 ON " + usersTable +
                            " (app_id, tenant_id, primary_or_recipe_user_time_joined DESC, primary_or_recipe_user_id DESC);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_users_pagination_index2 ON " + usersTable +
                    " (app_id, tenant_id, primary_or_recipe_user_time_joined ASC, primary_or_recipe_user_id DESC);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_users_pagination_index3 ON " + usersTable +
                    " (recipe_id, app_id, tenant_id, primary_or_recipe_user_time_joined DESC, primary_or_recipe_user_id DESC);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_users_pagination_index4 ON " + usersTable +
                    " (recipe_id, app_id, tenant_id, primary_or_recipe_user_time_joined ASC, primary_or_recipe_user_id DESC);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_users_primary_user_id_index ON " + usersTable +
                    " (primary_or_recipe_user_id, app_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_users_recipe_id_index ON " + usersTable +
                    " (app_id, recipe_id, tenant_id);");

            statement.execute("ALTER TABLE " + passwordResetTokensTable+
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(schema,
                    passwordResetTokensTable, "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + passwordResetTokensTable+
                    " ADD CONSTRAINT " + Utils.getConstraintName(schema,
                    passwordResetTokensTable, "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + appIdToUserIdTable +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + passwordResetTokensTable+
                    " ADD COLUMN IF NOT EXISTS email VARCHAR(256);");



        }
    }
}
