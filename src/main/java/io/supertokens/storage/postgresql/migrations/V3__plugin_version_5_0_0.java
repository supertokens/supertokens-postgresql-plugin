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

import io.supertokens.storage.postgresql.utils.Utils;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Statement;
import java.util.Map;

public class V3__plugin_version_5_0_0 extends BaseJavaMigration {

    @Override
    public void migrate(Context context) throws Exception {
        Map<String, String> ph = context.getConfiguration().getPlaceholders();
        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " ADD COLUMN IF NOT EXISTS primary_or_recipe_user_id CHAR(36) NOT NULL DEFAULT '0';");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " ADD COLUMN IF NOT EXISTS is_linked_or_is_a_primary_user BOOLEAN NOT NULL DEFAULT FALSE;");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " ADD COLUMN IF NOT EXISTS primary_or_recipe_user_time_joined BIGINT NOT NULL DEFAULT 0;");

            statement.execute("UPDATE " + ph.get("all_auth_recipe_users_table") +
                    " SET primary_or_recipe_user_id = user_id WHERE primary_or_recipe_user_id = '0';");

            statement.execute("UPDATE " + ph.get("all_auth_recipe_users_table") +
                    " SET primary_or_recipe_user_time_joined = time_joined WHERE primary_or_recipe_user_time_joined = 0;");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("all_auth_recipe_users_table"), "primary_or_recipe_user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("all_auth_recipe_users_table"), "primary_or_recipe_user_id", "fkey") + " FOREIGN KEY " +
                            "(app_id,primary_or_recipe_user_id) " +
                    "REFERENCES " + ph.get("app_id_to_user_id_table") + " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("all_auth_recipe_users_table") +
                    " ALTER primary_or_recipe_user_id DROP DEFAULT;");

            statement.execute("ALTER TABLE " + ph.get("app_id_to_user_id_table") +
                    " ADD COLUMN IF NOT EXISTS primary_or_recipe_user_id CHAR(36) NOT NULL DEFAULT '0';");

            statement.execute("ALTER TABLE " + ph.get("app_id_to_user_id_table") +
                    " ADD COLUMN IF NOT EXISTS is_linked_or_is_a_primary_user BOOLEAN NOT NULL DEFAULT FALSE;");

            statement.execute("UPDATE " + ph.get("app_id_to_user_id_table") +
                    " SET primary_or_recipe_user_id = user_id WHERE primary_or_recipe_user_id = '0';");

            statement.execute("ALTER TABLE " + ph.get("app_id_to_user_id_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("app_id_to_user_id_table") , "primary_or_recipe_user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("app_id_to_user_id_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                            ph.get("app_id_to_user_id_table"), "primary_or_recipe_user_id", "fkey") + " FOREIGN KEY " +
                            "(app_id,primary_or_recipe_user_id) " +
                    "REFERENCES " + ph.get("app_id_to_user_id_table") + " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("app_id_to_user_id_table") +
                    " ALTER primary_or_recipe_user_id DROP DEFAULT;");

            statement.execute("DROP INDEX IF EXISTS all_auth_recipe_users_pagination_index;");

            statement.execute(
                    "CREATE INDEX IF NOT EXISTS all_auth_recipe_users_pagination_index1 ON " + ph.get(
                            "all_auth_recipe_users_table") +
                            " (app_id, tenant_id, primary_or_recipe_user_time_joined DESC, primary_or_recipe_user_id DESC);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_users_pagination_index2 ON " + ph.get(
                    "all_auth_recipe_users_table") +
                    " (app_id, tenant_id, primary_or_recipe_user_time_joined ASC, primary_or_recipe_user_id DESC);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_users_pagination_index3 ON " + ph.get(
                    "all_auth_recipe_users_table") +
                    " (recipe_id, app_id, tenant_id, primary_or_recipe_user_time_joined DESC, primary_or_recipe_user_id DESC);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_users_pagination_index4 ON " + ph.get(
                    "all_auth_recipe_users_table") +
                    " (recipe_id, app_id, tenant_id, primary_or_recipe_user_time_joined ASC, primary_or_recipe_user_id DESC);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_users_primary_user_id_index ON " + ph.get(
                    "all_auth_recipe_users_table") +
                    " (primary_or_recipe_user_id, app_id);");

            statement.execute("CREATE INDEX IF NOT EXISTS all_auth_recipe_users_recipe_id_index ON " + ph.get(
                    "all_auth_recipe_users_table") +
                    " (app_id, recipe_id, tenant_id);");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_pswd_reset_tokens_table") +
                    " DROP CONSTRAINT IF EXISTS " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("emailpassword_pswd_reset_tokens_table"), "user_id", "fkey") + ";");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_pswd_reset_tokens_table") +
                    " ADD CONSTRAINT " + Utils.getConstraintName(ph.get("schema"),
                    ph.get("emailpassword_pswd_reset_tokens_table"), "user_id", "fkey") +
                    "   FOREIGN KEY (app_id, user_id) " +
                    "   REFERENCES " + ph.get("app_id_to_user_id_table") +
                    " (app_id, user_id) ON DELETE CASCADE;");

            statement.execute("ALTER TABLE " + ph.get("emailpassword_pswd_reset_tokens_table") +
                    " ADD COLUMN IF NOT EXISTS email VARCHAR(256);");
        }
    }
}
