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
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import java.sql.Statement;
import java.util.Map;

public class V5__plugin_version_5_0_4 extends BaseJavaMigration {

    @Override
    public void migrate(Context context) throws Exception {
        Map<String, String> ph = context.getConfiguration().getPlaceholders();
        Start start = MigrationContextManager.getContext(ph.get("process_id"));
        String appIdToUserIdTable = Config.getConfig(start).getAppIdToUserIdTable();

        try (Statement statement = context.getConnection().createStatement()) {
            statement.execute("CREATE INDEX IF NOT EXISTS app_id_to_user_id_primary_user_id_index ON "
                    + appIdToUserIdTable + "(primary_or_recipe_user_id, app_id);");
        }
    }
}