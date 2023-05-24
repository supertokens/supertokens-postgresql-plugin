/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
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
 *
 */

package io.supertokens.storage.postgresql.utils;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class Utils {
    public static String exceptionStacktraceToString(Exception e) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        e.printStackTrace(ps);
        ps.close();
        return baos.toString();
    }

    public static String getConstraintName(String schema, String prefixedTableName, String column, String typeSuffix) {
        StringBuilder constraintName = new StringBuilder(prefixedTableName);
        if (prefixedTableName.startsWith(schema)) {
            // We also have to delete the . after the schema name
            constraintName.delete(0, schema.length() + 1);
        }

        if (column != null) {
            constraintName.append('_').append(column);
        }
        constraintName.append('_').append(typeSuffix);
        return constraintName.toString();
    }
    public static String appendQuotes(String input) {
        return "'" + input + "'";
    }

    public static String getIndexName(String prefixedTableName, String column) {
        StringBuilder indexName = new StringBuilder(prefixedTableName);

        if (column != null) {
            indexName.append('_').append(column);
        }
        indexName.append('_').append("index");
        return indexName.toString();
    }

}
