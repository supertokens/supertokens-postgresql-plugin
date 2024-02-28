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

import com.google.gson.Gson;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static String generateCommaSeperatedQuestionMarks(int size) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; i++) {
            builder.append("?");
            if (i != size - 1) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    public static String[] getStringArrayFromJsonString(String input) {
        if (input == null) {
            return null;
        }
        return new Gson().fromJson(input, String[].class);
    }

    public static String maskDBPassword(String log) {
        String regex = "(\\|db_pass\\|)(.*?)(\\|db_pass\\|)";

        Matcher matcher = Pattern.compile(regex).matcher(log);
        StringBuffer maskedLog = new StringBuffer();

        while (matcher.find()) {
            String maskedPassword = "*".repeat(8);
            matcher.appendReplacement(maskedLog, "|" + maskedPassword + "|");
        }

        matcher.appendTail(maskedLog);
        return maskedLog.toString();
    }
}
