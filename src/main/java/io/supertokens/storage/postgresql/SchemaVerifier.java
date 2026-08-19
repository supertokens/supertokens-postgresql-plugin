/*
 *    Copyright (c) 2026, VRAI Labs and/or its affiliates. All rights reserved.
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

import io.supertokens.pluginInterface.exceptions.SchemaMismatchException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.queries.GeneralQueries;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;

/**
 * Verifies that the database has every table and column this plugin version reads or writes.
 *
 * <p>The expected schema is derived from the plugin's own {@code CREATE TABLE} statements
 * ({@link GeneralQueries#getAllCreateTableQueries}), so there is no second, hand-maintained column list that
 * could drift. {@code createTablesIfNotExists} only ever creates missing tables; a column added to an existing
 * table (a manual {@code ### Migration} step in the CHANGELOG) is never applied automatically, and without this
 * check the core would boot and fail per request with {@code column ... does not exist}.
 *
 * <p>Only run by {@link Start#verifySchema()} - i.e. once per storage at startup - never from the pool-init
 * path, which is re-entered on every tenant refresh and on lazy pool re-creation.
 */
public class SchemaVerifier {

    private static final String CREATE_TABLE_PREFIX = "CREATE TABLE IF NOT EXISTS";

    /** Number of times {@link #verify} actually ran in this JVM; lets tests assert "once per storage at startup". */
    public static final AtomicInteger verificationRunsForTesting = new AtomicInteger();
    private static final Set<String> TABLE_CONSTRAINT_KEYWORDS = Set.of(
            "CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "EXCLUDE", "LIKE");

    /** One table as the plugin expects it: the fully qualified name used in DDL and column -> definition text. */
    public static class ExpectedTable {
        /** Name exactly as it appears in the DDL, e.g. {@code myschema.st_session_info}. */
        public final String qualifiedName;
        /** Unqualified, lower-cased name as {@code information_schema} reports it. */
        public final String tableName;
        /** Lower-cased column name -> definition text after the name (e.g. {@code VARCHAR(128) NOT NULL}). */
        public final Map<String, String> columns;

        ExpectedTable(String qualifiedName, String tableName, Map<String, String> columns) {
            this.qualifiedName = qualifiedName;
            this.tableName = tableName;
            this.columns = columns;
        }
    }

    /**
     * Parses {@code CREATE TABLE IF NOT EXISTS <name> (<col def>, ..., <table constraint>, ...)} statements.
     * Statements that are not plain table definitions (e.g. {@code PARTITION OF}) are skipped.
     */
    public static List<ExpectedTable> parseExpectedSchema(List<String> createTableStatements) {
        List<ExpectedTable> tables = new ArrayList<>();
        for (String ddl : createTableStatements) {
            ExpectedTable table = parseCreateTable(ddl);
            if (table != null) {
                tables.add(table);
            }
        }
        return tables;
    }

    static ExpectedTable parseCreateTable(String ddl) {
        String trimmed = ddl.trim();
        if (!trimmed.regionMatches(true, 0, CREATE_TABLE_PREFIX, 0, CREATE_TABLE_PREFIX.length())) {
            throw new IllegalArgumentException("Not a CREATE TABLE IF NOT EXISTS statement: " + ddl);
        }
        int i = CREATE_TABLE_PREFIX.length();
        while (i < trimmed.length() && Character.isWhitespace(trimmed.charAt(i))) {
            i++;
        }
        int nameStart = i;
        while (i < trimmed.length() && !Character.isWhitespace(trimmed.charAt(i)) && trimmed.charAt(i) != '(') {
            i++;
        }
        String qualifiedName = trimmed.substring(nameStart, i);
        if (trimmed.substring(i).trim().toUpperCase(Locale.ROOT).startsWith("PARTITION OF")) {
            return null; // a partition inherits its parent's columns; nothing to verify here
        }
        int open = trimmed.indexOf('(', i);
        if (open < 0) {
            throw new IllegalArgumentException("No column list in: " + ddl);
        }
        int close = findMatchingCloseParenthesis(trimmed, open);
        String body = trimmed.substring(open + 1, close);

        Map<String, String> columns = new LinkedHashMap<>();
        for (String piece : splitTopLevel(body)) {
            String def = piece.trim();
            if (def.isEmpty()) {
                continue;
            }
            int sp = 0;
            while (sp < def.length() && !Character.isWhitespace(def.charAt(sp)) && def.charAt(sp) != '(') {
                sp++;
            }
            String first = def.substring(0, sp);
            if (TABLE_CONSTRAINT_KEYWORDS.contains(first.toUpperCase(Locale.ROOT))) {
                continue;
            }
            String rest = def.substring(sp).trim();
            columns.put(unquote(first).toLowerCase(Locale.ROOT), rest);
        }
        String tableName = qualifiedName.substring(qualifiedName.lastIndexOf('.') + 1);
        return new ExpectedTable(qualifiedName, unquote(tableName).toLowerCase(Locale.ROOT), columns);
    }

    private static String unquote(String ident) {
        if (ident.length() >= 2 && ident.startsWith("\"") && ident.endsWith("\"")) {
            return ident.substring(1, ident.length() - 1);
        }
        return ident;
    }

    private static int findMatchingCloseParenthesis(String s, int open) {
        int depth = 0;
        boolean inQuote = false;
        for (int i = open; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                inQuote = !inQuote;
            } else if (!inQuote) {
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                    if (depth == 0) {
                        return i;
                    }
                }
            }
        }
        throw new IllegalArgumentException("Unbalanced parentheses in: " + s);
    }

    private static List<String> splitTopLevel(String body) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        boolean inQuote = false;
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '\'') {
                inQuote = !inQuote;
            } else if (!inQuote) {
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                } else if (c == ',' && depth == 0) {
                    out.add(cur.toString());
                    cur.setLength(0);
                    continue;
                }
            }
            cur.append(c);
        }
        out.add(cur.toString());
        return out;
    }

    /** Lower-cased table name -> lower-cased column names, for every table in the configured schema. */
    public static Map<String, Set<String>> fetchActualSchema(Start start, Connection con)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = ?";
        return execute(con, QUERY,
                pst -> pst.setString(1, Config.getConfig(start).getTableSchema()),
                result -> {
                    Map<String, Set<String>> actual = new HashMap<>();
                    while (result.next()) {
                        actual.computeIfAbsent(result.getString("table_name").toLowerCase(Locale.ROOT),
                                        k -> new HashSet<>())
                                .add(result.getString("column_name").toLowerCase(Locale.ROOT));
                    }
                    return actual;
                });
    }

    /**
     * Compares the expected schema with the database and throws a {@link SchemaMismatchException} describing
     * every missing table and column, including the DDL that would add them.
     */
    public static void verify(Start start, Connection con) throws SchemaMismatchException, StorageQueryException {
        verificationRunsForTesting.incrementAndGet();
        List<ExpectedTable> expected = parseExpectedSchema(GeneralQueries.getAllCreateTableQueries(start));
        Map<String, Set<String>> actual;
        try {
            actual = fetchActualSchema(start, con);
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }

        List<String> missingTables = new ArrayList<>();
        Map<String, List<String>> missingColumns = new LinkedHashMap<>();
        List<String> suggestedSql = new ArrayList<>();

        for (ExpectedTable table : expected) {
            Set<String> actualColumns = actual.get(table.tableName);
            if (actualColumns == null) {
                missingTables.add(table.qualifiedName);
                continue;
            }
            List<String> missing = new ArrayList<>();
            for (Map.Entry<String, String> col : table.columns.entrySet()) {
                if (!actualColumns.contains(col.getKey())) {
                    missing.add(col.getKey());
                    suggestedSql.add("ALTER TABLE " + table.qualifiedName + " ADD COLUMN IF NOT EXISTS "
                            + col.getKey() + " " + col.getValue() + ";");
                }
            }
            if (!missing.isEmpty()) {
                missingColumns.put(table.qualifiedName, missing);
            }
        }

        if (missingTables.isEmpty() && missingColumns.isEmpty()) {
            return;
        }

        StringBuilder msg = new StringBuilder();
        msg.append("Database schema is out of date for this version of SuperTokens: it is missing tables/columns ")
                .append("that this version requires. This happens when a release's manual migration (the ")
                .append("\"### Migration\" section of the core and postgresql-plugin CHANGELOGs) was not applied ")
                .append("before upgrading.\n");
        if (!missingTables.isEmpty()) {
            // Tables are created at startup (CREATE TABLE IF NOT EXISTS) right before this check runs, so a
            // missing table means that DDL did not take effect, or we are looking in the wrong schema.
            msg.append("Missing tables: ").append(String.join(", ", missingTables)).append("\n")
                    .append("These are normally created automatically at startup. Check the core's error log ")
                    .append("for a failed CREATE TABLE, that the database user may create tables, and that ")
                    .append("postgresql_table_schema (\"").append(Config.getConfig(start).getTableSchema())
                    .append("\") is the schema the tables live in.\n");
        }
        if (!missingColumns.isEmpty()) {
            msg.append("Missing columns:\n");
            for (Map.Entry<String, List<String>> e : missingColumns.entrySet()) {
                msg.append("  - ").append(e.getKey()).append(": ").append(String.join(", ", e.getValue()))
                        .append("\n");
            }
            msg.append("Apply the migration SQL from the CHANGELOG of every version you upgraded across, then ")
                    .append("restart. Equivalent statements (review NOT NULL / DEFAULT clauses for tables that ")
                    .append("already contain rows):\n");
            for (String sql : suggestedSql) {
                msg.append("  ").append(sql).append("\n");
            }
        }
        throw new SchemaMismatchException(msg.toString().trim(), missingTables, missingColumns, suggestedSql);
    }
}
