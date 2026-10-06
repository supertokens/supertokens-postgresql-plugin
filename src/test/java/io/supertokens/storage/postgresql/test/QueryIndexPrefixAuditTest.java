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

package io.supertokens.storage.postgresql.test;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Node;
import com.github.javaparser.ast.body.CallableDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.AssignExpr;
import com.github.javaparser.ast.expr.BinaryExpr;
import com.github.javaparser.ast.expr.EnclosedExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.ast.expr.StringLiteralExpr;
import com.github.javaparser.ast.expr.TextBlockLiteralExpr;
import com.github.javaparser.ast.stmt.ReturnStmt;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.config.PostgreSQLConfig;
import io.supertokens.storageLayer.StorageLayer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryIndexPrefixAuditTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    private static final Set<String> TENANCY_COLUMNS = new HashSet<>(Arrays.asList("app_id", "tenant_id"));

    private static final Map<String, String> ALLOWED = new LinkedHashMap<>();

    static {
        ALLOWED.put("OAuthQueries#deleteExpiredOAuthM2MTokens",
                "cleanup cron: deletes the expired rows of every app");
        ALLOWED.put("WebAuthNQueries#deleteExpiredGeneratedOptions",
                "cleanup cron: deletes the expired rows of every app");
        ALLOWED.put("MigrationBackfillQueries#getBackfillPendingUsersCount",
                "migration backfill: counts the app's rows that are not backfilled yet");
        ALLOWED.put("MigrationBackfillQueries#backfillUsersBatch",
                "migration backfill: walks the app's rows that are not backfilled yet, in batches");
        ALLOWED.put("OAuthQueries#countTotalNumberOfClients",
                "counts the app's clients; the flag only splits the count");
        ALLOWED.put("GeneralQueries#checkIfUsesAccountLinking_new",
                "known debt: LIMIT 1 stops at the first linked user, but an app without one is read in full");
        ALLOWED.put("GeneralQueries#checkIfUsesAccountLinking_legacy",
                "known debt: LIMIT 1 stops at the first linked user, but an app without one is read in full");
    }

    private static final int MIN_EXPLAINED_STATEMENTS = 400;

    @AfterClass
    public static void afterTesting() {
        Utils.afterTesting();
    }

    @Before
    public void beforeEach() {
        Utils.reset();
    }

    @Test
    public void everyQueryNarrowsPastTheTenancyColumns() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) {
                return;
            }
            Start storage = (Start) StorageLayer.getStorage(main);
            PostgreSQLConfig config = Config.getConfig(storage);

            SqlSourceExtractor extractor = new SqlSourceExtractor(config);
            List<SqlSite> sites = extractor.extract(locateMainSourceRoot());

            List<Finding> findings = new ArrayList<>();
            List<String> explainErrors = new ArrayList<>();
            int explained = 0;

            try (Connection con = ConnectionPool.getConnection(storage)) {
                con.setAutoCommit(true);
                String schema = config.getTableSchema();
                PlanAuditor auditor = new PlanAuditor(loadIndexes(con, schema), loadTenantScopedColumns(con, schema));
                try {
                    exec(con, "SET plan_cache_mode = force_generic_plan");
                    exec(con, "SET max_parallel_workers_per_gather = 0");
                    exec(con, "SET statement_timeout = '10s'");
                    exec(con, "SET lock_timeout = '5s'");

                    for (SqlSite site : sites) {
                        JsonObject plan;
                        try {
                            plan = explainGeneric(con, site.sql);
                        } catch (SQLException e) {
                            explainErrors.add(site.location() + ": " + e.getMessage().split("\n")[0]);
                            continue;
                        }
                        explained++;
                        for (String problem : auditor.audit(plan)) {
                            findings.add(new Finding(site, problem));
                        }
                    }
                } finally {
                    exec(con, "DEALLOCATE ALL");
                    exec(con, "RESET ALL");
                }
            }

            System.out.println("QueryIndexPrefixAuditTest: " + sites.size() + " SQL statements rebuilt, "
                    + explained + " explained, " + explainErrors.size() + " could not be prepared, "
                    + extractor.unresolved.size() + " skipped (built with StringBuilder or helper methods).");
            explainErrors.forEach(e -> System.out.println("  not prepared: " + e));
            extractor.unresolved.forEach(u -> System.out.println("  skipped: " + u));

            assertTrue("Only " + explained + " statements were explained; the source extractor is probably broken "
                    + "(expected at least " + MIN_EXPLAINED_STATEMENTS + ").", explained >= MIN_EXPLAINED_STATEMENTS);

            List<Finding> violations = new ArrayList<>();
            Set<String> usedAllowances = new HashSet<>();
            for (Finding f : findings) {
                if (ALLOWED.containsKey(f.site.key())) {
                    usedAllowances.add(f.site.key());
                } else {
                    violations.add(f);
                }
            }
            Set<String> stale = new TreeSet<>(ALLOWED.keySet());
            stale.removeAll(usedAllowances);

            StringBuilder report = new StringBuilder();
            if (!violations.isEmpty()) {
                report.append(violations.size()).append(" table scan(s) cannot narrow past app_id / tenant_id. ")
                        .append("Add the missing leading key column to the WHERE / JOIN condition, add an index, ")
                        .append("or (if the wide read is intended) add the method to ALLOWED with a reason:\n");
                Map<SqlSite, List<String>> bySite = new LinkedHashMap<>();
                for (Finding f : violations) {
                    bySite.computeIfAbsent(f.site, s -> new ArrayList<>()).add(f.problem);
                }
                bySite.forEach((site, problems) -> {
                    report.append("\n").append(site.location()).append("\n    ").append(site.sql).append("\n");
                    problems.forEach(p -> report.append("    -> ").append(p).append("\n"));
                });
            }
            if (!stale.isEmpty()) {
                report.append("\nALLOWED entries that no longer match any finding (remove them): ").append(stale)
                        .append("\n");
            }
            if (report.length() > 0) {
                fail(report.toString());
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    private static final class PlanAuditor {
        private static final Set<String> SCANS =
                new HashSet<>(Arrays.asList("Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan"));
        private static final String[] SCAN_QUALS = {"Index Cond", "Recheck Cond", "Filter"};
        private static final String[] JOIN_QUALS = {"Hash Cond", "Merge Cond", "Join Filter"};

        private final Map<String, Map<String, List<String>>> indexes;
        private final Map<String, Set<String>> tenantScopedColumns;

        PlanAuditor(Map<String, Map<String, List<String>>> indexes, Map<String, Set<String>> tenantScopedColumns) {
            this.indexes = indexes;
            this.tenantScopedColumns = tenantScopedColumns;
        }

        Set<String> audit(JsonObject plan) {
            Set<String> problems = new LinkedHashSet<>();
            walk(plan, "", problems);
            return problems;
        }

        private void walk(JsonObject node, String joinQuals, Set<String> problems) {
            String relation = str(node, "Relation Name");
            if (SCANS.contains(str(node, "Node Type")) && tenantScopedColumns.containsKey(relation)) {
                checkScan(node, relation, joinQuals, problems);
            }
            StringBuilder childJoinQuals = new StringBuilder(joinQuals);
            for (String field : JOIN_QUALS) {
                if (node.has(field)) {
                    childJoinQuals.append(' ').append(str(node, field));
                }
            }
            if (node.has("Plans")) {
                JsonArray children = node.getAsJsonArray("Plans");
                for (int i = 0; i < children.size(); i++) {
                    walk(children.get(i).getAsJsonObject(), childJoinQuals.toString(), problems);
                }
            }
        }

        private void checkScan(JsonObject node, String relation, String joinQuals, Set<String> problems) {
            String alias = node.has("Alias") ? str(node, "Alias") : relation;
            List<String> scanQuals = new ArrayList<>();
            for (String field : SCAN_QUALS) {
                if (node.has(field)) {
                    scanQuals.add(field + " " + str(node, field));
                }
            }
            String scanQualText = String.join(" ", scanQuals);

            Set<String> constrained = new TreeSet<>();
            for (String column : tenantScopedColumns.get(relation)) {
                Pattern bare = Pattern.compile("(?<![\\w.\"])" + Pattern.quote(column) + "(?![\\w\"])");
                Pattern qualified = Pattern.compile("(?<![\\w.\"])" + Pattern.quote(alias) + "\\."
                        + Pattern.quote(column) + "(?![\\w\"])");
                if (bare.matcher(scanQualText).find() || qualified.matcher(scanQualText + " " + joinQuals).find()) {
                    constrained.add(column);
                }
            }
            Set<String> selective = new TreeSet<>(constrained);
            selective.removeAll(TENANCY_COLUMNS);
            if (selective.isEmpty()) {
                return;
            }

            List<String> narrowedBy = Collections.emptyList();
            List<String> hints = new ArrayList<>();
            for (Map.Entry<String, List<String>> index : indexes.getOrDefault(relation, Collections.emptyMap())
                    .entrySet()) {
                List<String> columns = index.getValue();
                int prefix = 0;
                while (prefix < columns.size() && constrained.contains(columns.get(prefix))) {
                    prefix++;
                }
                if (!TENANCY_COLUMNS.containsAll(columns.subList(0, prefix))) {
                    return;
                }
                if (prefix > narrowedBy.size()) {
                    narrowedBy = columns.subList(0, prefix);
                }
                for (int i = prefix; i < columns.size(); i++) {
                    if (selective.contains(columns.get(i))) {
                        List<String> missing = new ArrayList<>(columns.subList(prefix, i));
                        missing.removeAll(constrained);
                        hints.add(index.getKey() + " " + columns + " needs " + missing);
                        break;
                    }
                }
            }

            String reads = narrowedBy.isEmpty() ? "the whole table" : "every row with the same " + narrowedBy;
            problems.add(relation + ": the query constrains " + constrained + ", but no index narrows past "
                    + narrowedBy + ", so the scan reads " + reads + "."
                    + (hints.isEmpty() ? " No index covers " + selective + "." : " " + String.join("; ", hints) + ".")
                    + " Plan: " + str(node, "Node Type") + (scanQualText.isEmpty() ? "" : ", " + scanQualText));
        }

        private static String str(JsonObject node, String field) {
            return node.has(field) ? node.get(field).getAsString() : null;
        }
    }

    private static final class SqlSite {
        final String file;
        final String method;
        final int line;
        final String sql;

        SqlSite(String file, String method, int line, String sql) {
            this.file = file;
            this.method = method;
            this.line = line;
            this.sql = sql;
        }

        String key() {
            return file.replace(".java", "") + "#" + method;
        }

        String location() {
            return file + ":" + line + " " + method;
        }
    }

    private static final class Finding {
        final SqlSite site;
        final String problem;

        Finding(SqlSite site, String problem) {
            this.site = site;
            this.problem = problem;
        }
    }

    private static final class SqlSourceExtractor {
        private static final Pattern DML =
                Pattern.compile("^\\s*\\(?\\s*(SELECT|WITH|INSERT|UPDATE|DELETE)\\b", Pattern.CASE_INSENSITIVE);

        private final PostgreSQLConfig config;
        private final JavaParser parser = new JavaParser(
                new ParserConfiguration().setLanguageLevel(ParserConfiguration.LanguageLevel.JAVA_21));
        private final Map<String, SqlSite> sites = new LinkedHashMap<>();
        final Set<String> unresolved = new LinkedHashSet<>();

        private static final class Value {
            final String text;
            final int line;

            Value(String text, int line) {
                this.text = text;
                this.line = line;
            }
        }

        SqlSourceExtractor(PostgreSQLConfig config) {
            this.config = config;
        }

        List<SqlSite> extract(Path root) throws IOException {
            List<Path> files;
            try (Stream<Path> walk = Files.walk(root)) {
                files = walk.filter(p -> p.toString().endsWith(".java")).sorted().collect(Collectors.toList());
            }
            for (Path file : files) {
                ParseResult<CompilationUnit> result = parser.parse(file);
                if (!result.isSuccessful() || !result.getResult().isPresent()) {
                    throw new IllegalStateException("Could not parse " + file + ": " + result.getProblems());
                }
                extract(file.getFileName().toString(), result.getResult().get());
            }
            return new ArrayList<>(sites.values());
        }

        private void extract(String file, CompilationUnit cu) {
            Map<String, Value> constants = new HashMap<>();
            for (FieldDeclaration field : cu.findAll(FieldDeclaration.class)) {
                if (!field.isStatic() || !field.isFinal()) {
                    continue;
                }
                for (VariableDeclarator v : field.getVariables()) {
                    if (v.getInitializer().isPresent()) {
                        String text = eval(v.getInitializer().get(), constants);
                        if (text != null) {
                            constants.put(v.getNameAsString(), new Value(text, line(v)));
                        }
                    }
                }
            }

            for (CallableDeclaration<?> callable : cu.findAll(CallableDeclaration.class)) {
                String method = callable.getNameAsString();
                Map<String, Value> env = new HashMap<>(constants);
                callable.walk(Node.TreeTraversal.PREORDER, node -> visit(file, method, node, env));
            }
        }

        private void visit(String file, String method, Node node, Map<String, Value> env) {
            if (node instanceof VariableDeclarator) {
                VariableDeclarator v = (VariableDeclarator) node;
                if (v.getInitializer().isPresent()) {
                    Expression init = v.getInitializer().get();
                    assign(file, method, env, v.getNameAsString(), init, eval(init, env), line(v));
                }
            } else if (node instanceof AssignExpr) {
                AssignExpr a = (AssignExpr) node;
                if (!a.getTarget().isNameExpr()) {
                    return;
                }
                String name = a.getTarget().asNameExpr().getNameAsString();
                String text = eval(a.getValue(), env);
                int line = line(a);
                if (a.getOperator() == AssignExpr.Operator.PLUS) {
                    Value previous = env.get(name);
                    text = previous == null || text == null ? null : previous.text + text;
                    line = previous == null ? line : previous.line;
                } else if (a.getOperator() != AssignExpr.Operator.ASSIGN) {
                    return;
                }
                assign(file, method, env, name, a.getValue(), text, line);
            } else if (node instanceof NameExpr && isUsage((Expression) node)) {
                Value value = env.get(((NameExpr) node).getNameAsString());
                if (value != null) {
                    record(file, method, value.line, value.text);
                }
            } else if (node instanceof Expression && isConcatenationRoot((Expression) node)) {
                Expression expr = (Expression) node;
                Node parent = expr.getParentNode().orElse(null);
                if (parent instanceof VariableDeclarator || parent instanceof AssignExpr) {
                    return;
                }
                boolean builder = parent instanceof ObjectCreationExpr
                        || (parent instanceof MethodCallExpr
                        && ((MethodCallExpr) parent).getNameAsString().equals("append"));
                String text = builder ? null : eval(expr, env);
                if (text == null) {
                    markIfUnresolvedSql(file, method, expr);
                } else {
                    record(file, method, line(expr), text);
                }
            }
        }

        private void assign(String file, String method, Map<String, Value> env, String name, Expression expr,
                            String text, int line) {
            if (text == null) {
                env.remove(name);
                markIfUnresolvedSql(file, method, expr);
            } else {
                env.put(name, new Value(text, line));
            }
        }

        private void record(String file, String method, int line, String text) {
            if (DML.matcher(text).find()) {
                String sql = text.replaceAll("\\s+", " ").trim();
                sites.putIfAbsent(file + ":" + line + ":" + sql, new SqlSite(file, method, line, sql));
            }
        }

        private void markIfUnresolvedSql(String file, String method, Expression expr) {
            String leading = expr.findFirst(StringLiteralExpr.class).map(StringLiteralExpr::asString).orElse(null);
            if (leading != null && DML.matcher(leading).find()) {
                unresolved.add(file + ":" + line(expr) + " " + method);
            }
        }

        private String eval(Expression e, Map<String, Value> env) {
            if (e.isStringLiteralExpr()) {
                return e.asStringLiteralExpr().asString();
            }
            if (e.isTextBlockLiteralExpr()) {
                return e.asTextBlockLiteralExpr().asString();
            }
            if (e.isCharLiteralExpr()) {
                return String.valueOf(e.asCharLiteralExpr().asChar());
            }
            if (e.isIntegerLiteralExpr()) {
                return e.asIntegerLiteralExpr().getValue();
            }
            if (e.isEnclosedExpr()) {
                return eval(e.asEnclosedExpr().getInner(), env);
            }
            if (e.isBinaryExpr() && e.asBinaryExpr().getOperator() == BinaryExpr.Operator.PLUS) {
                String left = eval(e.asBinaryExpr().getLeft(), env);
                String right = left == null ? null : eval(e.asBinaryExpr().getRight(), env);
                return left == null || right == null ? null : left + right;
            }
            if (e.isNameExpr()) {
                Value value = env.get(e.asNameExpr().getNameAsString());
                return value == null ? null : value.text;
            }
            if (e.isMethodCallExpr()) {
                MethodCallExpr call = e.asMethodCallExpr();
                if (call.getNameAsString().equals("generateCommaSeperatedQuestionMarks")) {
                    return "?";
                }
                boolean onConfig = call.getScope().filter(Expression::isMethodCallExpr)
                        .map(s -> s.asMethodCallExpr().getNameAsString().equals("getConfig")).orElse(false);
                if (onConfig && call.getArguments().isEmpty()) {
                    return invokeConfigGetter(call.getNameAsString());
                }
            }
            return null;
        }

        private String invokeConfigGetter(String name) {
            try {
                Method getter = PostgreSQLConfig.class.getMethod(name);
                if (getter.getReturnType() != String.class) {
                    return null;
                }
                return (String) getter.invoke(config);
            } catch (ReflectiveOperationException e) {
                return null;
            }
        }

        private static boolean isUsage(Expression e) {
            Node parent = e.getParentNode().orElse(null);
            if (parent instanceof MethodCallExpr) {
                return ((MethodCallExpr) parent).getArguments().stream().anyMatch(a -> a == e);
            }
            return parent instanceof ObjectCreationExpr || parent instanceof ReturnStmt;
        }

        private static boolean isConcatenationRoot(Expression e) {
            boolean stringExpr = e instanceof StringLiteralExpr || e instanceof TextBlockLiteralExpr
                    || (e instanceof BinaryExpr && ((BinaryExpr) e).getOperator() == BinaryExpr.Operator.PLUS);
            if (!stringExpr) {
                return false;
            }
            Node parent = e.getParentNode().orElse(null);
            return !(parent instanceof EnclosedExpr)
                    && !(parent instanceof BinaryExpr && ((BinaryExpr) parent).getOperator() == BinaryExpr.Operator.PLUS);
        }

        private static int line(Node node) {
            return node.getBegin().map(p -> p.line).orElse(0);
        }
    }

    private static JsonObject explainGeneric(Connection con, String jdbcSql) throws SQLException {
        StringBuilder sql = new StringBuilder();
        int params = 0;
        boolean inString = false;
        boolean inIdentifier = false;
        for (char c : jdbcSql.replaceAll(";\\s*$", "").toCharArray()) {
            if (c == '\'' && !inIdentifier) {
                inString = !inString;
            } else if (c == '"' && !inString) {
                inIdentifier = !inIdentifier;
            }
            if (c == '?' && !inString && !inIdentifier) {
                sql.append('$').append(++params);
            } else {
                sql.append(c);
            }
        }

        exec(con, "PREPARE index_prefix_audit AS " + sql);
        try {
            String args = params == 0 ? "" : "(" + String.join(", ", Collections.nCopies(params, "NULL")) + ")";
            try (Statement st = con.createStatement();
                 ResultSet rs = st.executeQuery("EXPLAIN (FORMAT JSON) EXECUTE index_prefix_audit" + args)) {
                rs.next();
                JsonArray arr = new JsonParser().parse(rs.getString(1)).getAsJsonArray();
                return arr.get(0).getAsJsonObject().getAsJsonObject("Plan");
            }
        } finally {
            exec(con, "DEALLOCATE index_prefix_audit");
        }
    }

    private static Map<String, Map<String, List<String>>> loadIndexes(Connection con, String schema)
            throws SQLException {
        String query = "SELECT t.relname, i.relname, COALESCE(a.attname, '<expression>') "
                + "FROM pg_index x "
                + "JOIN pg_class i ON i.oid = x.indexrelid "
                + "JOIN pg_class t ON t.oid = x.indrelid "
                + "JOIN pg_namespace n ON n.oid = t.relnamespace "
                + "CROSS JOIN LATERAL unnest(x.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) "
                + "LEFT JOIN pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = k.attnum AND k.attnum > 0 "
                + "WHERE n.nspname = ? AND k.ord <= x.indnkeyatts "
                + "ORDER BY t.relname, i.relname, k.ord";
        Map<String, Map<String, List<String>>> out = new HashMap<>();
        try (PreparedStatement pst = con.prepareStatement(query)) {
            pst.setString(1, schema);
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    out.computeIfAbsent(rs.getString(1), k -> new LinkedHashMap<>())
                            .computeIfAbsent(rs.getString(2), k -> new ArrayList<>()).add(rs.getString(3));
                }
            }
        }
        return out;
    }

    private static Map<String, Set<String>> loadTenantScopedColumns(Connection con, String schema)
            throws SQLException {
        String query = "SELECT c.table_name, c.column_name FROM information_schema.columns c "
                + "WHERE c.table_schema = ? AND c.table_name IN (SELECT table_name FROM information_schema.columns "
                + "WHERE table_schema = ? AND column_name = 'app_id')";
        Map<String, Set<String>> out = new HashMap<>();
        try (PreparedStatement pst = con.prepareStatement(query)) {
            pst.setString(1, schema);
            pst.setString(2, schema);
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    out.computeIfAbsent(rs.getString(1), k -> new HashSet<>()).add(rs.getString(2));
                }
            }
        }
        return out;
    }

    private static void exec(Connection con, String sql) throws SQLException {
        try (Statement st = con.createStatement()) {
            st.execute(sql);
        }
    }

    private static Path locateMainSourceRoot() {
        String rel = "src/main/java/io/supertokens/storage/postgresql";
        String[] candidates = {rel, "supertokens-postgresql-plugin/" + rel, "../" + rel};
        for (String c : candidates) {
            Path p = Paths.get(c);
            if (Files.isDirectory(p)) {
                return p;
            }
        }
        throw new IllegalStateException("Could not locate the plugin's main sources (working dir = "
                + Paths.get("").toAbsolutePath() + "). Tried: " + String.join(", ", candidates));
    }
}
