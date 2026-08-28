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

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Baseline backstop for the compile-time auto-commit-write guard
 * (see {@code AuditEnforcementAspect}).
 *
 * <p>The aspect already guarantees that every {@code QueryExecutorTemplate
 * .update(Start, ...)} call in production code is annotated
 * ({@code @AtomicAutoCommitWrite} or {@code @UnauditedAutoCommitWrite}) or
 * structurally excluded (DDL/schema methods, tests). What a compiler cannot
 * express is "the debt tier may only shrink" and "the permanent tier does not
 * grow silently" — so those are pinned here, by scanning the sources the way a
 * reviewer would grep them:
 *
 * <ul>
 *   <li>{@code @AtomicAutoCommitWrite} is pinned exactly: adding one is a design
 *       claim ("this write is permanently fine outside a transaction") that
 *       must be reviewed, and removing one (a site legitimately deleted) must
 *       lower the pin.</li>
 *   <li>{@code @UnauditedAutoCommitWrite} is shrink-only: converting a legacy
 *       site to a transactional audited write must lower {@link #DEBT_BASELINE};
 *       it must never grow.</li>
 *   <li>a grep for {@code call()}-evading forms — a {@code
 *       QueryExecutorTemplate::update} method reference would slip past the
 *       aspect's {@code call(...)} pointcut.</li>
 * </ul>
 */
public class AutoCommitWriteGuardBaselineTest {

    // Pinned exactly: methods whose auto-commit write has nothing to be atomic with.
    private static final int ATOMIC_BASELINE = 17;

    // Shrink-only: legacy domain mutations still writing in auto-commit mode.
    // LOWER this when a site is converted to a transactional audited write.
    private static final int DEBT_BASELINE = 49;

    private static final Pattern ATOMIC = Pattern.compile("@AtomicAutoCommitWrite\\s*\\(");
    private static final Pattern DEBT = Pattern.compile("@UnauditedAutoCommitWrite\\s*\\(");
    // A method reference to update would bypass the aspect's call() pointcut.
    private static final Pattern METHOD_REF =
            Pattern.compile("QueryExecutorTemplate\\s*::\\s*update");

    @Test
    public void atomicTierIsPinnedExactly() throws IOException {
        int count = countMatches(ATOMIC);
        assertEquals("The @AtomicAutoCommitWrite (permanent) tier is pinned. Adding a permanent auto-commit write is a "
                        + "reviewed design decision; if you added or removed one on purpose, update ATOMIC_BASELINE.",
                ATOMIC_BASELINE, count);
    }

    @Test
    public void debtTierIsShrinkOnly() throws IOException {
        int count = countMatches(DEBT);
        assertTrue("The @UnauditedAutoCommitWrite (debt) tier must only shrink. Found " + count
                        + " but DEBT_BASELINE is " + DEBT_BASELINE + ". If you added a new auto-commit domain write, "
                        + "make it transactional instead; if you converted one, LOWER DEBT_BASELINE to " + count + ".",
                count <= DEBT_BASELINE);
    }

    @Test
    public void noMethodReferenceEvadesTheCallPointcut() throws IOException {
        List<Path> offenders = new ArrayList<>();
        for (Path p : mainSources()) {
            String src = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
            if (METHOD_REF.matcher(src).find()) {
                offenders.add(p);
            }
        }
        if (!offenders.isEmpty()) {
            fail("QueryExecutorTemplate::update method reference(s) found — these bypass the compile-time guard's "
                    + "call() pointcut. Call update(...) directly (so the aspect sees the call site) in: " + offenders);
        }
    }

    // Annotation occurrences are counted only under the queries package (where every DB write lives). This keeps
    // the count from also matching the aspect's own error-message string, which mentions both annotation names.
    private int countMatches(Pattern pattern) throws IOException {
        int total = 0;
        for (Path p : sourcesUnder("queries")) {
            String src = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
            Matcher m = pattern.matcher(src);
            while (m.find()) {
                total++;
            }
        }
        return total;
    }

    private List<Path> mainSources() throws IOException {
        return sourcesUnder("");
    }

    private List<Path> sourcesUnder(String subPackage) throws IOException {
        Path root = locateMainSourceRoot();
        Path scan = subPackage.isEmpty() ? root : root.resolve(subPackage);
        try (Stream<Path> walk = Files.walk(scan)) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".java"))
                    .collect(Collectors.toList());
        }
    }

    // Tests run with the working directory set to the plugin's project directory, but be defensive about the
    // exact layout (e.g. running from the multi-repo root) so a mislocated scan fails loudly rather than passing empty.
    private Path locateMainSourceRoot() {
        String rel = "src/main/java/io/supertokens/storage/postgresql";
        String[] candidates = {rel, "supertokens-postgresql-plugin/" + rel, "../" + rel};
        for (String c : candidates) {
            Path p = Paths.get(c);
            if (Files.isDirectory(p)) {
                return p;
            }
        }
        throw new IllegalStateException("Could not locate main source root for the auto-commit-write baseline scan "
                + "(working dir = " + Paths.get("").toAbsolutePath() + "). Tried: " + String.join(", ", candidates));
    }
}
