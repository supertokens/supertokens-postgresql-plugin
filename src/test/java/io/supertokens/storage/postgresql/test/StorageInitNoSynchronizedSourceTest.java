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
 *
 */

package io.supertokens.storage.postgresql.test;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Companion to {@link StorageInitNoSynchronizedTest}. That test is an ArchUnit rule over compiled bytecode,
 * and ArchUnit's domain model only exposes the {@code synchronized} <em>method modifier</em> — it cannot see
 * a {@code synchronized (obj) { ... }} <em>block</em> (a {@code MONITORENTER} instruction). The {@code Start}
 * half of the pin fix was exactly such blocks ({@code synchronized (appenderLock)}), so a regression that
 * re-introduced one would pass the ArchUnit rule silently.
 *
 * <p>This test closes that gap by scanning the source of the two storage-init classes for the
 * {@code synchronized} keyword (after stripping comments and string/char literals so the explanatory comments
 * in those files do not trip it). It catches both synchronized methods and synchronized blocks, which is what
 * matters for the virtual-thread pin: a virtual thread parked inside <em>any</em> synchronized section is
 * pinned to its carrier on JDK 21, and with enough tenant storages under DEBUG logging that hung core startup.
 * Use {@code java.util.concurrent.locks.ReentrantLock} instead.
 */
public class StorageInitNoSynchronizedSourceTest {

    private static final Path SRC_DIR =
            Paths.get("src", "main", "java", "io", "supertokens", "storage", "postgresql");

    private static final Pattern SYNCHRONIZED = Pattern.compile("\\bsynchronized\\b");

    @Test
    public void connectionPoolSourceHasNoSynchronized() throws Exception {
        assertNoSynchronized(SRC_DIR.resolve("ConnectionPool.java"));
    }

    @Test
    public void startSourceHasNoSynchronized() throws Exception {
        assertNoSynchronized(SRC_DIR.resolve("Start.java"));
    }

    private static void assertNoSynchronized(Path file) throws Exception {
        // A missing file would otherwise make the guard pass silently; fail loudly instead.
        assertTrue("expected storage-init source at " + file.toAbsolutePath()
                + " (test working directory: " + Paths.get("").toAbsolutePath() + ")", Files.isRegularFile(file));

        String code = stripCommentsAndLiterals(new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
        Matcher m = SYNCHRONIZED.matcher(code);
        if (m.find()) {
            fail(file.getFileName() + " uses `synchronized` on the storage-init path (offset " + m.start()
                    + ", outside comments and string literals). A virtual thread parked inside a synchronized "
                    + "method or block is pinned to its carrier on JDK 21, which hung core startup with many "
                    + "tenant storages under DEBUG logging. Use java.util.concurrent.locks.ReentrantLock instead.");
        }
    }

    /**
     * Replaces the body of every {@code //} line comment, {@code /* *}{@code /} block comment, string literal
     * and char literal with spaces, preserving length and newlines, so a later keyword scan only sees real code.
     * A small hand-rolled scanner — enough for well-formed Java source, which these two files are.
     */
    private static String stripCommentsAndLiterals(String s) {
        StringBuilder out = new StringBuilder(s.length());
        int n = s.length();
        int i = 0;
        while (i < n) {
            char c = s.charAt(i);
            char next = i + 1 < n ? s.charAt(i + 1) : '\0';

            if (c == '/' && next == '/') {
                out.append("  ");
                i += 2;
                while (i < n && s.charAt(i) != '\n') {
                    out.append(' ');
                    i++;
                }
            } else if (c == '/' && next == '*') {
                out.append("  ");
                i += 2;
                while (i < n && !(s.charAt(i) == '*' && i + 1 < n && s.charAt(i + 1) == '/')) {
                    out.append(s.charAt(i) == '\n' ? '\n' : ' ');
                    i++;
                }
                if (i < n) { // consume the closing */
                    out.append("  ");
                    i += 2;
                }
            } else if (c == '"' || c == '\'') {
                char quote = c;
                out.append(' ');
                i++;
                while (i < n && s.charAt(i) != quote) {
                    if (s.charAt(i) == '\\' && i + 1 < n) { // skip escapes like \" or \'
                        out.append("  ");
                        i += 2;
                    } else {
                        out.append(s.charAt(i) == '\n' ? '\n' : ' ');
                        i++;
                    }
                }
                if (i < n) { // consume the closing quote
                    out.append(' ');
                    i++;
                }
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }
}
