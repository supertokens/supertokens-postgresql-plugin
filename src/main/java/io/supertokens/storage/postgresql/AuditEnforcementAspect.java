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

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.DeclareError;

/**
 * Compile-time guard for auto-commit writes.
 *
 * <p>{@code QueryExecutorTemplate.update(Start, ...)} borrows a fresh
 * connection from the pool and runs in auto-commit mode, i.e. outside any
 * transaction. Such a write can never carry a transactional audit-log entry and
 * is invisible to a transaction-level guard. This aspect makes every such call
 * a build-time decision: a {@code update(Start, ...)} call must live in a method
 * that has justified it with either
 * {@link io.supertokens.storage.postgresql.annotations.AtomicAutoCommitWrite}
 * (permanent — nothing to be atomic with) or
 * {@link io.supertokens.storage.postgresql.annotations.UnauditedAutoCommitWrite}
 * (debt — to be converted to a transactional audited write), otherwise it fails
 * compilation at the offending line.
 *
 * <p>Out of scope by construction:
 * <ul>
 *   <li>the transactional {@code update(Connection, ...)} overload and the
 *       {@code execute(...)} read forms — not matched;</li>
 *   <li>test and test-fixture code, which legitimately issues raw writes;</li>
 *   <li>DDL / schema / partition setup, excluded by naming the schema methods
 *       below (a CREATE/DROP has nothing to be atomic with and is not a domain
 *       mutation, so it is not part of either annotation tier).</li>
 * </ul>
 *
 * <p>The rule is evaluated by ajc during compilation (see the
 * {@code io.freefair.aspectj} wiring in {@code build.gradle}). {@code declare
 * error} accepts only statically evaluable pointcuts
 * ({@code call}/{@code within}/{@code withincode}), so this is a true
 * compile-time check. IntelliJ surfaces it only on an actual build unless the
 * AspectJ facet is configured; Gradle/CI fail regardless.
 */
@Aspect
public class AuditEnforcementAspect {
    @DeclareError(
            "call(* io.supertokens.storage.postgresql.QueryExecutorTemplate.update(io.supertokens.storage.postgresql.Start, ..))"
                    + " && within(io.supertokens.storage.postgresql..*)"
                    + " && !within(io.supertokens.storage.postgresql.test..*)"
                    + " && !within(io.supertokens.storage.postgresql.testfixtures..*)"
                    + " && !withincode(@io.supertokens.storage.postgresql.annotations.AtomicAutoCommitWrite * *(..))"
                    + " && !withincode(@io.supertokens.storage.postgresql.annotations.UnauditedAutoCommitWrite * *(..))"
                    // DDL / schema / partition setup — nothing to be atomic with, not a domain mutation.
                    + " && !withincode(* io.supertokens.storage.postgresql.queries.GeneralQueries.createTablesIfNotExists(..))"
                    + " && !withincode(* io.supertokens.storage.postgresql.queries.GeneralQueries.deleteAllTables(..))"
                    + " && !withincode(* io.supertokens.storage.postgresql.queries.ActivityLogQueries.ensureMonthlyPartition(..))"
                    + " && !withincode(* io.supertokens.storage.postgresql.queries.ActivityLogQueries.dropPartitionsOlderThanRetention(..))")
    static final String AUTO_COMMIT_WRITE_MUST_BE_JUSTIFIED =
            "This QueryExecutorTemplate.update(Start, ...) call runs in auto-commit mode, outside any transaction. "
                    + "Prefer a transactional update(Connection, ...) reached through an audited transaction. "
                    + "If the write genuinely has nothing to be atomic with, annotate the enclosing method with "
                    + "@AtomicAutoCommitWrite(justification); if it is a domain mutation not yet converted, annotate it with "
                    + "@UnauditedAutoCommitWrite(justification). Allowlist additions are review-flagged by the baseline test.";
}
