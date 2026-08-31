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

package io.supertokens.storage.postgresql.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method that performs a genuine domain mutation via
 * {@code QueryExecutorTemplate.update(Start, ...)} in auto-commit mode, outside
 * any transaction. This is <b>technical debt</b>: such a write cannot carry a
 * transactional audit-log entry, so each site is meant to be converted over
 * time to a {@code *_Transaction} write reached through the audited-transaction
 * entry point (or an explicit un-audited transaction where appropriate).
 *
 * <p>Enforced by {@code AuditEnforcementAspect}: a {@code update(Start, ...)}
 * call in a method carrying neither this annotation nor
 * {@link AtomicAutoCommitWrite} fails compilation. The count of methods
 * carrying this annotation is a shrink-only baseline: a test fails if it grows.
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
public @interface UnauditedAutoCommitWrite {
    String justification();
}
