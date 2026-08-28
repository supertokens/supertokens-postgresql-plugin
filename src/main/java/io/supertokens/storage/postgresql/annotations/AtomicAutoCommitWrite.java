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
 * Marks a method that deliberately performs an auto-commit write via
 * {@code QueryExecutorTemplate.update(Start, ...)} which is <b>permanently</b>
 * allowed to run outside a transaction: a single-statement append, an
 * idempotent cache write, or a time-based cleanup sweep that has nothing to be
 * atomic with. Unlike {@link UnauditedAutoCommitWrite} this is not technical
 * debt and is not expected to be converted to a transactional write.
 *
 * <p>Enforced by {@code AuditEnforcementAspect}: a {@code update(Start, ...)}
 * call in a method carrying neither this annotation nor
 * {@link UnauditedAutoCommitWrite} fails compilation. The count of methods
 * carrying this annotation is pinned exactly by a baseline test.
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
public @interface AtomicAutoCommitWrite {
    String justification();
}
