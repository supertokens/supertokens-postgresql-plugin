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

import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.lang.ArchRule;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import org.junit.runner.RunWith;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;

/**
 * Guards the storage-initialisation path against {@code synchronized}. The core initialises tenant storages
 * from worker threads, and on JDK 21 a virtual thread that parks inside a synchronized section is pinned to
 * its carrier. {@code ConnectionPool.initialiseHikariDataSource} used to be synchronized while it opened
 * connections, ran DDL and logged every Hikari DEBUG line through the shared console appender; with enough
 * storages every carrier was held by a pinned waiter and core startup hung. The lock is a ReentrantLock now,
 * and this rule keeps it (and {@code Start}, which runs the rest of the init path) that way.
 */
@RunWith(ArchUnitRunner.class) // Remove this line for JUnit 5!!
@AnalyzeClasses(packages = "io.supertokens.storage.postgresql")
public class StorageInitNoSynchronizedTest {
    // @formatter:off
    @ArchTest
    public static final ArchRule storage_init_path_must_not_use_synchronized_methods = noMethods()
            .that().areDeclaredIn(ConnectionPool.class)
            .or().areDeclaredIn(Start.class)
            .should().haveModifier(JavaModifier.SYNCHRONIZED)
            .because("storage initialisation runs on threads the core chooses; a virtual thread parked inside "
                    + "a synchronized section is pinned to its carrier on JDK 21, which hung core startup with "
                    + "many tenants. Use java.util.concurrent.locks.ReentrantLock instead.");
    // @formatter:on
}
