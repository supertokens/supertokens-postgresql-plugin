/*
 *    Copyright (c) 2023, VRAI Labs and/or its affiliates. All rights reserved.
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

package io.supertokens.storage.postgresql.test.multitenancy;

import com.google.gson.JsonObject;
import io.supertokens.ProcessState;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.featureflag.exceptions.FeatureNotEnabledException;
import io.supertokens.multitenancy.Multitenancy;
import io.supertokens.multitenancy.exception.BadPermissionException;
import io.supertokens.multitenancy.exception.CannotModifyBaseConfigException;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.*;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.storage.postgresql.test.TestingProcessManager;
import io.supertokens.storage.postgresql.test.Utils;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.InvalidProviderConfigException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class TestUserPoolIdChangeBehaviour {
    TestingProcessManager.TestingProcess process;

    @AfterClass
    public static void afterTesting() {
        Utils.afterTesting();
    }

    @After
    public void afterEach() throws InterruptedException {
        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    @Before
    public void beforeEach() throws InterruptedException, InvalidProviderConfigException,
            StorageQueryException, FeatureNotEnabledException, TenantOrAppNotFoundException, IOException,
            InvalidConfigException, CannotModifyBaseConfigException, BadPermissionException {
        Utils.reset();

        String[] args = {"../"};

        this.process = TestingProcessManager.start(args);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{EE_FEATURES.MULTI_TENANCY});
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
    }

    @Test
    public void testUsersWorkAfterUserPoolIdChanges() throws Exception {
        JsonObject coreConfig = new JsonObject();
        StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess())
                .modifyConfigToAddANewUserPoolForTesting(coreConfig, 1);

        TenantIdentifier tenantIdentifier = new TenantIdentifier(null, "a1", null);
        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                tenantIdentifier,
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null,
                coreConfig
        ), false);

        TenantIdentifierWithStorage tenantIdentifierWithStorage = tenantIdentifier.withStorage(
                StorageLayer.getStorage(tenantIdentifier, process.getProcess()));

        String userPoolId = tenantIdentifierWithStorage.getStorage().getUserPoolId();

        AuthRecipeUserInfo userInfo = EmailPassword.signUp(
                tenantIdentifierWithStorage, process.getProcess(), "user@example.com", "password");

        coreConfig.addProperty("postgresql_host", "127.0.0.1");
        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                tenantIdentifier,
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null,
                coreConfig
        ), false);

        tenantIdentifierWithStorage = tenantIdentifier.withStorage(
                StorageLayer.getStorage(tenantIdentifier, process.getProcess()));
        String userPoolId2 = tenantIdentifierWithStorage.getStorage().getUserPoolId();
        assertNotEquals(userPoolId, userPoolId2);

        AuthRecipeUserInfo user2 = EmailPassword.signIn(tenantIdentifierWithStorage, process.getProcess(),
                "user@example.com", "password");

        assertEquals(userInfo, user2);

    }

    @Test
    public void testUsersWorkAfterUserPoolIdChangesAndServerRestart() throws Exception {
        JsonObject coreConfig = new JsonObject();
        StorageLayer.getStorage(new TenantIdentifier(null, null, null), process.getProcess())
                .modifyConfigToAddANewUserPoolForTesting(coreConfig, 1);

        TenantIdentifier tenantIdentifier = new TenantIdentifier(null, "a1", null);
        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                tenantIdentifier,
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null,
                coreConfig
        ), false);

        TenantIdentifierWithStorage tenantIdentifierWithStorage = tenantIdentifier.withStorage(
                StorageLayer.getStorage(tenantIdentifier, process.getProcess()));

        String userPoolId = tenantIdentifierWithStorage.getStorage().getUserPoolId();

        AuthRecipeUserInfo userInfo = EmailPassword.signUp(
                tenantIdentifierWithStorage, process.getProcess(), "user@example.com", "password");

        coreConfig.addProperty("postgresql_host", "127.0.0.1");
        Multitenancy.addNewOrUpdateAppOrTenant(process.getProcess(), new TenantConfig(
                tenantIdentifier,
                new EmailPasswordConfig(true),
                new ThirdPartyConfig(true, null),
                new PasswordlessConfig(true),
                null, null,
                coreConfig
        ), false);

        // Restart the process
        process.kill(false);
        String[] args = {"../"};
        this.process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        tenantIdentifierWithStorage = tenantIdentifier.withStorage(
                StorageLayer.getStorage(tenantIdentifier, process.getProcess()));
        String userPoolId2 = tenantIdentifierWithStorage.getStorage().getUserPoolId();
        assertNotEquals(userPoolId, userPoolId2);

        AuthRecipeUserInfo user2 = EmailPassword.signIn(tenantIdentifierWithStorage, process.getProcess(),
                "user@example.com",
                "password");

        assertEquals(userInfo, user2);
    }
}
