package io.supertokens.storage.postgresql.test;

import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeStorage;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.pluginInterface.totp.TOTPDevice;
import io.supertokens.pluginInterface.totp.TOTPUsedCode;
import io.supertokens.pluginInterface.totp.exception.UnknownTotpUserIdException;
import io.supertokens.pluginInterface.totp.exception.UsedCodeAlreadyExistsException;
import io.supertokens.pluginInterface.totp.sqlStorage.TOTPSQLStorage;
import io.supertokens.storageLayer.StorageLayer;

import io.supertokens.thirdparty.ThirdParty;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.*;

public class StorageLayerTest {

    @Rule
    public TestRule watchman = Utils.getOnFailure();

    @AfterClass
    public static void afterTesting() {
        Utils.afterTesting();
    }

    @Before
    public void beforeEach() {
        Utils.reset();
    }

    // TOTP recipe:

    public static void insertUsedCodeUtil(TOTPSQLStorage storage, TOTPUsedCode usedCode) throws Exception {
        try {
            storage.startTransaction(con -> {
                try {
                    storage.insertUsedCode_Transaction(con, new TenantIdentifier(null, null, null), usedCode);
                    storage.commitTransaction(con);
                    return null;
                } catch (UnknownTotpUserIdException | UsedCodeAlreadyExistsException e) {
                    throw new StorageTransactionLogicException(e);
                } catch (TenantOrAppNotFoundException e) {
                    throw new IllegalStateException(e);
                }
            });
        } catch (StorageTransactionLogicException e) {
            Exception actual = e.actualException;
            if (actual instanceof UnknownTotpUserIdException || actual instanceof UsedCodeAlreadyExistsException) {
                throw actual;
            } else {
                throw e;
            }
        }
    }

    @Test
    public void totpCodeLengthTest() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        if (StorageLayer.getStorage(process.getProcess()).getType() != STORAGE_TYPE.SQL) {
            return;
        }

        TOTPSQLStorage storage = (TOTPSQLStorage) StorageLayer.getStorage(process.getProcess());
        long now = System.currentTimeMillis();
        long nextDay = now + 1000 * 60 * 60 * 24; // 1 day from now

        TOTPDevice d1 = new TOTPDevice("user", "d1", "secret", 30, 1, false, System.currentTimeMillis());
        storage.createDevice(new AppIdentifier(null, null), d1);

        // Try code with length > 8
        try {
            TOTPUsedCode code = new TOTPUsedCode("user", "123456789", true, nextDay, now);
            insertUsedCodeUtil(storage, code);
            assert (false);
        } catch (StorageQueryException e) {
            assert e.getMessage().endsWith("ERROR: value too long for type character varying(8)");
        }

        // Try code with length < 8
        TOTPUsedCode code = new TOTPUsedCode("user", "12345678", true, nextDay, now);
        insertUsedCodeUtil(storage, code);
    }

    @Test
    public void testLinkedAccountUser() throws Exception {
        String[] args = {"../"};

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        AuthRecipeUserInfo user1 = EmailPassword.signUp(process.getProcess(), "test1@example.com", "password");
        Thread.sleep(50);
        AuthRecipeUserInfo user2 = ThirdParty.signInUp(process.getProcess(), "google", "googleid", "test2@example.com").user;
        Thread.sleep(50);
        Passwordless.CreateCodeResponse code1 = Passwordless.createCode(process.getProcess(), "test3@example.com", null, null, null);
        AuthRecipeUserInfo user3 = Passwordless.consumeCode(process.getProcess(), code1.deviceId, code1.deviceIdHash, code1.userInputCode, null).user;
        Thread.sleep(50);
        Passwordless.CreateCodeResponse code2 = Passwordless.createCode(process.getProcess(), null, "+919876543210", null, null);
        AuthRecipeUserInfo user4 = Passwordless.consumeCode(process.getProcess(), code2.deviceId, code2.deviceIdHash, code2.userInputCode, null).user;

        AuthRecipe.createPrimaryUser(process.getProcess(), user3.getSupertokensUserId());
        AuthRecipe.linkAccounts(process.getProcess(), user1.getSupertokensUserId(), user3.getSupertokensUserId());
        AuthRecipe.linkAccounts(process.getProcess(), user2.getSupertokensUserId(), user3.getSupertokensUserId());
        AuthRecipe.linkAccounts(process.getProcess(), user4.getSupertokensUserId(), user3.getSupertokensUserId());

        String[] userIds = new String[]{
                user1.getSupertokensUserId(),
                user2.getSupertokensUserId(),
                user3.getSupertokensUserId(),
                user4.getSupertokensUserId()
        };

        for (String userId : userIds){
            AuthRecipeUserInfo primaryUser = ((AuthRecipeStorage) StorageLayer.getStorage(process.getProcess())).getPrimaryUserById(
                    new AppIdentifier(null, null), userId);
            assertEquals(user3.getSupertokensUserId(), primaryUser.getSupertokensUserId());
            assertEquals(4, primaryUser.loginMethods.length);
            assertTrue(primaryUser.loginMethods[0].timeJoined < primaryUser.loginMethods[1].timeJoined);
            assertTrue(primaryUser.loginMethods[1].timeJoined < primaryUser.loginMethods[2].timeJoined);
            assertTrue(primaryUser.loginMethods[2].timeJoined < primaryUser.loginMethods[3].timeJoined);
            assertEquals(primaryUser.timeJoined, primaryUser.loginMethods[0].timeJoined);
        }

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

}
