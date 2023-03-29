/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
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

import io.supertokens.ProcessState;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.emailpassword.PasswordResetTokenInfo;
import io.supertokens.pluginInterface.emailpassword.UserInfo;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicateEmailException;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicatePasswordResetTokenException;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicateUserIdException;
import io.supertokens.pluginInterface.emailpassword.exceptions.UnknownUserIdException;
import io.supertokens.pluginInterface.emailpassword.sqlStorage.EmailPasswordSQLStorage;
import io.supertokens.pluginInterface.emailverification.EmailVerificationTokenInfo;
import io.supertokens.pluginInterface.emailverification.exception.DuplicateEmailVerificationTokenException;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.jwt.JWTSymmetricSigningKeyInfo;
import io.supertokens.pluginInterface.jwt.exceptions.DuplicateKeyIdException;
import io.supertokens.pluginInterface.jwt.sqlstorage.JWTRecipeSQLStorage;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.pluginInterface.thirdparty.exception.DuplicateThirdPartyUserException;
import io.supertokens.storageLayer.StorageLayer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ExceptionParsingTest {
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

    @Test
    public void thirdPartySignupExceptions() throws Exception {
        {
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            var storage = StorageLayer.getThirdPartyStorage(process.getProcess());

            String userId = "userId";
            String userId2 = "userId2";
            String tpId = "gugli";
            String thirdPartyUserId = "tp_userId";
            String userEmail = "useremail@asdf.fdas";

            var tp = new io.supertokens.pluginInterface.thirdparty.UserInfo.ThirdParty(tpId, thirdPartyUserId);
            var info = new io.supertokens.pluginInterface.thirdparty.UserInfo(userId, userEmail, tp,
                    System.currentTimeMillis());
            storage.signUp(new TenantIdentifier(null, null, null), info);
            try {
                storage.signUp(new TenantIdentifier(null, null, null), info);
                throw new Exception("This should throw");
            } catch (io.supertokens.pluginInterface.thirdparty.exception.DuplicateUserIdException ex) {
                // expected
            }
            var info2 = new io.supertokens.pluginInterface.thirdparty.UserInfo(userId2, userEmail, tp,
                    System.currentTimeMillis());

            try {
                storage.signUp(new TenantIdentifier(null, null, null), info2);
                throw new Exception("This should throw");
            } catch (DuplicateThirdPartyUserException ex) {
                // expected
            }

            assertEquals(storage.getUsersCount(new TenantIdentifier(null, null, null),
                    new RECIPE_ID[]{RECIPE_ID.THIRD_PARTY}), 1);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void emailPasswordSignupExceptions() throws Exception {
        {
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            EmailPasswordSQLStorage storage = (EmailPasswordSQLStorage) StorageLayer.getStorage(process.getProcess());

            String userId = "userId";
            String userId2 = "userId2";
            String pwHash = "fakehash";
            String userEmail = "useremail@asdf.fdas";

            var info = new UserInfo(userId, userEmail, pwHash, System.currentTimeMillis());
            storage.signUp(new TenantIdentifier(null, null, null), info);
            try {
                storage.signUp(new TenantIdentifier(null, null, null), info);
                throw new Exception("This should throw");
            } catch (DuplicateUserIdException ex) {
                // expected
            }
            var info2 = new UserInfo(userId2, userEmail, pwHash, System.currentTimeMillis());

            try {
                storage.signUp(new TenantIdentifier(null, null, null), info2);
                throw new Exception("This should throw");
            } catch (DuplicateEmailException ex) {
                // expected
            }

            assertEquals(storage.getUsersCount(new TenantIdentifier(null, null, null),
                    new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD}), 1);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void updateUsersEmail_TransactionExceptions()
            throws InterruptedException, StorageQueryException, NoSuchAlgorithmException, InvalidKeyException,
            SignatureException, InvalidAlgorithmParameterException, NoSuchPaddingException, BadPaddingException,
            UnsupportedEncodingException, InvalidKeySpecException, IllegalBlockSizeException,
            StorageTransactionLogicException, DuplicateUserIdException, DuplicateEmailException,
            TenantOrAppNotFoundException {
        {
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            EmailPasswordSQLStorage storage = (EmailPasswordSQLStorage) StorageLayer.getStorage(process.getProcess());

            String userId = "userId";
            String userId2 = "userId2";
            String pwHash = "fakehash";
            String userEmail = "useremail@asdf.fdas";
            String userEmail2 = "useremail2@asdf.fdas";
            String userEmail3 = "useremail3@asdf.fdas";

            var info = new UserInfo(userId, userEmail, pwHash, System.currentTimeMillis());
            var info2 = new UserInfo(userId2, userEmail2, pwHash, System.currentTimeMillis());
            storage.signUp(new TenantIdentifier(null, null, null), info);
            storage.signUp(new TenantIdentifier(null, null, null), info2);
            storage.startTransaction(conn -> {
                try {
                    storage.updateUsersEmail_Transaction(new AppIdentifier(null, null), conn, userId, userEmail2);
                    throw new StorageTransactionLogicException(new Exception("This should throw"));
                } catch (DuplicateEmailException ex) {
                    // expected
                }
                return true;
            });

            storage.startTransaction(conn -> {
                try {
                    storage.updateUsersEmail_Transaction(new AppIdentifier(null, null), conn, userId, userEmail3);
                } catch (DuplicateEmailException ex) {
                    throw new StorageQueryException(ex);
                }
                return true;
            });

            assertEquals(storage.getUsersCount(new TenantIdentifier(null, null, null),
                    new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD}), 2);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void updateIsEmailVerified_TransactionExceptions()
            throws InterruptedException, StorageQueryException, NoSuchAlgorithmException, InvalidKeyException,
            SignatureException, InvalidAlgorithmParameterException, NoSuchPaddingException, BadPaddingException,
            UnsupportedEncodingException, InvalidKeySpecException, IllegalBlockSizeException,
            StorageTransactionLogicException, DuplicateUserIdException, DuplicateEmailException {
        {
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            var storage = StorageLayer.getEmailVerificationStorage(process.getProcess());

            String userId = "userId";
            String userEmail = "useremail@asdf.fdas";

            storage.startTransaction(conn -> {
                try {
                    storage.updateIsEmailVerified_Transaction(new AppIdentifier(null, null), conn, userId, userEmail,
                                true);
                    storage.updateIsEmailVerified_Transaction(new AppIdentifier(null, null), conn, userId, userEmail, true);
                } catch (TenantOrAppNotFoundException e) {
                    throw new RuntimeException(e);
                }
                // The insert in this call throws, but it's swallowed in the method
                return true;
            });

            storage.startTransaction(conn -> {
                try {
                    // This call should throw, and the method shouldn't swallow it
                    storage.updateIsEmailVerified_Transaction(new AppIdentifier(null, null), conn, null, userEmail,
                            true);
                    throw new StorageTransactionLogicException(new Exception("This should throw"));
                } catch (StorageQueryException ex) {
                    // expected
                } catch (TenantOrAppNotFoundException e) {
                    throw new RuntimeException(e);
                }
                return true;
            });

            storage.startTransaction(conn -> {
                try {
                    storage.updateIsEmailVerified_Transaction(new AppIdentifier(null, null), conn, userId, userEmail,
                            false);
                } catch (TenantOrAppNotFoundException e) {
                    throw new RuntimeException(e);
                }
                return true;
            });

            // storage.startTransaction(conn -> {
            // try {
            // storage.updateIsEmailVerified_Transaction(conn, userId, userEmail2, true);
            // } catch (StorageQueryException ex) {
            // throw new StorageQueryException(ex);
            // }
            // return true;
            // });

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void addPasswordResetTokenExceptions() throws Exception {
        {
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            EmailPasswordSQLStorage storage = (EmailPasswordSQLStorage) StorageLayer.getStorage(process.getProcess());

            String userId = "userId";
            String tokenHash = "fakehash";
            String pwHash = "fakehash";
            String userEmail = "useremail@asdf.fdas";

            var userInfo = new UserInfo(userId, userEmail, pwHash, System.currentTimeMillis());
            var info = new PasswordResetTokenInfo(userId, tokenHash, System.currentTimeMillis() + 10000);
            try {
                storage.addPasswordResetToken(new AppIdentifier(null, null), info);
            } catch (UnknownUserIdException ex) {
                storage.signUp(new TenantIdentifier(null, null, null), userInfo);
            }
            storage.addPasswordResetToken(new AppIdentifier(null, null), info);
            try {
                storage.addPasswordResetToken(new AppIdentifier(null, null), info);
                throw new Exception("This should throw");
            } catch (DuplicatePasswordResetTokenException ex) {
                // expected
            }

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void addEmailVerificationTokenExceptions() throws Exception {
        {
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            var storage = StorageLayer.getEmailVerificationStorage(process.getProcess());

            String userId = "userId";
            String tokenHash = "fakehash";
            String userEmail = "useremail@asdf.fdas";

            var info = new EmailVerificationTokenInfo(userId, tokenHash, System.currentTimeMillis() + 10000, userEmail);
            storage.addEmailVerificationToken(new AppIdentifier(null, null), info);
            try {
                storage.addEmailVerificationToken(new AppIdentifier(null, null), info);
                throw new Exception("This should throw");
            } catch (DuplicateEmailVerificationTokenException ex) {
                // expected
            }

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void verifyEmailExceptions() throws Exception {
        {
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            EmailPasswordSQLStorage storage = (EmailPasswordSQLStorage) StorageLayer.getStorage(process.getProcess());

            String userId = "userId";
            String userId2 = "userId2";
            String pwHash = "fakehash";
            String userEmail = "useremail@asdf.fdas";

            var info = new UserInfo(userId, userEmail, pwHash, System.currentTimeMillis());
            storage.signUp(new TenantIdentifier(null, null, null), info);
            try {
                storage.signUp(new TenantIdentifier(null, null, null), info);
                throw new Exception("This should throw");
            } catch (DuplicateUserIdException ex) {
                // expected
            }
            var info2 = new UserInfo(userId2, userEmail, pwHash, System.currentTimeMillis());

            try {
                storage.signUp(new TenantIdentifier(null, null, null), info2);
                throw new Exception("This should throw");
            } catch (DuplicateEmailException ex) {
                // expected
            }

            assertEquals(storage.getUsersCount(new TenantIdentifier(null, null, null),
                    new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD}), 1);

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    @Test
    public void setJWTSigningKey_TransactionExceptions() throws InterruptedException, StorageQueryException,
            NoSuchAlgorithmException, InvalidKeyException, SignatureException, InvalidAlgorithmParameterException,
            NoSuchPaddingException, BadPaddingException, UnsupportedEncodingException, InvalidKeySpecException,
            IllegalBlockSizeException, StorageTransactionLogicException {
        {
            String[] args = {"../"};
            TestingProcessManager.TestingProcess process = TestingProcessManager.start(args);
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

            var storage = (JWTRecipeSQLStorage) StorageLayer.getJWTRecipeStorage(process.getProcess());

            String keyId = "testkeyId";
            String algorithm = "testalgo";
            String keyString = "fakeKey";

            // String keyId, long createdAtTime, String algorithm, String keyString
            var info = new JWTSymmetricSigningKeyInfo(keyId, System.currentTimeMillis(), algorithm, keyString);
            storage.startTransaction(con -> {
                try {
                    storage.setJWTSigningKey_Transaction(new AppIdentifier(null, null), con, info);
                } catch (DuplicateKeyIdException e) {
                    throw new StorageTransactionLogicException(e);
                }

                try {
                    storage.setJWTSigningKey_Transaction(new AppIdentifier(null, null), con, info);
                    throw new StorageTransactionLogicException(new Exception("This should throw"));
                } catch (DuplicateKeyIdException e) {
                    // expected
                }

                return true;
            });

            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

}
