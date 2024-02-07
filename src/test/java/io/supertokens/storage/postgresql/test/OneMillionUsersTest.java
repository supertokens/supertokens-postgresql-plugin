/*
 *    Copyright (c) 2024, VRAI Labs and/or its affiliates. All rights reserved.
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

package io.supertokens.storage.postgresql.test;/*
 *    Copyright (c) 2024, VRAI Labs and/or its affiliates. All rights reserved.
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

import com.google.gson.JsonObject;
import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.authRecipe.UserPaginationContainer;
import io.supertokens.authRecipe.exception.AccountInfoAlreadyAssociatedWithAnotherPrimaryUserIdException;
import io.supertokens.authRecipe.exception.InputUserIdIsNotAPrimaryUserException;
import io.supertokens.authRecipe.exception.RecipeUserIdAlreadyLinkedWithAnotherPrimaryUserIdException;
import io.supertokens.authRecipe.exception.RecipeUserIdAlreadyLinkedWithPrimaryUserIdException;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.emailpassword.ParsedFirebaseSCryptResponse;
import io.supertokens.emailpassword.exceptions.UnsupportedPasswordHashingFormatException;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.authRecipe.sqlStorage.AuthRecipeSQLStorage;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicateEmailException;
import io.supertokens.pluginInterface.emailpassword.exceptions.DuplicateUserIdException;
import io.supertokens.pluginInterface.emailpassword.exceptions.UnknownUserIdException;
import io.supertokens.pluginInterface.emailpassword.sqlStorage.EmailPasswordSQLStorage;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.exceptions.StorageTransactionLogicException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.multitenancy.exceptions.TenantOrAppNotFoundException;
import io.supertokens.pluginInterface.passwordless.exception.DuplicatePhoneNumberException;
import io.supertokens.pluginInterface.passwordless.sqlStorage.PasswordlessSQLStorage;
import io.supertokens.pluginInterface.thirdparty.sqlStorage.ThirdPartySQLStorage;
import io.supertokens.pluginInterface.useridmapping.exception.UnknownSuperTokensUserIdException;
import io.supertokens.pluginInterface.useridmapping.exception.UserIdMappingAlreadyExistsException;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;
import io.supertokens.useridmapping.UserIdMapping;
import io.supertokens.usermetadata.UserMetadata;
import io.supertokens.userroles.UserRoles;
import jakarta.servlet.ServletException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OneMillionUsersTest {
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

    static int TOTAL_USERS = 1000000;
    static int NUM_THREADS = 16;

//    private static class UserDataCreator implements Runnable {
//        private final BlockingQueue<AuthRecipeUserInfo> sharedQueue;
//        private Main main;
//
//        static AtomicLong usersUpdated = new AtomicLong(0);
//
//        public UserDataCreator(Main main, BlockingQueue<AuthRecipeUserInfo> sharedQueue) {
//            this.main = main;
//            this.sharedQueue = sharedQueue;
//            lastMil = System.currentTimeMillis();
//        }
//
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    AuthRecipeUserInfo user = sharedQueue.take();
//                    if (user.getSupertokensUserId().equals("")) {
//                        break;
//                    }
//
//                    Random random = new Random();
//
//                    // UserId mapping
//                    String userId = user.getSupertokensUserId();
//                    if (random.nextBoolean()) {
//                        userId = UUID.randomUUID().toString();
//                        UserIdMapping.createUserIdMapping(main, userId, userId, null, false);
//                    }
//
//                    // User Metadata
//                    JsonObject metadata = new JsonObject();
//                    metadata.addProperty("random", random.nextDouble());
//
//                    UserMetadata.updateUserMetadata(main, userId, metadata);
//
//                    // User Roles
//                    if (random.nextBoolean()) {
//                        UserRoles.addRoleToUser(main, userId, "admin");
//                    } else {
//                        UserRoles.addRoleToUser(main, userId, "user");
//                    }
//
//                    long updatedCount = usersUpdated.incrementAndGet();
//                    if (updatedCount % 10000 == 9999) {
//                        System.out.println("Updated " + (updatedCount) + " users in " + ((System.currentTimeMillis() - lastMil) / 1000) + " sec");
//                        lastMil = System.currentTimeMillis();
//                    }
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
//    }
//
//    private static class AccountLinkingConsumer implements Runnable {
//        private final BlockingQueue<String[]> sharedQueue;
//        private Main main;
//
//        private static AtomicLong accountsLinked = new AtomicLong(0);
//
//        public AccountLinkingConsumer(Main main, BlockingQueue<String[]> sharedQueue) {
//            this.main = main;
//            this.sharedQueue = sharedQueue;
//            lastMil = System.currentTimeMillis();
//        }
//
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    String[] userIds = sharedQueue.take();
//                    if (userIds[0] == null) {
//                        break;
//                    }
//
//                    AuthRecipeSQLStorage storage = (AuthRecipeSQLStorage) StorageLayer.getBaseStorage(main);
//
//                    storage.startTransaction(con -> {
//                        storage.makePrimaryUser_Transaction(new AppIdentifier(null, null), con, userIds[0]);
//                        storage.commitTransaction(con);
//                        return null;
//                    });
//
//                    for (int i = 1; i < userIds.length; i++) {
//                        int finalI = i;
//                        storage.startTransaction(con -> {
//                            storage.linkAccounts_Transaction(new AppIdentifier(null, null), con, userIds[finalI],
//                                    userIds[0]);
//                            storage.commitTransaction(con);
//                            return null;
//                        });
//                    }
//
//                    long total = accountsLinked.addAndGet(userIds.length);
//
//                    if (total % 10000 > 9996) {
//                        System.out.println("Linked " + (accountsLinked) + " users in " + ((System.currentTimeMillis() - lastMil) / 1000) + " sec");
//                        lastMil = System.currentTimeMillis();
//                    }
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
//    }
//
    private void createEmailPasswordUsers(Main main) throws Exception {
        System.out.println("Creating emailpassword users...");

        int firebaseMemCost = 14;
        int firebaseRounds = 8;
        String firebaseSaltSeparator = "Bw==";

        String salt = "/cj0jC1br5o4+w==";
        String passwordHash = "qZM035es5AXYqavsKD6/rhtxg7t5PhcyRgv5blc3doYbChX8keMfQLq1ra96O2Pf2TP/eZrR5xtPCYN6mX3ESA==";
        String combinedPasswordHash = "$" + ParsedFirebaseSCryptResponse.FIREBASE_SCRYPT_PREFIX + "$" + passwordHash
                + "$" + salt + "$m=" + firebaseMemCost + "$r=" + firebaseRounds + "$s=" + firebaseSaltSeparator;

        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);

        EmailPasswordSQLStorage storage = (EmailPasswordSQLStorage) StorageLayer.getBaseStorage(main);

        for (int i = 0; i < TOTAL_USERS / 4; i++) {
            int finalI = i;
            es.execute(() -> {
                try {
                    String userId = io.supertokens.utils.Utils.getUUID();
                    long timeJoined = System.currentTimeMillis();

                    storage.signUp(TenantIdentifier.BASE_TENANT, userId, "eptest" + finalI + "@example.com", combinedPasswordHash,
                            timeJoined);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (finalI % 10000 == 9999) {
                    System.out.println("Created " + ((finalI +1)) + " users");
                }
            });
        }

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }

    private void createPasswordlessUsersWithEmail(Main main) throws Exception {
        System.out.println("Creating passwordless (email) users...");

        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);
        PasswordlessSQLStorage storage = (PasswordlessSQLStorage) StorageLayer.getBaseStorage(main);

        for (int i = 0; i < TOTAL_USERS / 4; i++) {
            int finalI = i;
            es.execute(() -> {
                String userId = io.supertokens.utils.Utils.getUUID();
                long timeJoined = System.currentTimeMillis();
                try {
                    storage.createUser(TenantIdentifier.BASE_TENANT, userId, "pltest" + finalI + "@example", null, timeJoined);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (finalI % 10000 == 9999) {
                    System.out.println("Created " + ((finalI +1)) + " users");
                }
            });
        }

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }

    private void createPasswordlessUsersWithPhone(Main main) throws Exception {
        System.out.println("Creating passwordless (phone) users...");

        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);
        PasswordlessSQLStorage storage = (PasswordlessSQLStorage) StorageLayer.getBaseStorage(main);

        for (int i = 0; i < TOTAL_USERS / 4; i++) {
            int finalI = i;
            es.execute(() -> {
                String userId = io.supertokens.utils.Utils.getUUID();
                long timeJoined = System.currentTimeMillis();
                try {
                    storage.createUser(TenantIdentifier.BASE_TENANT, userId, null, "+91987654" + finalI, timeJoined);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (finalI % 10000 == 9999) {
                    System.out.println("Created " + ((finalI +1)) + " users");
                }
            });
        }

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }

    private void createThirdpartyUsers(Main main) throws Exception {
        System.out.println("Creating thirdparty users...");

        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);
        ThirdPartySQLStorage storage = (ThirdPartySQLStorage) StorageLayer.getBaseStorage(main);

        for (int i = 0; i < TOTAL_USERS / 4; i++) {
            int finalI = i;
            es.execute(() -> {
                String userId = io.supertokens.utils.Utils.getUUID();
                long timeJoined = System.currentTimeMillis();

                try {
                    storage.signUp(TenantIdentifier.BASE_TENANT, userId, "tptest" + finalI + "@example.com", new LoginMethod.ThirdParty("google", "googleid" + finalI), timeJoined );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (finalI % 10000 == 9999) {
                    System.out.println("Created " + (finalI +1) + " users");
                }
            });
        }

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }

    private void createOneMillionUsers(Main main) throws Exception {
        Thread.sleep(5000);

        createEmailPasswordUsers(main);
        createPasswordlessUsersWithEmail(main);
        createPasswordlessUsersWithPhone(main);
        createThirdpartyUsers(main);
    }

    private void createUserIdMappings(Main main) throws Exception {
        System.out.println("Creating user id mappings...");

        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);

        UserPaginationContainer usersResult = AuthRecipe.getUsers(main, 10000, "ASC", null,
                null, null);

        AtomicLong usersUpdated = new AtomicLong(0);

        while (true) {
            for (AuthRecipeUserInfo user : usersResult.users) {
                es.execute(() -> {
                    Random random = new Random();

                    // UserId mapping
                    for (LoginMethod lm : user.loginMethods) {
                        String userId = user.getSupertokensUserId();

                        if (random.nextBoolean()) {
                            userId = "ext" + UUID.randomUUID().toString();
                            try {
                                UserIdMapping.createUserIdMapping(main, lm.getSupertokensUserId(), userId, null, false);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }

                        long count = usersUpdated.incrementAndGet();
                        if (count % 10000 == 9999) {
                            System.out.println("Updated " + (count) + " users");
                        }
                    }
                });
            }
            if (usersResult.nextPaginationToken == null) {
                break;
            }
            usersResult = AuthRecipe.getUsers(main, 10000, "ASC", usersResult.nextPaginationToken,
                    null, null);
        }

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }

    private void createUserData(Main main) throws Exception {
//        System.out.println("Creating user data...");
//
//        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);
//
//        UserPaginationContainer usersResult = AuthRecipe.getUsers(main, 10000, "ASC", null,
//                null, null);
//
//        while (true) {
//            for (AuthRecipeUserInfo user : usersResult.users) {
//                es.execute(() -> {
//                    Random random = new Random();
//
//                    // UserId mapping
//                    for (LoginMethod lm : user.loginMethods) {
//                        if (random.nextBoolean()) {
//                            String userId = user.getSupertokensUserId();
//
//                            userId = "ext" + UUID.randomUUID().toString();
//                            try {
//                                UserIdMapping.createUserIdMapping(main, lm.getSupertokensUserId(), userId, null, false);
//                            } catch (Exception e) {
//                                throw new RuntimeException(e);
//                            }
//                        }
//                    }
//
//                    // User Metadata
//                    JsonObject metadata = new JsonObject();
//                    metadata.addProperty("random", random.nextDouble());
//
//                    UserMetadata.updateUserMetadata(main, userId, metadata);
//
//                    // User Roles
//                    if (random.nextBoolean()) {
//                        UserRoles.addRoleToUser(main, userId, "admin");
//                    } else {
//                        UserRoles.addRoleToUser(main, userId, "user");
//                    }
//                });
//            }
//            if (usersResult.nextPaginationToken == null) {
//                break;
//            }
//            usersResult = AuthRecipe.getUsers(main, 10000, "ASC", usersResult.nextPaginationToken,
//                    null, null);
//        }
    }

    private void doAccountLinking(Main main) throws Exception {
        Set<String> userIds = new HashSet<>();

        long st = System.currentTimeMillis();
        UserPaginationContainer usersResult = AuthRecipe.getUsers(main, 10000, "ASC", null,
                null, null);

        while (true) {
            for (AuthRecipeUserInfo user : usersResult.users) {
                userIds.add(user.getSupertokensUserId());
            }
            if (usersResult.nextPaginationToken == null) {
                break;
            }
            usersResult = AuthRecipe.getUsers(main, 10000, "ASC", usersResult.nextPaginationToken,
                    null, null);
        }

        long en = System.currentTimeMillis();

        System.out.println("Time taken to get " + TOTAL_USERS + " users (before account linking): " + ((en - st) / 1000) + " sec");

        assertEquals(TOTAL_USERS, userIds.size());

        AtomicLong accountsLinked = new AtomicLong(0);

        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);

        while (userIds.size() > 0) {
            int numberOfItemsToPick = Math.min(new Random().nextInt(4) + 1, userIds.size());
            String[] userIdsArray = new String[numberOfItemsToPick];

            Iterator<String> iterator = userIds.iterator();
            for (int i = 0; i < numberOfItemsToPick; i++) {
                userIdsArray[i] = iterator.next();
                iterator.remove();
            }

            AuthRecipeSQLStorage storage = (AuthRecipeSQLStorage) StorageLayer.getBaseStorage(main);

            es.execute(() -> {
                try {
                    storage.startTransaction(con -> {
                        storage.makePrimaryUser_Transaction(new AppIdentifier(null, null), con, userIdsArray[0]);
                        storage.commitTransaction(con);
                        return null;
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                try {
                    for (int i = 1; i < userIdsArray.length; i++) {
                        int finalI = i;
                        storage.startTransaction(con -> {
                            storage.linkAccounts_Transaction(new AppIdentifier(null, null), con, userIdsArray[finalI],
                                    userIdsArray[0]);
                            storage.commitTransaction(con);
                            return null;
                        });
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                long total = accountsLinked.addAndGet(userIdsArray.length);
                if (total % 10000 > 9996) {
                    System.out.println("Linked " + (accountsLinked) + " users");
                }
            });
        }

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }

    @Test
    public void testCreatingOneMillionUsers() throws Exception {
        String[] args = {"../"};
        Utils.setValueInConfig("firebase_password_hashing_signer_key",
                "gRhC3eDeQOdyEn4bMd9c6kxguWVmcIVq/SKa0JDPFeM6TcEevkaW56sIWfx88OHbJKnCXdWscZx0l2WbCJ1wbg==");
        Utils.setValueInConfig("postgresql_connection_pool_size", "500");

        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        {
            long st = System.currentTimeMillis();
            createOneMillionUsers(process.getProcess());
            long en = System.currentTimeMillis();
            System.out.println("Time taken to create " + TOTAL_USERS + " users: " + ((en - st) / 1000) + " sec");
            assertEquals(TOTAL_USERS, AuthRecipe.getUsersCount(process.getProcess(), null));
        }

        {
            long st = System.currentTimeMillis();
            doAccountLinking(process.getProcess());
            long en = System.currentTimeMillis();
            System.out.println("Time taken to link accounts: " + ((en - st) / 1000) + " sec");
        }

        {
            long st = System.currentTimeMillis();
            createUserIdMappings(process.getProcess());
            long en = System.currentTimeMillis();
            System.out.println("Time taken to create user id mappings: " + ((en - st) / 1000) + " sec");
        }

        {
            UserRoles.createNewRoleOrModifyItsPermissions(process.getProcess(), "admin", new String[]{"p1"});
            UserRoles.createNewRoleOrModifyItsPermissions(process.getProcess(), "user", new String[]{"p2"});
            long st = System.currentTimeMillis();
            createUserData(process.getProcess());
            long en = System.currentTimeMillis();
            System.out.println("Time taken to create user data: " + ((en - st) / 1000) + " sec");
        }

        process.kill(false);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }
}
