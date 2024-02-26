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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.supertokens.ActiveUsers;
import io.supertokens.Main;
import io.supertokens.ProcessState;
import io.supertokens.authRecipe.AuthRecipe;
import io.supertokens.authRecipe.UserPaginationContainer;
import io.supertokens.emailpassword.EmailPassword;
import io.supertokens.emailpassword.ParsedFirebaseSCryptResponse;
import io.supertokens.featureflag.EE_FEATURES;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.authRecipe.sqlStorage.AuthRecipeSQLStorage;
import io.supertokens.pluginInterface.emailpassword.sqlStorage.EmailPasswordSQLStorage;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.pluginInterface.passwordless.sqlStorage.PasswordlessSQLStorage;
import io.supertokens.pluginInterface.thirdparty.sqlStorage.ThirdPartySQLStorage;
import io.supertokens.session.Session;
import io.supertokens.session.info.SessionInformationHolder;
import io.supertokens.storage.postgresql.test.httpRequest.HttpRequestForTesting;
import io.supertokens.storageLayer.StorageLayer;
import io.supertokens.thirdparty.ThirdParty;
import io.supertokens.useridmapping.UserIdMapping;
import io.supertokens.usermetadata.UserMetadata;
import io.supertokens.userroles.UserRoles;
import io.supertokens.utils.SemVer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.Assert.*;

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

    private void createEmailPasswordUsers(Main main) throws Exception {
        System.out.println("Creating emailpassword users...");

        int firebaseMemCost = 14;
        int firebaseRounds = 8;
        String firebaseSaltSeparator = "Bw==";

        String salt = "/cj0jC1br5o4+w==";
        String passwordHash = "qZM035es5AXYqavsKD6/rhtxg7t5PhcyRgv5blc3doYbChX8keMfQLq1ra96O2Pf2TP/eZrR5xtPCYN6mX3ESA" +
                "==";
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
                    storage.createUser(TenantIdentifier.BASE_TENANT, userId, "pltest" + finalI + "@example.com", null, timeJoined);
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
        System.out.println("Creating user data...");

        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS / 2);

        UserPaginationContainer usersResult = AuthRecipe.getUsers(main, 500, "ASC", null,
                null, null);

        while (true) {
            UserIdMapping.populateExternalUserIdForUsers(
                    TenantIdentifier.BASE_TENANT.withStorage(StorageLayer.getBaseStorage(main)),
                    usersResult.users);

            for (AuthRecipeUserInfo user : usersResult.users) {
                es.execute(() -> {
                    Random random = new Random();

                    // User Metadata
                    JsonObject metadata = new JsonObject();
                    metadata.addProperty("random", random.nextDouble());

                    try {
                        UserMetadata.updateUserMetadata(main, user.getSupertokensOrExternalUserId(), metadata);

                        // User Roles
                        if (random.nextBoolean()) {
                            UserRoles.addRoleToUser(main, user.getSupertokensOrExternalUserId(), "admin");
                        } else {
                            UserRoles.addRoleToUser(main, user.getSupertokensOrExternalUserId(), "user");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            if (usersResult.nextPaginationToken == null) {
                break;
            }
            usersResult = AuthRecipe.getUsers(main, 500, "ASC", usersResult.nextPaginationToken,
                    null, null);
        }

        es.shutdown();
        es.awaitTermination(1, TimeUnit.MINUTES);
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

    private static String accessToken;
    private static String sessionUserId;

    private void createSessions(Main main) throws Exception {
        System.out.println("Creating sessions...");

        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);

        UserPaginationContainer usersResult = AuthRecipe.getUsers(main, 500, "ASC", null,
                null, null);

        while (true) {
            UserIdMapping.populateExternalUserIdForUsers(
                    TenantIdentifier.BASE_TENANT.withStorage(StorageLayer.getBaseStorage(main)),
                    usersResult.users);

            for (AuthRecipeUserInfo user : usersResult.users) {
                es.execute(() -> {
                    try {
                        for (LoginMethod lM : user.loginMethods) {
                            String userId = lM.getSupertokensOrExternalUserId();
                            SessionInformationHolder session = Session.createNewSession(main,
                                    userId, new JsonObject(), new JsonObject());

                            if (new Random().nextFloat() < 0.05) {
                                accessToken = session.accessToken.token;
                                sessionUserId = userId;
                            }
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            if (usersResult.nextPaginationToken == null) {
                break;
            }
            usersResult = AuthRecipe.getUsers(main, 500, "ASC", usersResult.nextPaginationToken,
                    null, null);
        }

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }

    @Test
    public void testCreatingOneMillionUsers() throws Exception {
//        if (System.getenv("ONE_MILLION_USERS_TEST") == null) {
//            return;
//        }

        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        Utils.setValueInConfig("firebase_password_hashing_signer_key",
                "gRhC3eDeQOdyEn4bMd9c6kxguWVmcIVq/SKa0JDPFeM6TcEevkaW56sIWfx88OHbJKnCXdWscZx0l2WbCJ1wbg==");
        Utils.setValueInConfig("postgresql_connection_pool_size", "500");

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

        {
            long st = System.currentTimeMillis();
            createSessions(process.getProcess());
            long en = System.currentTimeMillis();
            System.out.println("Time taken to create sessions: " + ((en - st) / 1000) + " sec");
        }

        sanityCheckAPIs(process.getProcess());

        Thread.sleep(10000);

        measureOperations(process.getProcess());

        process.kill();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
    }

    private void sanityCheckAPIs(Main main) throws Exception {
        { // Email password sign in
            JsonObject responseBody = new JsonObject();
            responseBody.addProperty("email", "eptest10@example.com");
            responseBody.addProperty("password", "testPass123");

            Thread.sleep(1); // add a small delay to ensure a unique timestamp
            long beforeSignIn = System.currentTimeMillis();

            JsonObject signInResponse = HttpRequestForTesting.sendJsonPOSTRequest(main, "",
                    "http://localhost:3567/recipe/signin", responseBody, 1000, 1000, null, SemVer.v4_0.get(),
                    "emailpassword");

            assertEquals(signInResponse.get("status").getAsString(), "OK");
            assertEquals(signInResponse.entrySet().size(), 3);

            JsonObject jsonUser = signInResponse.get("user").getAsJsonObject();
            JsonArray emails = jsonUser.get("emails").getAsJsonArray();
            boolean found = false;

            for (JsonElement elem : emails) {
                if (elem.getAsString().equals("eptest10@example.com")) {
                    found = true;
                    break;
                }
            }

            assertTrue(found);

            int activeUsers = ActiveUsers.countUsersActiveSince(main, beforeSignIn);
            assert (activeUsers == 1);
        }

        { // passwordless sign in
            long startTs = System.currentTimeMillis();

            String email = "pltest10@example.com";
            Passwordless.CreateCodeResponse createResp = Passwordless.createCode(main, email, null, null, null);

            JsonObject consumeCodeRequestBody = new JsonObject();
            consumeCodeRequestBody.addProperty("deviceId", createResp.deviceId);
            consumeCodeRequestBody.addProperty("preAuthSessionId", createResp.deviceIdHash);
            consumeCodeRequestBody.addProperty("userInputCode", createResp.userInputCode);

            JsonObject response = HttpRequestForTesting.sendJsonPOSTRequest(main, "",
                    "http://localhost:3567/recipe/signinup/code/consume", consumeCodeRequestBody, 1000, 1000, null,
                    SemVer.v5_0.get(), "passwordless");

            assertEquals("OK", response.get("status").getAsString());
            assertEquals(false, response.get("createdNewUser").getAsBoolean());
            assert (response.has("user"));

            JsonObject jsonUser = response.get("user").getAsJsonObject();
            JsonArray emails = jsonUser.get("emails").getAsJsonArray();
            boolean found = false;

            for (JsonElement elem : emails) {
                if (elem.getAsString().equals("pltest10@example.com")) {
                    found = true;
                    break;
                }
            }

            assertTrue(found);

            int activeUsers = ActiveUsers.countUsersActiveSince(main, startTs);
            assert (activeUsers == 1);
        }

        { // thirdparty sign in
            long startTs = System.currentTimeMillis();
            JsonObject emailObject = new JsonObject();
            emailObject.addProperty("id", "tptest10@example.com");
            emailObject.addProperty("isVerified", true);

            JsonObject signUpRequestBody = new JsonObject();
            signUpRequestBody.addProperty("thirdPartyId", "google");
            signUpRequestBody.addProperty("thirdPartyUserId", "googleid10");
            signUpRequestBody.add("email", emailObject);

            JsonObject response = HttpRequestForTesting.sendJsonPOSTRequest(main, "",
                    "http://localhost:3567/recipe/signinup", signUpRequestBody, 1000, 1000, null,
                    SemVer.v4_0.get(), "thirdparty");

            assertEquals("OK", response.get("status").getAsString());
            assertEquals(false, response.get("createdNewUser").getAsBoolean());
            assert (response.has("user"));

            JsonObject jsonUser = response.get("user").getAsJsonObject();
            JsonArray emails = jsonUser.get("emails").getAsJsonArray();
            boolean found = false;

            for (JsonElement elem : emails) {
                if (elem.getAsString().equals("tptest10@example.com")) {
                    found = true;
                    break;
                }
            }

            assertTrue(found);

            int activeUsers = ActiveUsers.countUsersActiveSince(main, startTs);
            assert (activeUsers == 1);
        }

        { // session for user
            JsonObject request = new JsonObject();
            request.addProperty("accessToken", accessToken);
            request.addProperty("doAntiCsrfCheck", false);
            request.addProperty("enableAntiCsrf", false);
            request.addProperty("checkDatabase", false);
            JsonObject response = HttpRequestForTesting.sendJsonPOSTRequest(main, "",
                    "http://localhost:3567/recipe/session/verify", request, 1000, 1000, null,
                    SemVer.v5_0.get(), "session");
            assertEquals("OK", response.get("status").getAsString());
            assertEquals(sessionUserId, response.get("session").getAsJsonObject().get("userId").getAsString());
        }

        { // check user roles
            JsonObject responseBody = new JsonObject();
            responseBody.addProperty("email", "eptest10@example.com");
            responseBody.addProperty("password", "testPass123");

            Thread.sleep(1); // add a small delay to ensure a unique timestamp
            JsonObject signInResponse = HttpRequestForTesting.sendJsonPOSTRequest(main, "",
                    "http://localhost:3567/recipe/signin", responseBody, 1000, 1000, null, SemVer.v4_0.get(),
                    "emailpassword");

            HashMap<String, String> QUERY_PARAMS = new HashMap<>();
            QUERY_PARAMS.put("userId", signInResponse.get("user").getAsJsonObject().get("id").getAsString());
            JsonObject response = HttpRequestForTesting.sendGETRequest(main, "",
                    "http://localhost:3567/recipe/user/roles", QUERY_PARAMS, 1000, 1000, null,
                    SemVer.v2_14.get(), "userroles");

            assertEquals(2, response.entrySet().size());
            assertEquals("OK", response.get("status").getAsString());

            JsonArray userRolesArr = response.getAsJsonArray("roles");
            assertEquals(1, userRolesArr.size());
            assertTrue(
                    userRolesArr.get(0).getAsString().equals("admin") || userRolesArr.get(0).getAsString().equals("user")
            );
        }

        { // check user metadata
            HashMap<String, String> QueryParams = new HashMap<String, String>();
            QueryParams.put("userId", sessionUserId);
            JsonObject resp = HttpRequestForTesting.sendGETRequest(main, "",
                    "http://localhost:3567/recipe/user/metadata", QueryParams, 1000, 1000, null,
                    SemVer.v2_13.get(), "usermetadata");

            assertEquals(2, resp.entrySet().size());
            assertEquals("OK", resp.get("status").getAsString());
            assert (resp.has("metadata"));
            JsonObject respMetadata = resp.getAsJsonObject("metadata");
            assertEquals(1, respMetadata.entrySet().size());
        }
    }

    private void measureOperations(Main main) throws Exception {
        AtomicLong errorCount = new AtomicLong(0);
        { // Emailpassword sign up
            System.out.println("Measure email password sign-ups");
            long time = measureTime(() -> {
                ExecutorService es = Executors.newFixedThreadPool(100);

                for (int i = 0; i < 500; i++) {
                    int finalI = i;
                    es.execute(() -> {
                        try {
                            EmailPassword.signUp(main, "ep" + finalI + "@example.com", "password" + finalI);
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            throw new RuntimeException(e);
                        }
                    });
                }

                es.shutdown();
                try {
                    es.awaitTermination(5, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
                return null;
            });
            assert time < 10000; // 10 sec
        }
        { // Emailpassword sign in
            System.out.println("Measure email password sign-ins");
            long time = measureTime(() -> {
                ExecutorService es = Executors.newFixedThreadPool(100);

                for (int i = 0; i < 500; i++) {
                    int finalI = i;
                    es.execute(() -> {
                        try {
                            EmailPassword.signIn(main, "ep" + finalI + "@example.com", "password" + finalI);
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            throw new RuntimeException(e);
                        }
                    });
                }

                es.shutdown();
                try {
                    es.awaitTermination(5, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
                return null;
            });
            assert time < 10000; // 10 sec
        }
        { // Passwordless sign-ups
            System.out.println("Measure passwordless sign-ups");
            long time = measureTime(() -> {
                ExecutorService es = Executors.newFixedThreadPool(100);
                for (int i = 0; i < 500; i++) {
                    int finalI = i;
                    es.execute(() -> {
                        try {
                            Passwordless.CreateCodeResponse code = Passwordless.createCode(main,
                                    "pl" + finalI + "@example.com", null, null, null);
                            Passwordless.consumeCode(main, code.deviceId, code.deviceIdHash, code.userInputCode, null);
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            throw new RuntimeException(e);
                        }
                    });
                }
                es.shutdown();
                try {
                    es.awaitTermination(5, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
                return null;
            });
            assert time < 3000; // 3 sec
        }
        { // Passwordless sign-ins
            System.out.println("Measure passwordless sign-ins");
            long time = measureTime(() -> {
                ExecutorService es = Executors.newFixedThreadPool(100);
                for (int i = 0; i < 500; i++) {
                    int finalI = i;
                    es.execute(() -> {
                        try {
                            Passwordless.CreateCodeResponse code = Passwordless.createCode(main,
                                    "pl" + finalI + "@example.com", null, null, null);
                            Passwordless.consumeCode(main, code.deviceId, code.deviceIdHash, code.userInputCode, null);
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            throw new RuntimeException(e);
                        }
                    });
                }
                es.shutdown();
                try {
                    es.awaitTermination(5, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
                return null;
            });
            assert time < 3000; // 3 sec
        }
        { // Thirdparty sign-ups
            System.out.println("Measure thirdparty sign-ups");
            long time = measureTime(() -> {
                ExecutorService es = Executors.newFixedThreadPool(100);
                for (int i = 0; i < 500; i++) {
                    int finalI = i;
                    es.execute(() -> {
                        try {
                            ThirdParty.signInUp(main, "twitter", "twitterid" + finalI, "twitter" + finalI + "@example.com");
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            throw new RuntimeException(e);
                        }
                    });
                }
                es.shutdown();
                try {
                    es.awaitTermination(5, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
            assert time < 3000; // 3 sec
        }
        { // Thirdparty sign-ins
            System.out.println("Measure thirdparty sign-ins");
            long time = measureTime(() -> {
                ExecutorService es = Executors.newFixedThreadPool(100);
                for (int i = 0; i < 500; i++) {
                    int finalI = i;
                    es.execute(() -> {
                        try {
                            ThirdParty.signInUp(main, "twitter", "twitterid" + finalI, "twitter" + finalI + "@example.com");
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            throw new RuntimeException(e);
                        }
                    });
                }
                es.shutdown();
                try {
                    es.awaitTermination(5, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
                return null;
            });
            assert time < 3000; // 3 sec
        }
        { // Measure user pagination
            long time = measureTime(() -> {
                try {
                    long count = 0;
                    UserPaginationContainer users = AuthRecipe.getUsers(main, 500, "ASC", null, null, null);
                    while (true) {
                        for (AuthRecipeUserInfo user : users.users) {
                            count += user.loginMethods.length;
                        }
                        if (users.nextPaginationToken == null) {
                            break;
                        }
                        users = AuthRecipe.getUsers(main, 500, "ASC", users.nextPaginationToken, null, null);
                    }
                    assertEquals(TOTAL_USERS + 1500, count);
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
                return null;
            });
            assert time < 5000; // 5 sec
        }

        assertEquals(0, errorCount.get());
    }

    private static long measureTime(Supplier<Void> function) {
        long startTime = System.nanoTime();

        // Call the function
        function.get();

        long endTime = System.nanoTime();

        // Calculate elapsed time in milliseconds
        return (endTime - startTime) / 1000000; // Convert to milliseconds
    }
}
