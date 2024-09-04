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

package io.supertokens.storage.postgresql.test;

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
import io.supertokens.featureflag.FeatureFlag;
import io.supertokens.featureflag.FeatureFlagTestContent;
import io.supertokens.passwordless.Passwordless;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.Storage;
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
import io.supertokens.storage.postgresql.Start;
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
import java.util.concurrent.atomic.AtomicBoolean;
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

    Object lock = new Object();
    Set<String> allUserIds = new HashSet<>();
    Set<String> allPrimaryUserIds = new HashSet<>();
    Map<String, String> userIdMappings = new HashMap<>();
    Map<String, String> primaryUserIdMappings = new HashMap<>();

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

                    storage.signUp(TenantIdentifier.BASE_TENANT, userId, "eptest" + finalI + "@example.com",
                            combinedPasswordHash,
                            timeJoined);
                    synchronized (lock) {
                        allUserIds.add(userId);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (finalI % 10000 == 9999) {
                    System.out.println("Created " + ((finalI + 1)) + " users");
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
                    storage.createUser(TenantIdentifier.BASE_TENANT, userId, "pltest" + finalI + "@example.com", null,
                            timeJoined);
                    synchronized (lock) {
                        allUserIds.add(userId);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (finalI % 10000 == 9999) {
                    System.out.println("Created " + ((finalI + 1)) + " users");
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
                    synchronized (lock) {
                        allUserIds.add(userId);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (finalI % 10000 == 9999) {
                    System.out.println("Created " + ((finalI + 1)) + " users");
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
                    storage.signUp(TenantIdentifier.BASE_TENANT, userId, "tptest" + finalI + "@example.com",
                            new LoginMethod.ThirdParty("google", "googleid" + finalI), timeJoined);
                    synchronized (lock) {
                        allUserIds.add(userId);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (finalI % 10000 == 9999) {
                    System.out.println("Created " + (finalI + 1) + " users");
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
        AtomicLong usersUpdated = new AtomicLong(0);

        for (String userId : allUserIds) {
            es.execute(() -> {
                String extUserId = "ext" + UUID.randomUUID().toString();
                try {
                    UserIdMapping.createUserIdMapping(main, userId, extUserId, null, false);
                    synchronized (lock) {
                        userIdMappings.put(userId, extUserId);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                long count = usersUpdated.incrementAndGet();
                if (count % 10000 == 9999) {
                    System.out.println("Updated " + (count) + " users");
                }
            });
        }

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }

    private void createUserData(Main main) throws Exception {
        System.out.println("Creating user data...");

        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS / 2);

        for (String userId : allPrimaryUserIds) {
            es.execute(() -> {
                Random random = new Random();

                // User Metadata
                JsonObject metadata = new JsonObject();
                metadata.addProperty("random", random.nextDouble());

                try {
                    UserMetadata.updateUserMetadata(main, userIdMappings.get(userId), metadata);

                    // User Roles
                    if (random.nextBoolean()) {
                        UserRoles.addRoleToUser(main, userIdMappings.get(userId), "admin");
                    } else {
                        UserRoles.addRoleToUser(main, userIdMappings.get(userId), "user");
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        es.shutdown();
        es.awaitTermination(1, TimeUnit.MINUTES);
    }

    private void doAccountLinking(Main main) throws Exception {
        Set<String> userIds = new HashSet<>();
        userIds.addAll(allUserIds);

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

                synchronized (lock) {
                    allPrimaryUserIds.add(userIdsArray[0]);
                    for (String userId : userIdsArray) {
                        primaryUserIdMappings.put(userId, userIdsArray[0]);
                    }
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

        for (String userId : allUserIds) {
            String finalUserId = userId;
            es.execute(() -> {
                try {
                    SessionInformationHolder session = Session.createNewSession(main,
                            userIdMappings.get(finalUserId), new JsonObject(), new JsonObject());

                    if (new Random().nextFloat() < 0.05) {
                        synchronized (lock) {
                            accessToken = session.accessToken.token;
                            sessionUserId = userIdMappings.get(primaryUserIdMappings.get(finalUserId));
                        }
                    }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }

    private void createActiveUserEntries(Main main) throws Exception {
        System.out.println("Creating active user entries...");

        ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);

        for (String userId : allPrimaryUserIds) {
            String finalUserId = userId;
            es.execute(() -> {
                try {
                    Storage storage = StorageLayer.getBaseStorage(main);
                    Start start = (Start) storage;

                    start.updateLastActive(new AppIdentifier(null, null), finalUserId, System.currentTimeMillis() - new Random().nextInt(1000 * 3600 * 24 * 60));

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }
    
    @Test
    public void testCreatingOneMillionUsers() throws Exception {
        if (System.getenv("ONE_MILLION_USERS_TEST") == null) {
            return;
        }

        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        Utils.setValueInConfig("firebase_password_hashing_signer_key",
                "gRhC3eDeQOdyEn4bMd9c6kxguWVmcIVq/SKa0JDPFeM6TcEevkaW56sIWfx88OHbJKnCXdWscZx0l2WbCJ1wbg==");
        Utils.setValueInConfig("postgresql_connection_pool_size", "500");

        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY, EE_FEATURES.MFA, EE_FEATURES.DASHBOARD_LOGIN});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        AtomicBoolean memoryCheckRunning = new AtomicBoolean(true);
        AtomicLong maxMemory = new AtomicLong(0);

//        {
//            long st = System.currentTimeMillis();
//            createOneMillionUsers(process.getProcess());
//            long en = System.currentTimeMillis();
//            System.out.println("Time taken to create " + TOTAL_USERS + " users: " + ((en - st) / 1000) + " sec");
//            assertEquals(TOTAL_USERS, AuthRecipe.getUsersCount(process.getProcess(), null));
//        }
//
//        {
//            long st = System.currentTimeMillis();
//            doAccountLinking(process.getProcess());
//            long en = System.currentTimeMillis();
//            System.out.println("Time taken to link accounts: " + ((en - st) / 1000) + " sec");
//        }
//
//        {
//            long st = System.currentTimeMillis();
//            createUserIdMappings(process.getProcess());
//            long en = System.currentTimeMillis();
//            System.out.println("Time taken to create user id mappings: " + ((en - st) / 1000) + " sec");
//        }
//
//        {
//            UserRoles.createNewRoleOrModifyItsPermissions(process.getProcess(), "admin", new String[]{"p1"});
//            UserRoles.createNewRoleOrModifyItsPermissions(process.getProcess(), "user", new String[]{"p2"});
//            long st = System.currentTimeMillis();
//            createUserData(process.getProcess());
//            long en = System.currentTimeMillis();
//            System.out.println("Time taken to create user data: " + ((en - st) / 1000) + " sec");
//        }
//
//        {
//            long st = System.currentTimeMillis();
//            createSessions(process.getProcess());
//            long en = System.currentTimeMillis();
//            System.out.println("Time taken to create sessions: " + ((en - st) / 1000) + " sec");
//        }
//
//        {
//            long st = System.currentTimeMillis();
//            createActiveUserEntries(process.getProcess());
//            long en = System.currentTimeMillis();
//            System.out.println("Time taken to create active user entries: " + ((en - st) / 1000) + " sec");
//        }
//
//        sanityCheckAPIs(process.getProcess());
        allUserIds.clear();
        allPrimaryUserIds.clear();
        userIdMappings.clear();
        primaryUserIdMappings.clear();

        process.kill(false);
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));

        Runtime.getRuntime().gc();
        System.gc();
        System.runFinalization();
        Thread.sleep(10000);

        process = TestingProcessManager.start(args, false);
        Utils.setValueInConfig("firebase_password_hashing_signer_key",
                "gRhC3eDeQOdyEn4bMd9c6kxguWVmcIVq/SKa0JDPFeM6TcEevkaW56sIWfx88OHbJKnCXdWscZx0l2WbCJ1wbg==");
        Utils.setValueInConfig("postgresql_connection_pool_size", "500");

        FeatureFlagTestContent.getInstance(process.getProcess())
                .setKeyValue(FeatureFlagTestContent.ENABLED_FEATURES, new EE_FEATURES[]{
                        EE_FEATURES.ACCOUNT_LINKING, EE_FEATURES.MULTI_TENANCY, EE_FEATURES.MFA, EE_FEATURES.DASHBOARD_LOGIN});
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));

        Thread memoryChecker = new Thread(() -> {
            while (memoryCheckRunning.get()) {
                Runtime rt = Runtime.getRuntime();
                long total_mem = rt.totalMemory();
                long free_mem = rt.freeMemory();
                long used_mem = total_mem - free_mem;

                if (used_mem > maxMemory.get()) {
                    maxMemory.set(used_mem);
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        memoryChecker.start();

        measureOperations(process.getProcess());

        memoryCheckRunning.set(false);
        memoryChecker.join();

        Thread.sleep(5000);

        Runtime rt = Runtime.getRuntime();
        long total_mem = rt.totalMemory();
        long free_mem = rt.freeMemory();
        long used_mem = total_mem - free_mem;

        System.out.println("Max memory used: " + (maxMemory.get() / (1024 * 1024)) + " MB");
        System.out.println("Current Memory user: " + (used_mem / (1024 * 1024)) + " MB");

        process.kill(false);
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
                    userRolesArr.get(0).getAsString().equals("admin") ||
                            userRolesArr.get(0).getAsString().equals("user")
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
//                            errorCount.incrementAndGet();
//                            throw new RuntimeException(e);
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
            System.out.println("EP sign up " + time);
            assert time < 15000;
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
            System.out.println("EP sign in " + time);
            assert time < 15000;
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
            System.out.println("PL sign up " + time);
            assert time < 5000;
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
            System.out.println("PL sign in " + time);
            assert time < 5000;
        }
        { // Thirdparty sign-ups
            System.out.println("Measure thirdparty sign-ups");
            long time = measureTime(() -> {
                ExecutorService es = Executors.newFixedThreadPool(100);
                for (int i = 0; i < 500; i++) {
                    int finalI = i;
                    es.execute(() -> {
                        try {
                            ThirdParty.signInUp(main, "twitter", "twitterid" + finalI,
                                    "twitter" + finalI + "@example.com");
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
            System.out.println("Thirdparty sign up " + time);
            assert time < 5000;
        }
        { // Thirdparty sign-ins
            System.out.println("Measure thirdparty sign-ins");
            long time = measureTime(() -> {
                ExecutorService es = Executors.newFixedThreadPool(100);
                for (int i = 0; i < 500; i++) {
                    int finalI = i;
                    es.execute(() -> {
                        try {
                            ThirdParty.signInUp(main, "twitter", "twitterid" + finalI,
                                    "twitter" + finalI + "@example.com");
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
            System.out.println("Thirdparty sign in " + time);
            assert time < 5000;
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
                        if (count >= 500) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
                return null;
            });
            System.out.println("User pagination " + time);
            assert time < 2000;
        }
        { // Measure update user metadata
            long time = measureTime(() -> {
                try {
                    UserPaginationContainer users = AuthRecipe.getUsers(main, 1, "ASC", null, null, null);
                    UserIdMapping.populateExternalUserIdForUsers(
                            new AppIdentifier(null, null),
                            (StorageLayer.getBaseStorage(main)),
                            users.users);

                    AuthRecipeUserInfo user = users.users[0];
                    for (int i = 0; i < 500; i++) {
                        UserMetadata.updateUserMetadata(main, user.getSupertokensOrExternalUserId(), new JsonObject());
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
                return null;
            });
            System.out.println("Update user metadata " + time);
            assert time < 3000;
        }

        { // measure user counting
            long time = measureTime(() -> {
                try {
                    AuthRecipe.getUsersCount(main, null);
                    AuthRecipe.getUsersCount(main, new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD});
                    AuthRecipe.getUsersCount(main, new RECIPE_ID[]{RECIPE_ID.EMAIL_PASSWORD, RECIPE_ID.THIRD_PARTY});
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
                return null;
            });
            System.out.println("User counting: " + time);
            assert time < 3000;
        }
        { // measure telemetry
            long time = measureTime(() -> {
                try {
                    FeatureFlag.getInstance(main).getPaidFeatureStats();
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    throw new RuntimeException(e);
                }
                return null;
            });
            System.out.println("Telemetry: " + time);
            assert time < 3000;
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
