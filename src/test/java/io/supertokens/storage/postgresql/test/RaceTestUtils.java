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

import io.supertokens.Main;
import io.supertokens.pluginInterface.RECIPE_ID;
import io.supertokens.pluginInterface.authRecipe.ACCOUNT_INFO_TYPE;
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;

import java.util.*;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;

/**
 * Utility class for race condition tests that need to directly query
 * the reservation tables (primary_user_tenants, recipe_user_tenants,
 * recipe_user_account_infos, app_id_to_user_id) to verify consistency
 * between user data and table reservations.
 */
public class RaceTestUtils {

    /**
     * Result of a consistency check between user data and reservation tables
     */
    public static class ConsistencyCheckResult {
        public final boolean isConsistent;
        public final List<String> issues;

        public ConsistencyCheckResult(boolean isConsistent, List<String> issues) {
            this.isConsistent = isConsistent;
            this.issues = issues;
        }

        @Override
        public String toString() {
            if (isConsistent) {
                return "ConsistencyCheckResult{CONSISTENT}";
            }
            return "ConsistencyCheckResult{INCONSISTENT, issues=" + issues + '}';
        }
    }

    /**
     * Backward-compatible delegate: checks only EMAIL reservations (I1-I3).
     */
    public static ConsistencyCheckResult checkEmailReservationConsistency(Main main, AuthRecipeUserInfo user)
            throws Exception {
        return checkReservationConsistency(main, user);
    }

    /**
     * Check complete consistency for a user object against all reservation tables.
     *
     * This method verifies the following invariants for ALL account_info_types
     * (EMAIL, PHONE_NUMBER, THIRD_PARTY):
     *
     * I1 (Primary Reservation Completeness): For every linked recipe user's account info,
     *     that info must be reserved in primary_user_tenants for the primary user.
     *
     * I2 (Primary Reservation Accuracy): Every entry in primary_user_tenants must
     *     correspond to an actual login method's account info (no orphaned reservations).
     *
     * I3 (Recipe Tables Consistency): recipe_user_tenants must match the login method's
     *     actual account info (no missing, mismatched, or orphaned entries).
     *
     * I4 (Recipe User Tenants Row Count): Every recipe user has the correct number of
     *     recipe_user_tenants rows per tenant (e.g., passwordless with both email+phone = 2).
     *
     * I5 (Recipe User Account Infos Row Count): Every recipe user has the correct number
     *     of recipe_user_account_infos rows (app-scoped).
     *
     * I6 (Time Joined Consistency): primary_or_recipe_user_time_joined in app_id_to_user_id
     *     is consistent for all rows sharing the same primary_or_recipe_user_id and equals
     *     MIN(time_joined) of the group.
     *
     * @param main The Main process
     * @param user The AuthRecipeUserInfo to check (from getUserById)
     * @return ConsistencyCheckResult indicating if the reservations are consistent
     */
    public static ConsistencyCheckResult checkReservationConsistency(Main main, AuthRecipeUserInfo user)
            throws Exception {
        List<String> issues = new ArrayList<>();

        if (user == null) {
            issues.add("User is null");
            return new ConsistencyCheckResult(false, issues);
        }

        String primaryUserId = user.getSupertokensUserId();
        boolean shouldCheckPrimaryUserTenants = user.isPrimaryUser;

        // For each tenant the user is in, check consistency
        for (String tenantId : user.tenantIds) {
            TenantIdentifier tenant = new TenantIdentifier(null, null, tenantId);

            // I1 + I2: Check primary_user_tenants for ALL account info types
            if (shouldCheckPrimaryUserTenants) {
                issues.addAll(checkPrimaryUserTenantsConsistency(main, tenant, user, ACCOUNT_INFO_TYPE.EMAIL));
                issues.addAll(checkPrimaryUserTenantsConsistency(main, tenant, user, ACCOUNT_INFO_TYPE.PHONE_NUMBER));
                issues.addAll(checkPrimaryUserTenantsConsistency(main, tenant, user, ACCOUNT_INFO_TYPE.THIRD_PARTY));
            }

            // I3 + I4: Check recipe_user_tenants for each login method in this tenant
            for (LoginMethod loginMethod : user.loginMethods) {
                if (loginMethod.tenantIds.contains(tenantId)) {
                    issues.addAll(checkRecipeUserTenantsConsistency(main, tenant, loginMethod));
                    issues.addAll(checkRecipeUserTenantsRowCount(main, tenant, loginMethod));
                }
            }
        }

        // I5: Check recipe_user_account_infos row count (app-scoped, not tenant-scoped)
        for (LoginMethod loginMethod : user.loginMethods) {
            issues.addAll(checkRecipeUserAccountInfosRowCount(main, loginMethod));
        }

        // I6: Check time_joined consistency in app_id_to_user_id
        issues.addAll(checkTimeJoinedConsistency(main, user));

        return new ConsistencyCheckResult(issues.isEmpty(), issues);
    }

    // ======================== I1 + I2: Primary User Tenants ========================

    /**
     * Check that primary_user_tenants exactly matches the linked recipe users for a tenant
     * and a given account_info_type.
     *
     * I1: All account info values from all login methods must be reserved.
     * I2: Each reserved value must correspond to an actual login method's value.
     */
    private static List<String> checkPrimaryUserTenantsConsistency(Main main, TenantIdentifier tenant,
                                                                    AuthRecipeUserInfo user,
                                                                    ACCOUNT_INFO_TYPE accountInfoType)
            throws Exception {
        List<String> issues = new ArrayList<>();
        String primaryUserId = user.getSupertokensUserId();

        // Get all reservations from primary_user_tenants for this type
        Set<String> reserved = getAllPrimaryUserReservations(main, tenant, primaryUserId, accountInfoType);

        // Build expected set from ALL login methods
        Set<String> expected = new HashSet<>();
        for (LoginMethod lm : user.loginMethods) {
            for (String value : getAccountInfoValues(lm, accountInfoType)) {
                expected.add(value);
            }
        }

        String typeName = accountInfoType.toString();

        // I1: Missing reservations
        for (String expectedValue : expected) {
            if (!reserved.contains(expectedValue)) {
                issues.add("MISSING PRIMARY RESERVATION (I1 violation): " + typeName + " '" + expectedValue +
                        "' is in user's login methods but NOT in primary_user_tenants for primary user " +
                        primaryUserId + " in tenant '" + tenant.getTenantId() +
                        "'. Reserved: " + reserved);
            }
        }

        // I2: Orphaned reservations
        for (String reservedValue : reserved) {
            if (!expected.contains(reservedValue)) {
                issues.add("ORPHANED PRIMARY RESERVATION (I2 violation): " + typeName + " '" + reservedValue +
                        "' is in primary_user_tenants for primary user " + primaryUserId +
                        " in tenant '" + tenant.getTenantId() +
                        "' but NOT in any login method. Expected: " + expected);
            }
        }

        return issues;
    }

    // ======================== I3: Recipe User Tenants Consistency ========================

    /**
     * Check that recipe_user_tenants matches the login method's actual account info.
     * Checks all account info types (email, phone, third_party).
     */
    private static List<String> checkRecipeUserTenantsConsistency(Main main, TenantIdentifier tenant,
                                                                   LoginMethod loginMethod) throws Exception {
        List<String> issues = new ArrayList<>();
        String recipeUserId = loginMethod.getSupertokensUserId();

        for (ACCOUNT_INFO_TYPE type : ACCOUNT_INFO_TYPE.values()) {
            Set<String> expectedValues = getAccountInfoValues(loginMethod, type);
            Set<String> reservedValues = getRecipeUserReservations(main, tenant, recipeUserId, type);

            String typeName = type.toString();

            // Missing
            for (String expected : expectedValues) {
                if (!reservedValues.contains(expected)) {
                    issues.add("MISSING RECIPE RESERVATION (I3 violation): Login method " + recipeUserId +
                            " has " + typeName + " '" + expected + "' in tenant '" + tenant.getTenantId() +
                            "' but NO reservation in recipe_user_tenants");
                }
            }

            // Orphaned
            for (String reserved : reservedValues) {
                if (!expectedValues.contains(reserved)) {
                    issues.add("ORPHANED RECIPE RESERVATION (I3 violation): Login method " + recipeUserId +
                            " has no matching " + typeName + " for value '" + reserved +
                            "' in recipe_user_tenants in tenant '" + tenant.getTenantId() + "'");
                }
            }
        }

        return issues;
    }

    // ======================== I4: Recipe User Tenants Row Count ========================

    /**
     * Check that recipe_user_tenants has the expected number of rows for a login method
     * in a given tenant. For example, a passwordless user with both email AND phone
     * should have 2 rows per tenant.
     */
    private static List<String> checkRecipeUserTenantsRowCount(Main main, TenantIdentifier tenant,
                                                                LoginMethod loginMethod) throws Exception {
        List<String> issues = new ArrayList<>();
        String recipeUserId = loginMethod.getSupertokensUserId();

        int expectedCount = countExpectedAccountInfos(loginMethod);
        int actualCount = getRecipeUserTenantsCount(main, tenant, recipeUserId);

        if (actualCount != expectedCount) {
            issues.add("WRONG ROW COUNT (I4 violation): Login method " + recipeUserId +
                    " has " + actualCount + " rows in recipe_user_tenants for tenant '" +
                    tenant.getTenantId() + "' but expected " + expectedCount);
        }

        return issues;
    }

    // ======================== I5: Recipe User Account Infos Row Count ========================

    /**
     * Check that recipe_user_account_infos has the expected number of rows for a login method.
     * This table is app-scoped (not tenant-scoped).
     */
    private static List<String> checkRecipeUserAccountInfosRowCount(Main main, LoginMethod loginMethod)
            throws Exception {
        List<String> issues = new ArrayList<>();
        String recipeUserId = loginMethod.getSupertokensUserId();

        int expectedCount = countExpectedAccountInfos(loginMethod);
        int actualCount = getRecipeUserAccountInfosCount(main, recipeUserId);

        if (actualCount != expectedCount) {
            issues.add("WRONG ROW COUNT (I5 violation): Login method " + recipeUserId +
                    " has " + actualCount + " rows in recipe_user_account_infos but expected " + expectedCount);
        }

        return issues;
    }

    // ======================== I6: Time Joined Consistency ========================

    /**
     * Check that primary_or_recipe_user_time_joined is consistent in app_id_to_user_id:
     * all rows sharing the same primary_or_recipe_user_id must have the same time_joined value,
     * and it must equal MIN(time_joined) of the group.
     */
    private static List<String> checkTimeJoinedConsistency(Main main, AuthRecipeUserInfo user) throws Exception {
        List<String> issues = new ArrayList<>();

        if (!user.isPrimaryUser || user.loginMethods.length <= 1) {
            return issues;
        }

        Start start = (Start) StorageLayer.getStorage(main);
        String table = Config.getConfig(start).getAppIdToUserIdTable();

        // Get all rows for users in this linked group
        String primaryUserId = user.getSupertokensUserId();
        List<String> userIds = new ArrayList<>();
        userIds.add(primaryUserId);
        for (LoginMethod lm : user.loginMethods) {
            if (!lm.getSupertokensUserId().equals(primaryUserId)) {
                userIds.add(lm.getSupertokensUserId());
            }
        }

        String placeholders = String.join(",", Collections.nCopies(userIds.size(), "?"));
        String QUERY = "SELECT user_id, primary_or_recipe_user_time_joined FROM " + table +
                " WHERE app_id = ? AND user_id IN (" + placeholders + ")";

        Map<String, Long> timeJoinedMap = execute(start, QUERY, pst -> {
            pst.setString(1, "public");
            for (int i = 0; i < userIds.size(); i++) {
                pst.setString(i + 2, userIds.get(i));
            }
        }, rs -> {
            Map<String, Long> map = new HashMap<>();
            while (rs.next()) {
                map.put(rs.getString("user_id").trim(), rs.getLong("primary_or_recipe_user_time_joined"));
            }
            return map;
        });

        // All should have the same primary_or_recipe_user_time_joined
        Set<Long> distinctTimeJoined = new HashSet<>(timeJoinedMap.values());
        if (distinctTimeJoined.size() > 1) {
            issues.add("INCONSISTENT TIME_JOINED (I6 violation): Users in linked group of primary " +
                    primaryUserId + " have different primary_or_recipe_user_time_joined values: " + timeJoinedMap);
        }

        // The time_joined should equal MIN(time_joined) from login methods
        long minTimeJoined = Long.MAX_VALUE;
        for (LoginMethod lm : user.loginMethods) {
            if (lm.timeJoined < minTimeJoined) {
                minTimeJoined = lm.timeJoined;
            }
        }

        for (Map.Entry<String, Long> entry : timeJoinedMap.entrySet()) {
            if (entry.getValue() != minTimeJoined) {
                issues.add("WRONG TIME_JOINED (I6 violation): User " + entry.getKey() +
                        " has primary_or_recipe_user_time_joined=" + entry.getValue() +
                        " but expected MIN(time_joined)=" + minTimeJoined +
                        " for primary user " + primaryUserId);
            }
        }

        return issues;
    }

    // ======================== Helper: Account Info Value Extraction ========================

    /**
     * Extract account info values from a LoginMethod for a given type.
     *
     * In the reservation tables:
     * - EMAIL type: account_info_value = the email address
     * - PHONE_NUMBER type: account_info_value = the phone number
     * - THIRD_PARTY type: account_info_value = "thirdPartyId::thirdPartyUserId"
     *
     * Note: For third-party users, the email is stored in a separate EMAIL row
     * with third_party_id and third_party_user_id fields populated.
     */
    private static Set<String> getAccountInfoValues(LoginMethod lm, ACCOUNT_INFO_TYPE type) {
        Set<String> values = new HashSet<>();
        switch (type) {
            case EMAIL:
                if (lm.email != null) {
                    values.add(lm.email);
                }
                break;
            case PHONE_NUMBER:
                if (lm.phoneNumber != null) {
                    values.add(lm.phoneNumber);
                }
                break;
            case THIRD_PARTY:
                if (lm.thirdParty != null) {
                    // THIRD_PARTY rows store "id::userId" as account_info_value
                    values.add(lm.thirdParty.getAccountInfoValue());
                }
                break;
        }
        return values;
    }

    /**
     * Count the expected number of account info entries for a login method.
     * Third-party users have 2 rows: one EMAIL + one THIRD_PARTY.
     * Passwordless users may have 1 (email or phone) or 2 (email + phone).
     * EmailPassword users have 1 (email).
     */
    private static int countExpectedAccountInfos(LoginMethod lm) {
        int count = 0;
        if (lm.email != null) {
            count++;
        }
        if (lm.phoneNumber != null) {
            count++;
        }
        if (lm.thirdParty != null) {
            // Third party has its own row (type=tparty) in addition to email row
            count++;
        }
        return count;
    }

    // ======================== SQL Query Helpers ========================

    /**
     * Get all reservations for a primary user in a tenant from primary_user_tenants table
     * for a given account_info_type.
     */
    public static Set<String> getAllPrimaryUserReservations(Main main, TenantIdentifier tenant,
                                                            String primaryUserId, ACCOUNT_INFO_TYPE type)
            throws Exception {
        Start start = (Start) StorageLayer.getStorage(main);
        String tableName = Config.getConfig(start).getPrimaryUserTenantsTable();

        String QUERY = "SELECT account_info_value FROM " + tableName +
                " WHERE app_id = ? AND tenant_id = ? AND primary_user_id = ? AND account_info_type = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenant.getAppId());
            pst.setString(2, tenant.getTenantId());
            pst.setString(3, primaryUserId);
            pst.setString(4, type.toString());
        }, rs -> {
            Set<String> values = new HashSet<>();
            while (rs.next()) {
                values.add(rs.getString("account_info_value"));
            }
            return values;
        });
    }

    /**
     * Get all reservations for a recipe user in a tenant from recipe_user_tenants table
     * for a given account_info_type.
     */
    public static Set<String> getRecipeUserReservations(Main main, TenantIdentifier tenant,
                                                         String recipeUserId, ACCOUNT_INFO_TYPE type)
            throws Exception {
        Start start = (Start) StorageLayer.getStorage(main);
        String tableName = Config.getConfig(start).getRecipeUserTenantsTable();

        String QUERY = "SELECT account_info_value FROM " + tableName +
                " WHERE app_id = ? AND tenant_id = ? AND recipe_user_id = ? AND account_info_type = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenant.getAppId());
            pst.setString(2, tenant.getTenantId());
            pst.setString(3, recipeUserId);
            pst.setString(4, type.toString());
        }, rs -> {
            Set<String> values = new HashSet<>();
            while (rs.next()) {
                values.add(rs.getString("account_info_value"));
            }
            return values;
        });
    }

    /**
     * Get total row count in recipe_user_tenants for a recipe user in a tenant.
     */
    private static int getRecipeUserTenantsCount(Main main, TenantIdentifier tenant, String recipeUserId)
            throws Exception {
        Start start = (Start) StorageLayer.getStorage(main);
        String tableName = Config.getConfig(start).getRecipeUserTenantsTable();

        String QUERY = "SELECT COUNT(*) as cnt FROM " + tableName +
                " WHERE app_id = ? AND tenant_id = ? AND recipe_user_id = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenant.getAppId());
            pst.setString(2, tenant.getTenantId());
            pst.setString(3, recipeUserId);
        }, rs -> {
            rs.next();
            return rs.getInt("cnt");
        });
    }

    /**
     * Get total row count in recipe_user_account_infos for a recipe user.
     */
    private static int getRecipeUserAccountInfosCount(Main main, String recipeUserId) throws Exception {
        Start start = (Start) StorageLayer.getStorage(main);
        String tableName = Config.getConfig(start).getRecipeUserAccountInfosTable();

        String QUERY = "SELECT COUNT(*) as cnt FROM " + tableName +
                " WHERE app_id = ? AND recipe_user_id = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, "public");
            pst.setString(2, recipeUserId);
        }, rs -> {
            rs.next();
            return rs.getInt("cnt");
        });
    }

    // ======================== Backward-compatible public helpers ========================

    /**
     * Get all email reservations for a primary user in a tenant from primary_user_tenants table.
     * Backward-compatible delegate to the generalized method.
     */
    public static Set<String> getAllPrimaryUserEmailReservations(Main main, TenantIdentifier tenant,
                                                                 String primaryUserId)
            throws Exception {
        return getAllPrimaryUserReservations(main, tenant, primaryUserId, ACCOUNT_INFO_TYPE.EMAIL);
    }

    /**
     * Get recipe user email reservation for a specific user in a tenant from recipe_user_tenants table.
     * Backward-compatible delegate.
     */
    public static String getRecipeUserEmailReservation(Main main, TenantIdentifier tenant, String recipeUserId)
            throws Exception {
        Set<String> emails = getRecipeUserReservations(main, tenant, recipeUserId, ACCOUNT_INFO_TYPE.EMAIL);
        return emails.isEmpty() ? null : emails.iterator().next();
    }

    // ======================== Debug Utilities ========================

    /**
     * Print all reservations for a user for debugging purposes
     */
    public static void printAllReservations(Main main, AuthRecipeUserInfo user) throws Exception {
        if (user == null) {
            System.out.println("=== Cannot print reservations: user is null ===");
            return;
        }

        String primaryUserId = user.getSupertokensUserId();
        System.out.println("=== Reservations for user " + primaryUserId + " ===");
        System.out.println("User tenants: " + user.tenantIds);
        System.out.println("Login methods:");
        for (LoginMethod lm : user.loginMethods) {
            System.out.println("  - " + lm.getSupertokensUserId() + ": email=" + lm.email +
                    ", phone=" + lm.phoneNumber +
                    ", thirdParty=" + (lm.thirdParty != null ? lm.thirdParty.id + "|" + lm.thirdParty.userId : "null") +
                    ", tenants=" + lm.tenantIds);
        }

        for (String tenantId : user.tenantIds) {
            TenantIdentifier tenant = new TenantIdentifier(null, null, tenantId);
            System.out.println("\nTenant: " + tenantId);

            // Primary user reservations
            if (user.isPrimaryUser) {
                for (ACCOUNT_INFO_TYPE type : ACCOUNT_INFO_TYPE.values()) {
                    Set<String> values = getAllPrimaryUserReservations(main, tenant, primaryUserId, type);
                    if (!values.isEmpty()) {
                        System.out.println("  primary_user_tenants " + type + ": " + values);
                    }
                }
            }

            // Recipe user reservations for each login method
            for (LoginMethod lm : user.loginMethods) {
                if (lm.tenantIds.contains(tenantId)) {
                    for (ACCOUNT_INFO_TYPE type : ACCOUNT_INFO_TYPE.values()) {
                        Set<String> values = getRecipeUserReservations(main, tenant, lm.getSupertokensUserId(), type);
                        if (!values.isEmpty()) {
                            System.out.println("  recipe_user_tenants for " + lm.getSupertokensUserId() +
                                    " " + type + ": " + values);
                        }
                    }
                }
            }
        }
        System.out.println("========================================");
    }
}
