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
import io.supertokens.pluginInterface.authRecipe.AuthRecipeUserInfo;
import io.supertokens.pluginInterface.authRecipe.LoginMethod;
import io.supertokens.pluginInterface.multitenancy.TenantIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storageLayer.StorageLayer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;

/**
 * Utility class for race condition tests that need to directly query
 * the reservation tables (primary_user_tenants, recipe_user_tenants)
 * to verify consistency between user data and table reservations.
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
     * Check complete consistency for a user object against reservation tables.
     *
     * This method verifies the following invariants (for email account_info_type only):
     *
     * I1 (Primary Reservation Completeness): For every linked recipe user's email,
     *     that email must be reserved in primary_user_tenants for the primary user.
     *
     * I2 (Primary Reservation Accuracy): Every email in primary_user_tenants must
     *     correspond to an actual login method's email (no orphaned reservations).
     *
     * I3 (Recipe Tables Consistency): recipe_user_tenants must match the login method's
     *     actual email (no missing, mismatched, or orphaned entries).
     *
     * Note: This method does NOT verify:
     * - Phone numbers or third-party identities (only emails)
     * - I4 (Recipe User Uniqueness) - same email across different recipe users
     * - recipe_user_account_infos table consistency
     *
     * @param main The Main process
     * @param user The AuthRecipeUserInfo to check (from getUserById)
     * @return ConsistencyCheckResult indicating if the reservations are consistent
     */
    public static ConsistencyCheckResult checkEmailReservationConsistency(Main main, AuthRecipeUserInfo user)
            throws Exception {
        List<String> issues = new ArrayList<>();

        if (user == null) {
            issues.add("User is null");
            return new ConsistencyCheckResult(false, issues);
        }

        String primaryUserId = user.getSupertokensUserId();

        // Check primary_user_tenants if user is a primary user.
        // Note: When querying by a linked recipe user's ID, getUserById returns the primary user,
        // so isPrimaryUser will be true. loginMethods.length > 1 is a redundant check since
        // multiple login methods implies linking which requires a primary user.
        boolean shouldCheckPrimaryUserTenants = user.isPrimaryUser;

        // For each tenant the user is in, check consistency
        for (String tenantId : user.tenantIds) {
            TenantIdentifier tenant = new TenantIdentifier(null, null, tenantId);

            // 1. Check primary_user_tenants if user is a primary user (verifies I1 and I2)
            if (shouldCheckPrimaryUserTenants) {
                List<String> primaryIssues = checkPrimaryUserTenantsConsistency(main, tenant, user);
                issues.addAll(primaryIssues);
            }

            // 2. Check recipe_user_tenants for each login method in this tenant (verifies I3)
            for (LoginMethod loginMethod : user.loginMethods) {
                if (loginMethod.tenantIds.contains(tenantId)) {
                    List<String> recipeIssues = checkRecipeUserTenantsConsistency(main, tenant, loginMethod);
                    issues.addAll(recipeIssues);
                }
            }
        }

        return new ConsistencyCheckResult(issues.isEmpty(), issues);
    }

    /**
     * Check that primary_user_tenants exactly matches the linked recipe users for a tenant.
     *
     * Verifies I1 (Primary Reservation Completeness) and I2 (Primary Reservation Accuracy):
     * - I1: All emails from all login methods must be reserved in this tenant
     * - I2: Each reserved email must correspond to some login method's email in the linked group
     *
     * IMPORTANT: The implementation reserves ALL emails from ALL login methods in ALL tenants
     * where ANY linked user exists. This is intentional to prevent identity conflicts:
     * - If P1 links R1, and P1 is only in tenant1 but R1 is in tenant1+tenant2
     * - P1's email must be reserved in tenant2 too, even though P1's login method isn't there
     * - Otherwise another primary could claim P1's email in tenant2, creating a conflict
     */
    private static List<String> checkPrimaryUserTenantsConsistency(Main main, TenantIdentifier tenant,
                                                                    AuthRecipeUserInfo user) throws Exception {
        List<String> issues = new ArrayList<>();
        String primaryUserId = user.getSupertokensUserId();

        // Get all email reservations from primary_user_tenants for this primary user in this tenant
        Set<String> reservedEmails = getAllPrimaryUserEmailReservations(main, tenant, primaryUserId);

        // Build expected set of emails from ALL login methods (not filtered by tenant).
        // The implementation reserves all emails in all tenants where any linked user exists,
        // to prevent identity conflicts across the linked group.
        Set<String> expectedEmails = new HashSet<>();
        for (LoginMethod lm : user.loginMethods) {
            if (lm.email != null) {
                expectedEmails.add(lm.email);
            }
        }

        // Check for missing reservations (emails in user but not in table) - I1 violation
        for (String expectedEmail : expectedEmails) {
            if (!reservedEmails.contains(expectedEmail)) {
                issues.add("MISSING PRIMARY RESERVATION (I1 violation): Email '" + expectedEmail +
                        "' is in user's login methods but NOT in primary_user_tenants for primary user " +
                        primaryUserId + " in tenant '" + tenant.getTenantId() +
                        "'. Reserved emails: " + reservedEmails);
            }
        }

        // Check for orphaned reservations (emails in table but not in any login method) - I2 violation
        for (String reservedEmail : reservedEmails) {
            if (!expectedEmails.contains(reservedEmail)) {
                issues.add("ORPHANED PRIMARY RESERVATION (I2 violation): Email '" + reservedEmail +
                        "' is in primary_user_tenants for primary user " + primaryUserId +
                        " in tenant '" + tenant.getTenantId() +
                        "' but NOT in any login method. Expected emails: " + expectedEmails);
            }
        }

        return issues;
    }

    /**
     * Check that recipe_user_tenants exactly matches the login method's email for a tenant.
     *
     * Verifies I3 (Recipe Tables Consistency) for the email account_info_type:
     * - If login method has email, recipe_user_tenants must have matching entry
     * - If login method has no email, recipe_user_tenants must NOT have an email entry (orphan check)
     */
    private static List<String> checkRecipeUserTenantsConsistency(Main main, TenantIdentifier tenant,
                                                                   LoginMethod loginMethod) throws Exception {
        List<String> issues = new ArrayList<>();
        String recipeUserId = loginMethod.getSupertokensUserId();
        String expectedEmail = loginMethod.email;

        // Get email reservation from recipe_user_tenants
        String reservedEmail = getRecipeUserEmailReservation(main, tenant, recipeUserId);

        if (expectedEmail != null) {
            if (reservedEmail == null) {
                issues.add("MISSING RECIPE RESERVATION (I3 violation): Login method " + recipeUserId +
                        " has email '" + expectedEmail + "' in tenant '" + tenant.getTenantId() +
                        "' but NO reservation in recipe_user_tenants");
            } else if (!reservedEmail.equals(expectedEmail)) {
                issues.add("MISMATCHED RECIPE RESERVATION (I3 violation): Login method " + recipeUserId +
                        " has email '" + expectedEmail + "' but recipe_user_tenants has '" +
                        reservedEmail + "' in tenant '" + tenant.getTenantId() + "'");
            }
        } else {
            // Login method has no email - check for orphaned entries
            if (reservedEmail != null) {
                issues.add("ORPHANED RECIPE RESERVATION (I3 violation): Login method " + recipeUserId +
                        " has NO email but recipe_user_tenants has '" + reservedEmail +
                        "' in tenant '" + tenant.getTenantId() + "'");
            }
        }

        return issues;
    }

    /**
     * Get all email reservations for a primary user in a tenant from primary_user_tenants table
     */
    public static Set<String> getAllPrimaryUserEmailReservations(Main main, TenantIdentifier tenant, String primaryUserId)
            throws Exception {
        Start start = (Start) StorageLayer.getStorage(main);
        String tableName = Config.getConfig(start).getPrimaryUserTenantsTable();

        String QUERY = "SELECT account_info_value FROM " + tableName +
                " WHERE app_id = ? AND tenant_id = ? AND primary_user_id = ? AND account_info_type = 'email'";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenant.getAppId());
            pst.setString(2, tenant.getTenantId());
            pst.setString(3, primaryUserId);
        }, rs -> {
            Set<String> emails = new HashSet<>();
            while (rs.next()) {
                emails.add(rs.getString("account_info_value"));
            }
            return emails;
        });
    }

    /**
     * Get recipe user email reservation for a specific user in a tenant from recipe_user_tenants table
     */
    public static String getRecipeUserEmailReservation(Main main, TenantIdentifier tenant, String recipeUserId)
            throws Exception {
        Start start = (Start) StorageLayer.getStorage(main);
        String tableName = Config.getConfig(start).getRecipeUserTenantsTable();

        String QUERY = "SELECT account_info_value FROM " + tableName +
                " WHERE app_id = ? AND tenant_id = ? AND recipe_user_id = ? AND account_info_type = 'email'";

        return execute(start, QUERY, pst -> {
            pst.setString(1, tenant.getAppId());
            pst.setString(2, tenant.getTenantId());
            pst.setString(3, recipeUserId);
        }, rs -> {
            if (rs.next()) {
                return rs.getString("account_info_value");
            }
            return null;
        });
    }

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
                    ", tenants=" + lm.tenantIds);
        }

        for (String tenantId : user.tenantIds) {
            TenantIdentifier tenant = new TenantIdentifier(null, null, tenantId);
            System.out.println("\nTenant: " + tenantId);

            // Primary user reservations
            Set<String> primaryEmails = getAllPrimaryUserEmailReservations(main, tenant, primaryUserId);
            System.out.println("  primary_user_tenants emails: " + primaryEmails);

            // Recipe user reservations for each login method
            for (LoginMethod lm : user.loginMethods) {
                if (lm.tenantIds.contains(tenantId)) {
                    String recipeEmail = getRecipeUserEmailReservation(main, tenant, lm.getSupertokensUserId());
                    System.out.println("  recipe_user_tenants for " + lm.getSupertokensUserId() + ": " + recipeEmail);
                }
            }
        }
        System.out.println("========================================");
    }
}
