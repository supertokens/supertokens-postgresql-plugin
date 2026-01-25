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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
     * This method verifies:
     * 1. If the user is a primary user (or linked), check that primary_user_tenants
     *    exactly matches the emails of all linked login methods for each tenant
     * 2. Check that recipe_user_tenants exactly matches the email for each login method
     *    in each tenant the login method belongs to
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
        boolean isPrimaryOrLinked = user.isPrimaryUser || user.loginMethods.length > 1;

        // For each tenant the user is in, check consistency
        for (String tenantId : user.tenantIds) {
            TenantIdentifier tenant = new TenantIdentifier(null, null, tenantId);

            // 1. Check primary_user_tenants if user is primary/linked
            if (isPrimaryOrLinked) {
                List<String> primaryIssues = checkPrimaryUserTenantsConsistency(main, tenant, user);
                issues.addAll(primaryIssues);
            }

            // 2. Check recipe_user_tenants for each login method in this tenant
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
     * Expected: For each login method in the user that belongs to this tenant,
     * if the login method has an email, that email should be reserved in primary_user_tenants.
     */
    private static List<String> checkPrimaryUserTenantsConsistency(Main main, TenantIdentifier tenant,
                                                                    AuthRecipeUserInfo user) throws Exception {
        List<String> issues = new ArrayList<>();
        String primaryUserId = user.getSupertokensUserId();

        // Get all email reservations from primary_user_tenants for this primary user in this tenant
        Set<String> reservedEmails = getAllPrimaryUserEmailReservations(main, tenant, primaryUserId);

        // Build expected set of emails from login methods in this tenant
        Set<String> expectedEmails = new HashSet<>();
        for (LoginMethod lm : user.loginMethods) {
            if (lm.tenantIds.contains(tenant.getTenantId()) && lm.email != null) {
                expectedEmails.add(lm.email);
            }
        }

        // Check for missing reservations (emails in user but not in table)
        for (String expectedEmail : expectedEmails) {
            if (!reservedEmails.contains(expectedEmail)) {
                issues.add("MISSING PRIMARY RESERVATION: Email '" + expectedEmail +
                        "' is in user's login methods for tenant '" + tenant.getTenantId() +
                        "' but NOT in primary_user_tenants for primary user " + primaryUserId +
                        ". Reserved emails: " + reservedEmails);
            }
        }

        // Check for orphaned reservations (emails in table but not in user)
        for (String reservedEmail : reservedEmails) {
            if (!expectedEmails.contains(reservedEmail)) {
                issues.add("ORPHANED PRIMARY RESERVATION: Email '" + reservedEmail +
                        "' is in primary_user_tenants for primary user " + primaryUserId +
                        " in tenant '" + tenant.getTenantId() +
                        "' but NOT in user's login methods. Expected emails: " + expectedEmails);
            }
        }

        return issues;
    }

    /**
     * Check that recipe_user_tenants exactly matches the login method's email for a tenant.
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
                issues.add("MISSING RECIPE RESERVATION: Login method " + recipeUserId +
                        " has email '" + expectedEmail + "' in tenant '" + tenant.getTenantId() +
                        "' but NO reservation in recipe_user_tenants");
            } else if (!reservedEmail.equals(expectedEmail)) {
                issues.add("MISMATCHED RECIPE RESERVATION: Login method " + recipeUserId +
                        " has email '" + expectedEmail + "' but recipe_user_tenants has '" +
                        reservedEmail + "' in tenant '" + tenant.getTenantId() + "'");
            }
        } else {
            // Login method has no email - recipe_user_tenants might still have phone number etc.
            // We only check email consistency here
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
