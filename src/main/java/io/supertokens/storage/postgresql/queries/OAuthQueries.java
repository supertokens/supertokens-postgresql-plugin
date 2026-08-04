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

package io.supertokens.storage.postgresql.queries;

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.pluginInterface.oauth.OAuthClient;
import io.supertokens.pluginInterface.oauth.OAuthLogoutChallenge;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;


public class OAuthQueries {

    public static String getQueryToCreateOAuthClientTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String oAuth2ClientTable = Config.getConfig(start).getOAuthClientsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + oAuth2ClientTable + " ("
                + "app_id VARCHAR(64),"
                + "client_id VARCHAR(255) NOT NULL,"
                + "client_secret TEXT,"
                + "enable_refresh_token_rotation BOOLEAN NOT NULL,"
                + "is_client_credentials_only BOOLEAN NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientTable, null, "pkey")
                + " PRIMARY KEY (app_id, client_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientTable, "app_id", "fkey")
                + " FOREIGN KEY(app_id) REFERENCES " + Config.getConfig(start).getAppsTable() + "(app_id) ON DELETE CASCADE "
                + ");";
        // @formatter:on
    }

    public static String getQueryToCreateOAuthSessionsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String oAuthSessionsTable = Config.getConfig(start).getOAuthSessionsTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + oAuthSessionsTable + " ("
                + "gid VARCHAR(255)," // needed for instrospect. It's much easier to find these records if we have a gid
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "client_id VARCHAR(255) NOT NULL,"
                + "session_handle VARCHAR(128),"
                + "external_refresh_token VARCHAR(255) UNIQUE,"
                + "internal_refresh_token VARCHAR(255) UNIQUE,"
                + "jti TEXT NOT NULL," // comma separated jti list
                + "exp BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuthSessionsTable, null, "pkey")
                + " PRIMARY KEY (gid),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuthSessionsTable, "client_id", "fkey")
                + " FOREIGN KEY(app_id, client_id) REFERENCES " + Config.getConfig(start).getOAuthClientsTable() + "(app_id, client_id) ON DELETE CASCADE);";
        // @formatter:on
    }

    public static String getQueryToCreateOAuthSessionsExpIndex(Start start) {
        String oAuth2SessionTable = Config.getConfig(start).getOAuthSessionsTable();
        return "CREATE INDEX IF NOT EXISTS oauth_session_exp_index ON "
                + oAuth2SessionTable + "(exp DESC);";
    }

    public static String getQueryToCreateOAuthSessionsExternalRefreshTokenIndex(Start start) {
        String oAuth2SessionTable = Config.getConfig(start).getOAuthSessionsTable();
        return "CREATE INDEX IF NOT EXISTS oauth_session_external_refresh_token_index ON "
                + oAuth2SessionTable + "(app_id, external_refresh_token DESC);";
    }

    public static String getQueryToCreateOAuthM2MTokensTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String oAuth2M2MTokensTable = Config.getConfig(start).getOAuthM2MTokensTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + oAuth2M2MTokensTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "client_id VARCHAR(255) NOT NULL,"
                + "iat BIGINT NOT NULL,"
                + "exp BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2M2MTokensTable, null, "pkey")
                + " PRIMARY KEY (app_id, client_id, iat),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2M2MTokensTable, "client_id", "fkey")
                + " FOREIGN KEY(app_id, client_id)"
                + " REFERENCES " + Config.getConfig(start).getOAuthClientsTable() + "(app_id, client_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static String getQueryToCreateOAuthM2MTokenIatIndex(Start start) {
        String oAuth2M2MTokensTable = Config.getConfig(start).getOAuthM2MTokensTable();
        return "CREATE INDEX IF NOT EXISTS oauth_m2m_token_iat_index ON "
                + oAuth2M2MTokensTable + "(iat DESC, app_id DESC);";
    }

    public static String getQueryToCreateOAuthM2MTokenExpIndex(Start start) {
        String oAuth2M2MTokensTable = Config.getConfig(start).getOAuthM2MTokensTable();
        return "CREATE INDEX IF NOT EXISTS oauth_m2m_token_exp_index ON "
                + oAuth2M2MTokensTable + "(exp DESC);";
    }

    public static String getQueryToCreateOAuthM2MTokenStatsTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String statsTable = Config.getConfig(start).getOAuthM2MTokenStatsTable();
        // Rollup counters replacing the per-token oauth_m2m_tokens rows: one row per
        // (app_id, iat hour-bucket, exp hour-bucket) holding how many tokens issued in that iat
        // hour expire in that exp hour. With one uniform token TTL an iat hour maps to ~1-3 exp
        // hours, so the 30-day window holds ~750-2,000 rows per app regardless of issuance volume.
        // client_id is intentionally not kept: neither stat is per-client, and dropping it collapses
        // an app's issuance onto a single hot counter row per (iat_bucket, exp_bucket).
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + statsTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "iat_bucket BIGINT NOT NULL,"
                + "exp_bucket BIGINT NOT NULL,"
                + "count BIGINT NOT NULL DEFAULT 0,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, statsTable, null, "pkey")
                + " PRIMARY KEY (app_id, iat_bucket, exp_bucket)"
                + ");";
        // @formatter:on
    }

    // Supports countTotalNumberOfOAuthM2MTokensAlive's (app_id, exp_bucket > now) range as an index
    // scan; the PK's (app_id, iat_bucket) prefix already serves the created-since range.
    public static String getQueryToCreateOAuthM2MTokenStatsExpBucketIndex(Start start) {
        String statsTable = Config.getConfig(start).getOAuthM2MTokenStatsTable();
        return "CREATE INDEX IF NOT EXISTS oauth_m2m_token_stats_exp_bucket_index ON "
                + statsTable + "(app_id, exp_bucket);";
    }

    // One-shot transition: bucket whatever per-token rows the legacy oauth_m2m_tokens table still
    // holds into the rollup. Emitted only in the DDL batch that first creates the rollup table (see
    // GeneralQueries.createTablesIfNotExists), so seed and table commit atomically -> it runs exactly
    // once. Cheap: the legacy table is capped by the 31-day retention cron. After this, new tokens go
    // only to the rollup and the legacy table drains to empty within 31 days. Idempotent via ON
    // CONFLICT should it ever be re-attempted.
    public static String getQueryToBackfillOAuthM2MTokenStatsFromLegacy(Start start) {
        String statsTable = Config.getConfig(start).getOAuthM2MTokenStatsTable();
        String legacyTable = Config.getConfig(start).getOAuthM2MTokensTable();
        return "INSERT INTO " + statsTable + " (app_id, iat_bucket, exp_bucket, count)"
                + " SELECT app_id, iat / 3600, exp / 3600, COUNT(*)"
                + " FROM " + legacyTable
                + " GROUP BY app_id, iat / 3600, exp / 3600"
                // Qualify the existing-row count with the table name: an unqualified `count` in a DO
                // UPDATE SET is ambiguous between the target row and the EXCLUDED pseudo-row (both carry
                // a `count` column), which Postgres rejects.
                + " ON CONFLICT (app_id, iat_bucket, exp_bucket) DO UPDATE SET count = "
                + statsTable + ".count + EXCLUDED.count;";
    }

    public static String getQueryToCreateOAuthLogoutChallengesTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String oAuth2LogoutChallengesTable = Config.getConfig(start).getOAuthLogoutChallengesTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + oAuth2LogoutChallengesTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "challenge VARCHAR(128) NOT NULL,"
                + "client_id VARCHAR(255) NOT NULL,"
                + "post_logout_redirect_uri VARCHAR(1024),"
                + "session_handle VARCHAR(128),"
                + "state VARCHAR(128),"
                + "time_created BIGINT NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2LogoutChallengesTable, null, "pkey")
                + " PRIMARY KEY (app_id, challenge),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2LogoutChallengesTable, "client_id", "fkey")
                + " FOREIGN KEY(app_id, client_id)"
                + " REFERENCES " + Config.getConfig(start).getOAuthClientsTable() + "(app_id, client_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static String getQueryToCreateOAuthLogoutChallengesTimeCreatedIndex(Start start) {
        String oAuth2LogoutChallengesTable = Config.getConfig(start).getOAuthLogoutChallengesTable();
        return "CREATE INDEX IF NOT EXISTS oauth_logout_challenges_time_created_index ON "
                + oAuth2LogoutChallengesTable + "(time_created DESC);";
    }

    public static OAuthClient getOAuthClientById(Start start, String clientId, AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT client_secret, is_client_credentials_only, enable_refresh_token_rotation FROM " + Config.getConfig(start).getOAuthClientsTable() +
            " WHERE client_id = ? AND app_id = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, clientId);
            pst.setString(2, appIdentifier.getAppId());
        }, (result) -> {
            if (result.next()) {
                return new OAuthClient(clientId, result.getString("client_secret"), result.getBoolean("is_client_credentials_only"), result.getBoolean("enable_refresh_token_rotation"));
            }
            return null;
        });
    }

    public static void createOrUpdateOAuthSession(Start start, AppIdentifier appIdentifier, @NotNull String gid, @NotNull String clientId,
                                                  String externalRefreshToken, String internalRefreshToken, String sessionHandle,
                                                  String jti, long exp)
            throws SQLException, StorageQueryException {
        String sessionTable = Config.getConfig(start).getOAuthSessionsTable();
        String QUERY = "INSERT INTO " + sessionTable +
                " (gid, client_id, app_id, external_refresh_token, internal_refresh_token, session_handle, jti, exp) VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (gid) DO UPDATE SET external_refresh_token = ?, internal_refresh_token = ?, " +
                "session_handle = ? , jti = CONCAT("+sessionTable+".jti, ?), exp = ?";
        update(start, QUERY, pst -> {
            String jtiToInsert = jti + ",";

            pst.setString(1, gid);
            pst.setString(2, clientId);
            pst.setString(3, appIdentifier.getAppId());
            pst.setString(4, externalRefreshToken);
            pst.setString(5, internalRefreshToken);
            pst.setString(6, sessionHandle);
            pst.setString(7, jtiToInsert); //the starting list element also has to have a "," at the end as the remove removes "jti + ,"
            pst.setLong(8, exp);

            pst.setString(9, externalRefreshToken);
            pst.setString(10, internalRefreshToken);
            pst.setString(11, sessionHandle);
            pst.setString(12, jtiToInsert);
            pst.setLong(13, exp);
        });
    }

    public static List<OAuthClient> getOAuthClients(Start start, AppIdentifier appIdentifier, List<String> clientIds)
            throws SQLException, StorageQueryException {
        if(clientIds.isEmpty()){
            return Collections.emptyList();
        }
        String QUERY = "SELECT * FROM " + Config.getConfig(start).getOAuthClientsTable()
                + " WHERE app_id = ? AND client_id IN ( "
                + Utils.generateCommaSeperatedQuestionMarks(clientIds.size())
                + " );";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            for (int i = 0; i < clientIds.size(); i++) {
                pst.setString(i + 2, clientIds.get(i));
            }
        }, (result) -> {
            List<OAuthClient> res = new ArrayList<>();
            while (result.next()) {
                res.add(new OAuthClient(result.getString("client_id"), result.getString("client_secret"), result.getBoolean("is_client_credentials_only"), result.getBoolean("enable_refresh_token_rotation")));
            }
            return res;
        });
    }

    public static void addOrUpdateOauthClient(Start start, AppIdentifier appIdentifier, String clientId, String clientSecret,
                                            boolean isClientCredentialsOnly, boolean enableRefreshTokenRotation)
            throws SQLException, StorageQueryException {
        String INSERT = "INSERT INTO " + Config.getConfig(start).getOAuthClientsTable()
                + "(app_id, client_id, client_secret, is_client_credentials_only, enable_refresh_token_rotation) VALUES(?, ?, ?, ?, ?) "
                + "ON CONFLICT (app_id, client_id) DO UPDATE SET client_secret = ?, is_client_credentials_only = ?, enable_refresh_token_rotation = ?";
        update(start, INSERT, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, clientId);
            pst.setString(3, clientSecret);
            pst.setBoolean(4, isClientCredentialsOnly);
            pst.setBoolean(5, enableRefreshTokenRotation);
            pst.setString(6, clientSecret);
            pst.setBoolean(7, isClientCredentialsOnly);
            pst.setBoolean(8, enableRefreshTokenRotation);
        });
    }

    public static boolean deleteOAuthClient(Start start, String clientId, AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        String DELETE = "DELETE FROM " + Config.getConfig(start).getOAuthClientsTable()
                + " WHERE app_id = ? AND client_id = ?";
        int numberOfRow = update(start, DELETE, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, clientId);
        });
        return numberOfRow > 0;
    }

    public static boolean deleteOAuthSessionByGID(Start start, AppIdentifier appIdentifier, String gid)
            throws SQLException, StorageQueryException {
        String DELETE = "DELETE FROM " + Config.getConfig(start).getOAuthSessionsTable()
                        + " WHERE gid = ? and app_id = ?;";
        int numberOfRows = update(start, DELETE, pst -> {
            pst.setString(1, gid);
            pst.setString(2, appIdentifier.getAppId());
        });
        return numberOfRows > 0;
    }

    public static boolean deleteOAuthSessionByClientId(Start start, AppIdentifier appIdentifier, String clientId)
            throws SQLException, StorageQueryException {
        String DELETE = "DELETE FROM " + Config.getConfig(start).getOAuthSessionsTable()
                + " WHERE app_id = ? and client_id = ?;";
        int numberOfRows = update(start, DELETE, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, clientId);
        });
        return numberOfRows > 0;
    }

    public static boolean deleteOAuthSessionBySessionHandle(Start start, AppIdentifier appIdentifier, String sessionHandle)
            throws SQLException, StorageQueryException {
        String DELETE = "DELETE FROM " + Config.getConfig(start).getOAuthSessionsTable()
                + " WHERE app_id = ? and session_handle = ?";
        int numberOfRows = update(start, DELETE, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, sessionHandle);
        });
        return numberOfRows > 0;
    }

    public static boolean deleteJTIFromOAuthSession(Start start, AppIdentifier appIdentifier, String gid, String jti)
            throws SQLException, StorageQueryException {
        //jti is a comma separated list. When deleting a jti, just have to delete from the list
        String DELETE = "UPDATE " + Config.getConfig(start).getOAuthSessionsTable()
                + " SET jti = REPLACE(jti, ?, '')" // deletion means replacing the jti with empty char
                + " WHERE app_id = ? and gid = ?";
        int numberOfRows = update(start, DELETE, pst -> {
            pst.setString(1, jti + ","); //removing with the "," to not leave behind trash
            pst.setString(2, appIdentifier.getAppId());
            pst.setString(3, gid);
        });
        return numberOfRows > 0;
    }

    public static int countTotalNumberOfClients(Start start, AppIdentifier appIdentifier,
            boolean filterByClientCredentialsOnly) throws SQLException, StorageQueryException {
        if (filterByClientCredentialsOnly) {
            String QUERY = "SELECT COUNT(*) as c FROM " + Config.getConfig(start).getOAuthClientsTable() +
                    " WHERE app_id = ? AND is_client_credentials_only = ?";
            return execute(start, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setBoolean(2, true);
            }, result -> {
                if (result.next()) {
                    return result.getInt("c");
                }
                return 0;
            });
        } else {
            String QUERY = "SELECT COUNT(*) as c FROM " + Config.getConfig(start).getOAuthClientsTable() +
                    " WHERE app_id = ?";
            return execute(start, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
            }, result -> {
                if (result.next()) {
                    return result.getInt("c");
                }
                return 0;
            });
        }
    }

    public static int countTotalNumberOfOAuthM2MTokensAlive(Start start, AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        // SUM the rollup over buckets whose exp hour is strictly in the future. Bounded by the number
        // of live exp-buckets for the app (index range on (app_id, exp_bucket)), independent of
        // issuance volume. Edge semantics: tokens in the current exp hour (exp_bucket == now_bucket)
        // are excluded even though some are still alive -> off by at most the current partial hour.
        long nowBucket = (System.currentTimeMillis() / 1000) / 3600;
        String QUERY = "SELECT COALESCE(SUM(count), 0) as c FROM "
                + Config.getConfig(start).getOAuthM2MTokenStatsTable() +
                " WHERE app_id = ? AND exp_bucket > ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, nowBucket);
        }, result -> {
            if (result.next()) {
                return result.getInt("c");
            }
            return 0;
        });
    }

    public static int countTotalNumberOfOAuthM2MTokensCreatedSince(Start start, AppIdentifier appIdentifier, long since)
            throws SQLException, StorageQueryException {
        // SUM the rollup over buckets whose iat hour is at or after `since` (a millisecond epoch).
        // Constant work: an index range on the PK's (app_id, iat_bucket) prefix. Edge semantics:
        // counts are exact per bucket, so a `since` landing inside a bucket over-counts by at most
        // that bucket's partial hour (versus the old implementation's unbounded undercount of bursty
        // issuers).
        long sinceBucket = (since / 1000) / 3600;
        String QUERY = "SELECT COALESCE(SUM(count), 0) as c FROM "
                + Config.getConfig(start).getOAuthM2MTokenStatsTable() +
                " WHERE app_id = ? AND iat_bucket >= ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, sinceBucket);
        }, result -> {
            if (result.next()) {
                return result.getInt("c");
            }
            return 0;
        });
    }

    public static void addOAuthM2MTokenForStats(Start start, AppIdentifier appIdentifier, String clientId, long iat, long exp)
            throws SQLException, StorageQueryException {
        // Bucket the token into hourly (epoch-hour) iat/exp buckets and increment a counter instead of
        // storing one row per issued token. This fixes the undercount of bursty issuers (the old PK
        // (app_id, client_id, iat-second) + ON CONFLICT DO NOTHING recorded at most one token per
        // client per second) and keeps both stat queries index-range SUMs whose cost is bounded by the
        // live bucket count, independent of issuance volume. `clientId` is unused now (see the table
        // comment); the parameter and the throws clause are kept so the OAuthStorage signature is
        // unchanged. High-rate note: all of an app's issuance upserts one hot row per
        // (iat_bucket, exp_bucket); if sustained multi-thousand-tokens/sec contention ever shows in
        // tests, add a small hash-shard column to the PK and SUM over shards.
        long iatBucket = iat / 3600;
        long expBucket = exp / 3600;
        String statsTable = Config.getConfig(start).getOAuthM2MTokenStatsTable();
        // Qualify the existing-row count with the table name: an unqualified `count` in a DO UPDATE
        // SET is ambiguous between the target row and the EXCLUDED pseudo-row (both carry a `count`
        // column), which Postgres rejects.
        String QUERY = "INSERT INTO " + statsTable +
                " (app_id, iat_bucket, exp_bucket, count) VALUES (?, ?, ?, 1)" +
                " ON CONFLICT (app_id, iat_bucket, exp_bucket) DO UPDATE SET count = " + statsTable + ".count + 1";
        update(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, iatBucket);
            pst.setLong(3, expBucket);
        });
    }

    public static void addOAuthLogoutChallenge(Start start, AppIdentifier appIdentifier, String challenge, String clientId,
            String postLogoutRedirectionUri, String sessionHandle, String state, long timeCreated) throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + Config.getConfig(start).getOAuthLogoutChallengesTable() +
                " (app_id, challenge, client_id, post_logout_redirect_uri, session_handle, state, time_created) VALUES (?, ?, ?, ?, ?, ?, ?)";
        update(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, challenge);
            pst.setString(3, clientId);
            pst.setString(4, postLogoutRedirectionUri);
            pst.setString(5, sessionHandle);
            pst.setString(6, state);
            pst.setLong(7, timeCreated);
        });
    }

    public static OAuthLogoutChallenge getOAuthLogoutChallenge(Start start, AppIdentifier appIdentifier, String challenge) throws SQLException, StorageQueryException {
        String QUERY = "SELECT challenge, client_id, post_logout_redirect_uri, session_handle, state, time_created FROM " +
                Config.getConfig(start).getOAuthLogoutChallengesTable() +
                " WHERE app_id = ? AND challenge = ?";
        
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, challenge);
        }, result -> {
            if (result.next()) {
                return new OAuthLogoutChallenge(
                    result.getString("challenge"),
                    result.getString("client_id"),
                    result.getString("post_logout_redirect_uri"),
                    result.getString("session_handle"),
                    result.getString("state"),
                    result.getLong("time_created")
                );
            }
            return null;
        });
    }

    public static void deleteOAuthLogoutChallenge(Start start, AppIdentifier appIdentifier, String challenge) throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + Config.getConfig(start).getOAuthLogoutChallengesTable() +
                " WHERE app_id = ? AND challenge = ?";
        update(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, challenge);
        });
    }

    public static void deleteOAuthLogoutChallengesBefore(Start start, long time) throws SQLException, StorageQueryException {
        String QUERY = "DELETE FROM " + Config.getConfig(start).getOAuthLogoutChallengesTable() +
                " WHERE time_created < ?";
        update(start, QUERY, pst -> {
            pst.setLong(1, time);
        });
    }

    public static String getRefreshTokenMapping(Start start, AppIdentifier appIdentifier, String externalRefreshToken) throws SQLException, StorageQueryException {
        String QUERY = "SELECT internal_refresh_token FROM " + Config.getConfig(start).getOAuthSessionsTable() +
                " WHERE app_id = ? AND external_refresh_token = ?";
        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, externalRefreshToken);
        }, result -> {
            if (result.next()) {
                return result.getString("internal_refresh_token");
            }
            return null;
        });
    }

    /**
     * SELECT FOR UPDATE variant — must be called inside an open transaction.
     * Locks the oauth_sessions row for the given externalRefreshToken so that no
     * other DB client can read or write it until the transaction is committed or
     * rolled back.
     */
    public static String getRefreshTokenMappingForUpdate(Start start, Connection con,
                                                         AppIdentifier appIdentifier,
                                                         String externalRefreshToken)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT internal_refresh_token FROM " + Config.getConfig(start).getOAuthSessionsTable() +
                " WHERE app_id = ? AND external_refresh_token = ? FOR UPDATE";
        return execute(con, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, externalRefreshToken);
        }, result -> {
            if (result.next()) {
                return result.getString("internal_refresh_token");
            }
            return null;
        });
    }

    /**
     * Updates the internal token and metadata for a non-rotating refresh.
     * Must be called inside the same transaction that previously called
     * {@link #getRefreshTokenMappingForUpdate}.
     */
    public static void updateOAuthSessionInternal(Start start, Connection con,
                                                   AppIdentifier appIdentifier,
                                                   String gid,
                                                   String newInternalRefreshToken,
                                                   String sessionHandle,
                                                   String jti,
                                                   long exp)
            throws SQLException, StorageQueryException {
        String QUERY = "UPDATE " + Config.getConfig(start).getOAuthSessionsTable() +
                " SET internal_refresh_token = ?, session_handle = ?, jti = CONCAT(jti, ?), exp = ?" +
                " WHERE gid = ? AND app_id = ?";
        update(con, QUERY, pst -> {
            pst.setString(1, newInternalRefreshToken);
            pst.setString(2, sessionHandle);
            pst.setString(3, jti + ",");
            pst.setLong(4, exp);
            pst.setString(5, gid);
            pst.setString(6, appIdentifier.getAppId());
        });
    }

    public static void deleteExpiredOAuthSessions(Start start, long exp) throws SQLException, StorageQueryException {
        // delete expired M2M tokens
        String QUERY = "DELETE FROM " + Config.getConfig(start).getOAuthSessionsTable() +
                " WHERE exp < ?";

        update(start, QUERY, pst -> {
            pst.setLong(1, exp);
        });
    }

    public static void deleteExpiredOAuthM2MTokens(Start start, long exp) throws SQLException, StorageQueryException {
        // Keep draining the legacy per-token table. New tokens no longer land here, so once every row
        // has aged past the retention window (`exp` is now-31d, in epoch seconds) this is a no-op and
        // the table stays empty.
        String QUERY = "DELETE FROM " + Config.getConfig(start).getOAuthM2MTokensTable() +
                " WHERE exp < ?";
        update(start, QUERY, pst -> {
            pst.setLong(1, exp);
        });

        // Matching sweep for the rollup: drop buckets whose whole exp hour is past the retention
        // window. exp_bucket >= iat_bucket always, so an exp_bucket strictly below the cutoff hour
        // means both buckets predate the window.
        String STATS_QUERY = "DELETE FROM " + Config.getConfig(start).getOAuthM2MTokenStatsTable() +
                " WHERE exp_bucket < ?";
        update(start, STATS_QUERY, pst -> {
            pst.setLong(1, exp / 3600);
        });
    }

    public static boolean isOAuthSessionExistsByJTI(Start start, AppIdentifier appIdentifier, String gid, String jti)
            throws SQLException, StorageQueryException {
        String SELECT = "SELECT jti FROM " + Config.getConfig(start).getOAuthSessionsTable()
                + " WHERE app_id = ? and gid = ?;";
        return execute(start, SELECT, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, gid);
        }, result -> {
            if(result.next()){
                List<String> jtis = Arrays.stream(result.getString(1).split(",")).filter(s -> !s.isEmpty()).collect(
                        Collectors.toList());
                return jtis.contains(jti);
            }
            return false;
        });
    }

    public static boolean isOAuthSessionExistsByGID(Start start, AppIdentifier appIdentifier, String gid)
            throws SQLException, StorageQueryException {
        String SELECT = "SELECT count(*) FROM " + Config.getConfig(start).getOAuthSessionsTable()
                + " WHERE app_id = ? and gid = ?;";
        return execute(start, SELECT, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, gid);
        }, result -> {
            if(result.next()){
                return result.getInt(1) > 0;
            }
            return false;
        });
    }

}
