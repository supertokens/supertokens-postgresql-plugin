/*
 *    Copyright (c) 2026, VRAI Labs and/or its affiliates. All rights reserved.
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
import io.supertokens.ProcessState;
import io.supertokens.ResourceDistributor;
import io.supertokens.pluginInterface.STORAGE_TYPE;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.ConnectionPool;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.queries.GeneralQueries;
import io.supertokens.storage.postgresql.queries.OAuthQueries;
import io.supertokens.storageLayer.StorageLayer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.*;

/**
 * Issue #357: the two M2M feature-flag stats used to be served from per-token rows in
 * {@code oauth_m2m_tokens} (PK {@code (app_id, client_id, iat-second)} + {@code ON CONFLICT DO
 * NOTHING}), which undercounted any client issuing more than one token per second and scaled the
 * table with monthly issuance. They are now served from the {@code oauth_m2m_token_stats} hourly
 * rollup. These tests pin the new semantics through the {@code OAuthStorage} interface:
 *
 * <ul>
 *   <li>a burst of N tokens in one second is counted as exactly N (the headline bug);</li>
 *   <li>created-since sums the correct iat-hour buckets at a bucket boundary;</li>
 *   <li>alive sums exp-hour buckets strictly in the future (current-hour and past excluded);</li>
 *   <li>the retention sweep drops rollup buckets whose exp hour is past the window;</li>
 *   <li>the one-shot transition buckets whatever the legacy table still holds.</li>
 * </ul>
 */
public class OAuthM2MTokenStatsTest {

    private static final long HOUR = 3600L;

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

    private TestingProcessManager.TestingProcess startProcess() throws Exception {
        String[] args = {"../"};
        TestingProcessManager.TestingProcess process = TestingProcessManager.start(args, false);
        process.startProcess();
        assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STARTED));
        return process;
    }

    private static AppIdentifier appId() {
        return ResourceDistributor.getAppForTesting().toAppIdentifier();
    }

    /**
     * The headline bug. Issue N tokens all bearing the same {@code iat} second (a one-second burst
     * from one client). The old per-token table, keyed on {@code (app_id, client_id, iat)} with
     * {@code ON CONFLICT DO NOTHING}, would have retained one row and reported 1. The rollup counter
     * must report exactly N.
     */
    @Test
    public void testBurstInOneSecondIsCountedExactly() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;
            Start storage = (Start) StorageLayer.getStorage(main);

            long nowSec = System.currentTimeMillis() / 1000;
            long iat = nowSec;
            long exp = nowSec + 24 * HOUR;
            int n = 50;
            for (int i = 0; i < n; i++) {
                // Same client, same iat-second: this is exactly the case the old PK collapsed to 1.
                storage.addOAuthM2MTokenForStats(appId(), "burst-client", iat, exp);
            }

            long since = (nowSec - 3 * HOUR) * 1000L; // ms, comfortably before the iat hour
            assertEquals("a one-second burst of N tokens must be counted as N",
                    n, storage.countTotalNumberOfOAuthM2MTokensCreatedSince(appId(), since));
            assertEquals("all N are alive (exp is a day out)",
                    n, storage.countTotalNumberOfOAuthM2MTokensAlive(appId()));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * created-since sums iat-hour buckets {@code >= floor(since)}. With tokens two hours apart, a
     * {@code since} landing between the two hours must include only the later hour; a {@code since}
     * before both must include both.
     */
    @Test
    public void testCreatedSinceBucketBoundary() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;
            Start storage = (Start) StorageLayer.getStorage(main);

            long nowSec = System.currentTimeMillis() / 1000;
            long exp = nowSec + 24 * HOUR;

            long iatOlder = nowSec - 2 * HOUR; // two hours ago
            long iatRecent = nowSec;           // this hour
            for (int i = 0; i < 3; i++) storage.addOAuthM2MTokenForStats(appId(), "c", iatOlder, exp);
            for (int i = 0; i < 5; i++) storage.addOAuthM2MTokenForStats(appId(), "c", iatRecent, exp);

            long sinceBetween = (nowSec - HOUR) * 1000L; // one hour ago -> excludes the two-hours-ago bucket
            assertEquals("since between the two hours counts only the recent bucket",
                    5, storage.countTotalNumberOfOAuthM2MTokensCreatedSince(appId(), sinceBetween));

            long sinceAll = (nowSec - 3 * HOUR) * 1000L;
            assertEquals("since before both buckets counts all",
                    8, storage.countTotalNumberOfOAuthM2MTokensCreatedSince(appId(), sinceAll));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * alive sums exp-hour buckets strictly greater than the current hour. Tokens expiring in a
     * future hour count; tokens already expired do not; tokens whose exp is in the current partial
     * hour are excluded by design (the documented current-hour edge).
     */
    @Test
    public void testAliveExcludesExpiredAndCurrentHour() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;
            Start storage = (Start) StorageLayer.getStorage(main);

            long nowSec = System.currentTimeMillis() / 1000;
            long iat = nowSec;

            // 7200 is an exact multiple of 3600, so these land two buckets above / below the current
            // hour regardless of where in the hour "now" falls -> deterministic.
            for (int i = 0; i < 3; i++) storage.addOAuthM2MTokenForStats(appId(), "c", iat, nowSec + 2 * HOUR);
            for (int i = 0; i < 2; i++) storage.addOAuthM2MTokenForStats(appId(), "c", iat, nowSec - 2 * HOUR);
            for (int i = 0; i < 4; i++) storage.addOAuthM2MTokenForStats(appId(), "c", iat, nowSec); // current hour

            assertEquals("only tokens expiring in a strictly-future hour are alive",
                    3, storage.countTotalNumberOfOAuthM2MTokensAlive(appId()));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * The rollup half of {@code deleteExpiredOAuthM2MTokens}: buckets whose exp hour is entirely
     * before the retention cutoff are dropped; buckets still inside the window survive.
     */
    @Test
    public void testRetentionSweepDropsOldBuckets() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;
            Start storage = (Start) StorageLayer.getStorage(main);

            long nowSec = System.currentTimeMillis() / 1000;
            long fortyDays = 40 * 24 * HOUR;

            // 4 old tokens (issued and expired ~40 days ago) + 2 recent (alive).
            for (int i = 0; i < 4; i++) {
                storage.addOAuthM2MTokenForStats(appId(), "c", nowSec - fortyDays, nowSec - fortyDays + HOUR);
            }
            for (int i = 0; i < 2; i++) {
                storage.addOAuthM2MTokenForStats(appId(), "c", nowSec, nowSec + 24 * HOUR);
            }

            long sinceAll = (nowSec - 100 * 24 * HOUR) * 1000L;
            assertEquals("all six present before the sweep",
                    6, storage.countTotalNumberOfOAuthM2MTokensCreatedSince(appId(), sinceAll));

            // The cron passes now-31d in epoch seconds.
            long monthAgo = nowSec - 31 * 24 * HOUR;
            storage.deleteExpiredOAuthM2MTokens(monthAgo);

            assertEquals("old-exp buckets are swept, recent ones remain",
                    2, storage.countTotalNumberOfOAuthM2MTokensCreatedSince(appId(), sinceAll));
            assertEquals("the two recent tokens are still alive after the sweep",
                    2, storage.countTotalNumberOfOAuthM2MTokensAlive(appId()));
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    /**
     * The one-shot transition. Seed the legacy {@code oauth_m2m_tokens} table with per-token rows
     * (as a pre-upgrade deployment would have), drop the rollup, then re-run
     * {@code createTablesIfNotExists} — which recreates the rollup and, in the same DDL batch,
     * buckets the legacy rows into it. The stats must then read back from the rollup.
     */
    @Test
    public void testTransitionBucketsLegacyRows() throws Exception {
        TestingProcessManager.TestingProcess process = startProcess();
        try {
            Main main = process.getProcess();
            if (StorageLayer.getStorage(main).getType() != STORAGE_TYPE.SQL) return;
            Start storage = (Start) StorageLayer.getStorage(main);

            String appIdStr = appId().getAppId();
            // The legacy table has a client_id FK to oauth_clients; create the client first.
            OAuthQueries.addOrUpdateOauthClient(storage, appId(), "legacy-client", "secret", false, false);

            long nowSec = System.currentTimeMillis() / 1000;
            long base = (nowSec / HOUR) * HOUR + 100; // 100s into the current hour: base..base+4 share a bucket
            long exp = base + 24 * HOUR;

            String legacyTable = Config.getConfig(storage).getOAuthM2MTokensTable();
            String statsTable = Config.getConfig(storage).getOAuthM2MTokenStatsTable();

            Connection con = ConnectionPool.getConnection(storage);
            try {
                con.setAutoCommit(true);
                // Five per-token legacy rows with distinct iat-seconds in one hour (the legacy PK on
                // (app_id, client_id, iat) forbids duplicate seconds — the very limitation this issue
                // is about).
                try (PreparedStatement pst = con.prepareStatement(
                        "INSERT INTO " + legacyTable + " (app_id, client_id, iat, exp) VALUES (?, ?, ?, ?)")) {
                    for (int i = 0; i < 5; i++) {
                        pst.setString(1, appIdStr);
                        pst.setString(2, "legacy-client");
                        pst.setLong(3, base + i);
                        pst.setLong(4, exp);
                        pst.executeUpdate();
                    }
                }
                assertEquals("legacy table seeded with 5 rows", 5, countRows(con, legacyTable, appIdStr));

                // Drop the rollup so the next createTablesIfNotExists recreates AND backfills it.
                try (Statement st = con.createStatement()) {
                    st.executeUpdate("DROP TABLE " + statsTable);
                }
            } finally {
                con.close();
            }

            Connection con2 = ConnectionPool.getConnection(storage);
            try {
                GeneralQueries.createTablesIfNotExists(storage, con2);
            } finally {
                con2.close();
            }

            long since = (nowSec - HOUR) * 1000L;
            assertEquals("legacy rows are bucketed into the rollup and counted",
                    5, storage.countTotalNumberOfOAuthM2MTokensCreatedSince(appId(), since));
            assertEquals("bucketed legacy rows are alive (exp a day out)",
                    5, storage.countTotalNumberOfOAuthM2MTokensAlive(appId()));

            Connection con3 = ConnectionPool.getConnection(storage);
            try {
                assertEquals("the 5 same-hour legacy rows collapse to a single rollup bucket",
                        1, countRows(con3, statsTable, appIdStr));
            } finally {
                con3.close();
            }
        } finally {
            process.kill();
            assertNotNull(process.checkOrWaitForEvent(ProcessState.PROCESS_STATE.STOPPED));
        }
    }

    private static int countRows(Connection con, String table, String appIdStr) throws Exception {
        try (PreparedStatement pst = con.prepareStatement(
                "SELECT COUNT(*) AS c FROM " + table + " WHERE app_id = ?")) {
            pst.setString(1, appIdStr);
            try (ResultSet rs = pst.executeQuery()) {
                rs.next();
                return rs.getInt("c");
            }
        }
    }
}
