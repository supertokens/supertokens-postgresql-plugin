package io.supertokens.storage.postgresql.queries;

import java.math.BigInteger;
import java.sql.SQLException;

import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;

public class ActiveUsersQueries {
    static String getQueryToCreateUserLastActiveTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();

        return "CREATE TABLE IF NOT EXISTS " + Config.getConfig(start).getUserLastActiveTable() + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "user_id VARCHAR(128),"
                + "last_active_time BIGINT,"
                + "PRIMARY KEY(app_id, user_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, Config.getConfig(start).getUserLastActiveTable(), "app_id", "fkey")
                + " FOREIGN KEY(app_id)"
                + " REFERENCES " + Config.getConfig(start).getAppsTable() +  " (app_id) ON DELETE CASCADE"
                + ");";
    }

    static String getQueryToCreateAppIdIndexForUserLastActiveTable(Start start) {
        return "CREATE INDEX IF NOT EXISTS user_last_active_app_id_index ON "
                + Config.getConfig(start).getUserLastActiveTable() + "(app_id);";
    }

    public static int countUsersActiveSince(Start start, AppIdentifier appIdentifier, long sinceTime) throws SQLException, StorageQueryException {
        String QUERY = "SELECT COUNT(*) as total FROM " + Config.getConfig(start).getUserLastActiveTable()
                + " WHERE app_id = ? AND last_active_time >= ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, sinceTime);
        }, result -> {
            if (result.next()) {
                return result.getInt("total");
            }
            return 0;
        });
    }

    public static int countUsersEnabledTotp(Start start, AppIdentifier appIdentifier) throws SQLException, StorageQueryException {
        String QUERY = "SELECT COUNT(*) as total FROM " + Config.getConfig(start).getTotpUsersTable()
                + " WHERE app_id = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
        }, result -> {
            if (result.next()) {
                return result.getInt("total");
            }
            return 0;
        });
    }

    public static int countUsersEnabledTotpAndActiveSince(Start start, AppIdentifier appIdentifier, long sinceTime) throws SQLException, StorageQueryException {
        String QUERY = "SELECT COUNT(*) as total FROM " + Config.getConfig(start).getTotpUsersTable() + " AS totp_users "
                + "INNER JOIN " + Config.getConfig(start).getUserLastActiveTable() + " AS user_last_active "
                + "ON totp_users.user_id = user_last_active.user_id "
                + "WHERE user_last_active.app_id = ? AND user_last_active.last_active_time >= ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, sinceTime);
        }, result -> {
            if (result.next()) {
                return result.getInt("total");
            }
            return 0;
        });
    }
    
    public static int countUsersEnabledMfa(Start start, AppIdentifier appIdentifier) throws SQLException, StorageQueryException {
        String QUERY = "SELECT COUNT(*) as total FROM (SELECT DISTINCT user_id FROM " + Config.getConfig(start).getMfaUserFactorsTable() + "WHERE app_id = ?) AS app_mfa_users";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
        }, result -> {
            if (result.next()) {
                return result.getInt("total");
            }
            return 0;
        });
    }

    public static int countUsersEnabledMfaAndActiveSince(Start start, AppIdentifier appIdentifier, long sinceTime) throws SQLException, StorageQueryException {
        // Find unique users from mfa_user_factors table and join with user_last_active table
        String QUERY = "SELECT COUNT(*) as total FROM (SELECT DISTINCT user_id FROM " + Config.getConfig(start).getMfaUserFactorsTable() + ") AS mfa_users "
                + "INNER JOIN " + Config.getConfig(start).getUserLastActiveTable() + " AS user_last_active "
                + "ON mfa_users.user_id = user_last_active.user_id "
                + "WHERE user_last_active.app_id = ?"
                + "AND user_last_active.last_active_time >= ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setLong(2, sinceTime);
        }, result -> {
            if (result.next()) {
                return result.getInt("total");
            }
            return 0;
        });
    }

    public static int updateUserLastActive(Start start, AppIdentifier appIdentifier, String userId) throws SQLException, StorageQueryException {
        String QUERY = "INSERT INTO " + Config.getConfig(start).getUserLastActiveTable()
                + "(app_id, user_id, last_active_time) VALUES(?, ?, ?) ON CONFLICT(app_id, user_id) DO UPDATE SET last_active_time = ?";

        long now = System.currentTimeMillis();
        return update(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
            pst.setLong(3, now);
            pst.setLong(4, now);
        });
    }

    public static Long getLastActiveByUserId(Start start, AppIdentifier appIdentifier, String userId)
            throws StorageQueryException {
        String QUERY = "SELECT last_active_time FROM " + Config.getConfig(start).getUserLastActiveTable()
                + " WHERE app_id = ? AND user_id = ?";

        try {
            return execute(start, QUERY, pst -> {
                pst.setString(1, appIdentifier.getAppId());
                pst.setString(2, userId);
            }, res -> {
                if (res.next()) {
                    return res.getLong("last_active_time");
                }
                return null;
            });
        } catch (SQLException e) {
            throw new StorageQueryException(e);
        }
    }

    public static void deleteUserActive(Start start, AppIdentifier appIdentifier, String userId)
            throws StorageQueryException, SQLException {
        String QUERY = "DELETE FROM " + Config.getConfig(start).getUserLastActiveTable()
                + " WHERE app_id = ? AND user_id = ?";

        update(start, QUERY, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, userId);
        });
    }
}
