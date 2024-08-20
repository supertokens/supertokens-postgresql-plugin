package io.supertokens.storage.postgresql.queries;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.supertokens.pluginInterface.exceptions.StorageQueryException;
import io.supertokens.pluginInterface.multitenancy.AppIdentifier;
import io.supertokens.storage.postgresql.Start;
import io.supertokens.storage.postgresql.config.Config;
import io.supertokens.storage.postgresql.utils.Utils;

import static io.supertokens.storage.postgresql.QueryExecutorTemplate.execute;
import static io.supertokens.storage.postgresql.QueryExecutorTemplate.update;


public class OAuthQueries {
    public static String getQueryToCreateOAuthClientTable(Start start) {
        String schema = Config.getConfig(start).getTableSchema();
        String oAuth2ClientTable = Config.getConfig(start).getOAuthClientTable();
        // @formatter:off
        return "CREATE TABLE IF NOT EXISTS " + oAuth2ClientTable + " ("
                + "app_id VARCHAR(64) DEFAULT 'public',"
                + "client_id VARCHAR(128) NOT NULL,"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientTable, "client_id", "pkey")
                + " PRIMARY KEY (app_id, client_id),"
                + "CONSTRAINT " + Utils.getConstraintName(schema, oAuth2ClientTable, "app_id", "fkey")
                + " FOREIGN KEY(app_id)"
                + " REFERENCES " + Config.getConfig(start).getAppsTable() + "(app_id) ON DELETE CASCADE"
                + ");";
        // @formatter:on
    }

    public static boolean isClientIdForAppId(Start start, String clientId, AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        String QUERY = "SELECT app_id FROM " + Config.getConfig(start).getOAuthClientTable() +
                " WHERE client_id = ? AND app_id = ?";

        return execute(start, QUERY, pst -> {
            pst.setString(1, clientId);
            pst.setString(2, appIdentifier.getAppId());
        }, ResultSet::next);
    }

    public static void insertClientIdForAppId(Start start, String clientId, AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        String INSERT = "INSERT INTO " + Config.getConfig(start).getOAuthClientTable()
                + "(app_id, client_id) VALUES(?, ?)";
        update(start, INSERT, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, clientId);
        });
    }

    public static boolean deleteClientIdForAppId(Start start, String clientId, AppIdentifier appIdentifier)
            throws SQLException, StorageQueryException {
        String DELETE = "DELETE FROM " + Config.getConfig(start).getOAuthClientTable()
                + " WHERE app_id = ? AND client_id = ?";
        int numberOfRow = update(start, DELETE, pst -> {
            pst.setString(1, appIdentifier.getAppId());
            pst.setString(2, clientId);
        });
        return numberOfRow > 0;
    }
}
