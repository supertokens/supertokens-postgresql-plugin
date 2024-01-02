package io.supertokens.storage.postgresql.migrations;

import io.supertokens.storage.postgresql.MigrationContextManager;
import io.supertokens.storage.postgresql.Start;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;

import static io.supertokens.storage.postgresql.ProcessState.PROCESS_STATE.STARTING_MIGRATION;
import static io.supertokens.storage.postgresql.ProcessState.getInstance;

public class V6__core_version_7_0_12 extends BaseJavaMigration {

    @Override
    public void migrate(Context context) throws Exception {
        try {
            Connection connection = context.getConnection();
            Map<String, String> ph = context.getConfiguration().getPlaceholders();
            Start start = MigrationContextManager.getContext(ph.get("process_id"));
            getInstance(start).addState(STARTING_MIGRATION, null);

            updatePhoneNumbers(connection, ph.get("passwordless_users_table"));
            updatePhoneNumbers(connection, ph.get("passwordless_devices_table"));
        } catch (SQLException e) {
            throw e;
        }
    }

    private void updatePhoneNumbers(Connection connection, String table) throws SQLException {
        final int batchSize = 1000;
        int offset = 0;

        while (true) {
            // Create a PreparedStatement for fetching rows
            PreparedStatement selectStatement = connection.prepareStatement(
                    String.format("SELECT * FROM %s WHERE phone_number IS NOT NULL LIMIT ? OFFSET ?", table));
            selectStatement.setInt(1, batchSize);
            selectStatement.setInt(2, offset);

            // Execute the query to get rows
            ResultSet resultSet = selectStatement.executeQuery();

            while (resultSet.next()) {
                // Get the current phone number from the result set
                String currentPhoneNumber = resultSet.getString("phone_number");

                // Normalize the phone number
                String normalizedPhoneNumber = getNormalizedPhoneNumber(currentPhoneNumber);

                // Check if normalization is successful and if there's a change in the phone number
                if (normalizedPhoneNumber != null && !normalizedPhoneNumber.equals(currentPhoneNumber)) {
                    // Create a PreparedStatement for updating rows
                    PreparedStatement updateStatement = connection.prepareStatement(
                            String.format("UPDATE %s SET phone_number = ? WHERE app_id = ? AND tenant_id = ? AND device_id_hash = ?", table));

                    // Set parameters for the update statement
                    updateStatement.setString(1, normalizedPhoneNumber);
                    updateStatement.setString(2, resultSet.getString("app_id"));
                    updateStatement.setString(3, resultSet.getString("tenant_id"));
                    updateStatement.setString(4, resultSet.getString("device_id_hash"));

                    // Execute the update query
                    updateStatement.executeUpdate();
                }
            }

            // Increment the offset for the next batch
            offset += batchSize;

            // Break the loop if we have processed all rows
            if (!resultSet.isLast()) {
                break;
            }
        }
    }

    private String getNormalizedPhoneNumber(String phoneNumber) {
        try {
            PhoneNumberUtil phoneNumberUtil = PhoneNumberUtil.getInstance();
            Phonenumber.PhoneNumber parsedPhoneNumber = phoneNumberUtil.parse(phoneNumber, "US");
            if (phoneNumberUtil.isValidNumber(parsedPhoneNumber)) {
                return phoneNumberUtil.format(parsedPhoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164);
            }
        } catch (NumberParseException e) {
            //Not sure if this should fail silently for migration to continue, or should halt the migration.
        }
        return null;
    }
}