import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class LoadPlayerData {
    private static final String JDBC_URL = "jdbc:oracle:thin:@acaddbprod.uta.edu:1523/pcse01p.data.uta.edu";
    private static final String USER = "rxs8010";
    private static final String PASS = "Qwertysingh1";
    private static final String CSV_FILE_PATH = "C:\\Users\\rajat\\OneDrive\\Desktop\\project1\\Q3\\csv\\Player.csv";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASS);
             PreparedStatement preparedStatement = connection.prepareStatement(
                     "INSERT INTO PLAYER (id, player_api_id, player_name, player_fifa_api_id, birthday, height, weight) VALUES (?, ?, ?, ?, ?, ?, ?)");
             BufferedReader br = new BufferedReader(new FileReader(CSV_FILE_PATH))) {

            connection.setAutoCommit(false); // Start transaction

            String line;
            br.readLine(); // Skip header line

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",", -1); // Split while retaining trailing empty fields

                if (values.length < 7) {
                    System.err.println("Skipping invalid line (not enough values): " + line);
                    continue; // Skip this line if not enough values
                }

                try {
                    // Set values for the prepared statement
                    preparedStatement.setInt(1, Integer.parseInt(values[0].trim()));
                    preparedStatement.setInt(2, Integer.parseInt(values[1].trim()));
                    preparedStatement.setString(3, values[2].trim());
                    preparedStatement.setInt(4, Integer.parseInt(values[3].trim()));

                    // Parse and set date
                    Date sqlDate = null;
                    try {
                        java.util.Date utilDate = DATE_FORMAT.parse(values[4].trim());
                        sqlDate = new Date(utilDate.getTime());
                    } catch (ParseException e) {
                        System.err.println("Skipping invalid date format: " + values[4]);
                        continue; // Skip this line if date parsing fails
                    }
                    preparedStatement.setDate(5, sqlDate);

                    preparedStatement.setDouble(6, Double.parseDouble(values[5].trim()));
                    preparedStatement.setDouble(7, Double.parseDouble(values[6].trim()));

                    // Add to batch
                    preparedStatement.addBatch();
                } catch (NumberFormatException e) {
                    System.err.println("Skipping invalid line due to number format: " + line);
                }
            }

            // Execute batch insert
            preparedStatement.executeBatch();
            connection.commit(); // Commit the transaction
            System.out.println("Data imported successfully!");

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
