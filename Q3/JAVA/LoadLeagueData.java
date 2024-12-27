import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class LeagueDataImporter {

    // Constants for database connection
    private static final String DB_URL = "jdbc:oracle:thin:@acaddbprod.uta.edu:1523/pcse01p.data.uta.edu";
    private static final String DB_USER = "rxs8010"; // Replace with your NetID
    private static final String DB_PASSWORD = "Qwertysingh1";

    public static void main(String[] args) {
        String csvPath = "C:\\Users\\rajat\\OneDrive\\Desktop\\project1\\Q3\\csv\\League.csv"; // CSV file path

        // Load league data into the database
        importLeagueData(csvPath);
    }

    private static void importLeagueData(String csvFilePath) {
        try {
            // Load the JDBC driver
            Class.forName("oracle.jdbc.driver.OracleDriver");

            // Establish a database connection
            Connection dbConnection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            System.out.println("Successfully connected to the Oracle database!");

            // Prepare the SQL insert statement
            String insertSQL = "INSERT INTO LEAGUE (id, country_id, name) VALUES (?, ?, ?)";
            PreparedStatement preparedStatement = dbConnection.prepareStatement(insertSQL);

            // Read data from the CSV file
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
                String currentLine;
                reader.readLine(); // Skip the header line

                while ((currentLine = reader.readLine()) != null) {
                    currentLine = currentLine.trim();
                    if (currentLine.isEmpty()) {
                        continue; // Skip empty lines
                    }

                    String[] fields = currentLine.split(",", -1); // Split line into fields

                    // Validate the number of columns
                    if (fields.length != 3) {
                        System.out.println("Invalid line (skipped): " + currentLine);
                        continue;
                    }

                    try {
                        int leagueId = Integer.parseInt(fields[0].trim());
                        int leagueCountryId = Integer.parseInt(fields[1].trim());
                        String leagueName = fields[2].trim();

                        // Set the values in the prepared statement
                        preparedStatement.setInt(1, leagueId);
                        preparedStatement.setInt(2, leagueCountryId);
                        preparedStatement.setString(3, leagueName);

                        // Add to the batch
                        preparedStatement.addBatch();
                    } catch (NumberFormatException e) {
                        System.out.println("Skipping line due to NumberFormatException: " + currentLine);
                    }
                }
            }

            // Execute batch insert
            preparedStatement.executeBatch();
            preparedStatement.close();
            dbConnection.close();

            System.out.println("League data has been successfully imported!");

        } catch (IOException e) {
            System.out.println("Error occurred while reading the CSV file.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Error occurred with the database operation.");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.out.println("Oracle JDBC Driver not found.");
            e.printStackTrace();
        }
    }
}
