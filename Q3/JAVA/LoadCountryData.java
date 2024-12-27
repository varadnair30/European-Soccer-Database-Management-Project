import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CountryDataLoader {

    // Database connection details
    private static final String DATABASE_URL = "jdbc:oracle:thin:@acaddbprod.uta.edu:1523/pcse01p.data.uta.edu";
    private static final String DATABASE_USER = "rxs8010";
    private static final String DATABASE_PASSWORD = "Qwertysingh1";

    public static void main(String[] args) {
        String csvFile = "C:/Users/rajat/OneDrive/Desktop/project1/Q3/csv/Country.csv";

        // Load country data from CSV file into the database
        loadCountryData(csvFile);
    }

    private static void loadCountryData(String filePath) {
        try {
            // Load the Oracle JDBC driver
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection dbConnection = DriverManager.getConnection(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);
            System.out.println("Successfully connected to the Oracle database!");

            String insertSQL = "INSERT INTO COUNTRY (id, name) VALUES (?, ?)";
            PreparedStatement preparedStatement = dbConnection.prepareStatement(insertSQL);

            // Read the CSV file
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String currentLine;
                reader.readLine(); // Skip the header line

                while ((currentLine = reader.readLine()) != null) {
                    String[] fields = currentLine.split(",");
                    int countryId = Integer.parseInt(fields[0].trim());
                    String countryName = fields[1].trim();

                    preparedStatement.setInt(1, countryId);
                    preparedStatement.setString(2, countryName);
                    preparedStatement.executeUpdate();
                }
            }

            // Clean up resources
            preparedStatement.close();
            dbConnection.close();
            System.out.println("Country data has been successfully inserted!");

        } catch (IOException ex) {
            System.out.println("Error while reading the CSV file.");
            ex.printStackTrace();
        } catch (SQLException ex) {
            System.out.println("Error with the database operation.");
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            System.out.println("Oracle JDBC Driver not found.");
            ex.printStackTrace();
        }
    }
}
