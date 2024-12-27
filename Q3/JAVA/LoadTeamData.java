import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LoadTeamData {
    static final String JDBC_URL = "jdbc:oracle:thin:@acaddbprod.uta.edu:1523/pcse01p.data.uta.edu";
    static final String USER = "rxs8010";
    static final String PASS = "Qwertysingh1";
    static final String CSV_FILE_PATH = "C:\\Users\\rajat\\OneDrive\\Desktop\\project1\\Q3\\csv\\Team.csv";

    public static void main(String[] args) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASS);
                 BufferedReader br = new BufferedReader(new FileReader(CSV_FILE_PATH))) {

                System.out.println("Connected to the Oracle database!");

                String insertSQL = "INSERT INTO TEAM (id, team_api_id, team_fifa_api_id, team_long_name, team_short_name) " +
                        "VALUES (?, ?, ?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                    br.readLine();  // Skip the header line
                    String line;
                    conn.setAutoCommit(false); // Disable auto-commit for batch processing

                    while ((line = br.readLine()) != null) {
                        String[] values = line.split(",");

                        // Ensure that all values are trimmed and the correct length
                        if (values.length >= 5) {
                            int id = Integer.parseInt(values[0].trim());
                            int teamApiId = Integer.parseInt(values[1].trim());
                            int teamFifaApiId = Integer.parseInt(values[2].trim());
                            String teamLongName = values[3].trim();
                            String teamShortName = values[4].trim();

                            // Check for existing record
                            if (!recordExists(conn, teamApiId, teamFifaApiId)) {
                                // Set parameters and add to batch
                                pstmt.setInt(1, id);
                                pstmt.setInt(2, teamApiId);
                                pstmt.setInt(3, teamFifaApiId);
                                pstmt.setString(4, teamLongName);
                                pstmt.setString(5, teamShortName);
                                pstmt.addBatch();
                            } else {
                                System.out.println("Duplicate record found, skipping: " + line);
                            }
                        } else {
                            System.err.println("Invalid line (not enough values): " + line);
                        }
                    }

                    // Execute batch after all inserts are prepared
                    pstmt.executeBatch();
                    conn.commit(); // Commit the transaction
                    System.out.println("Data has been inserted successfully!");
                } catch (SQLException e) {
                    System.err.println("SQL error during data insertion: " + e.getMessage());
                    conn.rollback(); // Rollback on error
                }
            } catch (SQLException e) {
                System.err.println("Database connection error: " + e.getMessage());
            }
        } catch (IOException e) {
            System.err.println("Error reading the CSV file: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("Oracle JDBC Driver not found: " + e.getMessage());
        }
    }

    // Method to check if a record exists based on team_api_id and team_fifa_api_id
    private static boolean recordExists(Connection conn, int teamApiId, int teamFifaApiId) throws SQLException {
        String checkQuery = "SELECT COUNT(*) FROM TEAM WHERE team_api_id = ? OR team_fifa_api_id = ?";
        try (PreparedStatement checkStmt = conn.prepareStatement(checkQuery)) {
            checkStmt.setInt(1, teamApiId);
            checkStmt.setInt(2, teamFifaApiId);
            ResultSet rs = checkStmt.executeQuery();
            return rs.next() && rs.getInt(1) > 0;  // Returns true if a record exists
        }
    }
}
