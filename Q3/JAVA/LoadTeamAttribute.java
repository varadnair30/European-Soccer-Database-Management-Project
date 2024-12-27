import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LoadTeamAttribute {
    private static final String JDBC_URL = "jdbc:oracle:thin:@acaddbprod.uta.edu:1523/pcse01p.data.uta.edu";
    private static final String USERNAME = "rxs8010";
    private static final String PASSWORD = "Qwertysingh1";
    private static final String CSV_FILE = "C:\\Users\\rajat\\OneDrive\\Desktop\\project1\\Q3\\csv\\Team_Attributes.csv";
    private static final String CSV_DELIMITER = ",";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
             BufferedReader br = new BufferedReader(new FileReader(CSV_FILE))) {

            String insertSQL = "INSERT INTO TEAM_ATTRIBUTES (id, team_fifa_api_id, team_api_id, dd, buildUpPlaySpeed, "
                    + "buildUpPlaySpeedClass, buildUpPlayDribbling, buildUpPlayDribblingClass, buildUpPlayPassing, "
                    + "buildUpPlayPassingClass, buildUpPlayPositioningClass, chanceCreationPassing, "
                    + "chanceCreationPassingClass, chanceCreationCrossing, chanceCreationCrossingClass, "
                    + "chanceCreationShooting, chanceCreationShootingClass, chanceCreationPositioningClass, "
                    + "defencePressure, defencePressureClass, defenceAggression, defenceAggressionClass, "
                    + "defenceTeamWidth, defenceTeamWidthClass, defenceDefenderLineClass) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                String line;
                boolean isFirstLine = true; // Flag to skip the header row

                while ((line = br.readLine()) != null) {
                    if (isFirstLine) {
                        isFirstLine = false; // Skip the first line (header)
                        continue;
                    }

                    String[] data = line.split(CSV_DELIMITER);

                    // Ensure correct data length
                    if (data.length < 25) {
                        System.err.println("Skipping invalid line (not enough values): " + line);
                        continue; // Skip if there are not enough values
                    }

                    int id = Integer.parseInt(data[0]); // Assuming 'id' is the first column

                    // Check for existing record
                    String checkSQL = "SELECT COUNT(*) FROM TEAM_ATTRIBUTES WHERE id = ?";
                    try (PreparedStatement checkStmt = conn.prepareStatement(checkSQL)) {
                        checkStmt.setInt(1, id);
                        try (ResultSet rs = checkStmt.executeQuery()) {
                            if (rs.next() && rs.getInt(1) > 0) {
                                System.out.println("Record with id " + id + " already exists. Skipping.");
                                continue; // Skip to the next line
                            }
                        }
                    } catch (SQLException e) {
                        System.err.println("Error checking for existing record: " + e.getMessage());
                        continue; // Skip to the next line if there is an error
                    }

                    // Set parameters for the insert statement
                    pstmt.setInt(1, id);
                    pstmt.setInt(2, Integer.parseInt(data[1])); // team_fifa_api_id
                    pstmt.setInt(3, Integer.parseInt(data[2])); // team_api_id
                    pstmt.setDate(4, java.sql.Date.valueOf(data[3])); // dd (assuming it's a date)
                    pstmt.setDouble(5, Double.parseDouble(data[4])); // buildUpPlaySpeed
                    pstmt.setString(6, data[5]); // buildUpPlaySpeedClass
                    pstmt.setDouble(7, Double.parseDouble(data[6])); // buildUpPlayDribbling
                    pstmt.setString(8, data[7]); // buildUpPlayDribblingClass
                    pstmt.setDouble(9, Double.parseDouble(data[8])); // buildUpPlayPassing
                    pstmt.setString(10, data[9]); // buildUpPlayPassingClass
                    pstmt.setString(11, data[10]); // buildUpPlayPositioningClass
                    pstmt.setDouble(12, Double.parseDouble(data[11])); // chanceCreationPassing
                    pstmt.setString(13, data[12]); // chanceCreationPassingClass
                    pstmt.setDouble(14, Double.parseDouble(data[13])); // chanceCreationCrossing
                    pstmt.setString(15, data[14]); // chanceCreationCrossingClass
                    pstmt.setDouble(16, Double.parseDouble(data[15])); // chanceCreationShooting
                    pstmt.setString(17, data[16]); // chanceCreationShootingClass
                    pstmt.setString(18, data[17]); // chanceCreationPositioningClass
                    pstmt.setDouble(19, Double.parseDouble(data[18])); // defencePressure
                    pstmt.setString(20, data[19]); // defencePressureClass
                    pstmt.setDouble(21, Double.parseDouble(data[20])); // defenceAggression
                    pstmt.setString(22, data[21]); // defenceAggressionClass
                    pstmt.setDouble(23, Double.parseDouble(data[22])); // defenceTeamWidth
                    pstmt.setString(24, data[23]); // defenceTeamWidthClass
                    pstmt.setString(25, data[24]); // defenceDefenderLineClass

                    // Execute the insert
                    pstmt.executeUpdate();
                }
            } catch (SQLException e) {
                System.err.println("SQL error during insertion: " + e.getMessage());
            }
        } catch (SQLException e) {
            System.err.println("Database connection error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
        }
    }
}
