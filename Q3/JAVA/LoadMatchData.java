import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MatchDataLoader {

    // Database connection parameters
    private static final String DATABASE_URL = "jdbc:oracle:thin:@acaddbprod.uta.edu:1523/pcse01p.data.uta.edu";
    private static final String DATABASE_USER = "rxs8010"; // Replace with your NetID
    private static final String DATABASE_PASSWORD = "Qwertysingh1"; // Replace with your password

    public static void main(String[] args) {
        String csvFilePath = "C:\\Users\\rajat\\OneDrive\\Desktop\\project1\\Q3\\csv\\Match.csv";
        loadMatchData(csvFilePath);
    }

    private static void loadMatchData(String csvFile) {
        String line;
        String csvDelimiter = ",";

        // SQL insert statement
        String insertSQL = "INSERT INTO MATCH (id, country_id, league_id, season, stage, match_date, match_api_id, "
                + "home_team_api_id, away_team_api_id, home_team_goal, away_team_goal, "
                + "home_player_X1, home_player_X2, home_player_X3, home_player_X4, home_player_X5, "
                + "home_player_X6, home_player_X7, home_player_X8, home_player_X9, home_player_X10, "
                + "home_player_X11, away_player_X1, away_player_X2, away_player_X3, away_player_X4, "
                + "away_player_X5, away_player_X6, away_player_X7, away_player_X8, away_player_X9, "
                + "away_player_X10, away_player_X11, home_player_Y1, home_player_Y2, home_player_Y3, "
                + "home_player_Y4, home_player_Y5, home_player_Y6, home_player_Y7, home_player_Y8, "
                + "home_player_Y9, home_player_Y10, home_player_Y11, away_player_Y1, away_player_Y2, "
                + "away_player_Y3, away_player_Y4, away_player_Y5, away_player_Y6, away_player_Y7, "
                + "away_player_Y8, away_player_Y9, away_player_Y10, away_player_Y11, home_player_1, "
                + "home_player_2, home_player_3, home_player_4, home_player_5, home_player_6, "
                + "home_player_7, home_player_8, home_player_9, home_player_10, home_player_11, "
                + "away_player_1, away_player_2, away_player_3, away_player_4, away_player_5, "
                + "away_player_6, away_player_7, away_player_8, away_player_9, away_player_10, "
                + "away_player_11, goal, shoton, shotoff, foulcommit, card, cross, corner, possession, "
                + "B365H, B365D, B365A, BWH, BWD, BWA, IWH, IWD, IWA, LBH, LBD, LBA, PSH, PSD, PSA, WHH, "
                + "WHD, WHA, SJH, SJD, SJA, VCH, VCD, VCA, GBH, GBD, GBA, BSH, BSD, BSA) VALUES (?, ?, ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection connection = DriverManager.getConnection(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);
             BufferedReader bufferedReader = new BufferedReader(new FileReader(csvFile))) {

            PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
            bufferedReader.readLine(); // Skip header line

            while ((line = bufferedReader.readLine()) != null) {
                try {
                    String[] fields = line.split(csvDelimiter);

                    // Set values for the prepared statement
                    preparedStatement.setInt(1, Integer.parseInt(fields[0])); // id
                    preparedStatement.setInt(2, fields[1].isEmpty() ? 0 : Integer.parseInt(fields[1])); // country_id
                    preparedStatement.setInt(3, fields[2].isEmpty() ? 0 : Integer.parseInt(fields[2])); // league_id
                    preparedStatement.setString(4, fields[3]); // season
                    preparedStatement.setInt(5, fields[4].isEmpty() ? 0 : Integer.parseInt(fields[4])); // stage

                    // Validate and parse date
                    if (!fields[5].isEmpty() && fields[5].matches("\\d{4}-\\d{2}-\\d{2}")) {
                        preparedStatement.setDate(6, java.sql.Date.valueOf(fields[5])); // match_date
                    } else {
                        preparedStatement.setNull(6, java.sql.Types.DATE); // Set to null if date is invalid
                    }

                    // Set match_api_id and team IDs
                    preparedStatement.setInt(7, Integer.parseInt(fields[6])); // match_api_id
                    preparedStatement.setInt(8, fields[7].isEmpty() ? 0 : Integer.parseInt(fields[7])); // home_team_api_id
                    preparedStatement.setInt(9, fields[8].isEmpty() ? 0 : Integer.parseInt(fields[8])); // away_team_api_id
                    preparedStatement.setInt(10, fields[9].isEmpty() ? 0 : Integer.parseInt(fields[9])); // home_team_goal
                    preparedStatement.setInt(11, fields[10].isEmpty() ? 0 : Integer.parseInt(fields[10])); // away_team_goal

                    // Set player values for home and away teams
                    for (int i = 12; i < 80; i++) {
                        preparedStatement.setObject(i + 1, fields[i].isEmpty() ? null : Integer.parseInt(fields[i]));
                    }

                    // Set additional match data
                    preparedStatement.setString(80, fields[80]); // goal
                    preparedStatement.setString(81, fields[81]); // shoton
                    preparedStatement.setString(82, fields[82]); // shotoff
                    preparedStatement.setString(83, fields[83]); // foulcommit
                    preparedStatement.setString(84, fields[84]); // card
                    preparedStatement.setString(85, fields[85]); // cross
                    preparedStatement.setString(86, fields[86]); // corner
                    preparedStatement.setString(87, fields[87]); // possession

                    // Set betting odds
                    for (int i = 88; i < 96; i++) {
                        preparedStatement.setDouble(i + 1, fields[i].isEmpty() ? 0.0 : Double.parseDouble(fields[i]));
                    }

                    // Execute the insert statement
                    preparedStatement.executeUpdate();
                } catch (Exception e) {
                    System.err.println("Skipping row due to error: " + e.getMessage());
                }
            }
            System.out.println("Match data inserted successfully!");

        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("General error: " + e.getMessage());
        }
    }
}
