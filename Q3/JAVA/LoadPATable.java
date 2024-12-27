import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoadPATable {
    private static final String JDBC_URL = "jdbc:oracle:thin:@acaddbprod.uta.edu:1523/pcse01p.data.uta.edu";
    private static final String USER = "rxs8010";
    private static final String PASS = "Qwertysingh1";
    private static final String CSV_FILE_PATH = "C:\\Users\\rajat\\OneDrive\\Desktop\\project1\\Q3\\csv\\Player_Attributes.csv";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASS);
             BufferedReader lineReader = new BufferedReader(new FileReader(CSV_FILE_PATH))) {

            String sql = "MERGE INTO PLAYER_ATTRIBUTES PA " +
                    "USING (SELECT ? as id, ? as player_fifa_api_id, ? as player_api_id, ? as attribute_date, ? as overall_rating, " +
                    "? as potential, ? as preferred_foot, ? as attacking_work_rate, ? as defensive_work_rate, ? as crossing, " +
                    "? as finishing, ? as heading_accuracy, ? as short_passing, ? as volleys, ? as dribbling, ? as curve, " +
                    "? as free_kick_accuracy, ? as long_passing, ? as ball_control, ? as acceleration, ? as sprint_speed, " +
                    "? as agility, ? as reactions, ? as balance, ? as shot_power, ? as jumping, ? as stamina, ? as strength, " +
                    "? as long_shots, ? as aggression, ? as interceptions, ? as positioning, ? as vision, ? as penalties, " +
                    "? as marking, ? as standing_tackle, ? as sliding_tackle, ? as gk_diving, ? as gk_handling, ? as gk_kicking, " +
                    "? as gk_positioning, ? as gk_reflexes FROM DUAL) SRC " +
                    "ON (PA.id = SRC.id) " +
                    "WHEN MATCHED THEN " +
                    "  UPDATE SET PA.player_fifa_api_id = SRC.player_fifa_api_id, PA.player_api_id = SRC.player_api_id, " +
                    "PA.attribute_date = SRC.attribute_date, PA.overall_rating = SRC.overall_rating, PA.potential = SRC.potential, " +
                    "PA.preferred_foot = SRC.preferred_foot, PA.attacking_work_rate = SRC.attacking_work_rate, " +
                    "PA.defensive_work_rate = SRC.defensive_work_rate, PA.crossing = SRC.crossing, PA.finishing = SRC.finishing, " +
                    "PA.heading_accuracy = SRC.heading_accuracy, PA.short_passing = SRC.short_passing, PA.volleys = SRC.volleys, " +
                    "PA.dribbling = SRC.dribbling, PA.curve = SRC.curve, PA.free_kick_accuracy = SRC.free_kick_accuracy, " +
                    "PA.long_passing = SRC.long_passing, PA.ball_control = SRC.ball_control, PA.acceleration = SRC.acceleration, " +
                    "PA.sprint_speed = SRC.sprint_speed, PA.agility = SRC.agility, PA.reactions = SRC.reactions, PA.balance = SRC.balance, " +
                    "PA.shot_power = SRC.shot_power, PA.jumping = SRC.jumping, PA.stamina = SRC.stamina, PA.strength = SRC.strength, " +
                    "PA.long_shots = SRC.long_shots, PA.aggression = SRC.aggression, PA.interceptions = SRC.interceptions, " +
                    "PA.positioning = SRC.positioning, PA.vision = SRC.vision, PA.penalties = SRC.penalties, PA.marking = SRC.marking, " +
                    "PA.standing_tackle = SRC.standing_tackle, PA.sliding_tackle = SRC.sliding_tackle, PA.gk_diving = SRC.gk_diving, " +
                    "PA.gk_handling = SRC.gk_handling, PA.gk_kicking = SRC.gk_kicking, PA.gk_positioning = SRC.gk_positioning, " +
                    "PA.gk_reflexes = SRC.gk_reflexes " +
                    "WHEN NOT MATCHED THEN " +
                    "  INSERT (id, player_fifa_api_id, player_api_id, attribute_date, overall_rating, potential, preferred_foot, " +
                    "attacking_work_rate, defensive_work_rate, crossing, finishing, heading_accuracy, short_passing, volleys, " +
                    "dribbling, curve, free_kick_accuracy, long_passing, ball_control, acceleration, sprint_speed, agility, " +
                    "reactions, balance, shot_power, jumping, stamina, strength, long_shots, aggression, interceptions, positioning, " +
                    "vision, penalties, marking, standing_tackle, sliding_tackle, gk_diving, gk_handling, gk_kicking, " +
                    "gk_positioning, gk_reflexes) " +
                    "  VALUES (SRC.id, SRC.player_fifa_api_id, SRC.player_api_id, SRC.attribute_date, SRC.overall_rating, " +
                    "SRC.potential, SRC.preferred_foot, SRC.attacking_work_rate, SRC.defensive_work_rate, SRC.crossing, " +
                    "SRC.finishing, SRC.heading_accuracy, SRC.short_passing, SRC.volleys, SRC.dribbling, SRC.curve, " +
                    "SRC.free_kick_accuracy, SRC.long_passing, SRC.ball_control, SRC.acceleration, SRC.sprint_speed, SRC.agility, " +
                    "SRC.reactions, SRC.balance, SRC.shot_power, SRC.jumping, SRC.stamina, SRC.strength, SRC.long_shots, " +
                    "SRC.aggression, SRC.interceptions, SRC.positioning, SRC.vision, SRC.penalties, SRC.marking, SRC.standing_tackle, " +
                    "SRC.sliding_tackle, SRC.gk_diving, SRC.gk_handling, SRC.gk_kicking, SRC.gk_positioning, SRC.gk_reflexes)";

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                String lineText;
                int lineNumber = 0;
                lineReader.readLine(); // Skip header line

                while ((lineText = lineReader.readLine()) != null) {
                    lineNumber++;
                    String[] data = lineText.split(",", -1); // -1 to keep trailing empty fields

                    if (data.length != 42) {
                        System.err.println("Line " + lineNumber + ": Data length mismatch. Expected 42, got " + data.length + ". Skipping row: " + lineText);
                        continue;
                    }

                    try {
                        for (int i = 0; i < 42; i++) {
                            if (data[i].isEmpty()) {
                                preparedStatement.setNull(i + 1, java.sql.Types.NULL);
                            } else if (i == 3) { // attribute_date
                                try {
                                    Date parsedDate = DATE_FORMAT.parse(data[i]);
                                    preparedStatement.setDate(i + 1, new java.sql.Date(parsedDate.getTime()));
                                } catch (ParseException e) {
                                    System.err.println("Line " + lineNumber + ": Invalid date format. Setting to NULL. Value: " + data[i]);
                                    preparedStatement.setNull(i + 1, java.sql.Types.DATE);
                                }
                            } else if (i == 6 || i == 7 || i == 8) { // String fields
                                preparedStatement.setString(i + 1, data[i]);
                            } else { // Integer fields
                                try {
                                    preparedStatement.setInt(i + 1, Integer.parseInt(data[i]));
                                } catch (NumberFormatException e) {
                                    System.err.println("Line " + lineNumber + ": Invalid integer format. Setting to NULL. Column: " + (i + 1) + ", Value: " + data[i]);
                                    preparedStatement.setNull(i + 1, java.sql.Types.INTEGER);
                                }
                            }
                        }

                        int result = preparedStatement.executeUpdate();
                        if (result == 0) {
                            System.out.println("Line " + lineNumber + ": No rows affected. This could indicate a problem.");
                        }
                    } catch (SQLException e) {
                        System.err.println("Line " + lineNumber + ": SQL error. " + e.getMessage() + " Skipping row: " + lineText);
                    }
                }
                System.out.println("Data loading completed. Check error log for any skipped rows.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
