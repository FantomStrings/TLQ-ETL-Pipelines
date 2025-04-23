package lambda;

// import java.io.BufferedReader;
// import java.io.FileNotFoundException;
// import java.io.FileReader;
// import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;

import saaf.Inspector;


public class Query {

    // Database configuration (initialized in loadDB method)
    private static String DB_HOST;
    private static String DB_USER;
    private static String DB_PASSWORD;
    private static String DB_NAME;
    private static String DB_PORT;
    private static String DB_URL;

    // Method to fetch all data (joined from both Order and Item tables)
    public static String fetchData(Inspector inspector) {
        //System.out.println("MADE IT TO QUERY");
        loadDB();
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            //System.out.println("QUERY - Database connected.");
            String query = "SELECT * FROM sales";
            String result = null;
            inspector.addTimeStamp("QUERY_EXECUTE_START");
            try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
                inspector.addTimeStamp("QUERY_EXECUTE_END");

                StringBuilder resultBuilder = new StringBuilder();
                int columnCount = rs.getMetaData().getColumnCount(); // Get the number of columns
                // Iterate through each row of the ResultSet
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        resultBuilder.append(rs.getMetaData().getColumnName(i))
                                .append(": ")
                                .append(rs.getString(i))
                                .append("\t");
                    }
                    resultBuilder.append("\n");
                }
                result = resultBuilder.toString();
                //System.out.println("QUERY Data Retrieved:\n" + result);
            }

            return result;
        } catch (SQLException e) {
            System.err.println("Error executing query: " + e.getMessage());
            return null;
        }
    }

    // Method to load database credentials from .env
    public static void loadDB() {
        try {
            DB_HOST = System.getenv("DB_HOST");
            DB_USER = System.getenv("DB_USER");
            DB_PASSWORD = System.getenv("DB_PASSWORD");
            DB_NAME = System.getenv("DB_NAME");
            DB_PORT = System.getenv("DB_PORT");
    
            // Make sure URL is properly formatted
            DB_URL = "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;
            
            // Print the final DB_URL to verify
            //System.out.println("DB_URL: " + DB_URL);
            
        } catch (Exception e) {
            System.err.println("Unexpected Error in Query.LoadDB: " + e.getMessage());
        }
    }

    // public static void main(String[] args) {
    //     // Load database configuration
    //     loadDB();

    //     // Fetch all data
    //     System.out.println("Fetching All Data...");
    //     fetchData();

    //     // Fetch aggregated data
    //     //System.out.println("\nFetching Aggregated Data...");
    //     //();
    // }

}
