package lambda;

import java.io.BufferedReader;
// import java.io.FileReader;
// import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
// import java.nio.file.Files;
// import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;

public class Load {
    // public static void main(String[] args) {
    //     System.out.println("main");
    //     try {
    //         //loadJSONtoDB();
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    // }

    public static void loadJSONtoDB(Request req, Inspector inspector) {
        // Step 1: Load environment variables
        String dbHost = System.getenv("DB_HOST");
        String dbUser = System.getenv("DB_USER");
        String dbPassword = System.getenv("DB_PASSWORD");
        String dbName = System.getenv("DB_NAME");
        String dbPort = System.getenv("DB_PORT");
        //System.out.println("host: " + dbHost + "\nuser: " + dbUser + "\npass: " + dbPassword + "\nname: " + dbName + "\nport: " + dbPort);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        try (S3Object s3Object = s3Client.getObject(new GetObjectRequest(req.getBucketname(), "transformed_data.json"))) {
            //get content of the file
            inspector.addTimeStamp("LOAD_FETCH_JSON_START");
            InputStream objectData = s3Object.getObjectContent();
            inspector.addTimeStamp("LOAD_FETCH_JSON_START");

            //Read the transformed JSON file
            List<Map<String, Object>> records = parseJSON(objectData);

            //Connect to the AWS RDS MySQL/MariaDB database
            String jdbcUrl = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
            //System.out.println("MADE IT RIGHT BEFORE JDBC CONNECTION, jdbcUrl: " + jdbcUrl);
            try (Connection connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)) {
                //System.out.println("Connected to the database successfully");

                // Restart the database (Necessary for testing purposes)
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP DATABASE IF EXISTS db;");
                    statement.execute("CREATE DATABASE IF NOT EXISTS db;");
                    statement.execute("USE db;");

                    String createTableQuery = """
                    CREATE TABLE IF NOT EXISTS sales
                    (item_type VARCHAR(32) NOT NULL,
                    order_priority    VARCHAR(1)      NOT NULL,
                    order_date VARCHAR(32) NOT NULL,
                    order_id BIGINT NOT NULL PRIMARY KEY,
                    units_sold BIGINT NOT NULL,
                    unit_price DECIMAL(10,2) NOT NULL,
                    unit_cost DECIMAL(10,2) NOT NULL,
                    total_revenue DECIMAL(10,2) NOT NULL,
                    total_cost DECIMAL(10,2) NOT NULL,
                    total_profit DECIMAL(10,2) NOT NULL);
                    """;
                    statement.execute(createTableQuery);
                }

                //Insert records into the database - USE BATCH INSERTS OF 10
                String insertQuery = """
                INSERT INTO sales (item_type, order_priority, order_date, order_id, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit )
                VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """;

                try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                    //For loop x10 here
                    for(int i = 0; i < req.getRow(); i += 10) {
                        for(int j = 0; j < 10; j++) {
                            preparedStatement.setString(1 + (j * 10), (String) records.get(i + j).get("Item Type"));
                            preparedStatement.setString(2 + (j * 10), (String) records.get(i + j).get("Order Priority"));
                            preparedStatement.setString(3 + (j * 10), (String) records.get(i + j).get("Order Date"));
                            preparedStatement.setLong(4 + (j * 10), ((Number) records.get(i + j).get("Order ID")).longValue());
                            preparedStatement.setLong(5 + (j * 10), ((Number) records.get(i + j).get("Units Sold")).longValue());
                            preparedStatement.setBigDecimal(6 + (j * 10), new java.math.BigDecimal(((Number) records.get(i + j).get("Unit Price")).doubleValue()));
                            preparedStatement.setBigDecimal(7 + (j * 10), new java.math.BigDecimal(((Number) records.get(i + j).get("Unit Cost")).doubleValue()));
                            preparedStatement.setBigDecimal(8 + (j * 10), new java.math.BigDecimal(((Number) records.get(i + j).get("Total Revenue")).doubleValue()));
                            preparedStatement.setBigDecimal(9 + (j * 10), new java.math.BigDecimal(((Number) records.get(i + j).get("Total Cost")).doubleValue()));
                            preparedStatement.setBigDecimal(10 + (j * 10), new java.math.BigDecimal(((Number) records.get(i + j).get("Total Profit")).doubleValue()));
                        }
                        //Insert every 10 json objects
                        preparedStatement.executeUpdate();
                    }
                }
                //System.out.println("Data loaded successfully!");
            }
        } catch (Exception e) {
            System.err.println("Database Connection Error");
            System.err.println("Error loading data: " + e.getMessage());
        }
    }



    private static List<Map<String, Object>> parseJSON(InputStream jsonData) {
        List<Map<String, Object>> records = new ArrayList<>();
    
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(jsonData))) {
            StringBuilder jsonBuilder = new StringBuilder();
            String line;
    
            // Read JSON data from the InputStream
            while ((line = reader.readLine()) != null) {
                jsonBuilder.append(line);
            }
    
            String jsonString = jsonBuilder.toString();
    
            // Remove square brackets (if they exist) for simpler parsing
            if (jsonString.startsWith("[") && jsonString.endsWith("]")) {
                jsonString = jsonString.substring(1, jsonString.length() - 1);
            }
    
            // Split objects within the array
            String[] objects = jsonString.split("},\\s*\\{");
    
            for (String object : objects) {
                object = object.trim();
    
                // Ensure objects have valid curly braces
                if (!object.startsWith("{")) {
                    object = "{" + object;
                }
                if (!object.endsWith("}")) {
                    object = object + "}";
                }
    
                // Parse the object into a Map
                Map<String, Object> record = parseJsonObject(object);
                records.add(record);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON data", e);
        }
    
        //System.out.println("LOAD - Parsed JSON size: " + records.size());
        return records;
    }
    
    private static Map<String, Object> parseJsonObject(String json) {
        Map<String, Object> map = new HashMap<>();
    
        // Remove outer curly braces
        json = json.substring(1, json.length() - 1).trim();
    
        // Split into key-value pairs
        String[] pairs = json.split(",\\s*");
    
        for (String pair : pairs) {
            String[] keyValue = pair.split(":", 2);
    
            // Extract key and value
            String key = keyValue[0].trim().replaceAll("^\"|\"$", ""); // Remove quotes
            String value = keyValue[1].trim().replaceAll("^\"|\"$", ""); // Remove quotes
    
            // Determine if the value is numeric or string
            if (value.matches("-?\\d+(\\.\\d+)?")) {
                map.put(key, value.contains(".") ? Double.parseDouble(value) : Integer.parseInt(value));
            } else {
                map.put(key, value);
            }
        }
    
        return map;
    }

}
