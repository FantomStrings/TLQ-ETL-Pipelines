package lambda;

import java.io.*;
import java.nio.charset.StandardCharsets;
//import java.nio.file.*;
import java.util.*;
// import java.util.stream.*;

// import com.amazonaws.services.lambda.runtime.Context;
// import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;

// import saaf.Inspector;
// import saaf.Response;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * lambda.Transform::handleRequest
 *
 * @author Caleb Krauter
 * @author Corey Young
 * @author Rick Adams
 */
public class Transform {

    public static void main(String[] args) {
        System.out.println("Entered Transform main.");
        try {
            Request req = new Request("Testing", "project.bucket462.tlq", "100_Sales_Records.csv", 100, 14);
            parseCSV(req, new Inspector());
            System.out.println("MADE IT PAST TRANSFORM PARSE IN MAIN");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Takes a request, reads from the expected csv file, trims some data and puts json data into s3
     * @param req
     * @return
     */
    public static String parseCSV(Request req, Inspector inspector) {

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        try (S3Object s3Object = s3Client.getObject(new GetObjectRequest(req.getBucketname(), req.getFilename()))) {
            //get content of the file
            inspector.addTimeStamp("TRANSFORM_FETCH_CSV_START");
            InputStream objectData = s3Object.getObjectContent();
            inspector.addTimeStamp("TRANSFORM_FETCH_CSV_END");
            //scanning data line by line
            String line;
            Scanner scanner = new Scanner(objectData);
            if(!scanner.hasNext()) {
                scanner.close();
                throw new IOException("CSV file is empty");
            }
            
            StringWriter sw = new StringWriter();
            //First line of file will be column titles
            String headerLine = scanner.nextLine();
            String[] headers = headerLine.split(",");
            sw.append("[\n");
            while (scanner.hasNext()) {
                line = scanner.nextLine();
                String[] values = line.split(",");
                sw.append("{\n");

                for (int i = 0; i < headers.length; i++) {
                    String header = headers[i].trim();
                    String value = values[i].trim();

                    // Skip unwanted fields
                    if (!header.equals("Region") && !header.equals("Country") && !header.equals("Sales Channel") && !header.equals("Ship Date")) {
                        sw.append('"' + header + "\": " + (isNumberField(header) ? value : '"' + value + '"') + ((i == headers.length - 1) ? "" : ",\n"));
                    }
                }
                sw.append("\n}");
                sw.append((scanner.hasNext() ? ",\n" : "\n]"));
            }
            scanner.close();
            byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
            InputStream is = new ByteArrayInputStream(bytes);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);
            meta.setContentType("text/plain");
            // Create new file on S3
            inspector.addTimeStamp("TRANSFORM_UPLOAD_JSON_START");
            s3Client.putObject(req.getBucketname(), "transformed_data.json", is, meta);
            inspector.addTimeStamp("TRANSFORM_UPLOAD_JSON_END");

            //System.out.println("PARSED JSON DATA =\n" + sw.toString());
            return sw.toString();

        } catch (IOException e) {
            System.err.println("Error while parsing CSV: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }


    private static boolean isNumberField(String header){
        //Or check contains unit || total || id
        return header.equals("Order ID") || header.equals("Units Sold") ||
        header.equals("Unit Price") || header.equals("Unit Cost") ||
        header.equals("Total Revenue") || header.equals("Total Cost") ||
        header.equals("Total Profit");
    }

    // private static String toJSONString(List<Map<String, String>> data) {
    //     StringBuilder json = new StringBuilder();
    //     json.append("[");

    //     for (int i = 0; i < data.size(); i++) {
    //         json.append("{\n");
    //         Map<String, String> record = data.get(i);
    //         List<String> fields = record.entrySet().stream()
    //             .map(entry -> String.format("  \"%s\": \"%s\"", entry.getKey(), entry.getValue()))
    //             .collect(Collectors.toList());
    //         json.append(String.join(",\n", fields));
    //         json.append("\n}");
    //         if (i < data.size() - 1) {
    //             json.append(",");
    //         }
    //     }

    //     json.append("\n]");
    //     return json.toString();
    // }

    // public static List<Map<String, Object>> fetchData() {
    //     // Mock implementation
    //     List<Map<String, Object>> result = new ArrayList<>();
    //     Map<String, Object> sample = new HashMap<>();
    //     sample.put("Key", "Value");
    //     result.add(sample);
    //     return result;
    // }

    // public static List<Map<String, Object>> fetchAggregatedData() {
    //     // Mock implementation
    //     List<Map<String, Object>> result = new ArrayList<>();
    //     Map<String, Object> sample = new HashMap<>();
    //     sample.put("AggregatedKey", "AggregatedValue");
    //     result.add(sample);
    //     return result;
    // }

}
