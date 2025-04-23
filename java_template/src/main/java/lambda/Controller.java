package lambda;

// import java.io.*;
// import java.nio.charset.StandardCharsets;
//import java.nio.file.*;
import java.util.*;
// import java.util.stream.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
// import com.amazonaws.services.s3.AmazonS3;
// import com.amazonaws.services.s3.AmazonS3ClientBuilder;
// import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;
import saaf.Response;

// import com.amazonaws.services.s3.model.GetObjectRequest;
// import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * lambda.Controller::handleRequest
 *
 * @author Corey Young
 */
public class Controller implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        int row = request.getRow();
        int col = request.getCol();
        String bucketname = request.getBucketname();
        String filename = request.getFilename();

        Response response = new Response();
        response.setValue("Bucket:" + bucketname + " filename:" + filename + " size: " + row + " rows, " + col + " cols.");

        inspector.addTimeStamp("ENTERING_TRANSFORM");
        Transform.parseCSV(request, inspector);
        inspector.addTimeStamp("LEAVING_TRANSFORM");

        inspector.addTimeStamp("ENTERING_LOAD");
        Load.loadJSONtoDB(request, inspector);
        inspector.addTimeStamp("LEAVING_LOAD");

        inspector.addTimeStamp("ENTERING_QUERY");
        Query.fetchData(inspector);
        inspector.addTimeStamp("LEAVING_QUERY");

        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
