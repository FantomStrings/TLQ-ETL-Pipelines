var AWS = require('aws-sdk');
var s3 = new AWS.S3();
const csv = require('csv-parser');
const stream = require('stream');
const { loadJSONtoDB } = require('./load');
const { fetchData } = require('./query');


// Helper function to retrieve data from the CSV file
function getCSVData(bucketName, fileName) {
    return new Promise((resolve, reject) => {
        var params = { Bucket: bucketName, Key: fileName };
        s3.getObject(params, (err, data) => {
            if (err) {
                console.error('Error retrieving file from S3:', err);
                return reject(err);
            }
            resolve(data.Body); // Return raw buffer to avoid encoding issues
        });
    });
}

// Function to parse CSV data
function parseCSV(csvBuffer) {
    return new Promise((resolve, reject) => {
        const results = [];
        const csvStream = new stream.Readable();
        csvStream._read = () => {}; // No-op
        csvStream.push(csvBuffer);
        csvStream.push(null);

        csvStream
            .pipe(csv())
            .on('data', (row) => {
                const { Region, Country, 'Sales Channel': salesChannel, 'Ship Date': shipDate, ...filteredElement } = row;
                results.push(filteredElement);
            })
            .on('end', () => resolve(JSON.stringify(results, null, 2)))
            .on('error', (error) => {
                console.error("Error parsing CSV data:", error);
                reject(error);
            });
    });
}

// Function to upload transformed data back to S3
function uploadTransformedData(bucketName, newFileName, jsonData) {
    return new Promise((resolve, reject) => {
        const params = {
            Bucket: bucketName,
            Key: newFileName,
            Body: jsonData,
            ContentType: 'application/json',
        };

        s3.putObject(params, (err, data) => {
            if (err) {
                console.error('Error uploading transformed data:', err);
                return reject(err);
            }
            console.log('File uploaded successfully:', newFileName);
            resolve(data);
        });
    });
}

// Lambda handler function
exports.handler = async function(event, context) {
    const inspector = new (require('./Inspector'))();


    inspector.inspectAll();

    const bucketName = event.bucketname;
    const originalFileName = event.filename;

    // const bucketName = 'project.bucket462.tlq';
    // const originalFileName = '10000_Sales_Records.csv';
    const newFileName = 'transformed.json';
    try {
        console.log("Fetching file from S3...");
        const csvBuffer = await getCSVData(bucketName, originalFileName);
        
        console.log("Parsing CSV data...");
        const startTransformTime = Date.now();
        inspector.addTimeStamp("------------------Transform START");
        const jsonData = await parseCSV(csvBuffer);
        inspector.addTimeStamp("------------------Transform END");
        inspector.addTimeStamp("------------------Transform RUNTIME", startTransformTime);

        console.log("Uploading transformed data back to S3...");
        const uploadTransformedJSONStart = Date.now();
        inspector.addTimeStamp("------------------UploadTransformedJSON START");
        await uploadTransformedData(bucketName, newFileName, jsonData);
        inspector.addTimeStamp("------------------UploadTransformedJSON END");
        inspector.addTimeStamp("------------------UploadTransformedJSON RUNTIME", uploadTransformedJSONStart);

        const loadDBStart = Date.now();
        inspector.addTimeStamp("------------------Load START");
        await loadJSONtoDB(inspector);
        inspector.addTimeStamp("------------------Load END");
        inspector.addTimeStamp("------------------Load RUNTIME", loadDBStart);

        const queryDBFetchStart = Date.now();
        inspector.addTimeStamp("------------------QuerySelectAllSales START");
        const result = await fetchData();
        inspector.addTimeStamp("------------------QuerySelectAllSales END");
        inspector.addTimeStamp("------------------QuerySelectAllSales RUNTIME", queryDBFetchStart);

        inspector.inspectAllDeltas();
        const inspectionResults = inspector.finish();

        console.log('Inspection Results:', inspectionResults);

        return {
            statusCode: 200,
            body: {
                result, // Already stringified
                metrics: inspectionResults,
            },
        };
    } catch (error) {
        console.error("Error in Lambda execution:", error);
        return {
            statusCode: 500,
            body: { error: 'Internal Server Error' },
        };
    }
};
