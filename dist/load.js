
"use strict";
const AWS = require('aws-sdk');
const mysql = require('mysql2/promise');
const dotenv = require('dotenv');
// Load environment variables
dotenv.config();
const s3 = new AWS.S3();

function getJSONData(bucketName, fileName) {
    return new Promise((resolve, reject) => {
        const params = { Bucket: bucketName, Key: fileName };
        s3.getObject(params, (err, data) => {
            if (err) {
                console.error('Error retrieving file from S3:', err);
                return reject(err);
            }
            try {
                const jsonData = JSON.parse(data.Body.toString('utf-8')); // Parse JSON
                resolve(jsonData);
            } catch (parseError) {
                console.error('Error parsing JSON data:', parseError);
                reject(parseError);
            }
        });
    });
}

const loadJSONtoDB = async (inspectorInstanceRef) => {
    const bucketName = 'javascripts3';
    const jsonFromS3 = 'transformed_100.json';
    let connection;
    try {
        // Fetch JSON data from S3
        const fetchS3JSONObjectStart= Date.now();
        inspectorInstanceRef.addTimeStamp("------------------FetchS3JSONObject_WITHIN_LOAD... START");
        const records = await getJSONData(bucketName, jsonFromS3);
        inspectorInstanceRef.addTimeStamp("------------------FetchS3JSONObject_WITHIN_LOAD... END");
        inspectorInstanceRef.addTimeStamp("------------------FetchS3JSONObject_WITHIN_LOAD... RUNTIME", fetchS3JSONObjectStart);

        console.log("JSON data retrieved:", records);

        // Step 2: Connect to the AWS RDS MySQL/MariaDB database
        console.log(process.env.DB_HOST);
        console.log(process.env.DB_USER);
        console.log(process.env.DB_PASSWORD);
        console.log(process.env.DB_NAME);
        console.log(process.env.DB_PORT);

        connection = await mysql.createConnection({
            host: process.env.DB_HOST,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            database: process.env.DB_NAME,
            port: process.env.DB_PORT,
        });
        console.log("Connected to DB successfully");

        // Create database and table
        const createDBQuery = `
          CREATE DATABASE IF NOT EXISTS db1;
        `;
        await connection.execute(createDBQuery);
        await connection.changeUser({ database: 'db1' });

        const createTableQuery = `
          CREATE TABLE IF NOT EXISTS sales (
            item_type         VARCHAR(32)     NOT NULL,
            order_priority    VARCHAR(1)      NOT NULL,
            order_date        VARCHAR(32)     NOT NULL,
            order_id          BIGINT          NOT NULL    PRIMARY KEY,
            units_sold        BIGINT          NOT NULL,
            unit_price        DECIMAL(10,2)   NOT NULL,
            unit_cost         DECIMAL(10,2)   NOT NULL,
            total_revenue     DECIMAL(10,2)   NOT NULL,
            total_cost        DECIMAL(10,2)   NOT NULL,
            total_profit      DECIMAL(10,2)   NOT NULL
          );
        `;
        await connection.execute(createTableQuery);
        const query = `
              INSERT IGNORE INTO sales (
                  item_type, 
                  order_priority, 
                  order_date, 
                  order_id, 
                  units_sold, 
                  unit_price, 
                  unit_cost, 
                  total_revenue, 
                  total_cost, 
                  total_profit
              ) VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ;
            `;
        // Insert records into the database

        for (let i = 0; i < records.length; i += 10) {
            const batch = records.slice(i, i + 10);          
            // Flatten the values for all records in the batch
            const values = batch.flatMap(record => [
              record["Item Type"],
              record["Order Priority"],
              record["Order Date"],
              record["Order ID"],
              record["Units Sold"],
              record["Unit Price"],
              record["Unit Cost"],
              record["Total Revenue"],
              record["Total Cost"],
              record["Total Profit"],
            ]);

            await connection.execute(query, values); // Use a batch query if supported
        }

        // for (const record of records) {
        //     const values = [
        //         record["Item Type"],
        //         record["Order Priority"],
        //         record["Order Date"],
        //         record["Order ID"],
        //         record["Units Sold"],
        //         record["Unit Price"],
        //         record["Unit Cost"],
        //         record["Total Revenue"],
        //         record["Total Cost"],
        //         record["Total Profit"],
        //     ];
        //     await connection.execute(query, values);
        // }
        console.log('Data loaded successfully!');
    } catch (error) {
        console.error('Error loading data:', error);
    } finally {
        await connection.end();
    }
};

exports.loadJSONtoDB = loadJSONtoDB;
(0, exports.loadJSONtoDB)().catch(console.error);
