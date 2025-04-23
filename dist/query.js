"use strict";

const Inspector = require('./Inspector'); // Import the Inspector class
const inspector = new Inspector();
const mysql = require("mysql2/promise"); // Using promise-based API for async/await
const dotenv = require("dotenv");

// Load environment variables from .env file
dotenv.config();

// Database configuration
const dbConfig = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: process.env.DB_PORT ? parseInt(process.env.DB_PORT, 10) : undefined,
};

// Function to fetch all data
function fetchData() {
    return inspector.inspectAll("fetchData", async () => {
        let connection;
        try {
            connection = await mysql.createConnection(dbConfig);
            const [results] = await connection.execute(`SELECT * FROM sales;`);
            console.log('Results:', results);
            return { success: true, data: results };
        } catch (err) {
            console.error('Error executing query', err);
            return { success: false, error: err };
        } finally {
            if (connection) {
                await connection.end();
            }
        }
    });
}

function fetchCustomData() {
    return inspector.inspectAll("fetchCustomData", async () => {
        let connection;
        try {
            connection = await mysql.createConnection(dbConfig);
            const [results] = await connection.execute(`SELECT * FROM sales;`);
            console.log('Custom Data:', results);
            return { success: true, data: results };
        } catch (err) {
            console.error('Error executing query', err);
            return { success: false, error: err };
        } finally {
            if (connection) {
                await connection.end();
            }
        }
    });
}

// Export the functions
exports.fetchData = fetchData;
exports.fetchCustomData = fetchCustomData;
