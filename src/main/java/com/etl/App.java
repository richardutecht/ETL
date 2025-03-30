package com.etl;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Consumer;


import java.util.Map;

// Boilerplate Java ETL Lambda
// Context: MongoDB query extraction, transformation and load to Postgres

public class App implements RequestHandler<Map<String, String>, String>{

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    /*
    
    MongDB Doc Must Have..
    
    {
        "type": "Apple"
    }

    Postgres Table:
    CREATE TABLE fruit_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    );
    
    */

    // MongoDB connection parameters
    private String MONGO_URI = "mongodb://localhost:27017";
    private String MONGO_DB_NAME = "fruitDB";
    private String MONGO_COLLECTION_NAME = "fruits";

    // PostgreSQL connection parameters
    private String POSTGRES_URL = "jdbc:postgresql://localhost:5432/fruitdb";
    private String POSTGRES_USER = "postgres";
    private String POSTGRES_PASSWORD = "password";

    @Override
    public String handleRequest(Map<String, String> input, Context context) {

        // Override the defaults with Lambda Parameters if they are set
        if (input.containsKey("MONGO_URI")) MONGO_URI = input.get("MONGO_URI");
        if (input.containsKey("MONGO_DB_NAME")) MONGO_URI = input.get("MONGO_DB_NAME");
        if (input.containsKey("MONGO_COLLECTION_NAME")) MONGO_URI = input.get("MONGO_COLLECTION_NAME");
        if (input.containsKey("POSTGRES_URL")) MONGO_URI = input.get("POSTGRES_URL");
        if (input.containsKey("POSTGRES_USER")) MONGO_URI = input.get("POSTGRES_USER");
        if (input.containsKey("POSTGRES_PASSWORD")) MONGO_URI = input.get("POSTGRES_PASSWORD");


        List<String> fruits = extractData();
        if (fruits != null && !fruits.isEmpty()) {
            transformData(fruits);
            loadData(fruits);
            return "ETL Process Successful";
        } else {
            return "No Data Found in MongoDB";
        }
    }

    private List<String> extractData() {
        List<String> fruits = new ArrayList<>();
        try (MongoClient mongoClient = new MongoClient(MONGO_URI)) {
            MongoDatabase database = mongoClient.getDatabase(MONGO_DB_NAME);
            MongoCollection<Document> collection = database.getCollection(MONGO_COLLECTION_NAME);

            collection.find().forEach((Consumer<? super Document>) (Document doc) -> {
                String fruitType = doc.getString("type");
                if (fruitType != null && !fruitType.isEmpty()) {
                    fruits.add(fruitType);
                }
            });
        } catch (Exception e) {
            logger.error("Error extracting data from MongoDB", e);
        }
        return fruits;
    }

    private void transformData(List<String> fruits) {
        // Function to perform any necessary transformations to format, roll up or split into multiple data sets
        logger.info("Transforming data: " + fruits);
    }

    private void loadData(List<String> fruits) {
        try (Connection connection = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD)) {
            String insertQuery = "INSERT INTO fruit_table (name) VALUES (?)";
            try (PreparedStatement stmt = connection.prepareStatement(insertQuery)) {
                for (String fruit : fruits) {
                    stmt.setString(1, fruit);
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
        } catch (Exception e) {
            logger.error("Error loading data into PostgreSQL", e);
        }
    }
}



