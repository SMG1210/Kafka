package com.ken;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConnectionTest {
    public static void main(String[] args) throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        try (InputStream is = Consumer.class.getClassLoader().getResourceAsStream("connection.props")) {
            properties.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load consumer configuration", e);
        }
        System.out.print("Successfully loaded the properties from config file...");
        String btstrp = properties.get("bootstrap.servers").toString();

        try (AdminClient adminClient = AdminClient.create(properties)) {

            System.out.println("\n" + "\n" + " WELCOME TO KAFKA :::: Connected successfully to Kafka broker " + btstrp
                    + "\n" + "\n");

        } catch (Exception e) {
            System.err.println("=====  Failed to connect to Kafka broker: ==========" + e.getMessage());
        }
    }
}
