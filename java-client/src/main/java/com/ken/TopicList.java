package com.ken;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.Consumer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

public class TopicList {
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
            // List all topics
            ListTopicsResult topics = adminClient.listTopics();
            System.out.println("Listing all topics in the environment:");
            // topics.names().get().forEach(System.out::println);
            Set<String> names = new TreeSet<>(topics.names().get());
            for (String name : names) {
                System.out.println("\t" + name);
            }
            System.out.println("Total topics count is = " + names.size());
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("===== Failed to list topics: ===== " + e.getMessage());
        } catch (Exception e) {
            System.err.println("===== Failed to connect to Kafka broker: ===== " + e.getMessage());
        }
    }
}
