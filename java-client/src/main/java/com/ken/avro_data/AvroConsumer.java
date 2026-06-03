package com.ken.avro_data;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class AvroConsumer {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        Properties props = new Properties();
        try (InputStream is = Consumer.class.getClassLoader().getResourceAsStream("kafka_avro_config.properties")) {
            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load consumer configuration", e);
        }

        // Create Kafka Consumer
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList((props.getProperty("kafka.topic"))));
        System.out.println("Subscribed to topic: " + props.getProperty("kafka.topic"));

        // Poll for messages
        try (FileWriter fileWriter = new FileWriter(
                "C:\\Users\\SMG01\\VSCODE-Projects\\kafka-java-api\\src\\main\\resources\\"
                        + (props.getProperty("kafka.topic")) +
                        ".json")) {
            System.out.println("Started consuming records ......");
            int count = 0;
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(10));

                if (records.isEmpty()) {
                    System.out.println("No messages received.... Polling again!!");
                }

                for (ConsumerRecord<String, GenericRecord> record : records) {

                    // System.out.printf("Key: %s | Value: %s%n", record.key(), record.value());
                    fileWriter.write(record.value().toString() + "\n");
                    count++;
                    System.out.println("Total processed messages: " + count);
                    fileWriter.flush();

                }
                // fileWriter.close();

            }
        } catch (WakeupException e) {
            System.out.println("Kafka Consumer interrupted!!");
        } catch (Exception e) {
            System.err.println("Error consuming messages: " + e.getMessage());
        } finally {
            // Clean up and close the consumer
            consumer.close();
            System.out.println("Kafka Consumer closed.");
        }
    }

}
