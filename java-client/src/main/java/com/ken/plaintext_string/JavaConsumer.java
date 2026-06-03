package com.ken.plaintext_string;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;

public class JavaConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        try (InputStream is = Consumer.class.getClassLoader().getResourceAsStream("kafka_config.properties")) {
            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load consumer configuration", e);
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList((props.getProperty("kafka.topic"))));
        System.out.println("Subscribing to the topic ===== " + props.getProperty("kafka.topic"));

        try {
            int count = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> consumerRecord : records) {

                    System.out.println("Record Value : " + consumerRecord.value());
                    count++;
                }
                System.out.println("Count of messages processed : " + count);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}