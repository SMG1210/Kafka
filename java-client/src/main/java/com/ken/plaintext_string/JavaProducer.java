package com.ken.plaintext_string;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

public class JavaProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        try (InputStream is = Producer.class.getClassLoader().getResourceAsStream("kafka_config.properties")) {
            props.load(is);
            System.out.println("###### Loaded the Producer configs");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load producer configuration", e);
        }
        // Create Producer Config
        System.out.println("###### 0000000000000000000");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        System.out.println("###### 11111111111111111");

        for (int i = 0; i < 2; i++) {
            System.out.println("###### 22222222222222222");

            String key = "id_" + Integer.toString(i);
            String value = "{Latest offset test messages}";
            ProducerRecord<String, String> prodRecord = new ProducerRecord<>(props.getProperty("kafka.topic"), key,
                    value);
            System.out.println("###### 3333333333333333");

            // send data with callback
            producer.send(prodRecord, new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("###### 44444444444444");

                    if (exception == null) {
                        System.out.println(
                                "Record {Key : value} " + "\t{" + key + " : " + value + "} sent successfully.");
                    } else {
                        System.err.println("Error sending record {Key : value} " + "\t{" + key + " : " + value
                                + "} due to " + exception.getMessage());
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
