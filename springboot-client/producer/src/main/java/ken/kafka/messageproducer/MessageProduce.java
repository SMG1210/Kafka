package ken.kafka.messageproducer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class MessageProduce {

    private static final Logger logger = LoggerFactory.getLogger(MessageProduce.class);

    @Value("${app.kafka.topic}")
    private String topic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public MessageProduce(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String key, String message) {
        if (message == null) {
            logger.warn("⚠️ Attempted to send a null message. Skipping.");
            return;
        }

        try {
            logger.info("🚀 Sending message to topic '{}': key='{}', value='{}'", topic, key, message);

            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message)
                    .toCompletableFuture();

            future.whenComplete((result, ex) -> {
                if (ex == null && result != null) {
                    RecordMetadata metadata = result.getRecordMetadata();
                    logger.info("✅ Message sent to topic '{}' partition {} with offset {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    logger.error("❌ Failed to send message: key='{}', value='{}'", key, message, ex);
                }
            });
        } catch (Exception e) {
            logger.error("❌ Exception while sending message: key='{}', value='{}'", key, message, e);
        }
    }

    public String getTopic() {
        return topic;
    }
}
