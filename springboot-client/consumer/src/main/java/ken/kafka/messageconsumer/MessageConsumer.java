
package ken.kafka.messageconsumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);
    private final AtomicInteger messageCount = new AtomicInteger(0);
    private final List<Map.Entry<String, String>> consumedMessages = new CopyOnWriteArrayList<>();
    private final AtomicBoolean consumerActive = new AtomicBoolean(false);

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<String, String> record) {
        if (consumerActive.get()) {
            int count = messageCount.incrementAndGet();
            consumedMessages.add(Map.entry(record.key(), record.value()));

            logger.info("📥 Consumed message from topic '{}': key='{}', value='{}'",
                    record.topic(), record.key(), record.value());
            logger.info("🔢 Total messages consumed: {}", count);
        }
    }

    public int getMessageCount() {
        return messageCount.get();
    }

    public List<Map.Entry<String, String>> getConsumedMessages() {
        return consumedMessages;
    }

    public void setConsumerActive(boolean active) {
        consumerActive.set(active);
    }
}
