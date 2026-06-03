package ken.kafka.controller;

import ken.kafka.messageconsumer.MessageConsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private MessageConsumer consumer;

    @GetMapping("/consume")
    public ResponseEntity<Map<String, Object>> consumeAndFetchMessages() {
        consumer.setConsumerActive(true);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "🟢 Consumer is active and listening (check logs)");
        response.put("totalMessages", consumer.getMessageCount());
        // List<Map<String, String>> messages = consumer.getConsumedMessages().stream()
        // .map(entry -> {
        // Map<String, String> map = new HashMap<>();
        // map.put("key", entry.getKey());
        // map.put("value", entry.getValue());
        // return map;
        // }).collect(Collectors.toList());
        response.put("messages", consumer.getConsumedMessages());
        return ResponseEntity.ok(response);
    }

}
