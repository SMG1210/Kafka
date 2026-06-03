
package ken.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import ken.kafka.messageproducer.MessageProduce;

import java.util.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private MessageProduce producer;

    @PostMapping("/produce")
    public ResponseEntity<String> produce(@RequestBody Map<String, String> payload) {
        String key = null;
        String value = null;

        if (payload.size() == 1) {
            Map.Entry<String, String> entry = payload.entrySet().iterator().next();
            key = entry.getKey();
            value = entry.getValue();

            // If key is same as value (e.g., {"welcome"}), treat key as null
            if (key.equals(value)) {
                value = key;
                key = null;
            }
        }

        producer.sendMessage(key, value);
        return ResponseEntity.ok(String.format(
                "✅ Message sent to topic '%s' with key='%s' and value='%s'",
                producer.getTopic(), key, value));
    }

}
