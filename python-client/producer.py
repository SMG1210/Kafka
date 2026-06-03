from confluent_kafka import Producer
import configparser

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
# Load configuration
config = configparser.ConfigParser()
config.read('config/kafka_config.properties')
kafka_config = dict(config['default'])

# Print loaded configuration for debugging
print("Loaded Kafka Configuration:", kafka_config)

# Initialize Consumer
try:
    producer=Producer({
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'group.id': kafka_config['group.id'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanism': kafka_config['sasl.mechanism'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password']        
    })
    topic = kafka_config['kafka.topic']
except KeyError as e:
    print(f"Configuration error: {e}")
except Exception as e:
    print(f"Error initializing consumer: {e}")
    
for i in range(10):
    key = f"id_{i}"
    value = f"Hello Kafka Producer for testing {i}"
    producer.produce(topic, key=key, value=value, callback=delivery_report)

producer.flush()
