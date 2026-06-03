from confluent_kafka import Consumer, TopicPartition
import configparser
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka-consumer")

# Load configuration
config = configparser.ConfigParser()
config.read('config/kafka_config.properties')
kafka_config = dict(config['default'])

# Log loaded configuration (mask sensitive info)
logger.info("Loaded Kafka Configuration (explicit):")
for key, value in kafka_config.items():
    masked_value = value if 'password' not in key.lower() else '****'
    logger.info(f"  {key}: {masked_value}")

# Define on_assign callback to log partition assignments
def on_assign(consumer, partitions):
    logger.info("Partitions assigned:")
    for p in partitions:
        logger.info(f"  Topic: {p.topic}, Partition: {p.partition}, Offset: {p.offset}")

# Initialize Consumer
try:
    consumer_conf = {
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'group.id': kafka_config['group.id'],
        'auto.offset.reset': kafka_config['auto.offset.reset'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanism': kafka_config['sasl.mechanism'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password']
    }

    logger.info("Kafka Consumer Configuration (used to initialize):")
    for key, value in consumer_conf.items():
        masked_value = value if 'password' not in key.lower() else '****'
        logger.info(f"  {key}: {masked_value}")

    consumer = Consumer(consumer_conf)

    topic = kafka_config['kafka.topic']
    consumer.subscribe([topic], on_assign=on_assign)
    logger.info(f"Subscribed to topic: {topic}")

except KeyError as e:
    logger.error(f"Configuration error: {e}")
    exit(1)
except Exception as e:
    logger.exception(f"Error initializing consumer: {e}")
    exit(1)

# Poll messages
try:
    count = 0
    while True:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue

        logger.info(f"Record Value: {msg.value().decode('utf-8')}")
        count += 1

        # Log current offset for the partition
        tp = TopicPartition(msg.topic(), msg.partition())
        offset = consumer.position([tp])[0].offset
        logger.info(f"Current offset for {msg.topic()} [Partition {msg.partition()}]: {offset}")
        logger.info(f"Total messages processed: {count}")

except KeyboardInterrupt:
    logger.info("Consumer interrupted by user.")
finally:
    consumer.close()
    logger.info("Kafka consumer closed.")
