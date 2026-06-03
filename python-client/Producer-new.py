import logging
from confluent_kafka import Producer, KafkaException
try:
        KAFKA_BOOTSTRAP_SERVERS = 'localhost:443'
        KAFKA_USERNAME = 'Test'
        KAFKA_PASSWORD = 'Test_Pass'
        KAFKA_TOPIC = 'prod-consum-test'

        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': KAFKA_USERNAME,
            'sasl.password': KAFKA_PASSWORD,
            'ssl.ca.location' : 'config/ca_dev.pem'
        }
        producer = Producer(conf)
        producer.produce(KAFKA_TOPIC, key="test-key", value="hello world1")
        producer.flush()
        logging.info("Message sent successfully")
        print("Message sent successfully")

except KafkaException as e:
        logging.error("KafkaError at ",str(e))
        print("KafkaError at ", str(e))

except Exception as exc:
        logging.error("Unhandled exception at ",str(exc), exc_info=True)
        print("Unhandled exception at ",str(exc), exc_info=True)

finally:
        try:
            producer.close()
        except Exception:
            pass