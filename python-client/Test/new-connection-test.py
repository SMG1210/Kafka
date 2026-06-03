from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Define user configurations
user_configs = [
{'bootstrap_servers': 'localhost:443', 'username': 'Test', 'password': 'Test_Pass'}
    # Add more user configurations as needed
]

def test_kafka_connection(config):
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=config['bootstrap_servers'],
            sasl_mechanism='PLAIN',
            security_protocol='SASL_PLAINTEXT',
            sasl_plain_username=config['username'],
            sasl_plain_password=config['password']
        )
        topics = consumer.topics()
        sorted_topics = sorted(topics)
        print(f"Topics for user {config['username']}:")
        for topic in sorted_topics:
            print(topic)
        print("\n")
    except KafkaError as e:
        print(f"Failed to connect for user {config['username']}: {e}")

for config in user_configs:
    test_kafka_connection(config)
