from kafka.admin import KafkaAdminClient
import configparser

# Load configuration
config = configparser.ConfigParser()
config.read('connection.props')
kafka_config = dict(config['default'])

bootstrap_servers = kafka_config['bootstrap.servers']
print("Successfully loaded the properties from config file...")

try:
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    print(f"\n\nWELCOME TO KAFKA :::: Connected successfully to Kafka broker {bootstrap_servers}\n\n")

    # List all topics
    topic_metadata = admin_client.list_topics()
    topic_names = sorted(topic_metadata)
    print("Listing all topics in the environment:")
    for name in topic_names:
        print(f"\t{name}")
    print(f"Total topics count is = {len(topic_names)}")

    admin_client.close()
except Exception as e:
    print(f"===== Failed to list topics: {e} =====")
