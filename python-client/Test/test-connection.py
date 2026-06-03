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
    admin_client.close()
except Exception as e:
    print(f"===== Failed to connect to Kafka broker: {e} =====")
