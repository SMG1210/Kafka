const { Kafka, logLevel } = require('kafkajs');
require('dotenv').config();

// Define Kafka configuration
const kafkaConfig = {
  clientId: 'nodejs-consumer',
  brokers: process.env.KAFKA_BROKERS.split(','),
  ssl: true,
  sasl: {
    mechanism: process.env.SASL_MECHANISM,
    username: process.env.SASL_USERNAME,
    password: process.env.SASL_PASSWORD,
  },
 // logLevel: logLevel.DEBUG,
  logCreator: KafkaJSLogger,
};

// Log the Kafka configuration (excluding sensitive info)
console.log("Kafka Config:", JSON.stringify({
  clientId: kafkaConfig.clientId,
  brokers: kafkaConfig.brokers,
  ssl: kafkaConfig.ssl,
  sasl: {
    mechanism: kafkaConfig.sasl.mechanism,
    username: kafkaConfig.sasl.username,
    // password intentionally omitted
  }
}, null, 2));

// Create Kafka instance
const kafka = new Kafka(kafkaConfig);
const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID });

// Custom logger for KafkaJS
function KafkaJSLogger(logLevel) {
  return ({ namespace, level, label, log }) => {
    console.log(`[${label}] ${log.timestamp} ${namespace} - ${log.message}`);
    if (log.extra) {
      console.log(log.extra);
    }
  };
}

// Consumer logic
const run = async () => {
  try {
    console.log('###### Connecting consumer...');
    await consumer.connect();
    console.log('###### Consumer connected.');

    await consumer.subscribe({ topic: process.env.KAFKA_TOPIC, fromBeginning: true });
    console.log(`###### Subscribed to topic: ${process.env.KAFKA_TOPIC}`);

    let count = 0;

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value ? message.value.toString() : '[null]';
        console.log(`Record Value: ${value}`);
        count++;
        console.log(`Count of messages processed: ${count}`);
      },
    });

  } catch (err) {
    console.error(`[❌ Kafka Consumer Error] ${err.message}`, err);
  }
};

run();
