const { Kafka, logLevel } = require('kafkajs');
require('dotenv').config();

// Define Kafka configuration
const kafkaConfig = {
  clientId: 'nodejs-producer',
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
const producer = kafka.producer();

// Custom logger for KafkaJS
function KafkaJSLogger(logLevel) {
  return ({ namespace, level, label, log }) => {
    console.log(`[${label}] ${log.timestamp} ${namespace} - ${log.message}`);
    if (log.extra) {
      console.log(log.extra);
    }
  };
}

// Producer logic
const run = async () => {
  try {
    console.log('###### Connecting to Kafka...');
    await producer.connect();
    console.log('###### Connected to Kafka');

    for (let i = 0; i < 10; i++) {
      const key = `id_${i}`;
      const value = `Node.js producer ${i}`;

      console.log('###### Sending message:', { key, value });

      await producer.send({
        topic: process.env.KAFKA_TOPIC,
        messages: [{ key, value }],
      });

      console.log(`Record {Key : Value} \t{${key} : ${value}} sent successfully.`);
    }

  } catch (err) {
    console.error('❌ Error in producer:', err);
  } finally {
    await producer.disconnect();
    console.log('###### Producer disconnected');
  }
};

run();
