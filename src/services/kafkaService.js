// src/services/kafkaService.js
const { Kafka, Partitioners } = require('kafkajs');
const logger = require('../config/logger');

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'chat-app',
  brokers: process.env.KAFKA_BROKERS ? 
    process.env.KAFKA_BROKERS.split(',') : 
    ['kafka:9093'],
  retry: {
    initialRetryTime: 100,
    retries: 8
  },
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
  idempotent: true,
  maxInFlightRequests: 1,
});

const consumer = kafka.consumer({ 
  groupId: process.env.KAFKA_CONSUMER_GROUP || 'chat-group',
  maxWaitTimeInMs: 50,
  maxBytes: 5242880,
});

async function setup(io) {
  try {
    await Promise.all([
      producer.connect(),
      consumer.connect()
    ]);

    await consumer.subscribe({ topic: 'chat-messages', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const parsedMessage = JSON.parse(message.value.toString());
          logger.info(`Processing Kafka message [${partition}] for room ${parsedMessage.roomId}`);
          io.to(parsedMessage.roomId).emit('receive-message', parsedMessage);
        } catch (error) {
          logger.error('Error processing Kafka message:', error);
        }
      },
    });

    logger.info('Kafka producer and consumer connected');
  } catch (error) {
    logger.error('Kafka setup failed:', error);
    throw error;
  }
}

async function shutdown() {
  await Promise.all([
    producer.disconnect(),
    consumer.disconnect()
  ]);
}

module.exports = {
  producer,
  consumer,
  setup,
  shutdown
};
