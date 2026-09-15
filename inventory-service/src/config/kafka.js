import { Kafka, logLevel } from "kafkajs";
import { config } from "./index.js";
import {logger} from "./logger.js";


export const kafka = new Kafka({
     clientId: config.KAFKA_CLIENT_ID,
     brokers: [config.KAFKA_BROKER || 'localhost:9093'],
     logLevel: logLevel.ERROR,
     retry: {
          initialRetryTime: 300,
          retries: 8,
          maxRetryTime: 30000,
     },
});

// Producer (for publishing SEAT_AVAILABILITY_UPDATED)
export const producer = kafka.producer({
     allowAutoTopicCreation: true,
     transactionTimeout: 30000,
     idempotent: true,
     maxInFlightRequests: 5,
     retry: {
          retries: 5,
     },
});

let isProducerConnected = false;

export const connectProducer = async () => {
     if (!isProducerConnected) {
          await producer.connect();
          isProducerConnected = true;
          logger.info('Kafka producer connected');
     }
};

export const disconnectProducer = async () => {
     if (isProducerConnected) {
          await producer.disconnect();
          isProducerConnected = false;
          logger.info('Kafka producer disconnected');
     }
};

// Consumer (for SCHEDULE_CREATED, SCHEDULE_CANCELLED)
export const consumer = kafka.consumer({
     groupId: 'inventory-service-group',
     sessionTimeout: 30000,
     heartbeatInterval: 3000,
});

export const disconnectConsumer = async () => {
     await consumer.disconnect();
     logger.info('Kafka consumer disconnected');
};

export const disconnectAll = async () => {
     await disconnectProducer();
     await disconnectConsumer();
};

