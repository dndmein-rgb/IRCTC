import { Kafka, logLevel } from "kafkajs";
import { config } from "./index.js";
import logger from "./logger.js";

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

export const producer = kafka.producer({
     allowAutoTopicCreation: true,
     transactionTimeout: 30000,
     idempotent: true, 
     maxInFlightRequests: 5,
     retry: {
          retries: 5,
     },
});

let isConnected = false;

export const connectProducer = async () => {
     if (!isConnected) {
          await producer.connect();
          isConnected = true;
          logger.info('Kafka producer connected');
     }
};

export const disconnectProducer = async () => {
     if (isConnected) {
          await producer.disconnect();
          isConnected = false;
          logger.info('Kafka producer disconnected');
     }
};

