import Redis from "ioredis";
import { config } from "./index.js";
import logger from "./logger.js";

class RedisClient {
  static instance;
  static isConnected = false;

  constructor() {
    // Prevent direct instantiation.
  }

  static getInstance() {
    if (!RedisClient.instance) {
      const redisUrl = config.REDIS_URL;

      if (!redisUrl) {
        throw new Error("REDIS_URL is not defined in the configuration.");
      }
      console.log(
        "REDIS_URL:",
        redisUrl?.replace(/:[^:@]+@/, ":****@")
      );
      RedisClient.instance = new Redis(redisUrl, {
        retryStrategy: (times) => {
          const delay = Math.min(times * 500, 5000);

          logger.warn(
            `Redis reconnect attempt ${times}. Retrying in ${delay}ms...`,
          );

          return delay;
        },

        maxRetriesPerRequest: 3,

        // Don't keep trying forever after an authentication failure.
        enableReadyCheck: true,
      });

      RedisClient.setupEventListeners();
    }

    return RedisClient.instance;
  }

  static setupEventListeners() {
    const client = RedisClient.instance;

    if (!client) {
      return;
    }

    client.on("connect", () => {
      logger.info("Redis TCP connection established");
    });

    client.on("ready", () => {
      RedisClient.isConnected = true;
      logger.info("Redis client is ready");
    });

    client.on("error", (error) => {
      RedisClient.isConnected = false;

      logger.error(`Redis connection error: ${error.message}`);
    });

    client.on("close", () => {
      RedisClient.isConnected = false;
      logger.warn("Redis connection closed");
    });

    client.on("reconnecting", () => {
      RedisClient.isConnected = false;
      logger.warn("Reconnecting to Redis...");
    });

    client.on("end", () => {
      RedisClient.isConnected = false;
      logger.warn("Redis connection ended");
    });
  }

  static isReady() {
    return RedisClient.isConnected;
  }

  static async testConnection() {
    try {
      const client = RedisClient.getInstance();

      const response = await client.ping();

      if (response === "PONG") {
        logger.info("Redis ping successful");
        return true;
      }

      return false;
    } catch (error) {
      logger.error(
        `Redis connection test failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );

      return false;
    }
  }

  static async closeConnection() {
    const client = RedisClient.instance;

    if (!client) {
      return;
    }

    try {
      await client.quit();

      RedisClient.instance = null;
      RedisClient.isConnected = false;

      logger.info("Redis connection closed gracefully");
    } catch (error) {
      logger.error(
        `Error closing Redis connection: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );

      client.disconnect();

      RedisClient.instance = null;
      RedisClient.isConnected = false;
    }
  }
}

export const redis = RedisClient.getInstance();

export default RedisClient;
