import "dotenv/config"
import { logger } from "./config/logger.js";
import { emailConsumer } from "./kafka/consumer/email.consumer.js";

process.on("unhandledRejection", (reason, promise) => {
  logger.error("Unhandled Rejection", {
    reason: reason instanceof Error ? reason.message : String(reason),
    promise,
  });

  process.exit(1);
});

process.on("uncaughtException", (error) => {
  logger.error("Uncaught Exception", {
    error: error.message,
    stack: error.stack,
  });

  process.exit(1);
});

async function startNotificationService() {
  try {
    logger.info("Starting Notification Service...");

    const requiredEnvVars = [
      "SENDGRID_API_KEY",
      "MAIL_SEND",
      "KAFKA_BROKER",
    ];

    const missing = requiredEnvVars.filter(
      (varName) => !process.env[varName],
    );

    if (missing.length > 0) {
      throw new Error(
        `Missing required environment variables: ${missing.join(", ")}`,
      );
    }

    await emailConsumer.start();

    logger.info("Notification Service started successfully");
    logger.info("Service is ready to process notifications");
  } catch (error) {
    console.error("STARTUP ERROR:", error);

    logger.error("Failed to start Notification Service", {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    });

    process.exit(1);
  }
}

startNotificationService();