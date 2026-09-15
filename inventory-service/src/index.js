import "dotenv/config";
import express from "express";
import { corsMiddleware } from "./middlewares/cors.middleware.js";
import helmet from "helmet";
import { reqLogger } from "./middlewares/req.middleware.js";
import cookieParser from "cookie-parser";
import prisma from "./config/prisma.js";
import {logger} from "./config/logger.js";
import { errorHandler } from "./middlewares/error.middleware.js";
import inventoryRoutes from "./routes/inventory.route.js";
import { inventoryConsumer } from "./kafka/consumer/inventory.consumer.js";
import { config } from "./config/index.js";
import { disconnectAll } from "./config/kafka.js";

const app = express();

app.use(corsMiddleware);
app.use(
  helmet({
    crossOriginOpenerPolicy: false,
    crossOriginEmbedderPolicy: false,
  }),
);
app.use(reqLogger);
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

app.get("/", (req, res) => {
  res.send("Hello from inventory-service");
});

// Health check
app.get("/health", async (req, res) => {
  let dbHealthy = false;
  try {
    await prisma.$queryRaw`SELECT 1`;
    dbHealthy = true;
  } catch (e) {
    logger.error("Health check: DB unreachable", {
      error: e instanceof Error ? e.message : String(e),
    });
  }

  res.status(dbHealthy ? 200 : 503).json({
    success: dbHealthy,
    message: dbHealthy
      ? "Inventory Service is healthy"
      : "Inventory Service is degraded",
    database: dbHealthy,
    timestamp: new Date().toISOString(),
  });
});

// API Routes
app.use(inventoryRoutes);

// Error handler
app.use(errorHandler);

const startServer = async () => {
  try {
    await inventoryConsumer.start();
    // startLockExpiryJob();

    const server = app.listen(config.PORT, () => {
      logger.info(`${config.SERVICE_NAME} is running on port ${config.PORT}`);
    });

    // Graceful shutdown
    const shutdown = async () => {
      logger.info("Shutting down gracefully...");
      // stopLockExpiryJob();

      server.close(async () => {
        await disconnectAll();
        logger.info("Server closed");
        process.exit(0);
      });
    };

    process.on("SIGTERM", shutdown);
    process.on("SIGINT", shutdown);
  } catch (error) {
    logger.error("Failed to start server", error);
    process.exit(1);
  }
};

startServer();
