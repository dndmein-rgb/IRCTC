import "dotenv/config";

import express from "express";
import cookieParser from "cookie-parser";
import helmet from "helmet";

import { config } from "./config/index.js";
import logger from "./config/logger.js";

import authRoutes from "./routes/auth.route.js";
import userRoutes from "./routes/user.route.js";

import { corsMiddleware } from "./middlewares/cors.middleware.js";
import {errorHandler} from "./middlewares/error.middleware.js";
import { reqLogger } from "./middlewares/req.middleware.js";
// import { disconnectProducer } from "./config/kafka.js";

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
app.use(cookieParser());

app.use("/auth", authRoutes);
app.use("/user", userRoutes);

app.get("/", (req, res) => {
  res.send("Hello from index.js of user-service");
});

app.get("/health", (req, res) => {
  res.status(200).json({
    message: "ok",
  });
});

app.use(errorHandler);

const startServer = async () => {
  try {
    const server = app.listen(config.PORT, () => {
      logger.info(
        `${config.SERVICE_NAME} is running on http://localhost:${config.PORT}`,
      );
    });

    const shutdown = async () => {
      logger.info("Shutting down gracefully...");

      server.close(async () => {
        // await disconnectProducer();

        logger.info("Server closed");

        process.exit(0);
      });
    };

    process.on("SIGTERM", shutdown);
    process.on("SIGINT", shutdown);
  } catch (error) {
    logger.error("Failed to Start Server", error);
    process.exit(1);
  }
};

startServer();
