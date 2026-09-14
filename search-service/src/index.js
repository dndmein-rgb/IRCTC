import "dotenv/config";

import express from "express";
import path from "path";
import helmet from "helmet";
import cookieParser from "cookie-parser";

import { corsMiddleware } from "./middlewares/cors.middleware.js";
import { reqLogger } from "./middlewares/req.middleware.js";
import { errorHandler } from "./middlewares/error.middleware.js";

import { config } from "./config/index.js";
import { logger } from "./config/logger.js";
import { initIndices, recreateIndices } from "./config/elasticsearch.js";
import { searchConsumer } from "./kafka/consumer/search.consumer.js";
import { disconnectAll } from "./config/kafka.js";
import searchRoutes from "./routes/search.route.js";

const app = express();

app.use(corsMiddleware);
app.use(
  helmet({
    crossOriginOpenerPolicy: false,
    crossOriginEmbedderPolicy: false,
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'", "'unsafe-inline'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        imgSrc: ["'self'", "data:"],
        connectSrc: ["'self'"],
      },
    },
  }),
);
app.use(reqLogger);
app.use(express.json());
app.use(cookieParser());

// Serve frontend static files
app.use(express.static(path.join(__dirname, "..", "public")));

// Mount search routes at root (gateway strips first path segment)
app.use(searchRoutes);

app.get("/health", (req, res) =>
  res.json({ status: "ok", service: config.SERVICE_NAME }),
);
app.use(errorHandler);

const startServer = async () => {
  if (process.env.ES_RECREATE_INDICES === "true") {
    await recreateIndices();
  } else {
    await initIndices();
  }
  await searchConsumer.start();

  const server = app.listen(config.PORT, () => {
    logger.info(
      `${config.SERVICE_NAME} running on http://localhost:${config.PORT}`,
    );
  });

  const shutdown = async () => {
    logger.info("Shutting down...");
    server.close(async () => {
      await disconnectAll();
      process.exit(0);
    });
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
};

startServer();
