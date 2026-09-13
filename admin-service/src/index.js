import "dotenv/config"
import express from "express"
import helmet from "helmet";
import cookieParser from "cookie-parser";

import { config } from "./config/index.js";
import { disconnectProducer } from "./config/kafka.js";

import { corsMiddleware } from "./middlewares/cors.middleware.js";
import { reqLogger } from "./middlewares/req.middleware.js";
import logger from "./config/logger.js";
import { errorHandler } from "./middlewares/error.middleware.js";

import stationRoutes from "./routes/station.route.js"
import trainRoutes from "./routes/train.route.js"
import scheduleRoutes from "./routes/schedule.route.js"

const app = express();

app.use(corsMiddleware);
const helmetMiddleware =
  /** @type {import("express").RequestHandler} */
  (
    /** @type {unknown} */
    (
      helmet({
        crossOriginOpenerPolicy: false,
        crossOriginEmbedderPolicy: false,
      })
    )
  );

app.use(helmetMiddleware);
app.use(reqLogger);
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

// Request logging
app.use((req, res, next) => {
     logger.info(`${req.method} ${req.path}`, {
          ip: req.ip,
          userAgent: req.get('user-agent')
     });
     next();
});

app.get("/", (req, res) => {
     res.send("Hello from index.js of admin-service");
})

// Health check
app.get('/health', (req, res) => {
     res.status(200).json({
          success: true,
          message: 'Admin Service is healthy',
          timestamp: new Date().toISOString()
     });
});

// API Routes - All protected by auth middleware
app.use("/stations", stationRoutes);
app.use("/trains", trainRoutes);
app.use("/schedules", scheduleRoutes);

// Error handler (must be last)
app.use(errorHandler);

const startServer = async () => {
     try {
          const server = app.listen(config.PORT, () => {
               logger.info(
                    `${config.SERVICE_NAME} is running on port ${config.PORT}`
               );
          });

          // Graceful shutdown
          const shutdown = async () => {
               logger.info('Shutting down gracefully...');

               server.close(async () => {
                    await disconnectProducer();
                    logger.info('Server closed');
                    process.exit(0);
               });
          };

          process.on('SIGTERM', shutdown);
          process.on('SIGINT', shutdown);

     } catch (error) {
          logger.error('Failed to start server', error);
          process.exit(1);
     }
};

startServer();
