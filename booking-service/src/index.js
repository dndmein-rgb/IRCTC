import 'dotenv/config';

import express from 'express';
import helmet from 'helmet';
import cookieParser from 'cookie-parser';

import {logger} from './config/logger.js';
import { config } from './config/index.js';

import { corsMiddleware } from './middlewares/cors.middleware.js';
import {errorHandler} from './middlewares/error.middleware.js';
import { reqLogger } from './middlewares/req.middleware.js';

import { disconnectAll } from './config/kafka.js';
import  RedisClient  from './config/redis.js';

import prisma from './config/prisma.js';

// import bookingRoutes from './routes/booking.route.js';
import * as  bookingConsumer from './kafka/consumer/booking.consumer.js';

import {
    startBookingExpiryJob,
    stopBookingExpiryJob,
} from './utils/bookingExpiry.js';

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
app.use(helmetMiddleware)
app.use(reqLogger);
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

app.get("/", (req, res) => {
     res.send("Hello from booking-service");
})

// Health check (includes Redis + PostgreSQL)
app.get('/health', async (req, res) => {
     let dbHealthy = false;
     try {
          await prisma.$queryRaw`SELECT 1`;
          dbHealthy = true;
     } catch (e) {
          logger.error('Health check: DB unreachable', { error: e instanceof Error? e.message:String(e) });
     }

     const redisHealthy = RedisClient.isReady();
     const healthy = dbHealthy && redisHealthy;

     res.status(healthy ? 200 : 503).json({
          success: healthy,
          message: healthy ? 'Booking Service is healthy' : 'Booking Service is degraded',
          redis: redisHealthy,
          database: dbHealthy,
          timestamp: new Date().toISOString(),
     });
});

// API Routes
// app.use(bookingRoutes);

// Error handler (must be last)
app.use(errorHandler);

const startServer = async () => {
     try {
          await bookingConsumer.start();
          startBookingExpiryJob();

          const server = app.listen(config.PORT, () => {
               logger.info(
                    `${config.SERVICE_NAME} is running on port ${config.PORT}`
               );
          });

          // Graceful shutdown
          const shutdown = async () => {
               logger.info('Shutting down gracefully...');
               stopBookingExpiryJob();

               server.close(async () => {
                    await disconnectAll();
                    await RedisClient.closeConnection();
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
