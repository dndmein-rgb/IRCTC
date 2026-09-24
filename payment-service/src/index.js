import 'dotenv/config';

import express from 'express';
import helmet from 'helmet';
import cookieParser from 'cookie-parser';

import logger from './config/logger.js';
import { config } from './config/index.js';

import { corsMiddleware } from './middlewares/cors.middleware.js';
import {errorHandler} from './middlewares/error.middleware.js';
import { reqLogger } from './middlewares/req.middleware.js';
import { disconnectProducer } from './config/kafka.js';

import prisma from './config/prisma.js';
import paymentRoutes from './routes/payment.route.js';
import webhookRoutes from './routes/webhook.route.js';

const app = express();

app.use(corsMiddleware);

app.use(
     helmet({
          crossOriginOpenerPolicy: false,
          crossOriginEmbedderPolicy: false,
     })
);

app.use(reqLogger);

// Webhook routes MUST be registered before express.json()
// because they need raw body for signature verification
app.use(webhookRoutes);

// JSON parsing for all other routes
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

app.get('/', (req, res) => {
     res.send('Hello from payment-service');
});

// Health check
app.get('/health', async (req, res) => {
     let dbHealthy = false;

     try {
          await prisma.$queryRaw`SELECT 1`;
          dbHealthy = true;
     } catch (e) {
          logger.error('Health check: DB unreachable', {
               error: e instanceof Error ? e.message : String(e),
          });
     }

     res.status(dbHealthy ? 200 : 503).json({
          success: dbHealthy,
          message: dbHealthy
               ? 'Payment Service is healthy'
               : 'Payment Service is degraded',
          database: dbHealthy,
          timestamp: new Date().toISOString(),
     });
});

// API Routes
app.use(paymentRoutes);

// Error handler MUST be last
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
          logger.error('Failed to start server', {
               error: error instanceof Error ? error.message : String(error),
          });

          process.exit(1);
     }
};

startServer();

export default app;