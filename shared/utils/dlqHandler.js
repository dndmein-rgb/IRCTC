/**
 * Dead-Letter Queue (DLQ) handler for Kafka consumers.
 *
 * Wraps eachMessage processing with retry tracking. After DLQ_MAX_RETRIES
 * consecutive failures the message is forwarded to a per-service DLQ topic
 * and the consumer moves on instead of blocking forever.
 *
 * Usage (in any consumer):
 *   import { withDLQ } from '../../../../shared/utils/dlqHandler.js';
 *   await consumer.run({ eachMessage: withDLQ(producer, dlqTopic, logger, handler) });
 */

import { DLQ_MAX_RETRIES } from '../constants/constants.js';

/**
 * Minimal logger interface (works with Winston, Pino, console, etc.)
 * @typedef {object} Logger
 * @property {(message: string, meta?: object) => void} error
 * @property {(message: string, meta?: object) => void} info
 */

/**
 * Minimal Kafka message shape we actually use
 * @typedef {object} KafkaMessage
 * @property {Buffer | string | null} [key]
 * @property {Buffer | string | null} [value]
 * @property {string} offset
 * @property {Record<string, Buffer | string | (Buffer | string)[] | undefined>} [headers]
 */

/**
 * Minimal producer interface (only what we need)
 * @typedef {object} KafkaProducer
 * @property {(payload: {
 *   topic: string,
 *   messages: Array<{
 *     key?: Buffer | string | null,
 *     value?: Buffer | string | null,
 *     headers?: Record<string, string | Buffer>
 *   }>
 * }) => Promise<any>} send
 */

/**
 * @param {KafkaProducer} producer
 * @param {string} dlqTopic
 * @param {Logger} logger
 * @param {(payload: {
 *   topic: string,
 *   partition: number,
 *   message: KafkaMessage,
 *   parsedValue: any
 * }) => Promise<void>} handler
 * @returns {(payload: {
 *   topic: string,
 *   partition: number,
 *   message: KafkaMessage
 * }) => Promise<void>}
 */
export function withDLQ(producer, dlqTopic, logger, handler) {
  // In-memory retry tracker: key = `${topic}:${partition}:${offset}` → attempt count
  const retryMap = new Map();

  return async ({ topic, partition, message }) => {
    const msgKey = `${topic}:${partition}:${message.offset}`;
    const attempt = (retryMap.get(msgKey) || 0) + 1;
    retryMap.set(msgKey, attempt);

    // Guard against null / undefined value
    if (message.value == null) {
      const err = new Error('Message value is null or undefined');
      logger.error(`Null message value on ${topic}, sending to DLQ`, {
        partition,
        offset: message.offset,
        error: err.message,
      });
      await sendToDLQ(producer, dlqTopic, topic, partition, message, err, logger);
      retryMap.delete(msgKey);
      return;
    }

    let parsedValue;
    try {
      parsedValue = JSON.parse(message.value.toString());
    } catch (parseErr) {
      // Completely unparseable — send to DLQ immediately
      logger.error(`Unparseable message on ${topic}, sending to DLQ`, {
        partition,
        offset: message.offset,
        error: parseErr instanceof Error ? parseErr.message : String(parseErr),
      });
      await sendToDLQ(producer, dlqTopic, topic, partition, message, parseErr, logger);
      retryMap.delete(msgKey);
      return;
    }

    try {
      await handler({ topic, partition, message, parsedValue });
      // Success — clean up
      retryMap.delete(msgKey);
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : String(error);

      logger.error(`Error processing ${topic} (attempt ${attempt}/${DLQ_MAX_RETRIES})`, {
        error: errMsg,
        partition,
        offset: message.offset,
      });

      if (attempt >= DLQ_MAX_RETRIES) {
        logger.error(`Max retries exceeded for ${topic}, sending to DLQ`, {
          partition,
          offset: message.offset,
        });
        await sendToDLQ(producer, dlqTopic, topic, partition, message, error, logger);
        retryMap.delete(msgKey);
      } else {
        // Re-throw so KafkaJS retries (it will re-deliver the same message)
        throw error;
      }
    }
  };
}

/**
 * @param {KafkaProducer} producer
 * @param {string} dlqTopic
 * @param {string} originalTopic
 * @param {number} partition
 * @param {KafkaMessage} message
 * @param {unknown} error
 * @param {Logger} logger
 */
export async function sendToDLQ(producer, dlqTopic, originalTopic, partition, message, error, logger) {
  try {
    const errorMessage = error instanceof Error ? error.message : String(error);

    /** @type {Record<string, string | Buffer>} */
    const headers = {
      ...(message.headers || {}),
      'dlq-original-topic': originalTopic,
      'dlq-original-partition': String(partition),
      'dlq-original-offset': message.offset,
      'dlq-error': errorMessage,
      'dlq-timestamp': new Date().toISOString(),
    };

    await producer.send({
      topic: dlqTopic,
      messages: [
        {
          key: message.key ?? null,
          value: message.value ?? null,
          headers,
        },
      ],
    });

    logger.info(`Message sent to DLQ: ${dlqTopic}`, {
      originalTopic,
      partition,
      offset: message.offset,
    });
  } catch (dlqError) {
    // If even the DLQ publish fails, log and move on — don't block the consumer forever
    logger.error(`Failed to send message to DLQ ${dlqTopic}`, {
      error: dlqError instanceof Error ? dlqError.message : String(dlqError),
      originalTopic,
      partition,
      offset: message.offset,
    });
  }
}