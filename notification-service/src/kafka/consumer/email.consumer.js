import { consumer } from "../../config/kafka.js";
import { logger } from "../../config/logger.js";
import { KAFKA_TOPICS } from "../../../../shared/constants/constants.js";
import { emailService } from "../../services/email.service.js";

class EmailConsumer {
  async start() {
    await consumer.connect();
    logger.info("Email consumer connected to Kafka");

    await consumer.subscribe({
      topics: Object.values(KAFKA_TOPICS),
      fromBeginning: false,
    });
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const rawValue = message.value?.toString();

          if (!rawValue) {
            logger.warn("Kafka message has no value");
            return;
          }

          const value = JSON.parse(rawValue);

          logger.info(`Processing messages from topic: ${topic}`, {
            partition,
            offset: message.offset,
            key: message.key?.toString(),
          });
          await this.handleMessage(topic, value);
        } catch (error) {
          logger.error("Error processing message", {
            topic,
            partition,
            offset: message.offset,
            error: error instanceof Error ? error.message : String(error),
            stack: error instanceof Error ? error.stack : undefined,
          });
        }
      },
    });

    logger.info("Email consumer is running and listening for messages...");
  }

  async handleMessage(topic, data) {
    switch (topic) {
      case KAFKA_TOPICS.OTP_EMAIL:
        await this.handleOtpEmail(data);
        break;

      case KAFKA_TOPICS.WELCOME_EMAIL:
        await this.handleWelcomeEmail(data);
        break;

      // case KAFKA_TOPICS.BOOKING_CONFIRMED:
      //   await this.handleBookingConfirmed(data);
      //   break;

      // case KAFKA_TOPICS.BOOKING_FAILED:
      //   await this.handleBookingFailed(data);
      //   break;

      // case KAFKA_TOPICS.BOOKING_CANCELLED:
      //   await this.handleBookingCancelled(data);
      //   break;

      default:
        logger.warn(`Unknown topic: ${topic}`);
    }
  }

  async handleOtpEmail(data) {
    const { email, otp, ttlMinutes } = data;

    if (!email || !otp) {
      throw new Error("Missing required fields: email or otp");
    }

    await emailService.sendOtpEmail(email, otp, ttlMinutes || 5);
    logger.info(`OTP email sent to ${email}`);
  }

  async handleWelcomeEmail(data) {
    const { email, firstName } = data;

    if (!email || !firstName) {
      throw new Error("Missing required fields: email or firstName");
    }

    await emailService.sendWelcomeEmail(email, firstName);
    logger.info(`Welcome email sent to ${email}`);
  }
}

export const emailConsumer = new EmailConsumer();
