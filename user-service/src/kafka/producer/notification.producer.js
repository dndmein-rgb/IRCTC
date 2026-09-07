import { connectProducer, producer } from "../../config/kafka.js";
import logger from "../../config/logger.js";
import { KAFKA_TOPICS } from "../../../../shared/constants/constants.js";

class NotificationProducer {
  constructor() {
    this.isInitialized = false;
  }

  async initialize() {
    if (!this.isInitialized) {
      await connectProducer();
      this.isInitialized = true;
    }
  }
  async sendMessage(topic, key, value) {
    try {
      await this.initialize();
      const message = {
        topic,
        messages: [
          {
            key: key || `${topic}--${Date.now()}`,
            value: JSON.stringify(value),
            timestamp: Date.now().toString(),
          },
        ],
      };
      const result = await producer.send(message);
      logger.info(`Message sent to kafka topic: ${topic}`, {
        key,
        partition: result[0].partition,
        offset: result[0].offset,
      });
      return result;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;
      logger.error(`Failed to send message to kafka topic: ${topic}`, {
        error: errorMessage,
        stack: errorStack,
        key,
      });
      throw error;
    }
  }
  async sendOtpEmail(email, otp, ttlMinutes = 5){
            return this.sendMessage(
                 KAFKA_TOPICS.OTP_EMAIL,
                 `otp-${email}`,
                 {email, otp, ttlMinutes}
            )
       }
  
       async sendWelcomeEmail(email, firstName){
            return this.sendMessage(
                 KAFKA_TOPICS.WELCOME_EMAIL,
                 `welcome-${email}`,
                 {email, firstName}
            )
       }
}
export const notificationProducer=new NotificationProducer()
