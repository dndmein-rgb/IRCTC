import { config } from "../config/index.js";
import sgMail from "@sendgrid/mail";
import { logger } from "../config/logger.js";
import { getOtpTemplate, getWelcomeTemplate } from "../templates/index.js";

class EmailService {
  
    constructor() {
      this.from = config.MAIL_SEND;
      this.maxRetries = 3;
    };
  
  async sendWithRetry(msg, retries = 0) {
    try {
      await sgMail.send(msg);
      logger.info(`Email successfully sent to ${msg.to}`, {
        subject: msg.subject,
        attempt: retries + 1,
      });
    } catch (error) {
      logger.error(
        `Email sending failed (attempt ${retries + 1}/${this.maxRetries})`,
        {
          to: msg.to,
          error: error instanceof Error ? error.message : String(error),
          code:
            error instanceof Error && "code" in error ? error.code : undefined,
        },
      );

      if (retries < this.maxRetries - 1) {
        //Exponential backoff
        const delay = Math.pow(2, retries) * 1000;
        await new Promise((resolve) => setTimeout(resolve, delay));
        return this.sendWithRetry(msg, retries + 1);
      }

      throw error;
    }
  }

  async sendOtpEmail(email, otp, ttlMinutes) {
    const msg = {
      to: email,
      from: this.from,
      subject: "Your verification code",
      html: getOtpTemplate(otp, ttlMinutes),
    };
    return this.sendWithRetry(msg);
  }

  async sendWelcomeEmail(email, firstName) {
    const msg = {
      to: email,
      from: this.from,
      subject: "Welcome to the site - Email Verified",
      html: getWelcomeTemplate(firstName),
    };

    return this.sendWithRetry(msg);
  }
}

export const emailService = new EmailService();
