import logger from "../config/logger.js";
import { asyncHandler } from "../utils/asyncHandler.js";
import * as paymentService from "../services/payment.service.js";

/**
 * Razorpay webhook handler.
 * IMPORTANT: This endpoint receives raw body (not JSON-parsed)
 * for signature verification. The route must use express.raw().
 */
export const razorpayWebhook = asyncHandler(async (req, res) => {
  const signature = req.headers["x-razorpay-signature"];
  if (!signature) {
    logger.warn("Webhook received without signature header");
    return res
      .status(400)
      .json({ status: "error", message: "Missing signature" });
  }
  const rawBody = req.body;
  const result = paymentService.handleWebhook(rawBody, signature);

  logger.info("Webhook processed", { result });
  // Always return 200 to the gateway to prevent retries for processed events
  res.status(200).json({ status: "ok", ...result });
});
