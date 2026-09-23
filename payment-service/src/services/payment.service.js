import { config } from "../config/index.js";
import logger from "../config/logger.js";
import prisma from "../config/prisma.js";
import { BadRequestError } from "../utils/error.js";
import { getGateway } from "./gateways/gateway.factory.js";

// ─── Idempotency Helper ──────────────────────────────────────────────────────
export const withIdempotency = async (key, fn) => {
  const existing = await prisma.idempotency.findUnique({
    where: { eventKey: key },
  });
  if (existing) {
    logger.info(`Idempotent request detected: ${key}`);
    return existing.response;
  }
  const result = await fn();

  await prisma.idempotencyRecord.create({
    data: { eventKey: key, response: result },
  });

  return result;
};

// ─── Create Payment Order ────────────────────────────────────────────────────
export const createPaymentOrder = (
  bookingId,
  userId,
  idempotencyKey,
  amount,
) => {
  if (!bookingId || !amount || !userId || !idempotencyKey) {
    throw new BadRequestError(
      "bookingId, amount, userId, and idempotencyKey are required",
    );
  }

  if (amount <= 0) {
    throw new BadRequestError("Amount must be greater than 0");
  }
  return withIdempotency(`payment-order:${idempotencyKey}`, async () => {
    const gateway = getGateway();
    const gatewayResult = await gateway.createOrder(amount, "INR", bookingId, {
      bookingId,
      userId,
    });
    const paymentOrder = await prisma.paymentOrder.create({
      data: {
        bookingId,
        amount,
        userId,
        idempotencyKey,
        currency: "INR",
        status: "CREATED",
        gatewayProvider: config.PAYMENT_GATEWAY,
        gatewayOrderId: gatewayResult.gatewayOrderId,
      },
    });
    // Audit log
    await prisma.paymentAuditLog.create({
      data: {
        paymentOrderId: paymentOrder.id,
        action: "ORDER_CREATED",
        gatewayResponse: gatewayResult.rawResponse,
        metadata: { bookingId, userId, amount },
      },
    });
    logger.info(`Payment order created: ${paymentOrder.id}`, {
      bookingId,
      gatewayOrderId: gatewayResult.gatewayOrderId,
    });

    return {
      paymentOrderId: paymentOrder.id,
      gatewayOrderId: gatewayResult.gatewayOrderId,
      amount: paymentOrder.amount,
      currency: paymentOrder.currency,
      status: paymentOrder.status,
      gatewayProvider: paymentOrder.gatewayProvider,
      keyId: config.RAZORPAY_KEY_ID,
    };
  });
};
