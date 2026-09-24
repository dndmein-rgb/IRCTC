import { config } from "../config/index.js";
import logger from "../config/logger.js";
import prisma from "../config/prisma.js";
import { paymentProducer } from "../kafka/producer/payment.producer.js";
import {
  BadRequestError,
  ConflictError,
  NotFoundError,
} from "../utils/error.js";
import { getGateway } from "./gateways/gateway.factory.js";

// ─── Optimistic Lock Helper (CAS — Compare-And-Swap) ─────────────────────────
// The webhook and the browser's verify call routinely arrive within milliseconds
// of each other. Without this, both read status CREATED, both write CAPTURED, and
// both publish PAYMENT_SUCCESS. The `version` column existed for exactly this and
// was never used — every status transition now goes through here.
//
// Returns true when this caller won the transition, false when another already did.
// Callers must only emit side effects (Kafka, audit-of-record) when it returns true.

const casUpdatePaymentOrder = async (paymentOrderId, expectedVersion, data) => {
  const result = await prisma.paymentOrder.updateMany({
    where: { id: paymentOrderId, version: expectedVersion },
    data: { ...data, version: { increment: 1 } },
  });
  if (result.count === 0) {
    logger.info(
      `CAS lost for payment order ${paymentOrderId} (expected version ${expectedVersion})`,
    );
    return false;
  }
  return true;
};

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

// ─── Handle Webhook ──────────────────────────────────────────────────────────
export const handleWebhook = async (rawBody, signature) => {
  const gateway = getGateway();
  // Verify signature
  const isValid = gateway.verifyWebhookSignature(rawBody, signature);
  if (!isValid) {
    logger.warn("Webhook signature verification failed");
    throw new BadRequestError("Invalid webhook signature", "INVALID_SIGNATURE");
  }
  const payload =
    rawBody.type === "string"
      ? JSON.parse(rawBody)
      : JSON.parse(rawBody.toString("utf8"));
  const event = payload.event;
  const paymentEntity = payload.payload?.payment?.entity;
  if (!paymentEntity) {
    logger.warn("Webhook payload missing payment entity", { event });
    return { status: "ignored", event };
  }
  const gatewayOrderId = paymentEntity.orderId;
  const gatewayPaymentId = paymentEntity.id;
  // Find payment order
  const paymentOrder = await prisma.paymentOrder.findUnique({
    where: { gatewayOrderId },
  });
  if (!paymentOrder) {
    logger.warn(`Payment order not found for gateway order: ${gatewayOrderId}`);
    return { status: "ignored", reason: "order_not_found" };
  }
  // Audit log the webhook
  await prisma.paymentAuditLog.create({
    data: {
      paymentOrderId: paymentOrder.id,
      action: `WEBHOOK_${event.toUpperCase().replace(/\./g, "_")}`,
      gatewayResponse: payload,
    },
  });

  if (event === "payment.captured" || event === "payment.authorized") {
    return handlePaymentCaptured(paymentOrder, gatewayPaymentId, paymentEntity);
  }

  if (event === "payment.failed") {
    return handlePaymentFailed(paymentOrder, gatewayPaymentId, paymentEntity);
  }

  if (event === "refund.processed" || event === "refund.created") {
    return handleRefundProcessed(paymentOrder, payload.payload?.refund?.entity);
  }

  logger.info(`Webhook event ignored: ${event}`);
  return { status: "ignored", event };
};

const handlePaymentCaptured = async (
  paymentOrder,
  gatewayPaymentId,
  paymentEntity,
) => {
  // Idempotent: already captured
  if (paymentOrder.status === "CAPTURED") {
    logger.info(`Payment already captured: ${paymentOrder.id}`);
    return { status: "already_processed" };
  }
  if (paymentOrder.status !== "CREATED") {
    logger.warn(`Cannot capture payment in status: ${paymentOrder.status}`, {
      paymentOrderId: paymentOrder.id,
    });
    return { status: "invalid_state", currentStatus: paymentOrder.status };
  }
  // Claim the transition. If the client-side verify captured it first, bail out
  // rather than publishing a second PAYMENT_SUCCESS for the same order.

  const won = casUpdatePaymentOrder(paymentOrder.id, paymentOrder.version, {
    status: "CAPTURED",
    gatewayPaymentId,
    gatewaySignature: paymentEntity.acquirer_data?.auth_code || null,
  });
  if (!won) {
    logger.info(
      `Payment ${paymentOrder.id} already captured by another path — webhook is a no-op`,
    );
    return { status: "already_processed" };
  }
  logger.info(`Payment captured: ${paymentOrder.id}`, { gatewayPaymentId });
  // Publish PAYMENT_SUCCESS to Kafka
  await paymentProducer
    .publishPaymentSuccess(
      paymentOrder.id,
      paymentOrder.bookingId,
      gatewayPaymentId,
      paymentOrder.amount,
    )
    .catch((err) => {
      logger.error("Failed to publish PAYMENT_SUCCESS", { error: err.message });
    });

  return { status: "captured", paymentOrderId: paymentOrder.id };
};

const handlePaymentFailed = async (
  paymentOrder,
  gatewayPaymentId,
  paymentEntity,
) => {
  // Idempotent: already failed
  if (paymentOrder.status === "FAILED") {
    return { status: "already_processed" };
  }

  if (paymentOrder.status !== "CREATED") {
    return { status: "invalid_state", currentStatus: paymentOrder.status };
  }

  const reason =
    paymentEntity.error_description ||
    paymentEntity.error_reason ||
    "payment_failed";

  const won = await casUpdatePaymentOrder(
    paymentOrder.id,
    paymentOrder.version,
    {
      status: "FAILED",
      gatewayPaymentId,
      failureReason: reason,
    },
  );

  if (!won) {
    logger.info(
      `Payment ${paymentOrder.id} already transitioned by another path — failure webhook is a no-op`,
    );
    return { status: "already_processed" };
  }

  logger.info(`Payment failed: ${paymentOrder.id}`, { reason });

  // Publish PAYMENT_FAILED to Kafka
  await paymentProducer
    .publishPaymentFailed(paymentOrder.id, paymentOrder.bookingId, reason)
    .catch((err) => {
      logger.error("Failed to publish PAYMENT_FAILED", { error: err.message });
    });

  return { status: "failed", paymentOrderId: paymentOrder.id };
};

const handleRefundProcessed = async (paymentOrder, refundEntity) => {
  if (!refundEntity) return { status: "ignored", reason: "no_refund_entity" };

  const gatewayRefundId = refundEntity.id;

  const refund = await prisma.refund.findUnique({
    where: { gatewayRefundId },
  });

  if (refund) {
    await prisma.refund.update({
      where: { id: refund.id },
      data: { status: "COMPLETED" },
    });

    // Recompute the parent order's status from the refunds that are now on record.
    // Read fresh and CAS: concurrent refund webhooks for the same order would
    // otherwise each compute a total from their own stale snapshot.
    for (let attempt = 1; attempt <= 2; attempt++) {
      const current = await prisma.paymentOrder.findUnique({
        where: { id: paymentOrder.id },
        include: { refunds: true },
      });

      if (!current) break;

      const totalRefunded = current.refunds
        .filter((r) => r.status === "COMPLETED")
        .reduce((sum, r) => sum + r.amount, 0);

      const newStatus =
        totalRefunded >= current.amount ? "REFUNDED" : "PARTIALLY_REFUNDED";

      const won = await casUpdatePaymentOrder(current.id, current.version, {
        status: newStatus,
      });

      if (won) {
        logger.info(`Refund processed: ${gatewayRefundId}`, {
          newStatus,
          totalRefunded,
        });
        break;
      }

      if (attempt === 2) {
        logger.warn(
          `Could not update order status after refund ${gatewayRefundId} — concurrent write won twice`,
          {
            paymentOrderId: current.id,
          },
        );
      }
    }
  }

  return { status: "refund_processed", gatewayRefundId };
};

// ─── Verify and Capture (client-side verification) ───────────────────────────
export const verifyAndCapturePayment = async (
  paymentOrderId,
  gatewayPaymentId,
  gatewaySignature,
) => {
  if (!paymentOrderId || !gatewayPaymentId || !gatewaySignature) {
    throw new BadRequestError(
      "paymentOrderId, gatewayPaymentId, and gatewaySignature are required",
    );
  }
  const paymentOrder = await prisma.paymentOrder.findUnique({
    where: { id: paymentOrderId },
  });
  if (!paymentOrder) {
    throw new NotFoundError("Payment order not found");
  }
  // Idempotent
  if (paymentOrder.status === "CAPTURED") {
    return {
      paymentOrderId: paymentOrder.id,
      status: "CAPTURED",
      gatewayPaymentId: paymentOrder.gatewayPaymentId,
      message: "Payment already captured",
    };
  }
  if (paymentOrder.status !== "CREATED") {
    throw new ConflictError(
      `Payment order is in ${paymentOrder.status} status`,
    );
  }

  const gateway = getGateway();
  // Verify signature

  const isValid = gateway.verifyPaymentSignature(
    paymentOrder.gatewayOrderId,
    gatewayPaymentId,
    gatewaySignature,
  );

  // Audit log the verification attempt
  await prisma.paymentAuditLog.create({
    data: {
      paymentOrderId: paymentOrder.id,
      action: isValid ? "SIGNATURE_VERIFIED" : "SIGNATURE_VERIFICATION_FAILED",
      metadata: { gatewayPaymentId, isValid },
    },
  });
  if (!isValid) {
    const wonFailure = await casUpdatePaymentOrder(
      paymentOrder.id,
      paymentOrder.version,
      {
        status: "FAILED",
        failureReason: "signature_verification_failed",
      },
    );

    // Only announce the failure if we actually moved the order into it — a
    // concurrent webhook may have legitimately captured it in the meantime.
    if (wonFailure) {
      await paymentProducer
        .publishPaymentFailed(
          paymentOrder.id,
          paymentOrder.bookingId,
          "signature_verification_failed",
        )
        .catch((err) => {
          logger.error("Failed to publish PAYMENT_FAILED after sig failure", {
            error: err.message,
          });
        });
    }
    throw new BadRequestError(
      "Payment signature verification failed",
      "INVALID_SIGNATURE",
    );
  }
  // Signature valid — claim the capture.
  const won = await casUpdatePaymentOrder(
    paymentOrder.id,
    paymentOrder.version,
    { status: "CAPTURED", gatewayPaymentId, gatewaySignature },
  );

  if (!won) {
    // The webhook captured it first. That path already published PAYMENT_SUCCESS,
    // so report success to the browser without emitting a duplicate event.
    const fresh = await prisma.paymentOrder.findUnique({
      where: { id: paymentOrder.id },
    });
    logger.info(
      `Payment ${paymentOrder.id} already captured by webhook — verify is a no-op`,
    );
    return {
      paymentOrderId: paymentOrder.id,
      gatewayPaymentId: fresh?.gatewayPaymentId || gatewayPaymentId,
      status: fresh?.status || "CAPTURED",
      message: "Payment already captured",
    };
  }
  await prisma.paymentAuditLog.create({
    data: {
      paymentOrderId: paymentOrder.id,
      action: "PAYMENT_CAPTURED_VIA_VERIFY",
      metadata: { gatewayPaymentId },
    },
  });
  logger.info(`Payment captured via verify: ${paymentOrder.id}`);

  // Publish PAYMENT_SUCCESS
  await paymentProducer
    .publishPaymentSuccess(
      paymentOrder.id,
      paymentOrder.bookingId,
      gatewayPaymentId,
      paymentOrder.amount,
    )
    .catch((err) => {
      logger.error("Failed to publish PAYMENT_SUCCESS after verify", {
        error: err.message,
      });
    });

  return {
    paymentOrderId: paymentOrder.id,
    status: "CAPTURED",
    gatewayPaymentId,
  };
};
