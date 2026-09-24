import { asyncHandler } from "../utils/asyncHandler.js";
import { BadRequestError } from "../utils/error.js";
import * as paymentService from "../services/payment.service.js";

export const createPaymentOrder = asyncHandler(async (req, res) => {
  const { bookingId, amount, userId, idempotencyKey } = req.body;
  if (!bookingId || !amount || !userId || !idempotencyKey) {
    throw new BadRequestError(
      "bookingId, amount, userId, and idempotencyKey are required",
    );
  }

  const result = await paymentService.createPaymentOrder(
    bookingId,
    amount,
    userId,
    idempotencyKey,
  );

  res.status(201).json({ success: true, data: result });
});

export const verifyAndCapturePayment = asyncHandler(async (req, res) => {
  const { paymentOrderId } = req.params;
  const { gatewayPaymentId, gatewaySignature } = req.body;
  if (!gatewayPaymentId || !gatewaySignature) {
    throw new BadRequestError(
      "gatewayPaymentId and gatewaySignature are required",
    );
  }
  const result = await paymentService.verifyAndCapturePayment(
    paymentOrderId,
    gatewayPaymentId,
    gatewaySignature,
  );
  res.status(200).json({ success: true, data: result });
});
