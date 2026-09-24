import express from 'express'
import { internalAuth } from '../middlewares/internalAuth.middleware.js';
import { createPaymentOrder } from '../controllers/payment.controller.js';

const router = express.Router();

// Internal routes (called by booking-service)
router.post('/orders', internalAuth, createPaymentOrder);
// router.get('/orders/:paymentOrderId', internalAuth, getPaymentOrder);
// router.post('/orders/:paymentOrderId/verify', internalAuth, verifyAndCapturePayment);
// router.post('/refunds', internalAuth, initiateRefund);

export default router;