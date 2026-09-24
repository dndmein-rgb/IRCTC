import express from 'express'
import { razorpayWebhook } from '../controllers/webhook.controller.js';

const router = express.Router();

// Public: Razorpay calls this endpoint with payment events
// IMPORTANT: Uses express.raw() so we get the raw body for signature verification
router.post('/webhooks/razorpay', express.raw({ type: 'application/json' }), razorpayWebhook);

export default  router;