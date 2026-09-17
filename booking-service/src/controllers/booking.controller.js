import { asyncHandler } from "../utils/asyncHandler.js";
import { BadRequestError } from "../utils/error.js";
import * as bookingService from "../services/booking.service.js"


export const createBooking = asyncHandler(async (req, res) => {
     const userId = req.user.id;
     const { scheduleId, seatIds, passengers, idempotencyKey, fromStationId, toStationId, fromSeq, toSeq } = req.body; // --- SEGMENT BOOKING: added segment params

     if (!scheduleId || !seatIds || !passengers || !idempotencyKey) {
          throw new BadRequestError('scheduleId, seatIds, passengers, and idempotencyKey are required');
     }

     // --- SEGMENT BOOKING: Pass segment params to service ---
     const result = await bookingService.createBooking(
          userId, scheduleId, seatIds, passengers, idempotencyKey,
          fromStationId, toStationId, fromSeq, toSeq
     );

     res.status(201).json({ success: true, data: result });
});

export const getBooking = asyncHandler(async (req, res) => {
     const userId = req.user.id;
     const { bookingId } = req.params;

     const result = await bookingService.getBooking(bookingId, userId);

     res.status(200).json({ success: true, data: result });
});

export const getUserBookings = asyncHandler(async (req, res) => {
     const userId = req.user.id;
     const { status, page, limit } = req.query;

     const result = await bookingService.getUserBookings(userId, {
          status,
          page: page ? parseInt(page, 10) : 1,
          limit: limit ? parseInt(limit, 10) : 10,
     });

     res.status(200).json({ success: true, data: result });
});

export const verifyPayment = asyncHandler(async (req, res) => {
     const userId = req.user.id;
     const { bookingId } = req.params;
     const { razorpayPaymentId, razorpaySignature } = req.body;

     if (!razorpayPaymentId || !razorpaySignature) {
          throw new BadRequestError('razorpayPaymentId and razorpaySignature are required');
     }

     const result = await bookingService.verifyPayment(bookingId, userId, razorpayPaymentId, razorpaySignature);

     res.status(200).json({ success: true, data: result });
});

export const cancelBooking = asyncHandler(async (req, res) => {
     const userId = req.user.id;
     const { bookingId } = req.params;

     const result = await bookingService.cancelBooking(bookingId, userId);

     res.status(200).json({
          success: true,
          message: 'Booking cancelled successfully',
          data: result,
     });
});