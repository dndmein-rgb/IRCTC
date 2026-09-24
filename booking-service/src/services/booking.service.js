import { config } from "../config/index.js";
import { logger } from "../config/logger.js";
import prisma from "../config/prisma.js";
import { bookingProducer } from "../kafka/producer/booking.producer.js";
import {
  acquireSeatLocks,
  forceReleaseSeatLocks,
  releaseSeatLocks,
} from "../utils/distributedLock.js";
import {
  BadRequestError,
  ConflictError,
  NotFoundError,
  StaleStateError,
} from "../utils/error.js";
import { inventoryClient } from "./inventoryClient.js";
import {
  paymentClient,
  extractError as extractPaymentError,
} from "./paymentClient.js";
import * as saga from "./saga.service.js";
import { stationClient } from "./stationClient.js";
import { userClient } from "./userClient.js";

// ─── Optimistic Lock Helper (CAS — Compare-And-Swap) ────────────────────────
// Atomically updates booking status ONLY IF the version hasn't changed since read.
// Returns the updated booking or throws StaleStateError if another process got there first.

const casUpdateBooking = async (bookingId, expectedVersion, data) => {
  const result = await prisma.booking.updateMany({
    where: { id: bookingId, version: expectedVersion },
    data: { ...data, version: { increment: 1 } },
  });

  if (result.count === 0) {
    throw new StaleStateError(
      `Booking ${bookingId} was modified by another process (expected version ${expectedVersion})`,
    );
  }
};

const checkIdempotency = async (key) => {
  const existing = await prisma.idempotencyRecord.findUnique({
    where: { eventKey: key },
  });
  if (existing) {
    logger.info(`Idempotent request: ${key}`);
    return existing.response;
  }
  return null;
};

const saveIdempotency = async (key, response) => {
  await prisma.idempotencyRecord.create({
    data: { eventKey: key, response },
  });
};

const fetchUserForNotification = async (userId) => {
  try {
    const user = await userClient.getUserById(userId);
    return user ? { email: user.email, firstName: user.firstName } : {};
  } catch (err) {
    logger.warn("Failed to enrich booking event with user details", {
      userId,
      error: err instanceof Error ? err.message : String(err),
    });
    return {};
  }
};
const fetchStationName = async (stationId) => {
     if (!stationId) return null;
     try {
          const station = await stationClient.getStationById(stationId);
          return station ? station.name : null;
     } catch (/**@type{any} */ err) {
          // A 404 here is a wiring bug, not a transient failure — the enrichment is
          // best-effort, so it degrades silently and can go unnoticed for a long time.
          // Log it at error level so a broken internal route is actually visible.
          if (err.response?.status === 404) {
               logger.error('Station lookup returned 404 — internal route may be misconfigured', {
                    stationId,
                    url: err.config?.url,
               });
          } else {
               logger.warn('Failed to enrich booking event with station name', {
                    stationId,
                    error: err.message,
               });
          }
          return null;
     }
};
export const createBooking = async (
  userId,
  scheduleId,
  seatIds,
  passengers,
  idempotencyKey,
  fromStationId,
  toStationId,
  fromSeq,
  toSeq,
) => {
  // 1. Validate input
  if (
    !scheduleId ||
    !seatIds ||
    !Array.isArray(seatIds) ||
    seatIds.length === 0
  ) {
    throw new BadRequestError(
      "scheduleId and seatIds (non-empty array) are required",
    );
  }
  if (!passengers || !Array.isArray(passengers) || passengers.length === 0) {
    throw new BadRequestError("passengers (non-empty array) is required");
  }
  if (seatIds.length !== passengers.length) {
    throw new BadRequestError(
      "Number of seats must match number of passengers",
    );
  }
  if (!idempotencyKey) {
    throw new BadRequestError("idempotencyKey is required");
  }

  // --- SEGMENT BOOKING: Validate segment params if provided ---
  if (fromSeq && toSeq && fromSeq >= toSeq) {
    throw new BadRequestError(
      "fromStation must come before toStation in route",
    );
  }

  // 2. Check idempotency
  const cached = await checkIdempotency(`booking:${idempotencyKey}`);
  if (cached) return cached;

  // 3. Fetch schedule availability and seat details from inventory
  const availability = await inventoryClient.getAvailability(scheduleId);
  if (availability.status !== "ACTIVE") {
    throw new BadRequestError("Schedule is not active");
  }

  // Prevent booking trains that have already departed
  if (new Date(availability.departureDate) < new Date()) {
    throw new BadRequestError("Cannot book a train that has already departed");
  }
  // --- SEGMENT BOOKING: Pass segment params to get segment-aware seat availability ---
  const seatData = await inventoryClient.getSeats(scheduleId, {
    fromSeq: fromSeq || undefined,
    toSeq: toSeq || undefined,
  });

  const seatMap = new Map(seatData.seats.map((s) => [s.seatId, s]));

  // Verify all requested seats exist and are available
  const bookingSeats = [];
  let totalAmount = 0;
  for (const seatId of seatIds) {
    const seat = seatMap.get(seatId);
    if (!seat) {
      throw new NotFoundError(`Seat ${seatId} not found in schedule`);
    }
    // --- SEGMENT BOOKING: Use segmentStatus when available for segment-aware validation ---
    const isAvailable =
      fromSeq && toSeq && seat.segmentStatus !== undefined
        ? seat.segmentStatus === "AVAILABLE"
        : seat.status === "AVAILABLE";
    if (!isAvailable) {
      throw new ConflictError(
        `Seat #${seat.seatNumber} is not available for this segment`,
        "SEATS_UNAVAILABLE",
      );
    }
    bookingSeats.push(seat);
    totalAmount += seat.price;
  }
  // 4. Sort seatIds (deadlock prevention for distributed locks)
  const sortedSeatIds = [...seatIds].sort();
  // 5. Acquire Redis distributed locks (segment-aware keys for segment bookings)
  const { acquired, lockValue } = await acquireSeatLocks(
    scheduleId,
    sortedSeatIds,
    `pre-${Date.now()}`, // temporary ID before booking is created
    config.BOOKING_TTL_SECONDS,
    fromSeq, // --- SEGMENT BOOKING: include in lock key
    toSeq, // --- SEGMENT BOOKING: include in lock key
  );
  if (!acquired || !lockValue) {
    throw new ConflictError(
      "One or more seats are being booked by another user. Please try again.",
      "SEATS_LOCKED",
    );
  }

  let booking;
  try {
    // 6. Create booking record in DB
    const lockExpiresAt = new Date(
      Date.now() + config.BOOKING_TTL_SECONDS * 1000,
    );

    booking = await prisma.booking.create({
      data: {
        userId,
        scheduleId,
        trainId: availability.trainId,
        trainNumber: availability.trainNumber,
        trainName: availability.trainName,
        departureDate: new Date(availability.departureDate),
        status: "PENDING",
        totalAmount,
        seatCount: seatIds.length,
        fromStationId: fromStationId || null, // --- SEGMENT BOOKING
        toStationId: toStationId || null, // --- SEGMENT BOOKING
        fromSeq: fromSeq || null, // --- SEGMENT BOOKING
        toSeq: toSeq || null, // --- SEGMENT BOOKING
        idempotencyKey,
        lockExpiresAt,
        seats: {
          create: bookingSeats.map((seat, index) => ({
            seatId: seat.seatId,
            seatNumber: seat.seatNumber,
            seatType: seat.seatType,
            price: seat.price,
          })),
        },
        passengers: {
          create: passengers.map((p, index) => ({
            name: p.name,
            age: p.age,
            gender: p.gender,
            seatId: seatIds[index] || null, // use original order to match user's intended seat assignment
          })),
        },
      },
      include: { seats: true, passengers: true },
    });

    // 7. Execute saga Step 1: Hold seats in inventory
    await saga.executeHoldSeats(
      booking,
      sortedSeatIds,
      config.LOCK_TTL_SECONDS,
      fromSeq,
      toSeq,
    ); // --- SEGMENT BOOKING

    // 8. Execute saga Step 2: Create payment order
    const paymentOrder = await saga.executeCreatePayment(booking);

    // Refresh booking after updates
    booking = await prisma.booking.findUnique({
      where: { id: booking.id },
      include: { seats: true, passengers: true },
    });
    // 9. Save idempotency
    const response = {
      bookingId: booking.id,
      status: booking.status,
      totalAmount: booking.totalAmount,
      lockExpiresAt: booking.lockExpiresAt,
      seats: booking.seats.map((s) => ({
        seatId: s.seatId,
        seatNumber: s.seatNumber,
        seatType: s.seatType,
        price: s.price,
      })),
      passengers: booking.passengers.map((p) => ({
        name: p.name,
        age: p.age,
        gender: p.gender,
      })),
      paymentOrder: {
        paymentOrderId: paymentOrder.paymentOrderId,
        gatewayOrderId: paymentOrder.gatewayOrderId,
        amount: paymentOrder.amount,
        currency: paymentOrder.currency,
        keyId: paymentOrder.keyId,
      },
    };

    await saveIdempotency(`booking:${idempotencyKey}`, response);

    return response;
  } catch (error) {
    // Compensate on failure
    logger.error(`Booking creation failed for user ${userId}`, {
      error: error instanceof Error ? error.message : String(error),
    });

    if (booking) {
      await saga.compensateAll(booking, sortedSeatIds);
      await prisma.booking.update({
        where: { id: booking.id },
        data: {
          status: "FAILED",
          failureReason:
            /**@type{any} */ (error).response?.data?.message ||
            (error instanceof Error ? error.message : String(error)),
        },
      });
    }

    // Release Redis locks (segment-aware)
    await releaseSeatLocks(
      scheduleId,
      sortedSeatIds,
      lockValue,
      fromSeq,
      toSeq,
    );

    throw error;
  }
};

const TERMINAL_STATUSES = ["EXPIRED", "FAILED", "CANCELLED"];

const refundOrphanedPayment = async (booking, gatewayPaymentId, amount) => {
  const refundAmount =
    typeof amount === "number" && amount > 0 ? amount : booking.totalAmount;
  if (!booking.paymentOrderId) {
    logger.error(
      "CRITICAL: payment succeeded for a booking with no paymentOrderId — manual reconciliation required",
      { bookingId: booking.id, gatewayPaymentId, amount: refundAmount },
    );
    return;
  }
  // Record the intent before calling out, so a crash mid-refund is visible in the saga log.
  const sagaLog = await prisma.sagaLog.create({
    data: {
      bookingId: booking.id,
      step: "CREATE_PAYMENT",
      status: "COMPENSATING",
      request: {
        reason: "late_payment_refund",
        bookingStatus: booking.status,
        paymentOrderId: booking.paymentOrderId,
        gatewayPaymentId,
        amount: refundAmount,
      },
    },
  });

  try {
    // Dedicated idempotency key: independent of whatever compensation already ran
    // for this booking, and safe under Kafka redelivery.
    const result = await paymentClient.initiateRefund(
      booking.paymentOrderId,
      refundAmount,
      `late_payment_after_${booking.status.toLowerCase()}`,
      `${booking.id}-late-payment-refund`,
    );
    await prisma.sagaLog.update({
      where: { id: sagaLog.id },
      data: { status: "COMPENSATED", response: result },
    });
    logger.info(
      `Refunded late payment for ${booking.status} booking ${booking.id}`,
      {
        paymentOrderId: booking.paymentOrderId,
        amount: refundAmount,
      },
    );
    // Tell the user their money is coming back — nothing else in the system will.
    try {
      const userInfo = await fetchUserForNotification(booking.userId);
      await bookingProducer.publishBookingCancelled({
        bookingId: booking.id,
        userId: booking.userId,
        email: userInfo.email,
        firstName: userInfo.firstName,
        scheduleId: booking.scheduleId,
        reason: "late_payment_refunded",
        refundAmount,
      });
    } catch (err) {
      logger.error("Failed to publish refund notification for late payment", {
        bookingId: booking.id,
        error: err instanceof Error ? err.message : String(err),
      });
    }
    return result;
  } catch (error) {
    const errorMsg = extractPaymentError(error).message;

    await prisma.sagaLog.update({
      where: { id: sagaLog.id },
      data: { status: "FAILED", error: errorMsg },
    });

    logger.error(
      "CRITICAL: failed to refund late payment — money captured with no ticket issued",
      {
        bookingId: booking.id,
        paymentOrderId: booking.paymentOrderId,
        gatewayPaymentId,
        amount: refundAmount,
        error: errorMsg,
      },
    );

    // Rethrow so the consumer retries and, failing that, the message lands in the
    // DLQ. An unrefunded capture must never be swallowed.
    throw error;
  }
};

// ─── Handle Payment Success (Kafka consumer) ─────────────────────────────────
export const handlePaymentSuccess = async (
  paymentOrderId,
  gatewayPaymentId,
  amount,
) => {
  const booking = await prisma.booking.findUnique({
    where: { paymentOrderId },
    include: { seats: true, passengers: true },
  });
  if (!booking) {
    logger.warn(`No booking found for paymentOrderId: ${paymentOrderId}`);
    return;
  }
  // Idempotent: already confirmed
  if (booking.status === "CONFIRMED") {
    logger.info(`Booking ${booking.id} already confirmed`);
    return;
  }
  // Money was captured but the booking is already dead — refund it.
  if (TERMINAL_STATUSES.includes(booking.status)) {
    logger.warn(
      `Payment captured for booking ${booking.id} already in terminal status ${booking.status} — refunding`,
      { paymentOrderId, gatewayPaymentId },
    );
    await refundOrphanedPayment(booking, gatewayPaymentId, amount);
    return;
  }
  // CONFIRMING / CANCELLING are transient: another process owns this booking right
  // now and will resolve it (or the stuck-booking sweeper will). Don't interfere.
  if (booking.status !== "PAYMENT_PENDING") {
    logger.warn(
      `Booking ${booking.id} in transient status ${booking.status} — leaving to the owning process`,
    );
    return;
  }
  const seatIds = booking.seats.map((s) => s.seatId).sort();
  try {
    // Atomically claim this booking — if expiry job or cancel already changed it, bail out
    await casUpdateBooking(booking.id, booking.version, {
      status: "CONFIRMING",
    });

    // Execute saga Step 3: Confirm seats in inventory
    await saga.executeConfirmSeats(
      booking,
      seatIds,
      booking.fromSeq,
      booking.toSeq,
    ); // --- SEGMENT BOOKING

    // Final status update (version was already incremented by CAS above)
    await prisma.booking.updateMany({
      where: { id: booking.id, status: "CONFIRMING" },
      data: { status: "CONFIRMED", version: { increment: 1 } },
    });

    // Release Redis locks (segment-aware)
    await forceReleaseSeatLocks(
      booking.scheduleId,
      seatIds,
      booking.fromSeq,
      booking.toSeq,
    );
    // Publish BOOKING_CONFIRMED (retried by producer — log but don't fail the booking)
    try {
      const [userInfo, fromStationName, toStationName] = await Promise.all([
        fetchUserForNotification(booking.userId),
        fetchStationName(booking.fromStationId),
        fetchStationName(booking.toStationId),
      ]);

      await bookingProducer.publishBookingConfirmed({
        bookingId: booking.id,
        userId: booking.userId,
        email: userInfo.email,
        firstName: userInfo.firstName,
        scheduleId: booking.scheduleId,
        trainNumber: booking.trainNumber,
        trainName: booking.trainName,
        fromStationName,
        toStationName,
        departureDate: booking.departureDate,
        seats: booking.seats.map((s) => ({
          seatNumber: s.seatNumber,
          seatType: s.seatType,
          price: s.price,
        })),
        passengers: booking.passengers.map((p) => ({
          name: p.name,
          age: p.age,
          gender: p.gender,
        })),
        totalAmount: booking.totalAmount,
      });
    } catch (err) {
      logger.error(
        "CRITICAL: Failed to publish BOOKING_CONFIRMED after retries — notification/search may be stale",
        {
          bookingId: booking.id,
          error: err instanceof Error ? err.message : String(err),
        },
      );
    }

    logger.info(`Booking ${booking.id} confirmed successfully`);
  } catch (error) {
    // Another process claimed this booking between our read and our CAS. The common
    // case is the expiry job winning by milliseconds — which leaves the capture
    // orphaned exactly like the terminal-status branch above, so re-check and refund.
    if (/** @type{any}*/ (error).code === "STALE_STATE") {
      const fresh = await prisma.booking.findUnique({
        where: { id: booking.id },
      });

      if (fresh && TERMINAL_STATUSES.includes(fresh.status)) {
        logger.warn(
          `Booking ${booking.id} moved to ${fresh.status} while confirming — refunding captured payment`,
          { paymentOrderId, gatewayPaymentId },
        );
        await refundOrphanedPayment(fresh, gatewayPaymentId, amount);
        return;
      }

      logger.info(
        `Booking ${booking.id} already handled by another process, skipping`,
      );
      return;
    }

    logger.error(`Failed to confirm booking ${booking.id}`, {
      error: error instanceof Error ? error.message : String(error),
    });

    // Compensate: refund payment and release seats
    await saga.compensateAll(booking, seatIds);

    await prisma.booking.updateMany({
      where: {
        id: booking.id,
        status: { in: ["PAYMENT_PENDING", "CONFIRMING"] },
      },
      data: {
        status: "FAILED",
        failureReason: `confirm_failed: ${error instanceof Error ? error.message : String(error)}`,
        version: { increment: 1 },
      },
    });

    await forceReleaseSeatLocks(
      booking.scheduleId,
      seatIds,
      booking.fromSeq,
      booking.toSeq,
    );

    try {
      const userInfo = await fetchUserForNotification(booking.userId);
      await bookingProducer.publishBookingFailed({
        bookingId: booking.id,
        userId: booking.userId,
        email: userInfo.email,
        firstName: userInfo.firstName,
        scheduleId: booking.scheduleId,
        reason: "confirm_seats_failed",
      });
    } catch (err) {
      logger.error("Failed to publish BOOKING_FAILED after retries", {
        bookingId: booking.id,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
};

// ─── Handle Payment Failure (Kafka consumer) ─────────────────────────────────

const handlePaymentFailure = async (paymentOrderId, reason) => {
     const booking = await prisma.booking.findUnique({
          where: { paymentOrderId },
          include: { seats: true },
     });

     if (!booking) {
          logger.warn(`No booking found for paymentOrderId: ${paymentOrderId}`);
          return;
     }

     // Idempotent
     if (booking.status === 'FAILED' || booking.status === 'CANCELLED' || booking.status === 'EXPIRED') {
          logger.info(`Booking ${booking.id} already in terminal state: ${booking.status}`);
          return;
     }

     if (booking.status !== 'PAYMENT_PENDING') {
          logger.warn(`Booking ${booking.id} in unexpected status: ${booking.status}`);
          return;
     }

     const seatIds = booking.seats.map(s => s.seatId).sort();

     // Atomically claim this booking before compensating
     try {
          await casUpdateBooking(booking.id, booking.version, {
               status: 'FAILED',
               failureReason: reason || 'payment_failed',
          });
     } catch (/**@type{any} */ error) {
          if (error.code === 'STALE_STATE') {
               logger.info(`Booking ${booking.id} already handled by another process, skipping`);
               return;
          }
          throw error;
     }

     // Compensate: release held seats
     await saga.compensateHoldSeats(booking, seatIds);

     // Release Redis locks (segment-aware)
     await forceReleaseSeatLocks(booking.scheduleId, seatIds, booking.fromSeq, booking.toSeq);

     // Publish BOOKING_FAILED
     try {
          const userInfo = await fetchUserForNotification(booking.userId);
          await bookingProducer.publishBookingFailed({
               bookingId: booking.id,
               userId: booking.userId,
               email: userInfo.email,
               firstName: userInfo.firstName,
               scheduleId: booking.scheduleId,
               reason: reason || 'payment_failed',
          });
     } catch (err) {
          logger.error('Failed to publish BOOKING_FAILED after retries', { bookingId: booking.id, error: err instanceof Error?err.message:String(err) });
     }

     logger.info(`Booking ${booking.id} failed: ${reason}`);
};
