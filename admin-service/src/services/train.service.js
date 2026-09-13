import logger from "../config/logger.js";
import prisma from "../config/prisma.js";
import { adminProducer } from "../kafka/producer/admin.producer.js";
import {
  BadRequestError,
  ConflictError,
  NotFoundError,
} from "../utils/error.js";

export const createTrain = async (data) => {
  const { trainNumber, trainName, coachName, seats } = data;
  const existing = await prisma.train.findUnique({
    where: { trainNumber },
  });
  if (existing) {
    throw new ConflictError("Train with this number already exists");
  }
  const seatNumbers = seats.map((s) => s.seatNumber);
  if (new Set(seatNumbers).size !== seatNumbers.length) {
    throw new BadRequestError("Duplicate seat numbers found");
  }
  const train = await prisma.train.create({
    data: {
      trainNumber,
      trainName,
      coachName: coachName || "AC",
      totalSeats: seats.length,
      seats: {
        create: seats.map((seat) => ({
          seatNumber: seat.seatNumber,
          seatType: seat.seatType,
          price: seat.price,
        })),
      },
    },
    include: { seats: { orderBy: { seatNumber: "asc" } } },
  });

  await adminProducer.publishTrainCreated(train).catch((err) => {
    logger.error("Failed to publish train created event", {
      error: err.message,
    });
  });

  return train;
};

export const getTrainById = async (id) => {
  const train = await prisma.train.findUnique({
    where: { id },
    include: {
      seats: { orderBy: { seatNumber: 'asc' } },
      route: {
        include: {
          routeStations: {
            include: { station: true },
            orderBy:{sequenceNumber:'asc'}
          }
        }
      }}
  })
  if (!train) throw new NotFoundError('Train not found');
       return train;
}

export const createRoute = async (data) => {
  const { trainId, stations } = data;

  const train = await prisma.train.findUnique({
    where: { id: trainId },
  });

  if (!train) {
    throw new NotFoundError("Train not found");
  }
  const existingRoute = await prisma.route.findUnique({
    where: { trainId },
  });
  if (existingRoute) {
    throw new ConflictError("Route already exists for this train");
  }

  const stationIds = stations.map((station) => station.stationId);
  const existingStations = await prisma.station.findMany({
    where: { id: { in: stationIds } },
  });

  if (existingStations.length !== stationIds.length) {
    throw new BadRequestError("One or more station IDs are invalid");
  }

  const sorted = [...stations].sort(
    (a, b) => a.sequenceNumber - b.sequenceNumber,
  );
  for (let i = 0; i < sorted.length; i++) {
    if (sorted[i].sequenceNumber !== i + 1) {
      throw new BadRequestError(
        "Sequence numbers must be continuous starting from 1",
      );
    }
  }
  const route = await prisma.route.create({
    data: {
      trainId,
      routeStations: {
        create: stations.map((s) => ({
          stationId: s.stationId,
          sequenceNumber: s.sequenceNumber,
          arrivalTime: s.arrivalTime || null,
          departureTime: s.departureTime || null,
          distanceFromOrigin: s.distanceFromOrigin || 0,
        })),
      },
    },
    include: {
      routeStations: {
        include: { station: true },
        orderBy: { sequenceNumber: "asc" },
      },
    },
  });

  const trainWithSeats = await prisma.train.findUnique({
    where: { id: trainId },
    include: { seats: { orderBy: { seatNumber: "asc" } } },
  });
  await adminProducer.publishRouteCreated({ ...route, train: trainWithSeats });
  return route;
};
