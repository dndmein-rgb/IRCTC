import logger from "../config/logger.js";
import prisma from "../config/prisma.js";
import { adminProducer } from "../kafka/producer/admin.producer.js";
import { BadRequestError, ConflictError } from "../utils/error.js";

export const createTrain = async (data) => {
  const { trainNumber, trainName, coachName, seats } = data;
  const existing = await prisma.train.findUnique({
    where:{trainNumber}
  })
  if (existing) {
     throw new ConflictError("Train with this number already exists");
  }
  const seatNumbers = seats.map((s) => s.seatNumber);
  if (new Set(seatNumbers).size !== seatNumbers.length) {
           throw new BadRequestError('Duplicate seat numbers found');
      }
  const train = await prisma.train.create({
            data: {
                 trainNumber,
                 trainName,
                 coachName: coachName || 'AC',
                 totalSeats: seats.length,
                 seats: {
                      create: seats.map((seat) => ({
                           seatNumber: seat.seatNumber,
                           seatType: seat.seatType,
                           price: seat.price
                      }))
                 }
            },
            include: { seats: { orderBy: { seatNumber: 'asc' } } }
       })

  await adminProducer.publishTrainCreated(train).catch((err) => {
            logger.error('Failed to publish train created event', { error: err.message });
       });
  
       return train;
}