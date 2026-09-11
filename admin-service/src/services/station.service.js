import logger from "../config/logger.js";
import prisma from "../config/prisma.js";
import { adminProducer } from "../kafka/producer/admin.producer.js";
import { BadRequestError, ConflictError } from "../utils/error.js";

export const createStation = async (data) => {
  const existing = await prisma.station.findUnique({
    where:{code:data.code}
  })
  if (existing) {
           throw new ConflictError('Station code already exists');
      }
  const station = await prisma.station.create({
    data
  })
  logger.info('Station Created', { id: station.id, code: station.code });

  // Publish event - fire and forget (non-critical)
  await adminProducer.publishStationCreated(station).catch((err => {
    logger.error('Failed to publish station created event', { error: err.message });
  }))
   return station;
}

export const getAllStations = async(page,limit,search) => {
  const skip = (page - 1) * limit;
  
}