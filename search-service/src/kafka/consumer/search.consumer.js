import { connectProducer, consumer } from "../../config/kafka.js";
import { logger } from "../../config/logger.js";
import { KAFKA_TOPICS } from "../../../../shared/constants/constants.js"
import {withDLQ}  from "../../../../shared/utils/dlqHandler.js"

class SearchConsumer {
     async start() {
          await consumer.connect();
          await connectProducer(); // needed for DLQ publishing
          logger.info('Search consumer connected');

          await consumer.subscribe({
               topics: [
                    KAFKA_TOPICS.STATION_CREATED,
                    KAFKA_TOPICS.ROUTE_CREATED,
                    KAFKA_TOPICS.SCHEDULE_CREATED,
                    KAFKA_TOPICS.SCHEDULE_CANCELLED,
                    KAFKA_TOPICS.SEAT_AVAILABILITY_UPDATED,
               ],
               fromBeginning: true,
          });

          await consumer.run({
               eachMessage: withDLQ(producer, KAFKA_TOPICS.DLQ_SEARCH, logger, async ({ topic, partition, message, parsedValue }) => {
                    logger.info(`Processing ${topic}`, { partition, offset: message.offset });

                    switch (topic) {
                         case KAFKA_TOPICS.STATION_CREATED:
                              await searchService.indexStation(parsedValue);
                              break;
                         case KAFKA_TOPICS.ROUTE_CREATED:
                              await searchService.indexTrainRoute(parsedValue);
                              break;
                         case KAFKA_TOPICS.SCHEDULE_CREATED:
                              await searchService.indexSchedule(parsedValue);
                              break;
                         case KAFKA_TOPICS.SCHEDULE_CANCELLED:
                              await searchService.cancelSchedule(parsedValue);
                              break;
                         case KAFKA_TOPICS.SEAT_AVAILABILITY_UPDATED:
                              await searchService.updateSeatAvailability(parsedValue);
                              break;
                         default:
                              logger.warn(`Unknown topic: ${topic}`);
                    }
               }),
          });

          logger.info('Search consumer running...');
     }
}

export const searchConsumer=new SearchConsumer()