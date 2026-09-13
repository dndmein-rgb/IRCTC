import { asyncHandler } from "../utils/asyncHandler.js";
import { BadRequestError } from "../utils/error.js";
import * as scheduleService  from "../services/schedule.service.js"

export const createSchedule = asyncHandler(async(req,res) => {
  const { trainId, departureDate } = req.body;
  if(!trainId || !departureDate){
            throw new BadRequestError('trainId and departureDate are required');
       }
  
       const schedule = await scheduleService.createSchedule({trainId, departureDate});
       return res.status(201).json({
            success: true,
            message: "Train Schedule created successfully",
            data: schedule
       })
})