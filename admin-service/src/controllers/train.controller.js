import  * as trainService  from "../services/train.service.js";
import { asyncHandler } from "../utils/asyncHandler.js";
import { BadRequestError } from "../utils/error.js";

export const createTrain = asyncHandler(async(req,res) => {
  const { trainName, trainNumber, seats, coachName } = req.body;
  if(!trainNumber || !trainName || !coachName || !seats){
           throw new BadRequestError("trainNumber, trainName, and seats are required");
      }
 
      if(seats.length === 0){
           throw new BadRequestError("Atleast one seat must be defined...")
  }
      const train = await trainService.createTrain({trainNumber, trainName, coachName, seats});
          return res.status(201).json({
               success: true,
               message: "Train added successfully",
               data: train
          })
})