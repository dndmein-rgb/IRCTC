import { asyncHandler } from "../utils/asyncHandler.js";
import { BadRequestError } from "../utils/error.js";
import * as stationService from "../services/station.service.js"

export const createStation = asyncHandler(async(req,res) => {
  const { name, code, city, state } = req.body;
  if (!name || !code || !city || !state) {
           throw new BadRequestError('stationCode, stationName, city and state are required');
      }
  const station = await stationService.createStation({
            code: code.toUpperCase(),
            name,
            city,
            state
       });
  
       res.status(201).json({
            success: true,
            message: 'Station Created Successfully',
            data: station
       })
  })
