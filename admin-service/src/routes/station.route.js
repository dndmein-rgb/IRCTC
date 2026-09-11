import express from 'express'
import { getUserContext } from '../middlewares/getUserContext.js';
import { createStation } from '../controllers/station.controller.js';

const router = express.Router();

// router.get("/station/internal/:stationId", internalAuth, getStationByIdInternal);

// router.get("/station", getUserContext, getAllStations);
// router.get("/station/:stationId", getUserContext, getStationById);
router.post("/station", getUserContext, createStation);

export default router
