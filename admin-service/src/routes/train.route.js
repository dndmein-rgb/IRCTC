import express from 'express'
import { getUserContext } from '../middlewares/getUserContext.js';
import { createTrain } from '../controllers/train.controller.js';

const router = express.Router();

router.post("/train", getUserContext, createTrain);
// router.get("/train", getUserContext, getAllTrains);
// router.get("/train/:trainId", getUserContext, getTrainById);
// router.post("/route", getUserContext, createRoute);

export default router