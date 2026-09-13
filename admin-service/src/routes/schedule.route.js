import express from 'express'
import { getUserContext } from '../middlewares/getUserContext.js';
import { createSchedule } from '../controllers/schedule.controller.js';

const router = express.Router();

router.post("/schedule", getUserContext, createSchedule);
// router.put('/schedule/:scheduleId', getUserContext, cancelSchedule);
// router.get('/schedule', getUserContext, getAllSchedules);
export default router;