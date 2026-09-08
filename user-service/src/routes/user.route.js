import express from "express"
import { getProfile } from "../controllers/user.controller.js";
import { getUserContext } from "../middlewares/getUserContext.js";

const router = express.Router();

router.get("/profile",getUserContext,getProfile)


export default router;