import express from "express"
import { requireAuth } from "../middlewares/auth.middleware.js";
import { getProfile } from "../controllers/user.controller.js";

const router = express.Router();

router.get("/user-profile",requireAuth,getProfile)


export default router;