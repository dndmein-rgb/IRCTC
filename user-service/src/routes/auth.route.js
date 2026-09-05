import express from "express";
import {
  login,
  rotateRefreshToken,
  sendOTP,
  verifyOTP,
} from "../controllers/auth.controller.js";

const router = express.Router();

router.post("/send-otp", sendOTP);
router.post("/verify-otp", verifyOTP);
router.post("/login", login);
router.post("/rotate", rotateRefreshToken);

export default router;
