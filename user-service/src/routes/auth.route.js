import express from "express";
import {
  login,
  rotateRefreshToken,
  sendOTP,
  verifyGoogleIdToken,
  verifyOTP,
} from "../controllers/auth.controller.js";

const router = express.Router();

router.post("/send-otp", sendOTP);
router.post("/verify-otp", verifyOTP);
router.post("/login", login);
router.post("/rotate", rotateRefreshToken);
router.post("/google-auth", verifyGoogleIdToken);

export default router;
