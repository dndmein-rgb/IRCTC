import { config } from "../config/index.js";
import { asyncHandler } from "../utils/asyncHandler.js";
import { BadRequestError } from "../utils/error.js";
import * as authService from "../services/auth.service.js";

export const sendOTP = asyncHandler(async (req, res) => {
  const { firstName, lastName, email, password, confirmPassword } = req.body;
  if (!firstName || !lastName || !email || !password || !confirmPassword) {
    throw new BadRequestError("All fields are mandatory");
  }

  if (password !== confirmPassword) {
    throw new BadRequestError("Password mismatch");
  }

  const { otpSessionId } = await authService.sendOTP(
    firstName,
    lastName,
    email,
    password,
  );
  res
    .cookie("otp_session", otpSessionId, {
      secure: true,
      httpOnly: true,
      sameSite: "strict",
      maxAge: Number(config.OTP_TTL) * 1000,
    })
    .status(200)
    .json({
      success: true,
      message: "OTP sent successfully",
    });
});

export const verifyOTP = asyncHandler(async (req, res) => {
  const { otp } = req.body;
  const otpSessionId = req.cookies.otp_session;

  if (!otp || !otpSessionId) {
    throw new BadRequestError("OTP or OTPSession is missing");
  }
  const user = await authService.verifyOTP(otp, otpSessionId);
  res.clearCookie("otp_session");
  return res.status(201).json({
    success: true,
    message: "User Account created successfully",
    data: user,
  });
});
