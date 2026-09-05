import logger from "../config/logger.js";
import prisma from "../config/prisma.js";
import { sendOtpEmail, verifyOtpEmail } from "../utils/email.js";
import { BadRequestError, ConflictError } from "../utils/error.js";
import bcrypt from "bcrypt";
import { generateAndStoreOtp, verifyOtp } from "../utils/otp.js";

export const sendOTP = async (firstName, lastName, email, password) => {
  const existingUser = await prisma.user.findUnique({
    where: { email },
  });

  if (existingUser) {
    throw new ConflictError("user already exists");
  }
  const hashedPassword = await bcrypt.hash(password, 12);
  const meta = { firstName, lastName, email, hashedPassword };
  const { otp, otpSessionId } = await generateAndStoreOtp(meta);
  await sendOtpEmail(email, otp);
  logger.info(`OTP email queued for : ${email}`);
  return { otpSessionId };
};

export const verifyOTP = async (otp, otpSessionId) => {
  const meta = await verifyOtp(otp, otpSessionId);
  if (meta === null) {
    throw new BadRequestError("Invalid or expired OTP", "OTP_INVALID");
  }
  const user = await prisma.user.create({
    data: {
      firstName: meta.firstName,
      lastName: meta.lastName,
      email: meta.email,
      password: meta.hashedPassword,
      emailVerified: true,
    },
  });
  await verifyOtpEmail(meta)
  return user
};
