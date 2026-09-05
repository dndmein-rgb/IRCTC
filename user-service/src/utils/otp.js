import { config } from "../config/index.js";
import { redis } from "../config/redis.js";
import { TooManyRequestsError } from "./error.js";
import otpGenerator from "otp-generator";
import crypto from "crypto";

const RATE_MAX = config.OTP_RATE_MAX_PER_HOUR;
const ATTEMPT_MAX = config.OTP_MAX_VERIFY_ATTEMPTS;
const OTP_TTL = config.OTP_TTL;
const HMAC_SECRET = config.OTP_HMAC_SECRET;

function hmacFor(email, otp) {
  return crypto
    .createHmac("sha256", HMAC_SECRET)
    .update(email + ":" + otp)
    .digest("hex");
}

export async function generateAndStoreOtp(meta) {
  // how many otp's you can send in an hour
  const rateKey = `otp:rate:${meta.email}`;
  const sentCount = parseInt((await redis.get(rateKey)) || "0", 10);
  if (sentCount >= RATE_MAX) {
    throw new TooManyRequestsError(
      "Too many OTP requests. Try again later.",
      "OTP_RATE_LIMIT",
    );
  }
  const otp = otpGenerator.generate(6, {
    upperCaseAlphabets: false,
    lowerCaseAlphabets: false,
    specialChars: false,
  });
  const otpSessionId = crypto.randomUUID();
  const hashed = hmacFor(meta.email, otp);
  await redis.set(
    `otp:session:${otpSessionId}`,
    JSON.stringify({
      hashedOtp: hashed,
      meta,
    }),
    "EX",
    OTP_TTL,
  );
  await redis.incr(rateKey);
  await redis.expire(rateKey, 3600);
  return { otp, otpSessionId };
}

export async function verifyOtp(otp, otpSessionId) {
  const rawData = await redis.get(`otp:session:${otpSessionId}`);
  if (!rawData) return null;
  const { hashedOtp: storedOtp, meta } = JSON.parse(rawData);
  const attemptsKey = `otp:attempts:${meta.email}`;
  const attemptCount = parseInt((await redis.get(attemptsKey)) || "0", 10);
  if (attemptCount >= ATTEMPT_MAX) {
    throw new TooManyRequestsError("Too many requests to verify OTP");
  }
  const hashedOtp = hmacFor(meta.email, otp);
  if (
    crypto.timingSafeEqual(
      Buffer.from(hashedOtp, "hex"),
      Buffer.from(storedOtp, "hex"),
    )
  ) {
    await redis.del(`otp:session:${otpSessionId}`, attemptsKey);
    await redis.del(`otp:rate:${meta.email}`);
    return meta;
  } else {
    await redis.incr(attemptsKey);
    await redis.expire(attemptsKey, OTP_TTL);
    return null;
  }
}
