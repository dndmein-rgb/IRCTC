/**
 * @typedef {import("jsonwebtoken").JwtPayload & {
 *   id: string,
 *   jti: string
 * }} RefreshTokenPayload
 */

/**
 * @param {string} refreshToken
 * @returns {RefreshTokenPayload}
 */
import crypto from "crypto";
import jwt from "jsonwebtoken";
import { config } from "../config/index.js";

export const hashToken = (refreshToken) => {
  return crypto.createHash("sha256").update(refreshToken).digest("hex");
};

export const generateAccessToken = (userId) => {
  const payload = { id: userId };
  return jwt.sign(payload, config.JWT_ACCESS_SECRET, {
    expiresIn: config.ACCESS_TOKEN_EXP,
  });
};

export const generateRefreshToken = (userId) => {
  const jti = crypto.randomUUID();

  const payload = {
    id: userId,
    jti,//refreshTokenId
  };
  const refreshToken = jwt.sign(payload, config.JWT_REFRESH_SECRET, {
      expiresIn: config.REFRESH_TOKEN_EXP,
    });
  
    return { refreshToken, jti };
};

export const verifyAccessToken = (accessToken) => {
  return jwt.verify(accessToken, config.JWT_ACCESS_SECRET);
};

export const verifyRefreshToken = (refreshToken) => {
  const payload = jwt.verify(
    refreshToken,
    config.JWT_REFRESH_SECRET
  );

  if (typeof payload === "string") {
    throw new Error("Invalid refresh token");
  }

  return /** @type {RefreshTokenPayload} */ (payload);
};