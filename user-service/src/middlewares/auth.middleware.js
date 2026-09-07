import jwt from "jsonwebtoken";
import { UnauthorizedError } from "../utils/error.js";
import { config } from "../config/index.js";

export const requireAuth = (req, res, next) => {
  try {
    let accessToken;
    // 1. Try Authorization header
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith("Bearer ")) {
      accessToken = authHeader.split(" ")[1];
    }
    // 2. Fall back to httpOnly cookie (browser clients)
    if (!accessToken && req.cookies) {
      accessToken = req.cookies.accessToken;
    }
    if (!accessToken) {
      throw new UnauthorizedError("Authorization token missing");
    }
    // Verify access token

    const payload = jwt.verify(accessToken, config.JWT_ACCESS_SECRET);

    // jwt.verify() => string | JwtPayload
    if (typeof payload === "string") {
      throw new UnauthorizedError("Invalid token payload", "TOKEN_INVALID");
    }

    // Now payload is JwtPayload
    if (!payload.id || typeof payload.id !== "string") {
      throw new UnauthorizedError("Invalid token payload", "TOKEN_INVALID");
    }

    // Attach user context to request for downstream services
    req.user = {
      id: payload.id,
    };
  } catch (err) {
    if (err instanceof jwt.TokenExpiredError) {
      return next(
        new UnauthorizedError("Access token expired", "TOKEN_EXPIRED"),
      );
    }

    if (err instanceof jwt.JsonWebTokenError) {
      return next(
        new UnauthorizedError("Invalid access token", "TOKEN_INVALID"),
      );
    }
    return next(err);
  }
}
