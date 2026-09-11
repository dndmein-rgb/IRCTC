import { config } from "../config/index.js";
import logger from "../config/logger.js";
import { AppError } from "../utils/error.js";

export const errorHandler = (err, req, res, next) => {
  // Always log the full error internally
  logger.error({
    message: err?.message || "Unknown error",
    stack: err?.stack,
    code: err?.code,
    path: req.path,
    method: req.method,
    // optional: userId if you attach it later
  });

  if (err instanceof AppError) {
    return res.status(err.statusCode).json({
      success: false,
      error: err.code,
      message: err.message,
    });
  }

  // Prisma known errors (optional but useful)
  if (err.code === "P2002") {
    // Unique constraint violation
    return res.status(409).json({
      success: false,
      error: "CONFLICT",
      message: "Resource already exists",
    });
  }

  if (err.code === "P2025") {
    // Record not found
    return res.status(404).json({
      success: false,
      error: "NOT_FOUND",
      message: "Resource not found",
    });
  }

  // Unexpected errors
  const isDev = config.NODE_ENV === "development";

  return res.status(500).json({
    success: false,
    error: "SERVER_ERROR",
    message: isDev ? err.message : "Internal Server Error",
    ...(isDev && { stack: err.stack }),
  });
};
