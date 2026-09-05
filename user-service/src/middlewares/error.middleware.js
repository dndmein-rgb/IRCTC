import logger from "../config/logger.js";
import { AppError } from "../utils/error.js";

export const errorHandler = (err, req, res, next) => {
  console.error("\n========== ERROR ==========");
  console.error("Message:", err?.message);
  console.error("Stack:", err?.stack);
  console.error("===========================\n");

  if (err instanceof AppError) {
    return res.status(err.statusCode).json({
      success: false,
      error: err.code,
      message: err.message,
    });
  }

  logger.error({
    message: err instanceof Error ? err.message : String(err),
    stack: err instanceof Error ? err.stack : undefined,
    path: req.path,
    method: req.method,
  });

  return res.status(500).json({
    success: false,
    error: "SERVER_ERROR",
    message: "Internal Server Error",
  });
};