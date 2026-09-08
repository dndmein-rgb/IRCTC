import "dotenv/config"
import packageJson from "../../package.json" with { type: "json" };

export const config = {
  PORT: process.env.PORT || 4000,
  SERVICE_NAME: packageJson.name,
  NODE_ENV: process.env.NODE_ENV || "development",

  REDIS_URL: process.env.REDIS_URL,
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS || "http://localhost:3000",

  JWT_ACCESS_SECRET:
    process.env.JWT_ACCESS_SECRET ||
    "0f8bf908f8d38527c188c93bda49d48bd421a43fa0bdf3e77de1f0db785e6f37",
  JWT_REFRESH_SECRET:
    process.env.JWT_REFRESH_SECRET ||
    "826d2c0edb5ad8f8ac7668556c034ea228931a49576aefccc80d6f469cc4a34c4da82ca43a5c43de91ffdad2f4644c655e2eb3ccbb8bc2848cb64fe7ea2a1ab9",
  ACCESS_TOKEN_EXP: process.env.ACCESS_TOKEN_EXP,
  REFRESH_TOKEN_EXP: process.env.REFRESH_TOKEN_EXP,
  ACCESS_TOKEN_EXP_SEC: parseInt(process.env.ACCESS_TOKEN_EXP_SEC || "900", 10),
  REFRESH_TOKEN_EXP_SEC: parseInt(
    process.env.REFRESH_TOKEN_EXP_SEC || "604800",
    10,
  ),

  RATE_LIMIT_WINDOW_MS: parseInt(
    process.env.RATE_LIMIT_WINDOW_MS || "900000",
    10,
  ),
  RATE_LIMIT_MAX_REQUESTS: parseInt(
    process.env.RATE_LIMIT_MAX_REQUESTS || "100",
    10,
  ),

  SERVICES: {
    USER_SERVICE_URL: process.env.USER_SERVICE_URL || "http://localhost:4001",
    SEARCH_SERVICE_URL:
      process.env.SEARCH_SERVICE_URL || "http://localhost:4002",
    ADMIN_SERVICE_URL: process.env.ADMIN_SERVICE_URL || "http://localhost:4003",
    NOTIFICATION_SERVICE_URL:
      process.env.NOTIFICATION_SERVICE_URL || "http://localhost:4004",
    BOOKING_SERVICE_URL:
      process.env.BOOKING_SERVICE_URL || "http://localhost:4005",
    PAYMENT_SERVICE_URL:
      process.env.PAYMENT_SERVICE_URL || "http://localhost:4006",
    INVENTORY_SERVICE_URL:
      process.env.INVENTORY_SERVICE_URL || "http://localhost:4007",
  },

  SERVICE_TIMEOUT_MS: parseInt(process.env.SERVICE_TIMEOUT_MS || "60000", 10),

  CIRCUIT_BREAKER_THRESHOLD: parseInt(
    process.env.CIRCUIT_BREAKER_THRESHOLD || "5",
    10,
  ),
  CIRCUIT_BREAKER_TIMEOUT: parseInt(
    process.env.CIRCUIT_BREAKER_TIMEOUT || "60000",
    10,
  ),
};

const requiredConfig = ["JWT_ACCESS_SECRET", "JWT_REFRESH_SECRET"];

requiredConfig.forEach((key) => {
  if (!config[key]) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
});
