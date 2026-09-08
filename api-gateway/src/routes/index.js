import express from "express";
import { createProxy } from "../services/proxy.js";
import { config } from "../config/index.js";
import { combinedRateLimit, endpointRateLimit } from "../middlewares/rateLimit.middleware.js";
import { requireAuth } from "../middlewares/auth.middleware.js";
const router = express.Router();

/**
 * USER SERVICE ROUTES
 * Gateway Path: /api/users/auth/login
 * Service Path: /auth/login
**/

const userServiceProxy = createProxy('userService', config.SERVICES.USER_SERVICE_URL);

// public routes
router.post(
     '/users/auth/send-otp',
     endpointRateLimit(5, 3600000), // 5 requests per hour
     userServiceProxy
);

router.post(
     '/users/auth/verify-otp',
     endpointRateLimit(10, 3600000), // 10 requests per hour
     userServiceProxy
);

router.post(
     '/users/auth/login',
     endpointRateLimit(100, 900000),// 100 requests per 15 minutes
     userServiceProxy
);

router.post(
     '/users/auth/google-auth',
     endpointRateLimit(10, 900000), // 10 requests per 15 minutes
     userServiceProxy
);

router.post(
     '/users/auth/refresh',
     endpointRateLimit(20, 900000), // 20 requests per 15 minutes
     userServiceProxy
);

// private routes
router.get(
     '/users/user/profile',
     requireAuth,
     combinedRateLimit(),
     userServiceProxy
)

router.put(
     '/users/user/profile',
     requireAuth,
     combinedRateLimit(),
     userServiceProxy
)

router.delete(
     '/users/user/profile',
     requireAuth,
     combinedRateLimit(),
     userServiceProxy
)


export default router;