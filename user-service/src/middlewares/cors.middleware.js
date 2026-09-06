import cors from "cors"
import { config } from "../config/index.js";

const allowedOrigins = config.ALLOWED_ORIGINS ? config.ALLOWED_ORIGINS.split(',').map(o => o.trim()) : []

export const corsMiddleware = cors({
  origin: function (origin, callback) {
    if (!origin) return callback(null, true)
    console.log("ALLOWED:", allowedOrigins);
    console.log("REQUEST ORIGIN:", origin);
    if (allowedOrigins.includes(origin)) {
      callback(null,true)
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
       methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
       allowedHeaders: ['Content-Type', 'Authorization'],
})

