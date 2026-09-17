import {Redis} from "ioredis";
import { logger } from "./logger.js";
import { config } from "./index.js";


class RedisClient {
     static instance;
     static isConnected = false;

     constructor(){
          // prevent direct instantiation
     }

     static getInstance(){
       if (!RedisClient.instance) {
         const redisUrl = config.REDIS_URL;
   
         if (!redisUrl) {
           throw new Error("REDIS_URL is not defined in the configuration.");
         }
               RedisClient.instance = new Redis(redisUrl, {
                    retryStrategy: (times) =>{
                         const delay = Math.min(times * 50, 2000);
                         return delay;
                    },
                    maxRetriesPerRequest: 3
               })

               RedisClient.setupEventListeners();
          }
          return RedisClient.instance;
     }

     static setupEventListeners(){
          RedisClient.instance.on('connect', () =>{
               RedisClient.isConnected = true;
               logger.info("Connected to Redis");
          })

          RedisClient.instance.on('error', (error) =>{
               RedisClient.isConnected = false;
               logger.error("Redis connection error", error);
          })

          RedisClient.instance.on('close', () =>{
               RedisClient.isConnected = false;
               logger.warn("Redis connection closed");
          })

          RedisClient.instance.on('reconnecting', () =>{
               logger.warn("Reconnecting to Redis...");
          })

          RedisClient.instance.on('ready', () =>{
               logger.info("Redis client is ready");
          })

          RedisClient.instance.on('end', () =>{
               RedisClient.isConnected = false;
               logger.warn("Redis connection ended");
          })
     }

     static async closeConnection(){
          if(RedisClient.instance){
               try{
                    await RedisClient.instance.quit();
                    logger.info("Redis connection closed");
               }catch(error){
                    logger.error("Error closing Redis connection: ", error);
               }
          }
     }

     static isReady(){
          return RedisClient.isConnected;
     }
}

export const redis = RedisClient.getInstance();

export default RedisClient;