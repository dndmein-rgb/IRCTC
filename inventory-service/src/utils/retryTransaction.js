import { logger } from "../config/logger.js";


export async function retryTransaction(fn, maxRetries = 3) {
     for (let attempt = 1; attempt <= maxRetries; attempt++) {
          try {
               return await fn();
          } catch (error) {
                         const errorCode =
                              error instanceof Error && "code" in error
                                   ? error.code
                                   : undefined;
          
                         const errorMessage =
                              error instanceof Error
                                   ? error.message
                                   : String(error);
          
                         const isRetryable =
                              errorCode === "P2034" ||
                              errorMessage.includes("could not serialize") ||
                              errorMessage.includes("could not obtain lock") ||
                              errorMessage.includes("deadlock detected");

               if (isRetryable && attempt < maxRetries) {
                    const delay = 50 * attempt;
                    logger.warn(`Transaction attempt ${attempt} failed (retryable), retrying in ${delay}ms...`);
                    await new Promise(r => setTimeout(r, delay));
                    continue;
               }
               throw error;
          }
     }
}
