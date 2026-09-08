import { NotFoundError } from "../utils/error.js";

export const notFound = (req,res,next) => {
  next(new NotFoundError(`Route ${req.method} ${req.path} not found`))
}