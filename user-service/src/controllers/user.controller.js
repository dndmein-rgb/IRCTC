import { asyncHandler } from "../utils/asyncHandler.js";
import { BadRequestError } from "../utils/error.js";
import * as userService from "../services/user.service.js";

export const getProfile = asyncHandler(async (req, res) => {
  const userId = req.user.id;
  if (!userId) {
    throw new BadRequestError("User Id is missing");
  }
  const user = await userService.getProfile(userId);
  return res.status(200).json({
    success: true,
    message: "Fetched user details",
    data: {
      user,
    },
  });
});
