import { asyncHandler } from "../utils/asyncHandler.js";
import { BadRequestError } from "../utils/error.js";
import * as searchService from "../services/search.service.js";

export const searchTrains = asyncHandler(async (req, res) => {
  const { from, to, date } = req.query;
  if (!from || !to)
    throw new BadRequestError("from and to station names/codes are required");
  const results = await searchService.searchTrains(from, to, date || null);
  res.json({ success: true, data: results });
});

export const autocomplete = asyncHandler(async (req, res) => {
  const { q } = req.query;
  if (!q || q.length < 2)
    throw new BadRequestError("Provide at least 2 characters");
  const suggestions = await searchService.autocompleteStation(q);
  res.json({ success: true, data: suggestions });
});

export const debugStations = asyncHandler(async (req, res) => {
  const data = await searchService.getAllStations();
  res.json({ success: true, count: data.length, data });
});

export const debugTrains = asyncHandler(async (req, res) => {
  const data = await searchService.getAllTrains();
  res.json({ success: true, count: data.length, data });
});
