/**
 * GET /api/colleges
 * Query params: limit (default 50), offset (default 0), country
 * Returns JSON array of colleges/universities.
 */
import type { VercelRequest, VercelResponse } from "@vercel/node";
import { loadMasterDatabase } from "../scripts/load-master-db";

function getErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err ?? "");
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.status(204).end();
  }

  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  try {
    const country = req.query.country as string | undefined;
    const limit = Math.min(Math.max(1, parseInt(String(req.query.limit ?? 50), 10) || 50), 500);
    const offset = Math.max(0, parseInt(String(req.query.offset ?? 0), 10) || 0);

    const db = loadMasterDatabase();
    let list = db.colleges;

    if (country) {
      const upper = country.toUpperCase();
      list = list.filter((c) => c.country?.toUpperCase() === upper);
    }

    const total = list.length;
    const slice = list.slice(offset, offset + limit);

    return res.status(200).json({
      data: slice,
      meta: { total, limit, offset },
    });
  } catch (err) {
    console.error("[api/colleges]", err);
    return res.status(500).json({ error: getErrorMessage(err) });
  }
}
