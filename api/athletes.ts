/**
 * GET /api/athletes
 * Query params: league (NHL|NBA|MLB|NFL), sport, limit (default 50), offset (default 0)
 * Returns JSON array of athletes.
 */
import type { VercelRequest, VercelResponse } from "@vercel/node";
import { loadMasterDatabase } from "../scripts/load-master-db";
import type { League } from "../schema/types";

const LEAGUES: League[] = ["NHL", "NBA", "MLB", "NFL"];

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
    const league = req.query.league as string | undefined;
    const sport = req.query.sport as string | undefined;
    const limit = Math.min(Math.max(1, parseInt(String(req.query.limit ?? 50), 10) || 50), 500);
    const offset = Math.max(0, parseInt(String(req.query.offset ?? 0), 10) || 0);

    const db = loadMasterDatabase();
    let list = db.athletes;

    if (league) {
      const normalized = league.toUpperCase();
      if (!LEAGUES.includes(normalized as League)) {
        return res.status(400).json({ error: `Invalid league. Use: ${LEAGUES.join(", ")}` });
      }
      list = list.filter((a) => a.league === normalized);
    }

    if (sport) {
      const lower = sport.toLowerCase();
      list = list.filter((a) => a.sport?.toLowerCase() === lower);
    }

    const total = list.length;
    const slice = list.slice(offset, offset + limit);

    return res.status(200).json({
      data: slice,
      meta: { total, limit, offset },
    });
  } catch (err) {
    console.error("[api/athletes]", err);
    return res.status(500).json({ error: getErrorMessage(err) });
  }
}
