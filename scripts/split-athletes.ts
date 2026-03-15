/**
 * One-time script: Split data/athletes.json into per-league files.
 * Run: npx tsx scripts/split-athletes.ts
 *
 * Partitions by athlete.league and writes athletes-nba.json, athletes-nfl.json,
 * athletes-mlb.json, athletes-nhl.json. Removes athletes.json after success.
 */

import { readFileSync, writeFileSync, unlinkSync, existsSync } from "fs";
import { join } from "path";
import { fileURLToPath } from "url";
import { dirname } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, "..", "data");
const ATHLETES_PATH = join(DATA_DIR, "athletes.json");

const LEAGUE_FILES = ["athletes-nba.json", "athletes-nfl.json", "athletes-mlb.json", "athletes-nhl.json"] as const;
const LEAGUES = ["NBA", "NFL", "MLB", "NHL"] as const;

interface Athlete {
  league?: string;
  [key: string]: unknown;
}

function main(): void {
  if (!existsSync(ATHLETES_PATH)) {
    console.error("athletes.json not found. Nothing to split.");
    process.exit(1);
  }

  console.log("Reading athletes.json...");
  const raw = readFileSync(ATHLETES_PATH, "utf-8");
  const athletes: Athlete[] = JSON.parse(raw);

  const byLeague: Record<string, Athlete[]> = { NBA: [], NFL: [], MLB: [], NHL: [] };
  let other = 0;

  for (const a of athletes) {
    const league = a?.league;
    if (league && byLeague[league]) {
      byLeague[league].push(a);
    } else {
      other++;
    }
  }

  if (other > 0) {
    console.warn(`Warning: ${other} athletes with missing/invalid league will be skipped`);
  }

  for (const league of LEAGUES) {
    const list = byLeague[league];
    const path = join(DATA_DIR, `athletes-${league.toLowerCase()}.json`);
    writeFileSync(path, JSON.stringify(list, null, 2), "utf-8");
    console.log(`  ${path}: ${list.length} athletes`);
  }

  unlinkSync(ATHLETES_PATH);
  console.log("Removed athletes.json");
  console.log("Done.");
}

main();
