/**
 * Load and merge the master database from JSON files.
 * Run: npm run load   or   npx tsx scripts/load-master-db.ts
 */

import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import type { MasterDatabase } from "../schema/types";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, "..", "data");

function loadJson<T>(filename: string): T {
  const path = join(DATA_DIR, filename);
  return JSON.parse(readFileSync(path, "utf-8")) as T;
}

export function loadMasterDatabase(): MasterDatabase {
  return {
    schools: loadJson("schools.json"),
    athletes: loadJson("athletes.json"),
    sports: loadJson("sports.json"),
    affiliations: loadJson("affiliations.json"),
  };
}

// CLI usage
const isMain = process.argv[1]?.includes("load-master-db");
if (isMain) {
  const db = loadMasterDatabase();
  console.log("Master database loaded:");
  console.log(`  Schools: ${db.schools.length}`);
  console.log(`  Athletes: ${db.athletes.length}`);
  console.log(`  Sports: ${db.sports.length}`);
  console.log(`  Affiliations: ${db.affiliations.length}`);
}
