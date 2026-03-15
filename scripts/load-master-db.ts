/**
 * Load and merge the master database from JSON files.
 * Run: npm run load   or   npx tsx scripts/load-master-db.ts
 */

import { readFileSync, existsSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import type { MasterDatabase } from "../schema/types";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, "..", "data");

function loadJson<T>(filename: string): T {
  const path = join(DATA_DIR, filename);
  return JSON.parse(readFileSync(path, "utf-8")) as T;
}

function loadJsonIfExists<T>(filename: string): T | undefined {
  const path = join(DATA_DIR, filename);
  if (!existsSync(path)) return undefined;
  return JSON.parse(readFileSync(path, "utf-8")) as T;
}

export function loadMasterDatabase(): MasterDatabase {
  const schoolCollegeLinks = loadJsonIfExists("school_college_links.json");
  return {
    schools: loadJson("schools.json"),
    colleges: loadJson("colleges.json"),
    athletes: loadJson("athletes.json"),
    sports: loadJson("sports.json"),
    affiliations: loadJson("affiliations.json"),
    educationAffiliations: loadJson("education_affiliations.json"),
    ...(schoolCollegeLinks && { schoolCollegeLinks: schoolCollegeLinks as MasterDatabase["schoolCollegeLinks"] }),
  };
}

// CLI usage
const isMain = process.argv[1]?.includes("load-master-db");
if (isMain) {
  const db = loadMasterDatabase();
  console.log("Master database loaded:");
  console.log(`  Schools: ${db.schools.length}`);
  console.log(`  Colleges: ${db.colleges.length}`);
  console.log(`  Athletes: ${db.athletes.length}`);
  console.log(`  Sports: ${db.sports.length}`);
  console.log(`  Affiliations: ${db.affiliations.length}`);
  console.log(`  Education affiliations: ${db.educationAffiliations.length}`);
  if (db.schoolCollegeLinks) {
    console.log(`  School-college links: ${db.schoolCollegeLinks.length}`);
  }
}
