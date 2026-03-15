/**
 * Bulk export master database to CSV files.
 * Output: data/exports/athletes.csv, schools.csv, colleges.csv, affiliations.csv
 * Run: npm run export
 */
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { loadMasterDatabase } from "./load-master-db";

const __dirname = dirname(fileURLToPath(import.meta.url));
const EXPORTS_DIR = join(__dirname, "..", "data", "exports");

/** Escape a CSV field per RFC 4180: wrap in quotes if contains comma, quote, or newline; double internal quotes. */
function escapeCsv(value: unknown): string {
  if (value === null || value === undefined) return "";
  const str = typeof value === "object" ? JSON.stringify(value) : String(value);
  if (str.includes(",") || str.includes('"') || str.includes("\n") || str.includes("\r")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function writeCsv(path: string, headers: string[], rows: string[][]): void {
  const headerLine = headers.map(escapeCsv).join(",");
  const dataLines = rows.map((row) => row.map(escapeCsv).join(","));
  const content = [headerLine, ...dataLines].join("\n");
  writeFileSync(path, content, "utf-8");
}

function main(): void {
  mkdirSync(EXPORTS_DIR, { recursive: true });
  const db = loadMasterDatabase();

  // Athletes
  const athleteHeaders = [
    "id", "name", "league", "sport", "nationality", "countryOfBirth", "birthDate",
    "birthCity", "birthRegion", "yearsActive", "era", "position", "height", "weight",
    "handedness", "teams", "active", "education", "awards", "honors", "bio",
  ];
  const athleteRows = db.athletes.map((a) => [
    a.id,
    a.name,
    a.league,
    a.sport,
    a.nationality ?? "",
    a.countryOfBirth ?? "",
    a.birthDate ?? "",
    a.birthCity ?? "",
    a.birthRegion ?? "",
    a.yearsActive ?? "",
    a.era ?? "",
    a.position ?? "",
    a.height ?? "",
    a.weight ?? "",
    a.handedness ?? "",
    Array.isArray(a.teams) ? a.teams.join(";") : "",
    a.active ?? "",
    a.education ? JSON.stringify(a.education) : "",
    Array.isArray(a.awards) ? a.awards.join(";") : "",
    Array.isArray(a.honors) ? a.honors.join(";") : "",
    a.bio ?? "",
  ]);
  writeCsv(join(EXPORTS_DIR, "athletes.csv"), athleteHeaders, athleteRows);
  console.log(`Exported ${athleteRows.length} athletes`);

  // Schools
  const schoolHeaders = ["id", "name", "city", "country", "region", "sportFocus", "leagues", "founded"];
  const schoolRows = db.schools.map((s) => [
    s.id,
    s.name,
    s.city,
    s.country,
    s.region ?? "",
    Array.isArray(s.sportFocus) ? s.sportFocus.join(";") : "",
    Array.isArray(s.leagues) ? s.leagues.join(";") : "",
    s.founded ?? "",
  ]);
  writeCsv(join(EXPORTS_DIR, "schools.csv"), schoolHeaders, schoolRows);
  console.log(`Exported ${schoolRows.length} schools`);

  // Colleges
  const collegeHeaders = ["id", "name", "aliases", "country", "region", "city"];
  const collegeRows = db.colleges.map((c) => [
    c.id,
    c.name,
    Array.isArray(c.aliases) ? c.aliases.join(";") : "",
    c.country ?? "",
    c.region ?? "",
    c.city ?? "",
  ]);
  writeCsv(join(EXPORTS_DIR, "colleges.csv"), collegeHeaders, collegeRows);
  console.log(`Exported ${collegeRows.length} colleges`);

  // Affiliations (school ↔ athlete)
  const affHeaders = ["id", "schoolId", "athleteId", "type", "league", "sport", "yearsAttended", "graduated", "notes"];
  const affRows = db.affiliations.map((a) => [
    a.id,
    a.schoolId,
    a.athleteId,
    a.type,
    a.league,
    a.sport,
    a.yearsAttended ?? "",
    a.graduated ?? "",
    a.notes ?? "",
  ]);
  writeCsv(join(EXPORTS_DIR, "affiliations.csv"), affHeaders, affRows);
  console.log(`Exported ${affRows.length} affiliations`);

  console.log(`\nExports written to ${EXPORTS_DIR}`);
}

main();
