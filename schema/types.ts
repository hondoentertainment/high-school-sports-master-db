/**
 * Master Database Schema
 * High Schools ↔ Professional Athletes (US & International)
 */

export type CountryCode = string; // ISO 3166-1 alpha-2 (US, GB, AU, etc.)

export interface HighSchool {
  id: string;
  name: string;
  city: string;
  country: CountryCode;
  region?: string; // state, province, etc.
  sportFocus?: string[];
  leagues?: League[];
  feederColleges?: string[]; // college IDs for known prep-to-college pipelines
  founded?: number;
  metadata?: Record<string, unknown>;
}

export interface SchoolCollegeLink {
  schoolId: string;
  collegeId: string;
}

export type League = "NHL" | "NBA" | "MLB" | "NFL";

export interface AthleteEducation {
  highSchool?: string[];
  prepSchool?: string[];
  academy?: string[];
  college?: string[];
  lastAffiliation?: string;
}

export interface College {
  id: string;
  name: string;
  aliases?: string[];
  country?: CountryCode;
  region?: string;
  city?: string;
  metadata?: Record<string, unknown>;
}

/**
 * Optional athlete metadata keys for cross-referencing external systems.
 * When available from source APIs, these provide stable external IDs.
 */
export interface AthleteMetadata {
  /** Data source (e.g. nflverse, nba_api, mlb_stats_api, nhl_landing) */
  source?: string;
  /** ISO 8601 date when record was last ingested */
  lastIngestedAt?: string;
  /** Wikidata entity ID (Q-number, e.g. Q213812) */
  wikidataId?: string;
  /** Baseball-Reference player ID */
  bbrefId?: string;
  /** Pro-Football-Reference player ID */
  pfrId?: string;
  /** Hockey-Reference player ID */
  hockeyReferenceId?: string;
  /** Basketball-Reference player ID */
  basketballReferenceId?: string;
  /** ESPN player ID */
  espnId?: string;
  [key: string]: unknown;
}

export interface ProfessionalAthlete {
  /** Deterministic ID from external source: a-{league}-{externalId} (e.g. a-nba-035, a-nfl-012, a-mlb-121578, a-nhl-005) */
  id: string;
  name: string;
  league: League;
  sport: string;
  nationality?: CountryCode;
  countryOfBirth?: CountryCode;
  birthDate?: string;
  birthCity?: string;
  birthRegion?: string;
  yearsActive?: string; // e.g. "1951-1968"
  era?: string; // pioneer, golden age, etc.
  position?: string;
  height?: string;
  weight?: string | number;
  handedness?: string;
  teams?: string[];
  active?: boolean;
  education?: AthleteEducation;
  awards?: string[];
  honors?: string[];
  bio?: string;
  metadata?: AthleteMetadata;
}

export interface Sport {
  id: string;
  name: string;
  league: League;
  category?: string;
  origins?: CountryCode[];
}

export type AffiliationType = 
  | "alumni"      // attended this school
  | "historic"    // pioneer/legacy tie
  | "honorary"    // honorary recognition
  | "feeder";     // school known for producing pros in this sport

export type EducationAffiliationType =
  | "highSchool"
  | "prepSchool"
  | "academy"
  | "college";

export interface Affiliation {
  id: string;
  schoolId: string;
  athleteId: string;
  type: AffiliationType;
  league: League;
  sport: string;
  yearsAttended?: string; // e.g. "1945-1948"
  graduated?: number;
  notes?: string;
  sources?: string[]; // for verification
  metadata?: Record<string, unknown>;
}

export interface EducationAffiliation {
  id: string;
  athleteId: string;
  league: League;
  educationType: EducationAffiliationType;
  institutionId?: string;
  institutionName: string;
  source: "school_affiliations" | "athlete_education";
  yearsAttended?: string;
  graduated?: number;
  metadata?: Record<string, unknown>;
}

export interface MasterDatabase {
  schools: HighSchool[];
  colleges: College[];
  athletes: ProfessionalAthlete[];
  sports: Sport[];
  affiliations: Affiliation[];
  educationAffiliations: EducationAffiliation[];
  schoolCollegeLinks?: SchoolCollegeLink[];
}
