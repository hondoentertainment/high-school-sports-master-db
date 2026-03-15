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
  founded?: number;
  metadata?: Record<string, unknown>;
}

export type League = "NHL" | "NBA" | "MLB" | "NFL";

export interface ProfessionalAthlete {
  id: string;
  name: string;
  league: League;
  sport: string;
  nationality: CountryCode;
  countryOfBirth?: CountryCode;
  yearsActive?: string; // e.g. "1951-1968"
  era?: string; // pioneer, golden age, etc.
  position?: string;
  teams?: string[];
  bio?: string;
  metadata?: Record<string, unknown>;
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

export interface MasterDatabase {
  schools: HighSchool[];
  athletes: ProfessionalAthlete[];
  sports: Sport[];
  affiliations: Affiliation[];
}
