# Data Dictionary

Reference: `schema/types.ts`. All collections use camelCase in JSON files.

---

## athletes

**File:** `data/athletes.json`  
**Type:** `ProfessionalAthlete[]`

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `id` | `string` | Required | Unique athlete identifier, format `a-{league}-{num}` | `"a-nba-035"` |
| `name` | `string` | Required | Full name | `"LeBron James"` |
| `league` | `League` | Required | One of `NHL`, `NBA`, `MLB`, `NFL` | `"NBA"` |
| `sport` | `string` | Required | Sport played | `"basketball"` |
| `nationality` | `CountryCode` | Optional | ISO 3166-1 alpha-2 country code | `"US"` |
| `countryOfBirth` | `CountryCode` | Optional | Birth country | `"US"` |
| `birthDate` | `string` | Optional | ISO date (YYYY-MM-DD) | `"1984-12-30"` |
| `birthCity` | `string` | Optional | Birth city | `"Akron"` |
| `birthRegion` | `string` | Optional | State, province, etc. | `"Ohio"` |
| `yearsActive` | `string` | Optional | Career span | `"2003-2024"` |
| `era` | `string` | Optional | Era classification (pioneer, golden age, modern) | `"modern"` |
| `position` | `string` | Optional | Position/role | `"Small Forward"` |
| `height` | `string` | Optional | Height | `"6' 9\""` |
| `weight` | `string \| number` | Optional | Weight | `"250"` |
| `handedness` | `string` | Optional | Batting/throwing hand | `"Bats: Right; Throws: Right"` |
| `teams` | `string[]` | Optional | Team names | `["Cleveland Cavaliers", "Miami Heat"]` |
| `active` | `boolean` | Optional | Currently active | `true` |
| `education` | `AthleteEducation` | Optional | School/college affiliations | see below |
| `awards` | `string[]` | Optional | Awards received | `["MVP"]` |
| `honors` | `string[]` | Optional | Honors (HOF, etc.) | `["Hall of Fame"]` |
| `bio` | `string` | Optional | Biographical text | — |
| `metadata` | `Record<string, unknown>` | Optional | Source IDs, extras | `{ "nbaId": "2544", "source": "nba_api" }` |

**AthleteEducation** (nested):

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `highSchool` | `string[]` | Optional | High schools attended | `["St. Vincent-St. Mary"]` |
| `prepSchool` | `string[]` | Optional | Prep schools | — |
| `academy` | `string[]` | Optional | Academies | — |
| `college` | `string[]` | Optional | Colleges | `["Ohio State"]` |
| `lastAffiliation` | `string` | Optional | Last known affiliation string | — |

---

## schools

**File:** `data/schools.json`  
**Type:** `HighSchool[]`

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `id` | `string` | Required | Unique school ID, format `s-XXX` | `"s-001"` |
| `name` | `string` | Required | School name | `"St. Vincent-St. Mary High School"` |
| `city` | `string` | Required | City | `"Akron"` |
| `country` | `CountryCode` | Required | ISO 3166-1 alpha-2 | `"US"` |
| `region` | `string` | Optional | State, province | `"Ohio"` |
| `sportFocus` | `string[]` | Optional | Primary sports | `["basketball"]` |
| `leagues` | `League[]` | Optional | Related leagues | `["NBA"]` |
| `founded` | `number` | Optional | Year founded | `1866` |
| `metadata` | `Record<string, unknown>` | Optional | Extra data | — |

---

## colleges

**File:** `data/colleges.json`  
**Type:** `College[]`

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `id` | `string` | Required | Unique college ID, format `c-*` | `"c-santa-clara"` |
| `name` | `string` | Required | Canonical name | `"Santa Clara"` |
| `aliases` | `string[]` | Optional | Alternate names | `["USC", "Southern Cal"]` |
| `country` | `CountryCode` | Optional | ISO 3166-1 alpha-2 | `"US"` |
| `region` | `string` | Optional | State, province | — |
| `city` | `string` | Optional | City | — |
| `metadata` | `Record<string, unknown>` | Optional | e.g. `normalizedName` | `{ "normalizedName": "santa-clara" }` |

---

## affiliations

**File:** `data/affiliations.json`  
**Type:** `Affiliation[]`  
Curated school ↔ athlete links.

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `id` | `string` | Required | Unique affiliation ID | `"af-001"` |
| `schoolId` | `string` | Required | FK to schools | `"s-001"` |
| `athleteId` | `string` | Required | FK to athletes | `"a-nba-035"` |
| `type` | `AffiliationType` | Required | `alumni`, `historic`, `honorary`, `feeder` | `"alumni"` |
| `league` | `League` | Required | League context | `"NBA"` |
| `sport` | `string` | Required | Sport | `"basketball"` |
| `yearsAttended` | `string` | Optional | Years | `"1999-2003"` |
| `graduated` | `number` | Optional | Graduation year | `2003` |
| `notes` | `string` | Optional | Notes | — |
| `sources` | `string[]` | Optional | Verification sources | — |
| `metadata` | `Record<string, unknown>` | Optional | Extra data | — |

---

## education_affiliations

**File:** `data/education_affiliations.json`  
**Type:** `EducationAffiliation[]`  
Normalized athlete ↔ school/college links.

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `id` | `string` | Required | Unique ID | `"edu-a-nba-030-college-santa-clara"` |
| `athleteId` | `string` | Required | FK to athletes | `"a-nba-030"` |
| `league` | `League` | Required | League | `"NBA"` |
| `educationType` | `EducationAffiliationType` | Required | `highSchool`, `prepSchool`, `academy`, `college` | `"college"` |
| `institutionId` | `string` | Optional | FK to colleges (or schools) | `"c-santa-clara"` |
| `institutionName` | `string` | Required | Display name | `"Santa Clara"` |
| `source` | `string` | Required | `school_affiliations` or `athlete_education` | `"athlete_education"` |
| `yearsAttended` | `string` | Optional | Years | — |
| `graduated` | `number` | Optional | Graduation year | — |
| `metadata` | `Record<string, unknown>` | Optional | Extras | — |

---

## sports

**File:** `data/sports.json`  
**Type:** `Sport[]`

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `id` | `string` | Required | Sport ID | `"nba"` |
| `name` | `string` | Required | League name | `"National Basketball Association"` |
| `league` | `League` | Required | League code | `"NBA"` |
| `category` | `string` | Optional | Category | `"team"` |
| `origins` | `CountryCode[]` | Optional | Origin countries | `["US"]` |

---

## Type reference

- **League:** `"NHL" | "NBA" | "MLB" | "NFL"`
- **CountryCode:** ISO 3166-1 alpha-2 (e.g. `US`, `CA`, `GB`)
- **AffiliationType:** `alumni` \| `historic` \| `honorary` \| `feeder`
- **EducationAffiliationType:** `highSchool` \| `prepSchool` \| `academy` \| `college`
