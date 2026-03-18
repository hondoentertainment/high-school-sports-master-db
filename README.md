# High School Sports - Master Database

A central sports data repository linking **high schools worldwide** with professional athletes from **NHL, NBA, MLB, and NFL**.

## Leagues

| League | Sport  | Athletes | Source |
|--------|--------|----------|--------|
| **NBA** | Basketball | 5,122 | `nba_api` + cached `CommonPlayerInfo` |
| **NFL** | Football | 24,354 | `nflverse` |
| **MLB** | Baseball | 23,615 | Chadwick + MLB Stats API |
| **NHL** | Hockey | 20,238 | `@nhl-api/players` + NHL landing profiles |

## Structure

```
High School Sports/
├── schema/types.ts       # Types (League, HighSchool, Athlete, Affiliation)
├── docs/
│   ├── DATA_DICTIONARY.md  # Field reference for all collections
│   └── SOURCES.md         # Data source links and attribution
├── api/                 # Vercel serverless routes (athletes, schools, colleges)
├── data/
│   ├── schema.json     # JSON Schema (athletes, schools, colleges, affiliations)
│   ├── schools.json     # High schools (US & international)
│   ├── colleges.json    # Normalized colleges / universities
│   ├── athletes-nba.json   # NBA athletes
│   ├── athletes-nfl.json   # NFL athletes
│   ├── athletes-mlb.json   # MLB athletes
│   ├── athletes-nhl.json   # NHL athletes
│   ├── sports.json      # League definitions
│   ├── affiliations.json # School ↔ Athlete links
│   ├── education_affiliations.json # Athlete ↔ school/college links
│   ├── ingest_metadata.json       # lastIngestedAt (after npm run ingest)
│   └── exports/        # CSV exports (npm run export; gitignored)
├── requirements.txt
├── LICENSE
├── scripts/load-master-db.ts
├── scripts/ingest_all_players.py
├── scripts/export_nhl_players.mjs
├── scripts/build_education_index.py
└── README.md
```

## Attribution / Data Sources

Data in this repository comes from the following sources. Please credit them when redistributing or publishing.

| Source | Data Used | Attribution |
|--------|-----------|-------------|
| **nflverse** | NFL players (rosters, birth dates, colleges, positions, etc.) | [nflverse/nflverse-data](https://github.com/nflverse/nflverse-data) — credit nflverse |
| **Chadwick Bureau** | MLB player identities (register) | [chadwickbureau/register](https://github.com/chadwickbureau/register) — ODC-By, attribute Chadwick Bureau |
| **NBA** | NBA player data (rosters, profiles) via nba_api | [nba_api](https://github.com/swar/nba_api) — cite NBA |
| **NHL** | NHL player data via @nhl-api/players + NHL landing API | [@nhl-api/players](https://www.npmjs.com/package/@nhl-api/players) — cite NHL |
| **MLB Stats API** | MLB player profiles (birth, position, handedness) | [statsapi.mlb.com](https://statsapi.mlb.com) — cite MLB |

See [docs/SOURCES.md](docs/SOURCES.md) for detailed links, usage, and license requirements.

## Data freshness

- Athlete data is refreshed by running `npm run ingest`. After a successful run, `data/ingest_metadata.json` is updated with `lastIngestedAt`.
- Recommended refresh cadence: seasonally or after major events (drafts, retirements).

## Geographic Coverage

- **US** — 238 schools (includes all with 1+ affiliated players from education data)
- **Canada** — Schools in Alberta, Quebec, Ontario, British Columbia (NHL, NBA, MLB)
- **International** — Germany, France, Japan, Puerto Rico, Sweden

## Data Model

| Collection     | Key Fields |
|----------------|------------|
| **schools**    | `name`, `city`, `country`, `region`, `leagues` |
| **colleges**   | `name`, `aliases`, `country`, `metadata.normalizedName` |
| **athletes**   | `name`, `league`, `birthDate`, `position`, `teams`, `height`, `weight`, `education`, `awards`, `honors`, `metadata` |
| **affiliations** | `schoolId`, `athleteId`, `league`, `type`, `yearsAttended` |
| **education_affiliations** | `athleteId`, `educationType`, `institutionId`, `institutionName`, `source` |

## Usage

```bash
npm install
pip install -r requirements.txt
npm run load          # Load and verify database
npm run export        # Bulk export to CSV (data/exports/)
npm run ingest        # Refresh the central athlete database
npm run build:education # Build normalized school/college links
npm run add-schools     # Add schools from education_affiliations with 1+ players
npm run validate      # Validate schema (required fields, references, no duplicates)
npm run audit         # Validate + audit consistency and field coverage
npm run api:dev       # Run read-only API locally (Vercel dev server)
```

The ingest pipeline uses `.cache/` to store expensive profile lookups so repeated enrichment runs are much faster.

## Read-only API

Deploy to Vercel for serverless read-only endpoints. All endpoints are public; no API keys required.

| Endpoint | Query params |
|----------|--------------|
| `GET /api/athletes` | `league`, `sport`, `limit` (default 50), `offset`, `q`/`search` (name), `position`, `country`/`nationality` |
| `GET /api/schools` | `league`, `limit`, `offset`, `q`/`search` (school name) |
| `GET /api/colleges` | `country`, `limit`, `offset`, `q`/`search` (college name) |

**Examples (curl):**
```bash
# Athletes in NBA named "Lebron"
curl "https://high-school-sports-xxx.vercel.app/api/athletes?q=lebron&league=NBA"

# Athletes by position (e.g. QB, PG, Pitcher)
curl "https://high-school-sports-xxx.vercel.app/api/athletes?position=QB&league=NFL&limit=10"

# Athletes by country/nationality (ISO code)
curl "https://high-school-sports-xxx.vercel.app/api/athletes?country=CA&league=NHL"

# Search schools by name
curl "https://high-school-sports-xxx.vercel.app/api/schools?q=St.+Vincent&league=NBA"

# Search colleges by name
curl "https://high-school-sports-xxx.vercel.app/api/colleges?q=Ohio&country=US"
```

**Examples (fetch):**
```javascript
// Athletes matching name + league
const res = await fetch('https://high-school-sports-xxx.vercel.app/api/athletes?q=lebron&league=NBA');
const { data, meta } = await res.json();

// Schools by search term
const schools = await fetch('/api/schools?search=Oak+Hill&limit=5').then(r => r.json());
```

## Deployment

1. **Deploy to Vercel**: Run `vercel` (or `vercel --prod` for production).
2. **Connect this repo to Vercel** for automatic deploys on push.

| Environment | URL |
|-------------|-----|
| Production | `https://high-school-sports-xxx.vercel.app` |
| Preview | Per-branch preview URLs from Vercel |

**Manual deploy steps:**
1. Install Vercel CLI: `npm i -g vercel`
2. Login: `vercel login`
3. Deploy: `vercel --prod`

## Bulk exports

Run `npm run export` to generate CSV files in `data/exports/`:

- `athletes.csv`, `schools.csv`, `colleges.csv`, `affiliations.csv`

Arrays are joined with `;`; nested objects use JSON. CSV escaping follows RFC 4180.

## JSON Schema validation

The schema is in `data/schema.json`. To validate data:

**Using ajv (npm):**
```bash
npm install -g ajv-cli
```

Because each data file is an array, use a wrapper schema. Example `validate-athletes.json`:
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "array",
  "items": { "$ref": "./data/schema.json#/definitions/professionalAthlete" }
}
```
Then: `ajv validate -s validate-athletes.json -d data/athletes-nba.json` (or validate each league file separately)

**Using Python (jsonschema):**
```python
import json
from jsonschema import validate, RefResolver
with open("data/schema.json") as f:
    schema = json.load(f)
for fname in ["athletes-nba.json", "athletes-nfl.json", "athletes-mlb.json", "athletes-nhl.json"]:
    with open(f"data/{fname}") as f:
        data = json.load(f)
    validate(data, {"type": "array", "items": schema["definitions"]["professionalAthlete"]}, resolver=RefResolver.from_schema(schema))
```

## Repository Notes

- Athlete data is split into `data/athletes-nba.json`, `data/athletes-nfl.json`, `data/athletes-mlb.json`, and `data/athletes-nhl.json` to stay under GitHub's 100 MB file limit; scripts merge them at load time.
- Refreshing the dataset is script-driven, so source code and generated outputs stay reproducible.
- `data/education_affiliations.json` is the normalized bridge table for athlete-to-high-school and athlete-to-college relationships.
- Existing curated high school links from `data/affiliations.json` are preserved and mirrored into the normalized education layer.

## Data Quality Notes

- **NFL** currently has the strongest structured detail from the bulk source: birth date, height, weight, college, position, latest team, draft fields, and active-year ranges.
- **NBA** now includes full all-time coverage plus cached profile enrichment for birth date, school/last affiliation, height, weight, honors, and active status where the API exposes it.
- **MLB** now has strong bio/profile coverage from the MLB Stats API layered on top of Chadwick identifiers: birth data, position, handedness, current team, and external IDs are populated for most players.
- **NHL** now combines the all-time player registry with the NHL landing endpoint for best-effort profile enrichment including birth data, height/weight, handedness, position, hall-of-fame flags, and honors when available.
- Education coverage is strongest for **NBA** and **NFL**; it remains sparse for **MLB** and **NHL** because the bulk profile sources do not expose schooling consistently.
- Awards remain sparse outside league-specific honors because there is no single fast bulk source for historical awards across all four leagues.
- Duplicate names within a league are expected for historical datasets; athlete IDs remain unique and should be used as the stable key.
- Run `npm run audit` after refreshes to check for orphan affiliations, duplicate IDs, and field coverage gaps.

## Education Model

- **High schools** remain canonical in `data/schools.json`.
- **Colleges** are normalized into `data/colleges.json` with stable `c-*` IDs, canonical `name` values, and `aliases` for observed source variants.
- **Athlete education links** are stored in `data/education_affiliations.json`.
- High school rows in the normalized education table come from:
  - curated `data/affiliations.json`
  - athlete profile `education.highSchool`, `education.prepSchool`, and `education.academy` values
- College rows come from athlete profile `education.college` values, are assigned stable `c-*` IDs, and now collapse obvious variants like abbreviations or location-qualified spellings into a single canonical record when safe, such as `USC` -> `Southern California`, `BYU` -> `Brigham Young`, and `UNC-Wilmington` -> `North Carolina-Wilmington`.

## Current Coverage Snapshot

Latest audit highlights:

- **NBA**: 0 missing nationality, 0 missing position, 45.0% missing birth date
- **NFL**: 0 missing years active, 0 missing position, 0 empty teams
- **MLB**: 96%+ coverage for birth country, birth date, position, height, weight, and handedness
- **NHL**: broadest identity coverage, but profile depth is still limited by upstream endpoint availability for many historical players

## Adding Data

1. Add schools to `data/schools.json` (include `leagues` array)
2. Add athletes to the appropriate `data/athletes-{league}.json` (NBA, NFL, MLB, NHL)
3. Link them in `data/affiliations.json` (include `league`)

Use consistent IDs (`s-XXX` for schools, `a-{league}-XXX` for athletes).

## Athlete ID Generation

Athlete IDs are **deterministic** and derived from stable external keys so re-runs do not change IDs:

| League | Format | External key source |
|--------|--------|---------------------|
| NBA | `a-nba-{nbaId}` | `nba_api` PERSON_ID |
| NFL | `a-nfl-{gsis_id}` | `nflverse` gsis_id |
| MLB | `a-mlb-{mlbId}` | Chadwick key_mlbam or key_person |
| NHL | `a-nhl-{nhlId}` | `@nhl-api/players` player.id |

See `schema/types.ts` for the `ProfessionalAthlete` interface and optional metadata fields (wikidataId, bbrefId, pfrId, hockeyReferenceId, espnId).

## License

MIT. See [LICENSE](LICENSE) for the full text.
