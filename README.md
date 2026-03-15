# High School Sports - Master Database

A central sports data repository linking **high schools worldwide** with professional athletes from **NHL, NBA, MLB, and NFL**.

## Leagues

| League | Sport  | Athletes | Source |
|--------|--------|----------|--------|
| **NBA** | Basketball | ~5,100 | nba_api (all-time) |
| **NFL** | Football | ~23,300 | nflverse |
| **MLB** | Baseball | ~22,700 | Chadwick Bureau |
| **NHL** | Hockey | ~19,900 merged | `@nhl-api/players` |

## Structure

```
High School Sports/
├── schema/types.ts       # Types (League, HighSchool, Athlete, Affiliation)
├── data/
│   ├── schools.json     # High schools (US & international)
│   ├── athletes.json    # Central athlete database
│   ├── sports.json      # League definitions
│   └── affiliations.json # School ↔ Athlete links
├── requirements.txt
├── LICENSE
├── scripts/load-master-db.ts
├── scripts/ingest_all_players.py
├── scripts/export_nhl_players.mjs
└── README.md
```

## Data Sources

- **NBA** - `nba_api`
- **NFL** - `nflverse`
- **MLB** - Chadwick Bureau register
- **NHL** - `@nhl-api/players`

## Geographic Coverage

- **US** — 30+ schools across all four leagues
- **Canada** — Schools in Alberta, Quebec, Ontario, British Columbia (NHL, NBA, MLB)
- **International** — Germany, France, Japan, Puerto Rico, Sweden

## Data Model

| Collection     | Key Fields |
|----------------|------------|
| **schools**    | `name`, `city`, `country`, `region`, `leagues` |
| **athletes**   | `name`, `league`, `position`, `teams`, `nationality` |
| **affiliations** | `schoolId`, `athleteId`, `league`, `type`, `yearsAttended` |

## Usage

```bash
npm install
pip install -r requirements.txt
npm run load          # Load and verify database
npm run ingest        # Refresh the central athlete database
```

## Repository Notes

- `data/athletes.json` is checked in so the repository contains a ready-to-use central database.
- The generated athlete dataset is large but remains under GitHub's normal single-file limit.
- Refreshing the dataset is script-driven, so source code and generated outputs stay reproducible.

## Adding Data

1. Add schools to `data/schools.json` (include `leagues` array)
2. Add athletes to `data/athletes.json` (include `league`)
3. Link them in `data/affiliations.json` (include `league`)

Use consistent IDs (`s-XXX` for schools, `a-{league}-XXX` for athletes).

## License

MIT
