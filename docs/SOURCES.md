# Data Sources

Each data source used by the ingest pipeline, with links, usage, and attribution requirements.

---

## NBA: nba_api

| Item | Value |
|------|-------|
| **Link** | https://github.com/swar/nba_api |
| **What we use** | NBA PlayerIndex (all-time roster), CommonPlayerInfo (profile enrichment: birth date, height, weight, college, last affiliation, honors) |
| **API** | stats.nba.com (via nba_api) |
| **License/Attribution** | nba_api is MIT. Data from NBA.com; cite NBA when using publicly. |

---

## NFL: nflverse

| Item | Value |
|------|-------|
| **Link** | https://github.com/nflverse/nflverse-data |
| **What we use** | `players.csv` release: player IDs, names, birth dates, colleges, positions, heights, weights, draft info, teams, active years |
| **License/Attribution** | nflverse data is open; credit nflverse when redistributing or publishing. |

---

## MLB: Chadwick Bureau

| Item | Value |
|------|-------|
| **Link** | https://github.com/chadwickbureau/register |
| **What we use** | Chadwick persons register (people-0..9, people-a..f): player IDs, names, MLB/Retrosheet/BRef/FanGraphs cross-refs, mlb_played filter |
| **License/Attribution** | Open Data Commons Attribution License (ODC-By). Must attribute Chadwick Bureau. |

---

## MLB: MLB Stats API

| Item | Value |
|------|-------|
| **Link** | https://statsapi.mlb.com/api/ |
| **Community docs** | https://github.com/toddrob99/MLB-StatsAPI/wiki |
| **What we use** | People/player profiles: birth date, birth place, position, handedness, height, weight, current team, debut date |
| **License/Attribution** | MLB data; follow MLB Terms of Use and cite MLB when using publicly. |

---

## NHL: @nhl-api/players

| Item | Value |
|------|-------|
| **Link** | https://www.npmjs.com/package/@nhl-api/players |
| **What we use** | All-time NHL player roster: IDs, names (used as primary player list) |
| **License/Attribution** | Package is MIT. Underlying NHL data; cite NHL when using publicly. |

---

## NHL: NHL landing

| Item | Value |
|------|-------|
| **Link** | NHL API (statsapi.web.nhl.com / api-web.nhle.com) |
| **What we use** | Player landing/profile endpoint: birth date, height, weight, handedness, position, Hall of Fame, honors |
| **License/Attribution** | NHL data; cite NHL when using publicly. |

---

## Refresh cadence

- **Ingest:** Run `npm run ingest` manually or on a schedule. Output written to `data/athletes-nba.json`, `data/athletes-nfl.json`, `data/athletes-mlb.json`, and `data/athletes-nhl.json`.
- **Metadata:** `data/ingest_metadata.json` (if present) records `lastIngestedAt` from the most recent successful ingest.
- **Recommendation:** Refresh seasonally or after major league events ( drafts, retirements, new rosters ).
