#!/usr/bin/env python3
"""
Audit the central sports database for consistency and field coverage.

Run: python scripts/audit_database.py
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"


def load_json(name: str):
    with open(DATA_DIR / name, "r", encoding="utf-8") as file:
        return json.load(file)


def pct(missing: int, total: int) -> str:
    if total == 0:
        return "0.0%"
    return f"{(missing / total) * 100:.1f}%"


def main() -> int:
    athletes = load_json("athletes.json")
    schools = load_json("schools.json")
    affiliations = load_json("affiliations.json")

    athlete_ids = {a["id"] for a in athletes}
    school_ids = {s["id"] for s in schools}

    duplicate_ids = [item for item, count in Counter(a["id"] for a in athletes).items() if count > 1]
    duplicate_names = [
        item
        for item, count in Counter((a.get("league"), a.get("name", "").strip().lower()) for a in athletes).items()
        if count > 1
    ]

    orphan_athlete_links = [a["id"] for a in affiliations if a["athleteId"] not in athlete_ids]
    orphan_school_links = [a["id"] for a in affiliations if a["schoolId"] not in school_ids]

    by_league: dict[str, list[dict]] = defaultdict(list)
    for athlete in athletes:
        by_league[athlete["league"]].append(athlete)

    print("Database audit")
    print(f"  Athletes: {len(athletes)}")
    print(f"  Schools: {len(schools)}")
    print(f"  Affiliations: {len(affiliations)}")
    print()

    for league in sorted(by_league):
        rows = by_league[league]
        total = len(rows)
        missing_nationality = sum(1 for row in rows if not row.get("nationality"))
        missing_birth = sum(1 for row in rows if not row.get("countryOfBirth"))
        missing_birth_date = sum(1 for row in rows if not row.get("birthDate"))
        missing_birth_city = sum(1 for row in rows if not row.get("birthCity"))
        missing_years = sum(1 for row in rows if not row.get("yearsActive"))
        missing_position = sum(1 for row in rows if not row.get("position"))
        missing_height = sum(1 for row in rows if not row.get("height"))
        missing_weight = sum(1 for row in rows if not row.get("weight"))
        missing_handedness = sum(1 for row in rows if not row.get("handedness"))
        empty_teams = sum(1 for row in rows if not row.get("teams"))
        missing_education = sum(1 for row in rows if not row.get("education"))
        missing_awards = sum(1 for row in rows if not row.get("awards"))
        missing_honors = sum(1 for row in rows if not row.get("honors"))
        missing_source = sum(1 for row in rows if not (row.get("metadata") or {}).get("source"))
        missing_cross_refs = sum(
            1
            for row in rows
            if not any(
                (row.get("metadata") or {}).get(key)
                for key in (
                    "nbaId",
                    "nflId",
                    "mlbId",
                    "nhlId",
                    "bbrefId",
                    "pfrId",
                    "espnId",
                    "wikidataId",
                    "retroId",
                    "fangraphsId",
                )
            )
        )
        print(f"{league}: {total}")
        print(f"  Missing nationality: {missing_nationality} ({pct(missing_nationality, total)})")
        print(f"  Missing birth country: {missing_birth} ({pct(missing_birth, total)})")
        print(f"  Missing birth date: {missing_birth_date} ({pct(missing_birth_date, total)})")
        print(f"  Missing birth city: {missing_birth_city} ({pct(missing_birth_city, total)})")
        print(f"  Missing years active: {missing_years} ({pct(missing_years, total)})")
        print(f"  Missing position: {missing_position} ({pct(missing_position, total)})")
        print(f"  Missing height: {missing_height} ({pct(missing_height, total)})")
        print(f"  Missing weight: {missing_weight} ({pct(missing_weight, total)})")
        print(f"  Missing handedness: {missing_handedness} ({pct(missing_handedness, total)})")
        print(f"  Empty teams: {empty_teams} ({pct(empty_teams, total)})")
        print(f"  Missing education: {missing_education} ({pct(missing_education, total)})")
        print(f"  Missing awards: {missing_awards} ({pct(missing_awards, total)})")
        print(f"  Missing honors: {missing_honors} ({pct(missing_honors, total)})")
        print(f"  Missing source tag: {missing_source} ({pct(missing_source, total)})")
        print(f"  Missing external IDs: {missing_cross_refs} ({pct(missing_cross_refs, total)})")

    print()
    print(f"Duplicate athlete IDs: {len(duplicate_ids)}")
    print(f"Duplicate athlete names within a league: {len(duplicate_names)}")
    print(f"Affiliations missing athlete: {len(orphan_athlete_links)}")
    print(f"Affiliations missing school: {len(orphan_school_links)}")

    if duplicate_ids[:5]:
        print("  Sample duplicate IDs:", ", ".join(duplicate_ids[:5]))
    if duplicate_names[:5]:
        preview = ", ".join(f"{league}:{name}" for league, name in duplicate_names[:5])
        print("  Sample duplicate league/name pairs:", preview)
    if orphan_athlete_links[:5]:
        print("  Sample affiliation IDs missing athlete:", ", ".join(orphan_athlete_links[:5]))
    if orphan_school_links[:5]:
        print("  Sample affiliation IDs missing school:", ", ".join(orphan_school_links[:5]))

    return 1 if duplicate_ids or orphan_athlete_links or orphan_school_links else 0


if __name__ == "__main__":
    raise SystemExit(main())
