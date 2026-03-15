#!/usr/bin/env python3
"""
Build normalized education data from athlete profiles and existing school affiliations.

Outputs:
- data/colleges.json
- data/education_affiliations.json
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

COUNTRY_MAP = {
    "USA": "US",
    "US": "US",
    "CANADA": "CA",
    "CAN": "CA",
    "FRANCE": "FR",
    "GERMANY": "DE",
    "GREECE": "GR",
    "ITALY": "IT",
    "SPAIN": "ES",
    "AUSTRALIA": "AU",
    "NEW ZEALAND": "NZ",
    "JAPAN": "JP",
}

COLLEGE_ALIAS_MAP = {
    "st-john-s-ny": "St. John's University",
    "st-john-s-n-y": "St. John's University",
    "st-john-s-university": "St. John's University",
    "acadia-can": "Acadia University",
    "acadia-university-canada": "Acadia University",
    "american": "American University",
    "american-international": "American International College",
    "byu": "Brigham Young",
    "cal-poly-obispo": "Cal Poly (San Luis Obispo)",
    "cal-poly-san-luis-obispo": "Cal Poly (San Luis Obispo)",
    "cal-santa-barbara": "California-Santa Barbara",
    "lsu": "Louisiana State",
    "nevada-las-vegas": "UNLV",
    "ole-miss": "Mississippi",
    "penn-state-beaver": "Penn State",
    "smu": "Southern Methodist",
    "tcu": "Texas Christian",
    "ucf": "Central Florida",
    "uconn": "Connecticut",
    "uab": "UAB",
    "unc-wilmington": "North Carolina-Wilmington",
    "university-of-california-santa-barbara": "California-Santa Barbara",
    "university-of-nevada-las-vegas": "UNLV",
    "usc": "Southern California",
}

COLLEGE_COUNTRY_HINTS = {
    "acadia university": "CA",
    "aek athens": "GR",
    "alberta canada": "CA",
    "mcgill": "CA",
    "toronto": "CA",
}


def load_college_enrichment() -> dict[str, dict]:
    """Load manual overrides for college country/region/city. Keys are normalized names."""
    path = DATA_DIR / "college_enrichment.json"
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("entries") or {}


def load_json(name: str):
    with open(DATA_DIR / name, "r", encoding="utf-8") as file:
        return json.load(file)


def save_json(name: str, payload) -> None:
    with open(DATA_DIR / name, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False)


def slugify(value: str) -> str:
    text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"\bhs\b", "high school", text)
    text = re.sub(r"\bprep\b", "preparatory", text)
    text = re.sub(r"\bst\.\b", "saint", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def normalize_name(value: str) -> str:
    return slugify(value)


def normalize_alias_key(value: str) -> str:
    text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"\bhs\b", "high school", text)
    text = re.sub(r"\bprep\b", "preparatory", text)
    text = re.sub(r"\bst\.\b", "saint", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def parse_last_affiliation(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    if "/" not in value:
        return value.strip(), None
    left, right = value.rsplit("/", 1)
    return left.strip() or None, right.strip() or None


def split_institutions(values: list[str] | None) -> list[str]:
    results: list[str] = []
    for value in values or []:
        for part in str(value).split(";"):
            cleaned = part.strip()
            if not cleaned or cleaned in {"-", "Unknown", "N/A"}:
                continue
            results.append(cleaned)
    return results


def normalize_country(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip().upper()
    return COUNTRY_MAP.get(text, text if len(text) == 2 else None)


def strip_known_college_suffixes(value: str) -> str:
    text = value.strip()
    text = re.sub(r"\s*\((?:NY|N\.Y\.|CAN|USA)\)\s*$", "", text).strip()
    text = re.sub(r",\s*(?:N\.Y\.|NY|USA|Canada)$", "", text).strip()
    return text


def canonicalize_college_name(name: str) -> tuple[str, str]:
    observed = name.strip()
    raw_normalized = normalize_alias_key(observed)
    if raw_normalized in COLLEGE_ALIAS_MAP:
        canonical = COLLEGE_ALIAS_MAP[raw_normalized]
        return canonical, normalize_alias_key(canonical)

    stripped = strip_known_college_suffixes(observed)
    stripped_normalized = normalize_alias_key(stripped)
    if stripped_normalized in COLLEGE_ALIAS_MAP:
        canonical = COLLEGE_ALIAS_MAP[stripped_normalized]
        return canonical, normalize_alias_key(canonical)

    if stripped.endswith(" University"):
        return stripped, stripped_normalized
    if stripped.endswith(" College"):
        return stripped, stripped_normalized

    return stripped, stripped_normalized


def infer_college_country(*names: str | None) -> str | None:
    """Infer country from college name patterns, e.g. 'Acadia (CAN)' -> CA."""
    for name in names:
        if not name:
            continue
        lower = name.lower()
        if "(can)" in lower or "(canada)" in lower or ", canada" in lower:
            return "CA"
        if "(usa)" in lower or "(us)" in lower or ", usa" in lower:
            return "US"
        if "france" in lower or "(fr)" in lower:
            return "FR"
        if "germany" in lower or "(de)" in lower:
            return "DE"
        if "greece" in lower or "(gr)" in lower:
            return "GR"
        if "italy" in lower or "(it)" in lower:
            return "IT"
        if "spain" in lower or "(es)" in lower:
            return "ES"
        if "australia" in lower or "(au)" in lower:
            return "AU"
        if "japan" in lower or "(jp)" in lower:
            return "JP"
        normalized = normalize_name(name)
        if normalized in COLLEGE_COUNTRY_HINTS:
            return COLLEGE_COUNTRY_HINTS[normalized]
    return None


def build_school_lookup(schools: list[dict]) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for school in schools:
        names = {
            school["name"],
            school["name"].replace("High School", "HS"),
            school["name"].replace("High School", "High"),
            school["name"].replace("Saint", "St."),
            school["name"].replace("Preparatory", "Prep"),
        }
        for name in names:
            key = normalize_name(name)
            if key:
                lookup.setdefault(key, school)
    return lookup


def main() -> int:
    athletes = load_json("athletes.json")
    schools = load_json("schools.json")
    school_affiliations = load_json("affiliations.json")
    college_enrichment = load_college_enrichment()

    school_lookup = build_school_lookup(schools)
    colleges: dict[str, dict] = {}
    education_affiliations: list[dict] = []
    seen_links: set[tuple[str, str, str]] = set()

    athlete_by_id = {athlete["id"]: athlete for athlete in athletes}
    school_by_id = {school["id"]: school for school in schools}

    def add_link(link: dict) -> None:
        key = (link["athleteId"], link["educationType"], link["institutionName"].strip().lower())
        if key in seen_links:
            return
        seen_links.add(key)
        education_affiliations.append(link)

    for affiliation in school_affiliations:
        school = school_by_id.get(affiliation["schoolId"])
        athlete = athlete_by_id.get(affiliation["athleteId"])
        if not school or not athlete:
            continue
        add_link(
            {
                "id": f"edu-{affiliation['id']}",
                "athleteId": affiliation["athleteId"],
                "league": affiliation["league"],
                "educationType": "highSchool",
                "institutionId": school["id"],
                "institutionName": school["name"],
                "source": "school_affiliations",
                "yearsAttended": affiliation.get("yearsAttended"),
                "graduated": affiliation.get("graduated"),
                "metadata": {
                    "derivedFromAffiliationId": affiliation["id"],
                    "notes": affiliation.get("notes"),
                },
            }
        )

    for athlete in athletes:
        education = athlete.get("education") or {}
        last_affiliation_name, _ = parse_last_affiliation(education.get("lastAffiliation"))
        league = athlete["league"]

        for edu_type in ("highSchool", "prepSchool", "academy"):
            for name in split_institutions(education.get(edu_type, []) or []):
                matched_school = school_lookup.get(normalize_name(name))
                link = {
                    "id": f"edu-{athlete['id']}-{edu_type}-{slugify(name)}",
                    "athleteId": athlete["id"],
                    "league": league,
                    "educationType": edu_type,
                    "institutionId": matched_school["id"] if matched_school else None,
                    "institutionName": matched_school["name"] if matched_school else name,
                    "source": "athlete_education",
                    "metadata": {
                        "lastAffiliation": education.get("lastAffiliation"),
                    },
                }
                add_link(link)

        for name in split_institutions(education.get("college", []) or []):
            canonical_name, normalized = canonicalize_college_name(name)
            if not normalized:
                continue
            college_id = f"c-{normalized}"
            if college_id not in colleges:
                enrichment = college_enrichment.get(normalized) or {}
                inferred_country = infer_college_country(name, canonical_name)
                colleges[college_id] = {
                    "id": college_id,
                    "name": canonical_name,
                    "aliases": [],
                    "country": enrichment.get("country") or inferred_country,
                    "region": enrichment.get("region"),
                    "city": enrichment.get("city"),
                    "metadata": {
                        "normalizedName": normalized,
                    },
                }
            if name != canonical_name and name not in colleges[college_id]["aliases"]:
                colleges[college_id]["aliases"].append(name)
            add_link(
                {
                    "id": f"edu-{athlete['id']}-college-{normalized}",
                    "athleteId": athlete["id"],
                    "league": league,
                    "educationType": "college",
                    "institutionId": college_id,
                    "institutionName": canonical_name,
                    "source": "athlete_education",
                    "metadata": {
                        "observedInstitutionName": name,
                        "lastAffiliation": education.get("lastAffiliation"),
                    },
                }
            )

    save_json("colleges.json", sorted(colleges.values(), key=lambda item: item["name"].lower()))
    save_json("education_affiliations.json", sorted(education_affiliations, key=lambda item: item["id"]))

    print(f"Colleges: {len(colleges)}")
    print(f"Education affiliations: {len(education_affiliations)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
