#!/usr/bin/env python3
import json
import re
import unicodedata
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

ROOT = Path("/home/claw/projects/hackathon")
OUTPUT_DIR = ROOT / "output"
EVENTS_PATH = OUTPUT_DIR / "events_kassir_unique_by_id.json"
PLACES_PATH = OUTPUT_DIR / "krasnoyarsk_places.geojson"
MATCHED_PATH = OUTPUT_DIR / "events_kassir_matched.json"
UNMATCHED_PATH = OUTPUT_DIR / "events_kassir_unmatched.json"
SUMMARY_PATH = OUTPUT_DIR / "events_kassir_match_summary.json"

ALIASES = {
    "максимилианс клубный ресторан": "maximilian s",
    "максимилианс": "maximilian s",
    "дом актера красноярск": "дом актера",
    "красноярский театр кукол": "театр кукол",
    "площадь мира": "музейный центр площадь мира",
    "хогвартсхолл": "хогвартс холл",
    "хогвартс холл": "хогвартс холл",
    "концертный зал афонтовский": "афонтовский",
    "ледовый дворец арена север": "арена север",
    "клуб галерея эрмитаж": "эрмитаж",
    "кз новая сцена": "новая сцена",
    "трк комсомолл": "комсомолл",
}

STOPWORDS = {
    "им",
    "имени",
    "г",
    "город",
    "красноярск",
    "красноярска",
    "красноярский",
    "красноярская",
    "красноярское",
    "государственный",
    "государственная",
    "государственное",
    "краевой",
    "краевая",
    "краевое",
    "сибирский",
    "сибирская",
    "имд",
    "д",
}

TYPE_BOOSTS = {
    "театр": {"theatre": 0.12, "arts_centre": 0.05},
    "спектакл": {"theatre": 0.12, "arts_centre": 0.05},
    "опера": {"theatre": 0.12, "music_venue": 0.08, "arts_centre": 0.05},
    "балет": {"theatre": 0.12, "music_venue": 0.08, "arts_centre": 0.05},
    "концерт": {"music_venue": 0.12, "arts_centre": 0.08, "theatre": 0.02},
    "оркестр": {"music_venue": 0.12, "arts_centre": 0.08},
    "музей": {"museum": 0.12, "gallery": 0.05},
    "кино": {"cinema": 0.12},
    "джаз": {"music_venue": 0.12},
}


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_text(value: str) -> str:
    value = strip_accents(value.lower().replace("ё", "е"))
    value = value.replace("&", " and ")
    value = re.sub(r"[«»\"'`]+", " ", value)
    value = re.sub(r"[()\[\]{}]", " ", value)
    value = re.sub(r"[–—−-]+", " ", value)
    value = re.sub(r"[^a-zа-я0-9]+", " ", value)
    value = re.sub(r"\bпроспект\b", "пр", value)
    value = re.sub(r"\bулица\b", "ул", value)
    value = re.sub(r"\bплощадь\b", "пл", value)
    tokens = [token for token in value.split() if token and token not in STOPWORDS]
    return " ".join(tokens)


def alias_name(value: str) -> str:
    normalized = normalize_text(value)
    return ALIASES.get(normalized, normalized)


def token_set(value: str) -> set[str]:
    return set(alias_name(value).split())


def similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, alias_name(left), alias_name(right)).ratio()


def token_overlap(left: str, right: str) -> float:
    left_tokens = token_set(left)
    right_tokens = token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    return intersection / union


def infer_event_hint(event: dict[str, Any]) -> str:
    parts = [
        str(event.get("category") or ""),
        str(event.get("title") or ""),
        " ".join(event.get("tags") or []),
    ]
    return normalize_text(" ".join(parts))


def build_place_record(feature: dict[str, Any]) -> dict[str, Any]:
    props = feature["properties"]
    coords = feature["geometry"]["coordinates"]
    return {
        "osm_type": props.get("osm_type"),
        "osm_id": props.get("osm_id"),
        "name": props.get("name") or "",
        "place_type": props.get("place_type") or "",
        "address_full": props.get("address_full") or "",
        "street": props.get("street") or "",
        "housenumber": props.get("housenumber") or "",
        "district": props.get("district") or "",
        "website": props.get("website") or "",
        "phone": props.get("phone") or "",
        "opening_hours": props.get("opening_hours") or "",
        "osm_url": props.get("osm_url") or "",
        "lat": coords[1],
        "lon": coords[0],
        "normalized_name": alias_name(props.get("name") or ""),
        "name_tokens": token_set(props.get("name") or ""),
    }


def score_candidate(event: dict[str, Any], venue: str, place: dict[str, Any]) -> tuple[float, list[str]]:
    reasons: list[str] = []
    venue_normalized = alias_name(venue)
    if not venue_normalized or not place["normalized_name"]:
        return 0.0, reasons

    name_ratio = similarity(venue, place["name"])
    overlap = token_overlap(venue, place["name"])
    score = name_ratio * 0.7 + overlap * 0.3
    reasons.append(f"name_ratio={name_ratio:.3f}")
    reasons.append(f"token_overlap={overlap:.3f}")

    if venue_normalized == place["normalized_name"]:
        score = max(score, 0.95)
        reasons.append("normalized_exact")

    venue_tokens = token_set(venue)
    if venue_tokens and venue_tokens <= place["name_tokens"]:
        score += 0.08
        reasons.append("venue_tokens_subset")

    hint = infer_event_hint(event)
    for marker, boosts in TYPE_BOOSTS.items():
        if marker in hint and place["place_type"] in boosts:
            score += boosts[place["place_type"]]
            reasons.append(f"type_boost={marker}:{place['place_type']}")

    if "ресторан" in venue_normalized and place["place_type"] == "restaurant":
        score += 0.08
        reasons.append("restaurant_hint")
    if "кино" in venue_normalized and place["place_type"] == "cinema":
        score += 0.08
        reasons.append("cinema_hint")
    if "театр" in venue_normalized and place["place_type"] == "theatre":
        score += 0.08
        reasons.append("theatre_hint")

    if not place["name"]:
        score -= 0.25
        reasons.append("missing_place_name_penalty")

    return score, reasons


def classify_match(venue: str, place: dict[str, Any], score: float) -> str:
    venue_normalized = alias_name(venue)
    place_normalized = place["normalized_name"]
    if venue_normalized == place_normalized:
        return "exact"
    if ALIASES.get(normalize_text(venue)) == place_normalized:
        return "alias"
    if score >= 0.86:
        return "fuzzy"
    return "unmatched"


def match_event(event: dict[str, Any], places: list[dict[str, Any]]) -> dict[str, Any]:
    venue = str(event.get("venue") or "").strip()
    base = dict(event)
    if not venue:
        base["match"] = {
            "status": "unmatched",
            "score": 0.0,
            "reasons": ["missing_venue"],
            "place": None,
        }
        return base

    scored = []
    for place in places:
        score, reasons = score_candidate(event, venue, place)
        if score <= 0:
            continue
        scored.append((score, place, reasons))

    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, best_place, reasons = scored[0] if scored else (0.0, None, ["no_candidates"])
    second_score = scored[1][0] if len(scored) > 1 else 0.0

    if best_place is None:
        status = "unmatched"
    else:
        status = classify_match(venue, best_place, best_score)
        if best_score < 0.72:
            status = "unmatched"
            reasons = reasons + ["below_threshold"]
        elif best_score - second_score < 0.03 and best_score < 0.9:
            status = "unmatched"
            reasons = reasons + ["ambiguous_top_match"]

    base["match"] = {
        "status": status,
        "score": round(best_score, 4),
        "second_score": round(second_score, 4),
        "reasons": reasons,
        "place": None
        if status == "unmatched" or best_place is None
        else {
            "osm_type": best_place["osm_type"],
            "osm_id": best_place["osm_id"],
            "name": best_place["name"],
            "place_type": best_place["place_type"],
            "address_full": best_place["address_full"],
            "street": best_place["street"],
            "housenumber": best_place["housenumber"],
            "district": best_place["district"],
            "lat": best_place["lat"],
            "lon": best_place["lon"],
            "osm_url": best_place["osm_url"],
        },
    }
    return base


def main() -> None:
    events = load_json(EVENTS_PATH)
    geojson = load_json(PLACES_PATH)
    places = [build_place_record(feature) for feature in geojson.get("features", [])]

    matched_events = [match_event(event, places) for event in events]
    unmatched_events = [event for event in matched_events if event["match"]["status"] == "unmatched"]

    status_counts = Counter(event["match"]["status"] for event in matched_events)
    unmatched_venues = Counter(str(event.get("venue") or "").strip() for event in unmatched_events)

    summary = {
        "total_events": len(matched_events),
        "status_counts": dict(status_counts),
        "top_unmatched_venues": [
            {"venue": venue, "count": count}
            for venue, count in unmatched_venues.most_common(30)
            if venue
        ],
    }

    save_json(MATCHED_PATH, matched_events)
    save_json(UNMATCHED_PATH, unmatched_events)
    save_json(SUMMARY_PATH, summary)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
