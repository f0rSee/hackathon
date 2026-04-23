#!/usr/bin/env python3
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
NOMINATIM_REVERSE_URL = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "krasnoyarsk-osm-places-parser/1.0 (research script)"
OUTPUT_DIR = Path("/home/claw/projects/hackathon/output")
GEOJSON_PATH = OUTPUT_DIR / "krasnoyarsk_places.geojson"
CSV_PATH = OUTPUT_DIR / "krasnoyarsk_places.csv"
AREA_NAME = "Красноярск"
COUNTRY_NAME = "Россия"
NOMINATIM_DELAY_SECONDS = 1.1
REVERSE_CACHE_PATH = OUTPUT_DIR / "nominatim_reverse_cache.json"
CITY_SEARCH_CACHE_PATH = OUTPUT_DIR / "city_search_cache.json"

CATEGORY_RULES = [
    (("tourism", "museum"), "museum"),
    (("amenity", "cinema"), "cinema"),
    (("amenity", "theatre"), "theatre"),
    (("amenity", "library"), "library"),
    (("tourism", "gallery"), "gallery"),
    (("amenity", "arts_centre"), "arts_centre"),
    (("amenity", "music_venue"), "music_venue"),
    (("amenity", "concert_hall"), "music_venue"),
    (("amenity", "restaurant"), "restaurant"),
]

CSV_FIELDS = [
    "osm_type",
    "osm_id",
    "name",
    "place_type",
    "lat",
    "lon",
    "address_full",
    "street",
    "housenumber",
    "district",
    "website",
    "phone",
    "opening_hours",
    "osm_url",
]


def build_overpass_query(bbox: tuple[float, float, float, float]) -> str:
    selectors = [
        '["tourism"="museum"]',
        '["amenity"="cinema"]',
        '["amenity"="theatre"]',
        '["amenity"="library"]',
        '["tourism"="gallery"]',
        '["amenity"="arts_centre"]',
        '["amenity"="music_venue"]',
        '["amenity"="concert_hall"]',
        '["amenity"="restaurant"]',
        '["theatre:type"="concert_hall"]',
    ]

    south, north, west, east = bbox
    bbox_clause = f"({south},{west},{north},{east})"

    parts = []
    for selector in selectors:
        parts.append(f"node{selector}{bbox_clause};")
        parts.append(f"way{selector}{bbox_clause};")
        parts.append(f"relation{selector}{bbox_clause};")

    body = "\n    ".join(parts)
    return f'''
[out:json][timeout:180];
(
    {body}
);
out center tags;
'''.strip()


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_overpass_data(session: requests.Session, bbox: tuple[float, float, float, float]) -> list[dict[str, Any]]:
    query = build_overpass_query(bbox)
    response = session.post(OVERPASS_URL, data={"data": query}, timeout=(15, 180))
    response.raise_for_status()
    payload = response.json()
    return payload.get("elements", [])


def get_coordinates(element: dict[str, Any]) -> tuple[float | None, float | None]:
    if "lat" in element and "lon" in element:
        return element["lat"], element["lon"]

    center = element.get("center")
    if isinstance(center, dict):
        return center.get("lat"), center.get("lon")

    return None, None


def detect_place_type(tags: dict[str, str]) -> str | None:
    for (key, value), place_type in CATEGORY_RULES:
        if tags.get(key) == value:
            return place_type

    if tags.get("theatre:type") == "concert_hall":
        return "music_venue"

    return None


def build_address_from_tags(tags: dict[str, str]) -> tuple[str, str, str]:
    street = tags.get("addr:street", "")
    housenumber = tags.get("addr:housenumber", "")

    parts = [
        tags.get("addr:postcode", ""),
        tags.get("addr:city", ""),
        street,
        housenumber,
    ]
    address_full = ", ".join(part for part in parts if part)
    return address_full, street, housenumber


def build_address_from_nominatim(address: dict[str, Any]) -> tuple[str, str, str]:
    road = address.get("road") or address.get("pedestrian") or address.get("street") or ""
    housenumber = address.get("house_number") or ""
    city = address.get("city") or address.get("town") or address.get("municipality") or ""
    postcode = address.get("postcode") or ""

    parts = [postcode, city, road, housenumber]
    address_full = ", ".join(part for part in parts if part)
    return address_full, road, housenumber


def load_json_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json_cache(path: Path, cache: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def load_reverse_cache() -> dict[str, dict[str, Any]]:
    return load_json_cache(REVERSE_CACHE_PATH)


def save_reverse_cache(cache: dict[str, dict[str, Any]]) -> None:
    save_json_cache(REVERSE_CACHE_PATH, cache)


def load_city_search_cache() -> dict[str, Any]:
    return load_json_cache(CITY_SEARCH_CACHE_PATH)


def save_city_search_cache(cache: dict[str, Any]) -> None:
    save_json_cache(CITY_SEARCH_CACHE_PATH, cache)


def find_city_bbox(session: requests.Session, cache: dict[str, Any]) -> tuple[float, float, float, float]:
    cache_key = f"{AREA_NAME}|{COUNTRY_NAME}"
    cached = cache.get(cache_key)
    if cached:
        return tuple(cached)

    response = session.get(
        NOMINATIM_SEARCH_URL,
        params={
            "q": f"{AREA_NAME}, {COUNTRY_NAME}",
            "format": "jsonv2",
            "limit": 5,
        },
        timeout=(15, 60),
    )
    response.raise_for_status()
    results = response.json()

    for result in results:
        if result.get("osm_type") == "relation" and result.get("type") == "city":
            bbox = tuple(float(value) for value in result["boundingbox"])
            cache[cache_key] = list(bbox)
            return bbox

    if not results:
        raise RuntimeError(f"City lookup returned no results for {AREA_NAME}")

    bbox = tuple(float(value) for value in results[0]["boundingbox"])
    cache[cache_key] = list(bbox)
    return bbox


def is_krasnoyarsk_result(reverse_data: dict[str, Any] | None) -> bool:
    if not reverse_data:
        return False

    address = reverse_data.get("address", {})
    candidates = [
        address.get("city"),
        address.get("town"),
        address.get("municipality"),
        address.get("county"),
    ]
    return AREA_NAME in {value for value in candidates if value}


def build_cache_key(lat: float, lon: float) -> str:
    return f"{lat:.6f},{lon:.6f}"


def reverse_geocode(
    session: requests.Session,
    lat: float,
    lon: float,
    cache: dict[str, dict[str, Any]],
    rate_limiter: dict[str, float],
) -> dict[str, Any]:
    cache_key = build_cache_key(lat, lon)
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    now = time.monotonic()
    elapsed = now - rate_limiter.get("last_call", 0.0)
    if elapsed < NOMINATIM_DELAY_SECONDS:
        time.sleep(NOMINATIM_DELAY_SECONDS - elapsed)

    response = session.get(
        NOMINATIM_REVERSE_URL,
        params={
            "format": "jsonv2",
            "lat": lat,
            "lon": lon,
            "zoom": 18,
            "addressdetails": 1,
        },
        timeout=(15, 60),
    )
    response.raise_for_status()
    data = response.json()
    rate_limiter["last_call"] = time.monotonic()
    cache[cache_key] = data
    return data


def extract_district(tags: dict[str, str], reverse_data: dict[str, Any] | None) -> str:
    tag_candidates = [
        tags.get("addr:suburb"),
        tags.get("addr:district"),
        tags.get("is_in:suburb"),
        tags.get("is_in:city_district"),
        tags.get("addr:neighbourhood"),
    ]
    for candidate in tag_candidates:
        if candidate:
            return candidate

    if reverse_data:
        address = reverse_data.get("address", {})
        reverse_candidates = [
            address.get("suburb"),
            address.get("city_district"),
            address.get("district"),
            address.get("neighbourhood"),
            address.get("quarter"),
            address.get("borough"),
        ]
        for candidate in reverse_candidates:
            if candidate:
                return candidate

    return ""


def build_osm_url(osm_type: str, osm_id: int) -> str:
    return f"https://www.openstreetmap.org/{osm_type}/{osm_id}"


def normalize_element(
    element: dict[str, Any],
    session: requests.Session,
    cache: dict[str, dict[str, Any]],
    rate_limiter: dict[str, float],
) -> dict[str, Any] | None:
    tags = element.get("tags", {})
    place_type = detect_place_type(tags)
    lat, lon = get_coordinates(element)

    if not place_type or lat is None or lon is None:
        return None

    reverse_data = None
    address_full, street, housenumber = build_address_from_tags(tags)
    district = extract_district(tags, None)

    needs_reverse = not district or not address_full
    if needs_reverse:
        try:
            reverse_data = reverse_geocode(session, lat, lon, cache, rate_limiter)
        except requests.RequestException:
            reverse_data = None

    if reverse_data and not is_krasnoyarsk_result(reverse_data):
        return None

    if reverse_data and not address_full:
        fallback_address, fallback_street, fallback_housenumber = build_address_from_nominatim(
            reverse_data.get("address", {})
        )
        address_full = fallback_address
        street = street or fallback_street
        housenumber = housenumber or fallback_housenumber

    if not district:
        district = extract_district(tags, reverse_data)

    return {
        "osm_type": element["type"],
        "osm_id": element["id"],
        "name": tags.get("name", ""),
        "place_type": place_type,
        "lat": lat,
        "lon": lon,
        "address_full": address_full,
        "street": street,
        "housenumber": housenumber,
        "district": district,
        "website": tags.get("website", "") or tags.get("contact:website", ""),
        "phone": tags.get("phone", "") or tags.get("contact:phone", ""),
        "opening_hours": tags.get("opening_hours", ""),
        "osm_url": build_osm_url(element["type"], element["id"]),
        "source_tags": tags,
    }


def deduplicate_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, int]] = set()
    unique_records = []
    for record in records:
        key = (record["osm_type"], record["osm_id"])
        if key in seen:
            continue
        seen.add(key)
        unique_records.append(record)
    return unique_records


def write_geojson(records: list[dict[str, Any]]) -> None:
    features = []
    for record in records:
        properties = {key: value for key, value in record.items() if key not in {"lat", "lon"}}
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [record["lon"], record["lat"]],
                },
                "properties": properties,
            }
        )

    collection = {"type": "FeatureCollection", "features": features}
    with GEOJSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(collection, f, ensure_ascii=False, indent=2)


def write_csv(records: list[dict[str, Any]]) -> None:
    with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for record in records:
            row = {field: record.get(field, "") for field in CSV_FIELDS}
            writer.writerow(row)


def summarize(records: list[dict[str, Any]]) -> dict[str, int]:
    stats: dict[str, int] = {}
    for record in records:
        place_type = record["place_type"]
        stats[place_type] = stats.get(place_type, 0) + 1
    return dict(sorted(stats.items()))


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    session = create_session()
    reverse_cache = load_reverse_cache()
    city_search_cache = load_city_search_cache()
    rate_limiter = {"last_call": 0.0}

    try:
        bbox = find_city_bbox(session, city_search_cache)
        elements = fetch_overpass_data(session, bbox)
    except (requests.RequestException, RuntimeError) as exc:
        print(f"Failed to fetch data from OSM services: {exc}", file=sys.stderr)
        return 1

    records = []
    for element in elements:
        record = normalize_element(element, session, reverse_cache, rate_limiter)
        if record is not None:
            records.append(record)

    records = deduplicate_records(records)
    records.sort(key=lambda item: (item["place_type"], item["name"], item["osm_id"]))

    write_geojson(records)
    write_csv(records)
    save_reverse_cache(reverse_cache)
    save_city_search_cache(city_search_cache)

    print(f"Using bbox: {bbox}")
    print(f"Saved {len(records)} records to {GEOJSON_PATH}")
    print(f"Saved CSV to {CSV_PATH}")
    print(json.dumps(summarize(records), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
