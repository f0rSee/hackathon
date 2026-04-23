#!/usr/bin/env python3
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any

import requests

ROOT = Path('/home/claw/projects/hackathon')
OUTPUT = ROOT / 'output'
EVENTS_PATH = OUTPUT / 'events_kassir_unique_by_id.json'
OUT_EVENTS = OUTPUT / 'events_kassir_krasnoyarsk_heatmap.json'
OUT_VENUES = OUTPUT / 'venues_kassir_krasnoyarsk_heatmap.json'
OUT_EVENTS_GEOJSON = OUTPUT / 'kepler_kassir_krasnoyarsk_events.geojson'
OUT_VENUES_GEOJSON = OUTPUT / 'kepler_kassir_krasnoyarsk_venues.geojson'
OUT_SUMMARY = OUTPUT / 'events_kassir_krasnoyarsk_heatmap_summary.json'
CACHE_PATH = OUTPUT / 'nominatim_search_cache.json'
USER_AGENT = 'kassir-krasnoyarsk-heatmap/1.0 (research script)'
DELAY = 1.1
SEARCH_URL = 'https://nominatim.openstreetmap.org/search'

EXCLUDE_MARKERS = {
    'абакан', 'норильск', 'ачинск', 'канск', 'зеленогорск', 'лесосибирск'
}

MANUAL_COORDS = {
    'Красноярский театр кукол': {
        'query': 'manual',
        'name': 'Театр кукол',
        'display_name': 'Театр кукол, улица Ленина, 119, Красноярск',
        'lat': 56.0123995,
        'lon': 92.8567633,
        'osm_type': 'way',
        'osm_id': 0,
        'class': 'manual',
        'type': 'theatre',
    },
    'Гранд-холл Сибирь': {
        'query': 'manual',
        'name': 'Международный выставочно-деловой центр «Сибирь»',
        'display_name': 'Международный выставочно-деловой центр «Сибирь», улица Авиаторов, 19, Красноярск',
        'lat': 56.0403113,
        'lon': 92.9236642,
        'osm_type': 'way',
        'osm_id': 0,
        'class': 'manual',
        'type': 'commercial',
    },
    'Концертный зал Афонтовский': {
        'query': 'manual',
        'name': 'Концертный зал Афонтовский',
        'display_name': 'Концертный зал Афонтовский, Красноярск',
        'lat': 56.021221,
        'lon': 92.798623,
        'osm_type': 'way',
        'osm_id': 0,
        'class': 'manual',
        'type': 'music_venue',
    },
    'КЗ Новая Сцена': {
        'query': 'manual',
        'name': 'Новая сцена',
        'display_name': 'Новая сцена, Красноярск',
        'lat': 56.017346,
        'lon': 92.880493,
        'osm_type': 'way',
        'osm_id': 0,
        'class': 'manual',
        'type': 'music_venue',
    },
}

QUERY_ALIASES = {
    'Красноярский драмтеатр им. А.С. Пушкина': [
        'Красноярский драматический театр имени А. С. Пушкина, Красноярск',
    ],
    'Дом Актера (Красноярск)': [
        'Дом актера, Красноярск',
    ],
    'Красноярский государственный театр оперы и балета': [
        'Красноярский театр оперы и балета, Красноярск',
    ],
    'Красноярский театр кукол': [
        'Красноярский театр кукол, Красноярск',
    ],
    'Гранд-холл Сибирь': [
        'Гранд Холл Сибирь, Красноярск',
        'МВДЦ Сибирь, Красноярск',
    ],
    'Kitchen Lab': [
        'Kitchen Lab, Красноярск',
    ],
    'Красноярская краевая филармония': [
        'Красноярская краевая филармония, Красноярск',
    ],
    'Circus Concert Hall': [
        'Circus Concert Hall, Красноярск',
    ],
    'Площадь Мира': [
        'Музейный центр Площадь Мира, Красноярск',
        'Площадь Мира, Красноярск',
    ],
    'МАКСИМИЛИАНС – Клубный ресторан': [
        'Максимилианс, Красноярск',
        'Максимилианс клубный ресторан, Красноярск',
    ],
    'Сибирский государственный институт искусств им. Д.Хворостовского': [
        'Сибирский государственный институт искусств имени Дмитрия Хворостовского, Красноярск',
    ],
    'Ледовый дворец Арена Север': [
        'Арена Север, Красноярск',
    ],
    'ДК Комбайностроителей': [
        'ДК Комбайностроителей, Красноярск',
    ],
    'Дворец спорта им. Ивана Ярыгина': [
        'Дворец спорта имени Ивана Ярыгина, Красноярск',
    ],
    'Клуб-галерея Эрмитаж': [
        'Клуб-галерея Эрмитаж, Красноярск',
        'Эрмитаж, Красноярск',
    ],
    'Концертный зал Афонтовский': [
        'Концертный зал Афонтовский, Красноярск',
    ],
    'Дворец культуры и спорта Металлургов': [
        'Дворец культуры и спорта Металлургов, Красноярск',
    ],
    'ТРЦ Планета.': [
        'ТРЦ Планета, Красноярск',
    ],
    'ТРК КомсоМолл': [
        'ТРК Комсомолл, Красноярск',
    ],
    'КЗ Новая Сцена': [
        'Новая Сцена, Красноярск',
        'КЗ Новая сцена, Красноярск',
    ],
    'Стадион Енисей': [
        'Стадион Енисей, Красноярск',
    ],
    'Дом Кино': [
        'Дом кино, Красноярск',
    ],
    'Baker Street на Д.Мартынова': [
        'Baker Street, улица Дмитрия Мартынова, Красноярск',
    ],
    'Арт-Холл Синема': [
        'Арт Холл Синема, Красноярск',
    ],
    'Бар Радость': [
        'Бар Радость, Красноярск',
    ],
    'Закрытый клуб 64/6': [
        '64/6, Красноярск',
    ],
    'Большой академический концертный зал Красноярского государственного института искусств': [
        'Большой академический концертный зал, Красноярск',
    ],
    'Бар "Дело не в тебе"': [
        'Дело не в тебе, Красноярск',
    ],
    'ДК Железнодорожников (Красноярск)': [
        'ДК Железнодорожников, Красноярск',
    ],
    'Парковка ТЦ Планета': [
        'ТЦ Планета, Красноярск',
    ],
    'Grek Land': [
        'Grek Land, Красноярск',
    ],
    'Дом Художника – Easy Draw': [
        'Дом художника, Красноярск',
        'Easy Draw, Красноярск',
    ],
    'ХогвартсХолл': [
        'Хогвартс Холл, Красноярск',
    ],
}


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding='utf-8'))


def save_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def build_geojson(features: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        'type': 'FeatureCollection',
        'features': features,
    }


def build_event_feature(row: dict[str, Any]) -> dict[str, Any] | None:
    lat = row.get('lat')
    lon = row.get('lon')
    if lat is None or lon is None:
        return None
    return {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [lon, lat],
        },
        'properties': {
            'id': row.get('id'),
            'title': row.get('title'),
            'category': row.get('category'),
            'tags': ', '.join(row.get('tags') or []),
            'venue': row.get('venue'),
            'resolvedVenue': row.get('resolvedVenue'),
            'startDate': row.get('startDate'),
            'endDate': row.get('endDate'),
            'sourceUrl': row.get('sourceUrl'),
        },
    }


def build_venue_feature(row: dict[str, Any]) -> dict[str, Any] | None:
    lat = row.get('lat')
    lon = row.get('lon')
    if lat is None or lon is None:
        return None
    return {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [lon, lat],
        },
        'properties': {
            'venue': row.get('venue'),
            'resolvedVenue': row.get('resolvedVenue'),
            'eventCount': row.get('eventCount'),
            'resolvedQuery': row.get('resolvedQuery'),
            'resolvedDisplayName': row.get('resolvedDisplayName'),
            'osm_type': row.get('osm_type'),
            'osm_id': row.get('osm_id'),
            'osm_class': row.get('osm_class'),
            'osm_place_type': row.get('osm_place_type'),
        },
    }


def is_krasnoyarsk_result(item: dict[str, Any]) -> bool:
    address = item.get('address') or {}
    display = (item.get('display_name') or '').lower()
    values = [
        address.get('city'),
        address.get('town'),
        address.get('municipality'),
        address.get('state_district'),
        address.get('county'),
    ]
    values = [str(v).lower() for v in values if v]
    return 'красноярск' in values or 'красноярск' in display


def should_exclude_event(event: dict[str, Any]) -> bool:
    text = ' '.join([
        str(event.get('venue') or ''),
        str(event.get('title') or ''),
        str((event.get('sourceMeta') or {}).get('url') or ''),
    ]).lower()
    return any(marker in text for marker in EXCLUDE_MARKERS)


def venue_queries(venue: str) -> list[str]:
    queries = []
    if venue in QUERY_ALIASES:
        queries.extend(QUERY_ALIASES[venue])
    queries.append(f'{venue}, Красноярск')
    return list(dict.fromkeys(queries))


def search(session: requests.Session, cache: dict[str, Any], query: str, last_call: list[float]) -> list[dict[str, Any]]:
    if query in cache:
        return cache[query]
    elapsed = time.monotonic() - last_call[0]
    if elapsed < DELAY:
        time.sleep(DELAY - elapsed)
    resp = session.get(
        SEARCH_URL,
        params={
            'q': query,
            'format': 'jsonv2',
            'limit': 8,
            'addressdetails': 1,
            'accept-language': 'ru',
        },
        timeout=(15, 60),
    )
    resp.raise_for_status()
    last_call[0] = time.monotonic()
    data = resp.json()
    cache[query] = data
    return data


def is_specific_result(item: dict[str, Any]) -> bool:
    place_type = (item.get('type') or '').lower()
    if place_type in {'city', 'administrative', 'bus_stop'}:
        return False
    return True


def resolve_venue(session: requests.Session, cache: dict[str, Any], venue: str, last_call: list[float]) -> dict[str, Any] | None:
    if venue in MANUAL_COORDS:
        return MANUAL_COORDS[venue]
    for query in venue_queries(venue):
        results = search(session, cache, query, last_call)
        for item in results:
            if not is_krasnoyarsk_result(item) or not is_specific_result(item):
                continue
            return {
                'query': query,
                'name': item.get('name') or venue,
                'display_name': item.get('display_name') or '',
                'lat': float(item['lat']),
                'lon': float(item['lon']),
                'osm_type': item.get('osm_type') or '',
                'osm_id': item.get('osm_id') or '',
                'class': item.get('class') or '',
                'type': item.get('type') or '',
            }
    return None


def main() -> int:
    events = load_json(EVENTS_PATH, [])
    local_events = [event for event in events if not should_exclude_event(event)]

    venue_counts = Counter(str(event.get('venue') or '').strip() for event in local_events if event.get('venue'))
    venues = [venue for venue in venue_counts if venue]

    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT})
    cache = load_json(CACHE_PATH, {})
    last_call = [0.0]

    resolved: dict[str, Any] = {}
    unresolved: list[str] = []
    for venue in sorted(venues):
        match = resolve_venue(session, cache, venue, last_call)
        if match:
            resolved[venue] = match
        else:
            unresolved.append(venue)

    heatmap_events = []
    for event in local_events:
        venue = str(event.get('venue') or '').strip()
        point = resolved.get(venue)
        heatmap_events.append({
            'id': event.get('id'),
            'title': event.get('title'),
            'category': event.get('category'),
            'tags': event.get('tags') or [],
            'startDate': event.get('startDate'),
            'endDate': event.get('endDate'),
            'venue': venue,
            'sourceUrl': (event.get('sourceMeta') or {}).get('url'),
            'lat': point.get('lat') if point else None,
            'lon': point.get('lon') if point else None,
            'resolvedVenue': point.get('name') if point else None,
            'resolvedQuery': point.get('query') if point else None,
            'resolvedDisplayName': point.get('display_name') if point else None,
        })

    venue_rows = []
    for venue, count in venue_counts.most_common():
        point = resolved.get(venue)
        venue_rows.append({
            'venue': venue,
            'eventCount': count,
            'lat': point.get('lat') if point else None,
            'lon': point.get('lon') if point else None,
            'resolvedVenue': point.get('name') if point else None,
            'resolvedQuery': point.get('query') if point else None,
            'resolvedDisplayName': point.get('display_name') if point else None,
            'osm_type': point.get('osm_type') if point else None,
            'osm_id': point.get('osm_id') if point else None,
            'osm_class': point.get('class') if point else None,
            'osm_place_type': point.get('type') if point else None,
        })

    event_features = [feature for row in heatmap_events if (feature := build_event_feature(row)) is not None]
    venue_features = [feature for row in venue_rows if (feature := build_venue_feature(row)) is not None]
    geocoded_events = len(event_features)
    geocoded_venues = len(venue_features)
    summary = {
        'total_events_source': len(events),
        'total_events_krasnoyarsk': len(local_events),
        'excluded_non_krasnoyarsk_events': len(events) - len(local_events),
        'unique_venues_krasnoyarsk': len(venues),
        'resolved_venues': len(resolved),
        'unresolved_venues': unresolved,
        'geocoded_events': geocoded_events,
        'geocoded_venues': geocoded_venues,
        'geocoded_ratio': round(geocoded_events / len(local_events), 4) if local_events else 0.0,
        'kepler_events_geojson': str(OUT_EVENTS_GEOJSON),
        'kepler_venues_geojson': str(OUT_VENUES_GEOJSON),
        'top_venues': [
            {'venue': venue, 'count': count}
            for venue, count in venue_counts.most_common(20)
        ],
    }

    save_json(CACHE_PATH, cache)
    save_json(OUT_EVENTS, heatmap_events)
    save_json(OUT_VENUES, venue_rows)
    save_json(OUT_EVENTS_GEOJSON, build_geojson(event_features))
    save_json(OUT_VENUES_GEOJSON, build_geojson(venue_features))
    save_json(OUT_SUMMARY, summary)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
