"""Shared test fixtures — no live API calls anywhere in tests/."""

SAMPLE_FEATURE = {
    "id": "us7000abc1",
    "type": "Feature",
    "properties": {
        "mag": 3.5,
        "place": "10km NE of Somewhere, CA",
        "time": 1713571200000,  # 2024-04-20 00:00:00 UTC
        "status": "reviewed",
        "type": "earthquake",
        "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000abc1",
    },
    "geometry": {
        "type": "Point",
        "coordinates": [-118.25, 34.05, 10.0],
    },
}

SAMPLE_FEATURE_NO_MAG = {
    "id": "us7000abc2",
    "type": "Feature",
    "properties": {
        "mag": None,
        "place": "Unknown",
        "time": 1713571200000,
        "status": "automatic",
        "type": "earthquake",
        "url": None,
    },
    "geometry": {"type": "Point", "coordinates": [-100.0, 35.0, 5.0]},
}

SAMPLE_FEATURE_MISSING_TIME = {
    "id": "us7000abc3",
    "type": "Feature",
    "properties": {
        "mag": 1.2,
        "place": "Somewhere",
        "time": None,
        "status": "automatic",
        "type": "earthquake",
        "url": None,
    },
    "geometry": {"type": "Point", "coordinates": [0.0, 0.0, 0.0]},
}

GEOJSON_PAGE = {
    "type": "FeatureCollection",
    "metadata": {"count": 2},
    "features": [SAMPLE_FEATURE, SAMPLE_FEATURE_NO_MAG],
}

COUNT_RESPONSE = {"count": 2}
