"""Tests for API client — all HTTP calls mocked."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
import requests

from earthquake_tracker.api import count_events, fetch_events, parse_event
from .fixtures import (
    COUNT_RESPONSE,
    GEOJSON_PAGE,
    SAMPLE_FEATURE,
    SAMPLE_FEATURE_MISSING_TIME,
    SAMPLE_FEATURE_NO_MAG,
)


def _mock_response(json_data: dict, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status >= 400:
        http_err = requests.exceptions.HTTPError(response=resp)
        resp.raise_for_status.side_effect = http_err
    return resp


class TestCountEvents:
    def test_returns_count(self):
        session = MagicMock()
        session.get.return_value = _mock_response(COUNT_RESPONSE)

        result = count_events(date(2024, 3, 21), date(2024, 4, 20), session)

        assert result == 2
        assert session.get.call_count == 1

    def test_zero_when_no_count_key(self):
        session = MagicMock()
        session.get.return_value = _mock_response({})

        result = count_events(date(2024, 3, 21), date(2024, 4, 20), session)

        assert result == 0


class TestFetchEvents:
    def test_yields_all_features(self):
        session = MagicMock()
        session.get.side_effect = [
            _mock_response(COUNT_RESPONSE),   # count call
            _mock_response(GEOJSON_PAGE),     # page 1
        ]

        features = list(fetch_events(date(2024, 3, 21), date(2024, 4, 20), session))

        assert len(features) == 2
        assert features[0]["id"] == "us7000abc1"

    def test_paginates_when_needed(self):
        page1 = {
            "type": "FeatureCollection",
            "features": [SAMPLE_FEATURE],
        }
        page2 = {
            "type": "FeatureCollection",
            "features": [SAMPLE_FEATURE_NO_MAG],
        }

        session = MagicMock()
        session.get.side_effect = [
            _mock_response({"count": 2}),
            _mock_response(page1),
            _mock_response(page2),
        ]

        with patch("earthquake_tracker.api.PAGE_SIZE", 1):
            features = list(fetch_events(date(2024, 3, 21), date(2024, 4, 20), session))

        assert len(features) == 2

    def test_empty_when_zero_count(self):
        session = MagicMock()
        session.get.return_value = _mock_response({"count": 0})

        features = list(fetch_events(date(2024, 3, 21), date(2024, 4, 20), session))

        assert features == []
        assert session.get.call_count == 1  # only the count call

    def test_stops_on_empty_page(self):
        session = MagicMock()
        session.get.side_effect = [
            _mock_response({"count": 5}),
            _mock_response({"type": "FeatureCollection", "features": []}),
        ]

        features = list(fetch_events(date(2024, 3, 21), date(2024, 4, 20), session))

        assert features == []

    def test_retries_on_server_error(self):
        good_resp = _mock_response(COUNT_RESPONSE)
        server_err = _mock_response({}, status=503)
        page_resp = _mock_response(GEOJSON_PAGE)

        session = MagicMock()
        session.get.side_effect = [good_resp, server_err, server_err, page_resp]

        with patch("earthquake_tracker.api.RETRY_BACKOFF", 0):
            features = list(fetch_events(date(2024, 3, 21), date(2024, 4, 20), session))

        assert len(features) == 2

    def test_raises_after_max_retries(self):
        server_err = _mock_response({}, status=503)

        session = MagicMock()
        # count call succeeds, but every page request fails
        session.get.side_effect = [_mock_response({"count": 1})] + [server_err] * 10

        with patch("earthquake_tracker.api.RETRY_BACKOFF", 0):
            with pytest.raises(RuntimeError, match="All .* attempts failed"):
                list(fetch_events(date(2024, 3, 21), date(2024, 4, 20), session))


class TestParseEvent:
    def test_parses_normal_feature(self):
        event = parse_event(SAMPLE_FEATURE)

        assert event is not None
        assert event["id"] == "us7000abc1"
        assert event["magnitude"] == 3.5
        assert event["date"] == "2024-04-20"
        assert event["longitude"] == -118.25
        assert event["latitude"] == 34.05
        assert event["depth_km"] == 10.0

    def test_parses_feature_with_null_magnitude(self):
        event = parse_event(SAMPLE_FEATURE_NO_MAG)

        assert event is not None
        assert event["magnitude"] is None

    def test_returns_none_for_missing_time(self):
        event = parse_event(SAMPLE_FEATURE_MISSING_TIME)

        assert event is None

    def test_returns_none_for_malformed_feature(self):
        event = parse_event({"id": "bad", "properties": None, "geometry": None})

        assert event is None
