"""Tests for the Eurostat JSON-stat 1.0 parser."""

import pytest
from lib.eurostat_parser import parse_eurostat_json


@pytest.fixture
def minimal_eurostat_json():
    """A 2x2 grid: 2 countries x 2 years, 4 values total."""
    return {
        "id": ["geo", "time"],
        "dimension": {
            "geo": {"category": {"index": {"FR": 0, "DE": 1}}},
            "time": {"category": {"index": {"2020": 0, "2021": 1}}},
        },
        "value": {
            "0": 100.0,
            "1": 200.0,
            "2": 300.0,
            "3": 400.0,
        },
    }


def test_parses_correct_number_of_rows(minimal_eurostat_json):
    rows = parse_eurostat_json(minimal_eurostat_json)
    assert len(rows) == 4


def test_parses_correct_dimension_values(minimal_eurostat_json):
    rows = parse_eurostat_json(minimal_eurostat_json)
    countries = {row["geo"] for row in rows}
    years = {row["time"] for row in rows}
    assert countries == {"FR", "DE"}
    assert years == {"2020", "2021"}


def test_parses_correct_value_assignment(minimal_eurostat_json):
    rows = parse_eurostat_json(minimal_eurostat_json)
    fr_2020 = next(r for r in rows if r["geo"] == "FR" and r["time"] == "2020")
    de_2021 = next(r for r in rows if r["geo"] == "DE" and r["time"] == "2021")
    assert fr_2020["valeur"] == 100.0
    assert de_2021["valeur"] == 400.0


def test_handles_empty_payload():
    """An empty dataset produces no rows, never crashes."""
    empty = {
        "id": ["geo", "time"],
        "dimension": {
            "geo": {"category": {"index": {}}},
            "time": {"category": {"index": {}}},
        },
        "value": {},
    }
    rows = parse_eurostat_json(empty)
    assert rows == []
