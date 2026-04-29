"""Tests for src/utils/cities.py — Tunisia city registry & helpers."""

import pytest
from src.utils.cities import (
    TUNISIA_CITIES,
    get_all_cities,
    get_cities_by_region,
    get_cities_by_governorate,
    get_governorate_capitals,
    get_city_by_name,
    list_governorates,
    list_regions,
    city_count,
)
#

# ── Registry integrity ───────────────────────────────────────

class TestRegistryIntegrity:

    def test_city_count_matches_list_length(self):
        assert city_count() == len(TUNISIA_CITIES)

    def test_all_cities_have_required_keys(self):
        required = {"name", "lat", "lon", "governorate", "region"}
        for city in TUNISIA_CITIES:
            assert required.issubset(city.keys()), (
                f"{city.get('name', '?')} missing keys: "
                f"{required - city.keys()}"
            )

    def test_all_latitudes_in_tunisia_range(self):
        """Tunisia spans roughly lat 30°–37.5°N."""
        for city in TUNISIA_CITIES:
            assert 30.0 <= city["lat"] <= 38.0, (
                f"{city['name']}: lat {city['lat']} out of Tunisia range"
            )

    def test_all_longitudes_in_tunisia_range(self):
        """Tunisia spans roughly lon 7°–12°E."""
        for city in TUNISIA_CITIES:
            assert 7.0 <= city["lon"] <= 12.0, (
                f"{city['name']}: lon {city['lon']} out of Tunisia range"
            )

    def test_no_duplicate_city_names(self):
        names = [c["name"] for c in TUNISIA_CITIES]
        assert len(names) == len(set(names)), (
            f"Duplicate cities: {[n for n in names if names.count(n) > 1]}"
        )


# ── get_all_cities ────────────────────────────────────────────

class TestGetAllCities:

    def test_returns_full_list(self):
        assert get_all_cities() is TUNISIA_CITIES

    def test_non_empty(self):
        assert len(get_all_cities()) > 0


# ── get_cities_by_region ──────────────────────────────────────

class TestGetCitiesByRegion:

    def test_nord_returns_cities(self):
        cities = get_cities_by_region("Nord")
        assert len(cities) > 0
        assert all(c["region"] == "Nord" for c in cities)

    def test_all_7_regions_have_cities(self):
        for region in list_regions():
            assert len(get_cities_by_region(region)) > 0

    def test_unknown_region_returns_empty(self):
        assert get_cities_by_region("Atlantis") == []


# ── get_cities_by_governorate ─────────────────────────────────

class TestGetCitiesByGovernorate:

    def test_sfax_returns_cities(self):
        cities = get_cities_by_governorate("Sfax")
        assert len(cities) > 0
        assert all(c["governorate"] == "Sfax" for c in cities)

    def test_all_24_governorates_have_cities(self):
        for gov in list_governorates():
            assert len(get_cities_by_governorate(gov)) > 0

    def test_unknown_governorate_returns_empty(self):
        assert get_cities_by_governorate("Narnia") == []


# ── get_governorate_capitals ──────────────────────────────────

class TestGetGovernorateCapitals:

    def test_returns_24_capitals(self):
        capitals = get_governorate_capitals()
        assert len(capitals) == 24

    def test_each_capital_from_different_governorate(self):
        capitals = get_governorate_capitals()
        governorates = [c["governorate"] for c in capitals]
        assert len(governorates) == len(set(governorates))

    def test_tunis_is_a_capital(self):
        capitals = get_governorate_capitals()
        capital_names = [c["name"] for c in capitals]
        assert "Tunis" in capital_names


# ── get_city_by_name ──────────────────────────────────────────

class TestGetCityByName:

    def test_known_city(self):
        city = get_city_by_name("Tunis")
        assert city is not None
        assert city["name"] == "Tunis"
        assert city["governorate"] == "Tunis"

    def test_unknown_city_returns_none(self):
        assert get_city_by_name("Atlantis") is None

    def test_case_sensitive(self):
        assert get_city_by_name("tunis") is None


# ── list_governorates ─────────────────────────────────────────

class TestListGovernorates:

    def test_returns_24(self):
        assert len(list_governorates()) == 24

    def test_sorted(self):
        govs = list_governorates()
        assert govs == sorted(govs)

    def test_contains_known_governorates(self):
        govs = list_governorates()
        for expected in ["Tunis", "Sfax", "Sousse", "Nabeul", "Gabes"]:
            assert expected in govs


# ── list_regions ──────────────────────────────────────────────

class TestListRegions:

    def test_returns_7_regions(self):
        assert len(list_regions()) == 7

    def test_sorted(self):
        regions = list_regions()
        assert regions == sorted(regions)

    def test_expected_regions(self):
        regions = list_regions()
        expected = [
            "Centre", "Centre-Ouest", "Nord", "Nord-Est",
            "Nord-Ouest", "Sud-Est", "Sud-Ouest",
        ]
        assert regions == expected
