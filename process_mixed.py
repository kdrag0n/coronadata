#!/usr/bin/env python

from datetime import datetime, timezone, timedelta
from collections import defaultdict
import csv
import json
import sys

import pycountry
import pytz
import requests

country_id_overrides = {
    "DP": "Diamond Princess",
    "CD": "Democratic Republic of the Congo",
    "COD": "Democratic Republic of the Congo",
    "VA": "Holy See",
    "VAT": "Holy See",
    "IR": "Iran",
    "IRN": "Iran",
    "PS": "Palestine",
    "PSE": "Palestine",
    "RU": "Russia",
    "RUS": "Russia",
    "KP": "North Korea",
    "PRK": "North Korea",
    "KR": "South Korea",
    "KOR": "South Korea",
    "XK": "Kosovo",
    "XKX": "Kosovo",
    "VG": "British Virgin Islands",
    "VGB": "British Virgin Islands",
    "LA": "Laos",
    "LAO": "Laos",
    "SX": "Sint Maarten",
    "SXM": "Sint Maarten",
    "VI": "United States Virgin Islands",
    "VIR": "United States Virgin Islands",
    "FK": "Falkland Islands",
    "FLK": "Falkland Islands",
    "SYR": "Syria",
    "SY": "Syria"
}

geo_id_overrides = {
    "JPG11668": "Diamond Princess"
}

# From Worldometer
FALLBACK_POPULATION = 33_082_146  # world population / countries
population_overrides = {
    "Anguilla": 14969,
    "Eritrea": 3_533_929,
    "Falkland Islands": 3454
}

ecdc_combined_countries = {
    # Missing country: merged into country
    "French Guiana": "France",
    "Hong Kong": "China",
}

country_cache = {}

def get_country_name(country_id, geo_id=None, provided_name=""):
    cache_key = (country_id, geo_id)
    if cache_key in country_cache:
        return country_cache[cache_key]

    if country_id in country_id_overrides:
        country_name = country_id_overrides[country_id]
    elif geo_id in geo_id_overrides:
        country_name = geo_id_overrides[geo_id]
    else:
        kwargs = {"alpha_" + str(len(country_id)): country_id}
        iso_country = pycountry.countries.get(**kwargs)
        if iso_country is None:
            print(f"WARNING: Unknown country with ID '{country_id}', geo ID '{geo_id}', provided name '{provided_name}'", file=sys.stderr)
            country_name = provided_name.replace("_", " ")
        else:
            country_name = getattr(iso_country, "common_name", iso_country.name)

    country_cache[cache_key] = country_name
    return country_name

# Load ECDC historical data
with open(sys.argv[1], "r", encoding="iso-8859-1") as ef:
    reader = csv.DictReader(ef)
    ecdc = list(reader)

# Load thevirustracker.com live data
live = {}
with open(sys.argv[2], "r") as lf:
    live_map = json.load(lf)["countryitems"][0]
    del live_map["stat"]
    for entry in live_map.values():
        country = get_country_name(entry["code"])
        entry["total_cases"] = int(entry["total_cases"])
        entry["total_deaths"] = int(entry["total_deaths"])
        live[country] = entry

# Load US state data from New York Times
with open(sys.argv[3], "r") as statef:
    reader = csv.DictReader(statef)
    states = list(reader)

# Load US county data from New York Times
with open(sys.argv[4], "r") as countyf:
    reader = csv.DictReader(countyf)
    counties = list(reader)

# Parse ECDC dates
for entry in ecdc:
    day = int(entry["day"])
    year = int(entry["year"])
    month = int(entry["month"])
    # 10 AM CET/CEST
    date = datetime(year, month, day, 10, 0, 0)
    tz_date = pytz.timezone("Europe/Brussels").localize(date)
    entry["date"] = tz_date

# Parse NYT dates
def parse_nyt_date(entry):
    year, month, day = map(int, entry["date"].split("-"))
    # 6 PM EST/EDT
    date = datetime(year, month, day, 18, 0, 0)
    tz_date = pytz.timezone("America/New_York").localize(date)
    entry["date"] = tz_date

for entry in states:
    parse_nyt_date(entry)

for entry in counties:
    parse_nyt_date(entry)

# Get start and end dates
start_date = min(ecdc, key=lambda e: e["date"])["date"]
end_date = datetime.now(timezone.utc).replace(microsecond=0)

# Calculate elapsed days
n_days = (end_date - start_date).days + 1

# Create maps
def date_list():
    return [0] * n_days

metrics = {
    "cases": {
        "absolute": defaultdict(date_list),
        "relative": defaultdict(date_list),
        "growth": defaultdict(date_list),
    },
    "deaths": {
        "absolute": defaultdict(date_list),
        "relative": defaultdict(date_list),
        "growth": defaultdict(date_list),
    }
}

populations = {}

# Populate historical relative metrics, and populations
for entry in ecdc:
    country_code = entry["countryterritoryCode"] or entry["geoId"]
    country_name = get_country_name(country_code, entry["geoId"], entry["countriesAndTerritories"])
    cases = int(entry["cases"])
    deaths = int(entry["deaths"])
    offset = (entry["date"] - start_date).days

    metrics["cases"]["relative"][country_name][offset] += cases
    metrics["deaths"]["relative"][country_name][offset] += deaths

    try:
        population = int(entry["popData2018"] or population_overrides[country_name])
    except KeyError:
        print(f"WARNING: Unknown population for country {country_name} ({country_code}), using average", file=sys.stderr)
        population = FALLBACK_POPULATION

    populations[country_name] = population

# Pre-process live metrics to account for ECDC combined countries
for missing, target in ecdc_combined_countries.items():
    if missing in live:
        live[target]["total_cases"] += live[missing]["total_cases"]
        live[target]["total_deaths"] += live[missing]["total_deaths"]
        del live[missing]

# Populate live relative metrics based on absolute
for entry in live.values():
    country = get_country_name(entry["code"])
    cases = entry["total_cases"]
    deaths = entry["total_deaths"]

    cases_rel = metrics["cases"]["relative"][country]
    cases_rel[-1] = cases - sum(cases_rel)
    deaths_rel = metrics["deaths"]["relative"][country]
    deaths_rel[-1] = deaths - sum(deaths_rel)

# Add a meta "Total" country
for metric in metrics.values():
    vals = metric["relative"]
    for i in range(n_days):
        vals["Total"][i] = sum(cases[i] for cases in vals.values())

# Populate US state metrics
for entry in states:
    country = f"{entry['state']}, US"
    cases = int(entry["cases"])
    deaths = int(entry["deaths"])
    offset = (entry["date"] - start_date).days

    cases_abs = metrics["cases"]["absolute"][country]
    cases_abs[offset] = cases
    deaths_abs = metrics["deaths"]["absolute"][country]
    deaths_abs[offset] = deaths

    if offset > 0:
        metrics["cases"]["relative"][country][offset] = cases - cases_abs[offset - 1]
        metrics["deaths"]["relative"][country][offset] = deaths - deaths_abs[offset - 1]

# Populate US county metrics
for entry in counties:
    country = f"{entry['county']} County, {entry['state']}, US"
    cases = int(entry["cases"])
    deaths = int(entry["deaths"])
    offset = (entry["date"] - start_date).days

    cases_abs = metrics["cases"]["absolute"][country]
    cases_abs[offset] = cases
    deaths_abs = metrics["deaths"]["absolute"][country]
    deaths_abs[offset] = deaths

    if offset > 0:
        metrics["cases"]["relative"][country][offset] = cases - cases_abs[offset - 1]
        metrics["deaths"]["relative"][country][offset] = deaths - deaths_abs[offset - 1]

# Calculate absolute values
for metric in metrics.values():
    for country, rel_vals in metric["relative"].items():
        for i in range(len(rel_vals)):
            metric["absolute"][country][i] = sum(rel_vals[:i+1])

# Calculate growth
for metric in metrics.values():
    for country, rel_vals in metric["relative"].items():
        for i, new_cases in enumerate(rel_vals):
            if i == 0:
                continue

            prev_cases = rel_vals[i - 1]
            grw = new_cases / prev_cases if prev_cases else 0
            # Round to 2 digits for friendly user presentation
            metric["growth"][country][i] = round(grw, 2)

# Remove first day from metrics (for growth factor)
start_date += timedelta(days=1)
n_days -= 1
for metric in metrics.values():
    for format_map in metric.values():
        for country, vals in format_map.items():
            format_map[country] = vals[1:]

# FIXME: strip missing last day for US states and counties
for metric in metrics.values():
    for countries in metric.values():
        for country, values in countries.items():
            if country.endswith(", US"):
                values = values[:-1]

data = {
    "dates": {
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "count": n_days
    },
    **metrics,
    "populations": populations
}

with open(sys.argv[5], "w+") as out:
    json.dump(data, out)
