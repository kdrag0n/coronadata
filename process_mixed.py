#!/usr/bin/env python

from datetime import datetime, timezone, timedelta
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
import argparse
import csv
import json
import sys
import os
import io
import traceback

import pycountry
import pytz
import requests

# Overrides -> proper alpha-3 code
alpha2_overrides = {
    "XK": "XKX",
    "DP": "_DP",
    "AI": "AIA",
    "JPG11668": "_DP"
}

alpha3_overrides = {
    "XKX": "XKX"
}

name_alpha3_overrides = {
    "Diamond Princess": "_DP",
    "MS Zaandam": "_MZ"
}

alpha3_name_overrides = {
    "_DP": "Diamond Princess",
    "_MZ": "MS Zaandam",
    "_TO": "Total",
    "_TS": "Total",
    "_TC": "Total",

    "COD": "Democratic Republic of the Congo",
    "VAT": "Holy See",
    "IRN": "Iran",
    "PSE": "Palestine",
    "RUS": "Russia",
    "PRK": "North Korea",
    "KOR": "South Korea",
    "XKX": "Kosovo",
    "VGB": "British Virgin Islands",
    "LAO": "Laos",
    "SXM": "Sint Maarten",
    "VIR": "United States Virgin Islands",
    "FLK": "Falkland Islands",
    "SYR": "Syria",
}

# From Worldometer
FALLBACK_POPULATION = 33_082_146  # world population / countries
population_overrides = {
    "AIA": 14969,
    "ERI": 3_533_929,
    "FLK": 3454,
    "BES": 26167,
    "BLM": 9870,
    "CZE": 10_704_466
}

ecdc_combined_countries = {
    # Missing country: merged into country
    "GUF": "FRA",
    "HKG": "CHN",
}


# No need for full-blown logging
def warn(msg):
    print(f"WARNING: {msg}", file=sys.stderr)


def fatal(msg):
    print(f"ERROR: {msg}", file=sys.stderr)
    exit(1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--novel-live", help="NovelCOVID live JSON")
    parser.add_argument("-t", "--tvt-live", help="TheVirusTracker live JSON")
    parser.add_argument("-e", "--ecdc", help="ECDC historical CSV")
    parser.add_argument("-s", "--nyt-states", help="New York Times historical US state CSV")
    parser.add_argument("-c", "--nyt-counties", help="New York Times historical US county CSV")
    parser.add_argument("-o", "--output", help="Path to output directory")
    return parser.parse_args()


def open_input(path, url, **kwargs):
    if path is not None:
        return open(path, "r", **kwargs)
    
    resp = requests.get(url)
    if not resp.ok:
        fatal(f"failed to fetch {url}: status {resp.status_code}")

    return io.StringIO(resp.text)


def read_csv(path, url, **kwargs):
    with open_input(path, url, **kwargs) as infile:
        return list(csv.DictReader(infile))


def write_out(args, name, obj):
    path_base = args.output
    try:
        os.makedirs(path_base)
    except FileExistsError:
        pass

    with open(f"{path_base}/{name}.json", "w+") as out:
        json.dump(obj, out, separators=(",", ":"))


@dataclass(eq=True, unsafe_hash=True)
class Location:
    code: str
    state: Optional[str] = None
    county: Optional[str] = None
    fips: Optional[str] = None

    is_country: bool = field(init=False)
    is_state: bool = field(init=False)
    is_county: bool = field(init=False)
    is_internal: bool = field(init=False)
    name: str = field(init=False)

    def __post_init__(self):
        if self.code == "_TS":
            self.state = "Total"
        elif self.code == "_TC":
            self.county = "Total"

        self.is_state = self.state and not self.county
        self.is_county = bool(self.county)
        self.is_country = not (self.is_state or self.is_county)
        self.is_internal = self.code.startswith("_")
        self.name = str(self)

    def to_geo_id(self):
        return f"0500000US{self.fips}"

    def __str__(self):
        if self.is_country:
            if self.code in alpha3_name_overrides:
                return alpha3_name_overrides[self.code]

            country = pycountry.countries.get(alpha_3=self.code)
            if country is not None:
                return getattr(country, "common_name", country.name)
        elif self.is_state:
            return self.state
        elif self.is_county:
            if self.is_internal:
                return self.county
            else:
                return f"{self.county} County, {self.state}"

        fatal(f"no name for {repr(self)}")

    @staticmethod
    def _normalize_code(code, geo_id=None, given_name=None):
        if code:
            if len(code) == 3:
                if code in alpha3_overrides:
                    return alpha3_overrides[code]
                else:
                    # Validate
                    country = pycountry.countries.get(alpha_3=code)
                    if country is not None:
                        return code

            if len(code) == 2:
                if code in alpha2_overrides:
                    return alpha2_overrides[code]
                else:
                    # Translate to alpha-3
                    country = pycountry.countries.get(alpha_2=code)
                    if country is not None:
                        return country.alpha_3

        if geo_id and geo_id in alpha2_overrides:
            return alpha2_overrides[geo_id]

        if given_name and given_name in name_alpha3_overrides:
            return name_alpha3_overrides[given_name]

        warn(f"dropping unknown country with code '{code}', geo ID '{geo_id}', given name '{given_name}'")
        return None

    @classmethod
    def from_code(cls, code, geo_id=None, given_name=None):
        normalized = cls._normalize_code(code, geo_id, given_name)
        if normalized:
            return cls(normalized)

        return None


args = parse_args()

# Load Worldometer live data
live = {}

try:
    print("Loading live data from NovelCOVID")
    with open_input(args.novel_live, "https://corona.lmao.ninja/countries") as lf:
        entries = json.load(lf)
        if not entries:
            raise ValueError("No data found")

        for entry in entries:
            loc = Location.from_code(entry["countryInfo"]["iso3"], given_name=entry["country"])
            if loc is None:
                continue

            entry["total_cases"] = entry["cases"]
            entry["total_deaths"] = entry["deaths"]
            live[loc] = entry
except Exception:
    traceback.print_exc()

    print("Loading live data from TheVirusTracker")
    with open_input(args.tvt_live, "https://api.thevirustracker.com/free-api?countryTotals=ALL") as lf:
        live_map = json.load(lf)["countryitems"][0]
        del live_map["stat"]
        for entry in live_map.values():
            loc = Location.from_code(entry["code"], given_name=entry["title"])
            if loc is None:
                continue

            entry["total_cases"] = int(entry["total_cases"])
            entry["total_deaths"] = int(entry["total_deaths"])
            live[loc] = entry

print("Loading historical data from ECDC")
ecdc = read_csv(args.ecdc, "https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/", encoding="iso-8859-1")

print("Loading historical data for US states from New York Times")
states = read_csv(args.nyt_states, "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv")

print("Loading historical data for US counties from New York Times")
counties = read_csv(args.nyt_counties, "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv")

print("Parsing ECDC dates")
for entry in ecdc:
    day = int(entry["day"])
    year = int(entry["year"])
    month = int(entry["month"])
    # 10 AM CET/CEST
    date = datetime(year, month, day, 10, 0, 0)
    tz_date = pytz.timezone("Europe/Brussels").localize(date)
    entry["date"] = tz_date

print("Parsing NYT dates")
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
n_days = (end_date - start_date).days + 1
print(f"Time period: {n_days} days (from {start_date} to {end_date})")

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
relations = list(metrics["cases"].keys())

populations = {}

# Populate historical relative metrics, and populations
print("Processing ECDC data")
for entry in ecdc:
    _code = entry["countryterritoryCode"] or entry["geoId"]
    _name = entry["countriesAndTerritories"]
    loc = Location.from_code(_code, entry["geoId"], _name)
    if loc is None:
        continue

    cases = int(entry["cases"])
    deaths = int(entry["deaths"])
    offset = (entry["date"] - start_date).days

    metrics["cases"]["relative"][loc][offset] += cases
    metrics["deaths"]["relative"][loc][offset] += deaths

    try:
        population = int(entry["popData2018"] or population_overrides[loc.code])
    except KeyError:
        warn(f"unknown population for {loc} ({_name}), using average")
        population = FALLBACK_POPULATION

    populations[loc] = population

# Preprocess live metrics to account for ECDC combined countries
print("Accounting for combined countries in live metrics")
for missing, target in ecdc_combined_countries.items():
    if missing in live:
        live[target]["total_cases"] += live[missing]["total_cases"]
        live[target]["total_deaths"] += live[missing]["total_deaths"]
        del live[missing]

# Populate live relative metrics based on absolute
print("Calculating relative values")
for loc, entry in live.items():
    cases = entry["total_cases"]
    deaths = entry["total_deaths"]

    cases_rel = metrics["cases"]["relative"][loc]
    cases_rel[-1] = cases - sum(cases_rel)
    deaths_rel = metrics["deaths"]["relative"][loc]
    deaths_rel[-1] = deaths - sum(deaths_rel)

# Populate US state metrics
print("Processing US states")
for entry in states:
    loc = Location("USA", entry["state"])
    cases = int(entry["cases"])
    deaths = int(entry["deaths"])
    offset = (entry["date"] - start_date).days

    cases_abs = metrics["cases"]["absolute"][loc]
    cases_abs[offset] = cases
    deaths_abs = metrics["deaths"]["absolute"][loc]
    deaths_abs[offset] = deaths

    if offset > 0:
        metrics["cases"]["relative"][loc][offset] = cases - cases_abs[offset - 1]
        metrics["deaths"]["relative"][loc][offset] = deaths - deaths_abs[offset - 1]

# Populate US county metrics
print("Processing US counties")
for entry in counties:
    loc = Location("USA", entry["state"], entry["county"], entry["fips"])
    cases = int(entry["cases"])
    deaths = int(entry["deaths"])
    offset = (entry["date"] - start_date).days

    cases_abs = metrics["cases"]["absolute"][loc]
    cases_abs[offset] = cases
    deaths_abs = metrics["deaths"]["absolute"][loc]
    deaths_abs[offset] = deaths

    if offset > 0:
        metrics["cases"]["relative"][loc][offset] = cases - cases_abs[offset - 1]
        metrics["deaths"]["relative"][loc][offset] = deaths - deaths_abs[offset - 1]

# Add totals as is_internal locations
print("Calculating totals")
_TO = Location("_TO")
_TS = Location("_TS")
_TC = Location("_TC")
for metric in metrics.values():
    vals = metric["relative"]
    for i in range(n_days):
        vals[_TO][i] = sum(cases[i] for loc, cases in vals.items() if loc.is_country)
        vals[_TS][i] = sum(cases[i] for loc, cases in vals.items() if loc.is_state)
        vals[_TC][i] = sum(cases[i] for loc, cases in vals.items() if loc.is_county)

# Calculate absolute values
print("Calculating absolute values")
for metric in metrics.values():
    for loc, rel_vals in metric["relative"].items():
        for i in range(len(rel_vals)):
            metric["absolute"][loc][i] = sum(rel_vals[:i+1])

# Calculate growth
print("Calculating growth factors")
for metric in metrics.values():
    for loc, rel_vals in metric["relative"].items():
        for i, new_cases in enumerate(rel_vals):
            if i == 0:
                continue

            prev_cases = rel_vals[i - 1]
            grw = new_cases / prev_cases if prev_cases else 0
            # Round to 2 digits for friendly user presentation
            metric["growth"][loc][i] = round(grw, 2)

# Remove first day from metrics (for growth factor)
print("Removing first day")
start_date += timedelta(days=1)
n_days -= 1
for metric in metrics.values():
    for relation_map in metric.values():
        for loc, vals in relation_map.items():
            relation_map[loc] = vals[1:]

# FIXME: strip missing last day for US states and counties
print("Removing last US day")
for metric in metrics.values():
    for locations in metric.values():
        for loc, values in locations.items():
            if loc.is_state or loc.is_county:
                locations[loc] = values[:-1]

def filter_metrics(metric=None, relation=None, location=None, fmt_vals=None, fmt_loc=lambda l: l.name, finalize=None):
    filtered = {}
    for metric_name, metric_vals in metrics.items():
        if metric and not metric(metric_name):
            continue

        filtered[metric_name] = {}

        for relation_name, countries in metric_vals.items():
            if relation and not relation(relation_name):
                continue

            filtered[metric_name][relation_name] = {}

            for loc, values in countries.items():
                if location and not location(loc):
                    continue

                if fmt_vals:
                    values = fmt_vals(values)

                filtered[metric_name][relation_name][fmt_loc(loc)] = values

    if finalize:
        return finalize(filtered)
    else:
        return filtered

# Location format functions are only used for maps
loc_types = {
    "country": (lambda l: l.is_country, lambda l: l.code),
    "state": (lambda l: l.is_state, lambda l: l.name),
    "county": (lambda l: l.is_county, lambda l: l.to_geo_id())
}

print("Exporting chart data")
chart_base = {
    "dates": {
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "count": n_days
    }
}

def chart_finalize(filtered):
    filtered.update(chart_base)
    return filtered

for loc, (predicate, _) in loc_types.items():
    write_out(args, f"chart_{loc}", filter_metrics(location=predicate, fmt_loc=lambda l: l.name, finalize=chart_finalize))

print("Exporting map data")
def map_finalize(metric, relation, ltype):
    def _finalize(filtered):
        locations = filtered[metric][relation]
        locs = list(locations.keys())
        vals = list(locations.values())

        return {
            "locations": locs,
            "z": vals,
            "zmin": min(vals),
            "zmax": max(vals)
        }

    return _finalize

for relation in relations:
    for metric in metrics.keys():
        for loc, (predicate, fmt_loc) in loc_types.items():
            write_out(args, f"map_{relation}_{metric}_{loc}", filter_metrics(
                metric=lambda m: m == metric,
                relation=lambda r: r == relation,
                location=lambda l: predicate(l) and not l.is_internal,
                fmt_vals=lambda v: v[-1],
                fmt_loc=fmt_loc,
                finalize=map_finalize(metric, relation, loc)
            ))

#for c, v in map_fmt.items():
#    pass#if v < 0: print(c, v)
