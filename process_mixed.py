#!/usr/bin/env python

# Province/State,Country/Region,Lat,Long,1/22/20,1/23/20,...

from datetime import datetime, timezone, timedelta
from collections import defaultdict
import json
import sys

import pycountry

country_overrides = {
    "DP": "Diamond Princess",
    "CD": "Democratic Republic of the Congo",
    "VA": "Holy See",
    "IR": "Iran",
    "PS": "Palestine",
    "RU": "Russia",
    "KP": "North Korea",
    "KR": "South Korea",
    "XK": "Kosovo"
}

ecdc_countries = {
    "French Guiana": "",
    "Hong Kong": "",
    "Puerto Rico": "",
    "Congo": "",
    "Botswana": "",
    "Burundi": "",
    "Falkland Islands (Malvinas)": "",
    "French Southern Territories": ""
}
country_cache = {}

def get_country_name(country_id):
    if country_id in country_cache:
        return country_cache[country_id]

    if country_id in country_overrides:
        country_name = country_overrides[country_id]
    else:
        iso_country = pycountry.countries.get(alpha_2=country_id)
        if iso_country is None:
            print(country_id)
            exit()
        else:
            country_name = getattr(iso_country, "common_name", iso_country.name)

    country_cache[country_id] = country_name
    return country_name

with open(sys.argv[1], "r") as tf:
    timeline = json.load(tf)["data"]

with open(sys.argv[2], "r") as lf:
    live = json.load(lf)["countryitems"][0].values()

with open(sys.argv[3], "r") as ef:
    ecdc = json.load(ef)

# Parse dates
for entry in timeline:
    month, day, year = map(int, entry["date"].split("/"))
    year += 2000
    date = datetime(year, month, day, 23, 59, 59, 0, timezone.utc)
    entry["date"] = date

# Get start and end dates
start_date = min(timeline, key=lambda e: e["date"])["date"]
end_date = datetime.now(timezone.utc)
ecdc_date_offset = (start_date - datetime.fromisoformat(ecdc["dates"]["start"])).days

# Calculate elapsed days
n_days = (end_date - start_date).days + 2
print(start_date, end_date, n_days)

# Create maps
def date_list():
    return [0] * n_days

data = {
    "cases": {
        "absolute": defaultdict(date_list),
        "relative": defaultdict(date_list),
        "growth": defaultdict(date_list),
    },
    "deaths": {
        "absolute": defaultdict(date_list),
        "relative": defaultdict(date_list),
        "growth": defaultdict(date_list),
    },
    "recovered": {
        "absolute": defaultdict(date_list),
        "relative": defaultdict(date_list),
        "growth": defaultdict(date_list),
    },
}

# Populate historical absolute data
for entry in timeline:
    country = get_country_name(entry["countrycode"])
    cases = int(entry["cases"])
    deaths = int(entry["deaths"])
    recovered = int(entry["recovered"])

    offset = (entry["date"] - start_date).days
    print(entry["date"], offset)
    data["cases"]["absolute"][country][offset] = cases
    data["deaths"]["absolute"][country][offset] = deaths
    data["recovered"]["absolute"][country][offset] = recovered

# Populate live absolute data
for entry in live:
    # Ignore final {"stat": "ok"} value
    if isinstance(entry, str):
        continue

    country = get_country_name(entry["code"])
    cases = int(entry["total_cases"])
    deaths = int(entry["total_deaths"])
    recovered = int(entry["total_recovered"])

    data["cases"]["absolute"][country][-1] = cases
    data["deaths"]["absolute"][country][-1] = deaths
    data["recovered"]["absolute"][country][-1] = recovered

# Don't track recovered:
#     1. Unreliable
#     2. ECDC doesn't have it, so we can't backfill
del data["recovered"]

# Backfill historical data from ECDC
for metric_name, metric in data.items():
    for country, values in metric["absolute"].items():
        for i, value in enumerate(values):
            # Only backfill values older than the last two
            if i == len(values) - 1:
                break

            # Fetch value from ECDC dataset if possible
            if country in ecdc[metric_name]["absolute"]:
                ecdc_vals = ecdc[metric_name]["absolute"][country]
                backfill_idx = i + ecdc_date_offset
                if backfill_idx < len(ecdc_vals):
                    values[i] = ecdc_vals[backfill_idx]

# Add a meta "Total" country
for metric in data.values():
    vals = metric["absolute"]
    for i in range(n_days):
        vals["Total"][i] = sum(cases[i] for cases in vals.values())

# Calculate relative values
for metric in data.values():
    for country, abs_vals in metric["absolute"].items():
        for i, val in enumerate(abs_vals):
            if i == 0:
                continue

            metric["relative"][country][i] = val - abs_vals[i - 1]

# Calculate growth
for metric in data.values():
    for country, rel_vals in metric["relative"].items():
        for i, new_cases in enumerate(rel_vals):
            if i < 2:
                continue

            prev_cases = rel_vals[i - 1]
            grw = new_cases / prev_cases if prev_cases else 0
            # Round to 2 digits for friendly user presentation
            metric["growth"][country][i] = round(grw, 2)

# Remove first 2 days from data
# 1 for relative, 1 for growth
start_date += timedelta(days=2)
n_days -= 2
for metric in data.values():
    for format_map in metric.values():
        for country, vals in format_map.items():
            format_map[country] = vals[2:]

data["dates"] = {
    "start": start_date.isoformat(),
    "end": end_date.isoformat(),
    "count": n_days
}

with open(sys.argv[4], "w+") as out:
    json.dump(data, out)
