#!/usr/bin/env python

# Province/State,Country/Region,Lat,Long,1/22/20,1/23/20,...

import json
import sys

import pycountry
import xlrd

cases_abs = {}
cases_rel = {}
cases_grw = {}

deaths_abs = {}
deaths_rel = {}
deaths_grw = {}

country_overrides = {
    "JPG11668": "Japan (cruise ship)",
    "CG": "Republic of the Congo",
    "CD": "Democratic Republic of the Congo",
    "VA": "Holy See",
    "IR": "Iran",
    "PS": "Palestine",
    "RU": "Russia",
    "KR": "South Korea",
}

def row_to_iso_date(row):
    return f"{row[3].value:02.0f}-{row[2].value:02.0f}-{row[1].value:02.0f}"

with xlrd.open_workbook(sys.argv[1]) as wb:
    sheet = wb.sheet_by_name("COVID-19-geographic-disbtributi")
    rows = list(sheet.get_rows())
    cols = rows.pop(0)

    start_row = min(rows, key=lambda row: row[0].value)
    start_xldate = int(start_row[0].value)
    start_isodate = row_to_iso_date(start_row)

    end_row = max(rows, key=lambda row: row[0].value)
    end_xldate = int(end_row[0].value)
    end_isodate = row_to_iso_date(end_row)

    n_days = end_xldate - start_xldate + 1

    for xldate, day, month, year, cases, deaths, country, country_id in reversed(rows):
        # Excel cells -> Python types
        xldate = int(xldate.value)
        day = int(day.value)
        month = int(month.value)
        year = int(year.value)
        cases = int(cases.value)
        deaths = int(deaths.value)
        country = country.value
        country_id = country_id.value

        if country_id in country_overrides:
            country = country_overrides[country_id]
        else:
            iso_country = pycountry.countries.get(alpha_2=country_id)
            if iso_country is None:
                country = country.replace("_", " ")
            else:
                country = getattr(iso_country, "common_name", iso_country.name)

        ## 1. relative
        if country not in cases_rel:
            cases_rel[country] = [0] * n_days
            deaths_rel[country] = [0] * n_days

        date_offset = xldate - start_xldate
        cases_rel[country][date_offset] += cases
        deaths_rel[country][date_offset] += deaths

# Undo the reversal
cases_rel = dict(reversed(cases_rel.items()))
deaths_rel = dict(reversed(deaths_rel.items()))

# Add a meta "Total" country
total_cases_rel = []
for i in range(n_days):
    total_cases_rel.append(sum(cases[i] for cases in cases_rel.values()))
cases_rel["Total"] = total_cases_rel

total_deaths_rel = []
for i in range(n_days):
    total_deaths_rel.append(sum(deaths[i] for deaths in deaths_rel.values()))
deaths_rel["Total"] = total_deaths_rel

## 2. abs
for country, cases in cases_rel.items():
    cases_abs[country] = []
    for i in range(len(cases)):
        cases_abs[country].append(sum(cases[:i+1]))

for country, deaths in deaths_rel.items():
    deaths_abs[country] = []
    for i in range(len(deaths)):
        deaths_abs[country].append(sum(deaths[:i+1]))

## 3. grw
for country, cases in cases_rel.items():
    cases_grw[country] = [0]
    for i, new_cases in enumerate(cases):
        if i == 0:
            continue

        prev_cases = cases[i - 1]
        grw = new_cases / prev_cases if prev_cases else 0
        cases_grw[country].append(grw)

for country, deaths in deaths_rel.items():
    deaths_grw[country] = [0]
    for i, new_deaths in enumerate(deaths):
        if i == 0:
            continue

        prev_deaths = deaths[i - 1]
        grw = new_deaths / prev_deaths if prev_deaths else 0
        deaths_grw[country].append(grw)

data = {
    "dates": {
        "start": start_isodate,
        "end": end_isodate,
    },
    "cases": {
        "absolute": cases_abs,
        "relative": cases_rel,
        "growth": cases_grw,
    },
    "deaths": {
        "absolute": deaths_abs,
        "relative": deaths_rel,
        "growth": deaths_grw,
    },
}

with open(sys.argv[2], "w+") as out:
    json.dump(data, out)
