#!/usr/bin/env python

# Province/State,Country/Region,Lat,Long,1/22/20,1/23/20,...

import csv
import sys

with open(sys.argv[1], "r") as infile, open(sys.argv[2], "w+") as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    cols = []

    for ri, row in enumerate(reader):
        if ri == 0:
            cols = row
            writer.writerow([*cols[:4], *cols[6:]])
            continue

        #
        meta = row[:4]
        date_cases = [int(val) for val in row[4:]]

        for di, cases in enumerate(date_cases):
            if di < 2:
                continue

            prev_cases = date_cases[di - 1]
            new_cases = cases - prev_cases
            
        date_increases = []  # first removed
        for di, cases in enumerate(date_cases):
            if di == 0:
                continue
            
            date_increases.append(cases - date_cases[di - 1])

        date_growths = []  # first 2 removed
        for di, inc in enumerate(date_increases):
            if di == 0:
                continue

            prev_inc = date_increases[di - 1]
            if prev_inc:
                growth = inc / prev_inc
            else:
                growth = 0

            date_growths.append(growth)

        writer.writerow([*meta, *date_growths])
