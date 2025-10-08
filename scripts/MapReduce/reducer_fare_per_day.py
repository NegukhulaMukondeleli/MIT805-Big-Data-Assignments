#!/usr/bin/env python3
import sys

current_date = None
current_total = 0.0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    date, fare = line.split('\t')
    try:
        fare = float(fare)
    except:
        continue

    if current_date == date:
        current_total += fare
    else:
        if current_date:
            print(f"{current_date}\t{current_total}")
        current_date = date
        current_total = fare

if current_date:
    print(f"{current_date}\t{current_total}")
