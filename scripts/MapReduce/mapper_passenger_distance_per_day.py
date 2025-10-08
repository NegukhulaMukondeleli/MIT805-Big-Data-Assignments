#!/usr/bin/env python3
import sys
import csv

# Read CSV from stdin
reader = csv.DictReader(sys.stdin)
for row in reader:
    try:
        date = row['tpep_pickup_datetime'].split(' ')[0]  # extract date only
        passengers = float(row['passenger_count']) if row['passenger_count'] else 0
        distance = float(row['trip_distance']) if row['trip_distance'] else 0
        # Emit date as key and passengers,distance as values
        print(f"{date}\t{passengers},{distance}")
    except Exception:
        continue
