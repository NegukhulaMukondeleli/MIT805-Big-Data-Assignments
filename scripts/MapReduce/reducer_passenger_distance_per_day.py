#!/usr/bin/env python3
import sys

current_date = None
total_passengers = 0
total_distance = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    date, values = line.split('\t')
    passengers, distance = map(float, values.split(','))
    
    if current_date == date:
        total_passengers += passengers
        total_distance += distance
    else:
        if current_date:
            # Output previous date totals
            print(f"{current_date}\t{total_passengers}\t{total_distance}")
        current_date = date
        total_passengers = passengers
        total_distance = distance

# Print the last date totals
if current_date:
    print(f"{current_date}\t{total_passengers}\t{total_distance}")
