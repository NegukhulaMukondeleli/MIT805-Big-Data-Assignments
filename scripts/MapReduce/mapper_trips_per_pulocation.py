#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("lpep_pickup_datetime"):
        continue

    parts = line.split(',')
    try:
        pulocation = parts[21].strip()  # PULocationID
        print(f"{pulocation}\t1")
    except:
        continue
