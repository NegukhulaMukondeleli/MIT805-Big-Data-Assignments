#!/usr/bin/env python3
import sys
import datetime

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("lpep_pickup_datetime"):
        continue

    parts = line.split(',')
    try:
        pickup_str = parts[1].strip()  # lpep_pickup_datetime
        fare = float(parts[12].strip())  # total_amount
        pickup_date = datetime.datetime.strptime(pickup_str, "%Y-%m-%d %H:%M:%S").date()
        print(f"{pickup_date}\t{fare}")
    except:
        continue
