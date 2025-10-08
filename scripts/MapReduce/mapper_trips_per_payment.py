#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("lpep_pickup_datetime"):
        continue

    parts = line.split(',')
    try:
        payment_type = parts[19].strip()  # Payment_type
        print(f"{payment_type}\t1")
    except:
        continue
