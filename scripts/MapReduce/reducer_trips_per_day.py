#!/usr/bin/env python3
import sys

current_date = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    date, count = line.split('\t')
    try:
        count = int(count)
    except:
        continue

    if current_date == date:
        current_count += count
    else:
        if current_date:
            print(f"{current_date}\t{current_count}")
        current_date = date
        current_count = count

if current_date:
    print(f"{current_date}\t{current_count}")
