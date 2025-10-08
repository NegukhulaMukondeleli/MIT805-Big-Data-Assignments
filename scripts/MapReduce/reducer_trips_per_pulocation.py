#!/usr/bin/env python3
import sys

current_loc = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    loc, count = line.split('\t')
    try:
        count = int(count)
    except:
        continue

    if current_loc == loc:
        current_count += count
    else:
        if current_loc:
            print(f"{current_loc}\t{current_count}")
        current_loc = loc
        current_count = count

if current_loc:
    print(f"{current_loc}\t{current_count}")
