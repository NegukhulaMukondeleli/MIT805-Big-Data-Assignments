#!/usr/bin/env python3
import sys

current_type = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    ptype, count = line.split('\t')
    try:
        count = int(count)
    except:
        continue

    if current_type == ptype:
        current_count += count
    else:
        if current_type:
            print(f"{current_type}\t{current_count}")
        current_type = ptype
        current_count = count

if current_type:
    print(f"{current_type}\t{current_count}")
