import sys
from collections import defaultdict

counts = defaultdict(int)

for line in sys.stdin:
    (country, percent_change) = line.strip().split(",")
    counts[(country, percent_change)] += 1

for key in sorted(counts):
    print("{} @ {}% = {}".format(key[0], key[1], counts[key]))
