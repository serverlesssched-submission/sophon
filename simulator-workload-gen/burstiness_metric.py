#!/usr/bin/env python3

import os, sys
import csv
import collections

tracedir = os.environ['AZURE_TRACE_DIR']
infile = 'invocations_per_function_md.anon.d01.csv'
outfile = 'burstiness.d01.csv'

def burstiness(invocations):
    bursts = {}
    total = 0
    for i in range(len(invocations)-1):
        diff = int(invocations[i+1]) - int(invocations[i])
        if diff > 0:
            total += diff
            bursts[i+1] = diff
    return total, {k: v for k, v in sorted(bursts.items(), key=lambda item: item[1], reverse=True)[:10]}

apps = collections.OrderedDict()
with open(tracedir+'/'+infile, newline='') as fin:
    with open(outfile, 'w+') as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        reader.__next__()
        counter = 0
        for row in reader:
            total, top = burstiness(row[4:])
            if total > 10000 and next(iter(top.values())) > 1000:
                writer.writerow((row[2], total, top))
            counter += 1
            print(counter)
