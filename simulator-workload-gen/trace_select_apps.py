#!/usr/bin/env python3

import os
import csv
import collections

tracedir = os.environ['AZURE_TRACE_DIR']
infile = 'function_durations_percentiles.anon.d01.csv'
outfile = 'chosen_apps.csv'

def select_apps(pred, outfile=outfile):
    apps = collections.OrderedDict()
    with open(tracedir+'/'+infile, newline='') as fin:
            reader = csv.reader(fin)
            reader.__next__()
            for row in reader:
                apps[row[1]] = None

    with open(outfile, 'w+') as fout:
        writer = csv.writer(fout)
        for app in apps.keys():
            if pred(app):
                writer.writerow((app,))

if __name__ == '__main__':
    select_apps(lambda app: int(app, 16) % 128==7)
