#!/usr/bin/env python3

import sys, os
from functools import reduce
import json
import hashlib

import common
from cluster import Cluster, Host, Function, Invocation, logs, metrics
import workload
import plot

def main(seed, workloads, hosts, cluster, **kwargs):
    common.init_gen(seed)

    wklds = {}
    for wl_type in workloads:
        wl_gen = getattr(workload, wl_type['type'])
        wl, _ = wl_gen(**wl_type['parameters'])
        workload.extend_workload(wklds, wl)

    host_list = []
    i = 0
    for host_type in hosts:
        for _ in range(host_type['amount']):
            host_list.append(Host(i, host_type['configs']))
            i += 1
    clstr = Cluster(host_list, cluster['configs'])

    # ticks
    while len(wklds) > 0 or not clstr.is_idle():
        for i in wklds.get(clstr.epoch, []):
            clstr.request(i)
        wklds.pop(clstr.epoch, None)
        clstr.tick()
        print('epoch:', clstr.epoch, file=sys.stderr)

    return clstr.epoch, logs, metrics

if __name__ == '__main__':
    params = json.load(sys.stdin)
    if len(sys.argv) == 2:
        run_id = sys.argv[1]
    else:
        run_id = hashlib.md5(json.dumps(params).encode('utf-8')).hexdigest()

    try:
        os.mkdir('runs')
    except FileExistsError:
        pass
    with open('runs/' + run_id + '.json', 'w') as f:
        json.dump(params, f)

    outfile = 'runs/' + run_id + '.pdf'
    plot.plot(*main(**params), outfile)
    print('Finished. Plotted:', outfile, file=sys.stderr)
