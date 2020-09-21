#!/usr/bin/env python3

import sys, os
import time
import datetime as DT
from functools import reduce
import json
import threading
import queue
import hashlib
import requests
from requests_futures.sessions import FuturesSession
import urllib3
import subprocess
import numpy as np
from collections import defaultdict
import operator

import common
from cluster import Cluster, Host, Function, Invocation, logs, metrics
import workload
import plot

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100)
session.mount('http://', adapter)
session.verify = False

APIHOST = None

future_session = None
future_queue = queue.Queue()

assigned_count = defaultdict(lambda: 0)
n_requested = 0
n_responded = 0
n_error = 0

def get_func(invocation):
    return invocation.function.function_name

def create_function(function):
    if not function.function_name:
        demand = function.demand
        function.function_name = 'loop_new_%d_%d' % (demand * 128, function.function_id)
        assigned_count[demand] += 1
    res = subprocess.run(['wsk', '-i', 'action', 'update', function.function_name, '--kind', 'python:3', '-m', str(int(function.demand*128)), '-t', '300000', 'functions/sleep.py'])
    res.check_returncode()

def print_responses():
    while True:
        global n_responded, n_error
        future = future_queue.get()
        n_responded += 1
        if not future.result().ok:
            n_error += 1
            # print('response:', future.result().json(), file=sys.stderr)
        future_queue.task_done()

def async_request(invocation):
    # start = time.time()
    global n_requested
    n_requested += 1
    fn = invocation.function.function_name
    # print('requesting:', fn, 'at', time.time(), 'for', invocation.duration/1000, 'seconds', file=sys.stderr)
    url = 'https://' + APIHOST + '/api/v1/namespaces/_/actions/' + fn
    headers = {'Content-Type':'application/json'}
    jsondata = {"duration":invocation.duration/1000, "long":False}
    future = future_session.request('post', url, params={'blocking':'false'}, headers=headers, json=jsondata)
    future_queue.put(future)
    # if n_requested % 1000 == 0:
    #     print('total requested :', n_requested)
    # print('elapse:', time.time()-start)
    return

def request(invocation):
    global n_requested
    n_requested += 1
    fn = get_func(invocation)
    # print('.', file=sys.stderr)
    # print('requesting:', fn, 'at', time.time(), 'for', invocation.duration/1000, 'seconds', file=sys.stderr)
    url = 'https://' + APIHOST + '/api/v1/namespaces/_/actions/' + fn
    headers = {'Content-Type':'application/json'}
    jsondata = {"duration":invocation.duration/1000, "long":False}
    resp = session.request('post', url, params={'blocking':'false'}, headers=headers, json=jsondata)
    # if resp.status_code > 299:
    #     print('response:', resp.json(), file=sys.stderr)
    print('response:', resp.json(), file=sys.stderr)
    if n_requested % 1000 == 0:
        print('total requested :', n_requested)
    return

def totalwork(invocs):
    return sum([i.duration * i.function.demand for i in invocs])

def stats(workloads):
    works = []
    durations = []
    memorys = []
    all_invocs = [i for w in workloads for i in w]
    perfunctionworks = {}
    for i in all_invocs:
        durations.append(i.duration)
        memorys.append(i.function.demand)
        works.append(i.duration * i.function.demand)
        perfunctionworks[i.function.function_name] = perfunctionworks.get(i.function.function_name, 0) + i.duration * i.function.demand
    top10 = sorted(perfunctionworks.items(), key=lambda x: x[1], reverse=True)[:10]
    print('duration: average', np.average(durations), np.percentile(durations,1), np.percentile(durations,25), np.percentile(durations,50), np.percentile(durations,75), np.percentile(durations,99))
    print('memorys:', np.percentile(memorys,1), np.percentile(memorys,25), np.percentile(memorys,50), np.percentile(memorys,75), np.percentile(memorys,99))
    print('highest invocations:', max([len(sec) for sec in workloads]))
    print('total invocations:', len(all_invocs), ', workload:', sum(works)/1000, 'sec x core, average ', sum(works)/1000/len(workloads), 'cores')
    print('top 10 functions:',[(k[:8], v/sum(works)) for k,v in top10])


def main(seed, workloads, *args, **kwargs):
    common.init_gen(seed)

    wklds = {}
    all_functions = set() # order is arbitrary
    for wl_type in workloads:
        wl_gen = getattr(workload, wl_type['type'])
        wl, fns = wl_gen(**wl_type['parameters'])
        workload.extend_workload(wklds, wl)
        all_functions.update(fns)

    list_of_workloads = list(wklds.values())
    lengths = [len(s) for s in list_of_workloads]
    stats(wklds.values())
    print()

    for f in all_functions:
       create_function(f)

    threading.Thread(target=print_responses, daemon=True).start()

    start = time.time() + 1
    print('starting at', start, '...', file=sys.stderr)
    
    def sleep_till(ts):
        cur = time.time()
        if ts <= cur + 0.05:
            return
        time.sleep(ts - cur - 0.05)

    def request_with_offset(start, invocs):
        n = len(invocs)
        nbatches = 10
        barriers = np.linspace(0,1,nbatches+1)
        batches = [int(v*n) for v in barriers]
        for b in range(nbatches):
            sleep_till(start+barriers[b])
            for i in invocs[batches[b]:batches[b+1]]:
                async_request(i)

    # start requests
    epoch = 0
    while len(wklds) > 0:
        sleep_till(start + epoch)
        offset = time.time()-start-epoch
        invocs = wklds.get(epoch, [])
        request_with_offset(start+epoch, invocs)
        wklds.pop(epoch, None)
        epoch += 1
        print('epoch:', epoch, 'offset:', offset, 'workload:', totalwork(invocs), 'requested:', n_requested, 'responded:', n_responded, 'error:', n_error, file=sys.stderr)

    future_queue.join()

    print('requested', n_requested, 'invocations', file=sys.stderr)
    since=DT.datetime.utcfromtimestamp(start).isoformat()[:23]
    print('SINCE='+since, file=sys.stderr)
    print('FINISH=%sZ' % DT.datetime.utcfromtimestamp(time.time()).isoformat(), file=sys.stderr)
    print('totalerror=%d' % n_error)
    return

if __name__ == '__main__':
    session.auth = tuple(os.environ['AUTH'].split(':'))
    APIHOST = os.environ['APIHOST']
    future_session = FuturesSession(session=session, max_workers=8)

    params = json.load(sys.stdin)
    if len(sys.argv) == 2:
        run_id = sys.argv[1]
    else:
        run_id = hashlib.md5(json.dumps(params).encode('utf-8')).hexdigest()

    try:
        os.mkdir('real_runs')
    except FileExistsError:
        pass
    with open('real_runs/' + run_id + '.json', 'w') as f:
        json.dump(params, f)

    main(**params)
    print('run_id=%s' % run_id, file=sys.stderr)
