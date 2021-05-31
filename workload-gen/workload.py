#!/usr/bin/env python3

import sys, os
import math
import csv
import numpy as np

import common
from cluster import Function, Invocation
from collections import defaultdict
import trace_select_apps

tracedir = os.environ['AZURE_TRACE_DIR']
fn_counter = 0

def new_fnid():
    global fn_counter
    fn_counter += 1
    return fn_counter - 1

def itemized(n_functions, ids, names, mem_demand, durations, n_invocations_at_ts, **kwargs):
    invocs = {}
    assert n_functions == len(ids) == len(names) == len(mem_demand) == len(durations) == len(n_invocations_at_ts)

    fns = []
    for i in range(n_functions):
        name = names[i]
        fns.append(Function(ids[i], mem_demand[i], name))
        for ts in range(len(n_invocations_at_ts[i])):
            if n_invocations_at_ts[i][ts] != 0:
                existing_list = invocs.get(ts, [])
                existing_list.extend([Invocation(fns[i], durations[i]) for _ in range(n_invocations_at_ts[i][ts])])
                invocs[ts] = existing_list

    return invocs, fns

def linear_dist(id, name, span, a, b, mem_demand, duration, **kwargs):
    invocs = {}
    fn = Function(id, mem_demand, name)

    for t in range(span):
        invocs[t] = []
        invocs[t].extend([Invocation(fn, duration) for _ in range(round(a*t+b))])

    return invocs, [fn]

def uniform_dist(span, nfunc, b, mem_demand, duration, start_window, start_load, **kwargs):
    invocs = {}
    fns = []
    for i in range(nfunc):
        new_invocs, new_fn = linear_dist(i+1, "linear_%d" % (i+1), span, 0, b, mem_demand, duration)
        assert(len(new_fn) == 1)
        extend_workload(invocs, new_invocs)
        fns.append(new_fn[0])
    slowed = slowstart(invocs, span/60, start_window, start_load)
    return slowed, fns

def burst(func, start, duration, parallelism):
    invocs = {}
    invocs[start] = [Invocation(func, duration) for _ in range(parallelism)]
    return invocs

def burst_parallel(id, name, parallelism, start, end, n_bursts, mem_demand, duration):
    fn = Function(id, mem_demand, name)
    invocs = {}
    for i in range(n_bursts):
        extend_workload(invocs, burst(fn, int(common.gen.randint(start, end)), duration, parallelism))
    return invocs, [fn]

def faas(ntasks):
    pass

"""
See Azure's "Serverless in the Wild" paper.
Knobs:
    - timespan in seconds
    - number of functions (possibly not all functions have invocations)
    - number of invocations (possibly more or less than this number)
"""
def azure(span, n_functions, n_invocations, mem_hist, mem_bins, dist_mu, dist_sigma, CV, dur_mu, dur_sigma, start_window, start_load, BP_percentage, **kwargs):
    # memory demand distribution, stats from https://www.datadoghq.com/state-of-serverless/. the paper has memory usage stats, consistent w/ demand from datadog.
    mems = common.random_from_histogram(mem_hist, mem_bins, n_functions)

    # create functions
    fns = [Function(new_fnid(), mems[i]) for i in range(n_functions)]

    # allocate invocations to functions
    function_dist = [0] * n_functions # fn -> invocations of that fn
    created = 0
    if 'dist_file' in kwargs:
        dist_file = tracedir + '/' + kwargs['dist_file']
        with open(dist_file, 'r') as f:
            hist = [float(l.strip()) for l in f]
        indices = common.choose_from_histogram(hist[:n_functions], n_invocations)
        for idx in indices:
            function_dist[idx] += 1
    else:
        while created < n_invocations:
            fnid = int(common.gen.lognormvariate(dist_mu, dist_sigma) * n_functions)
            if fnid < n_functions:
                function_dist[fnid] += 1
                created += 1

    # allocate CVs of IAT for each function
    CVs = []
    for _ in range(n_functions):
        CVs.append(1) # TODO: use CV distribution, using random.choice(), make sure in [0, 10]

    # duration distribution
    durations = []
    while len(durations) < n_invocations:
        dur = common.gen.lognormvariate(dur_mu, dur_sigma)
        if dur > 60.0: # ignore timeouts
            continue
        durations.append(dur * 1000)
        # if dur < 1.0:
        #     durations.append(1)
        # else:
        #     durations.append(round(dur))

    # distribution function
    def dist_func(x):
        # if x >= 0.2 and x < 0.3:
        #     return 7.5 * x - 1.25
        # else:
        #     return 1
        if x >= start_window:
            return 1
        else:
            return start_load + x * (1 - start_load) / start_window

    # create {Tn} using function_dist and CVs, use lognormal distribution for inter-arrival time
    invocs = {} # invocs w/ timestamps
    created = 0
    for i in range(n_functions):
        new_invocs = {}
        if function_dist[i] == 0: break
        if function_dist[i] >= 300 and function_dist[i] < 3000 and common.gen.random() < BP_percentage: # assume some of these are burst-parallel apps
            # calculate A2, A3
            step = span // 3
            startpoint = common.gen.randint(0, step)
            batch1 = []
            batch2 = []
            batch3 = []
            for _ in range(int(function_dist[i]/3)):
                dur = 2 # TODO: use distribution
                batch1.append(Invocation(fns[i], dur))
                batch2.append(Invocation(fns[i], dur))
                batch3.append(Invocation(fns[i], dur))
            existing_list = invocs.get(startpoint, list())
            existing_list += batch1
            invocs[startpoint] = existing_list
            existing_list = invocs.get(startpoint+step, list())
            existing_list += batch2
            invocs[startpoint+step] = existing_list
            existing_list = invocs.get(startpoint+step*2, list())
            existing_list += batch3
            invocs[startpoint+step*2] = existing_list
            print('bursty app of', function_dist[i], 'invocations')
            continue
        # normal apps
        sigma = math.sqrt(math.log(CVs[i] ** 2 + 1))
        mean = span / function_dist[i] # inter-arrival time expectation
        mu = math.log(mean) - sigma ** 2 / 2
        current_ts = 0.0 # start point

        # create IAT sequence {An}, then calculate {Tn}
        while True:
            interval = common.gen.lognormvariate(mu, sigma)
            interval = interval / dist_func(current_ts / span) # reshape by dist function
            current_ts += interval
            ts = round(current_ts)
            if ts >= span: break # will have more or less invocations
            # duration
            dur = durations[created]
            invoc = Invocation(fns[i], dur)
            existing_list = new_invocs.get(ts, list())
            existing_list.append(invoc)
            new_invocs[ts] = existing_list
            created += 1

        extend_workload(invocs, new_invocs)

    return invocs, fns

def azure_trace(app_predicate, invocations, durations, memory, start_minute, length, start_window, start_load, downsample_factor):
    apps = {} # appname -> [funcnames]
    fns = {} # funcname -> Function
    duration_weights = {}
    memory_demands = {}
    invocs = {}

    if app_predicate != '':
        chosen_apps = 'chosen_apps.csv'
        trace_select_apps.select_apps(eval(app_predicate), chosen_apps)

    with open(chosen_apps, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            apps[row[0]] = []
    
    with open(tracedir+'/'+durations, newline='') as f:
        reader = csv.reader(f)
        reader.__next__()
        for row in reader:
            appname = row[1]
            if appname in apps:
                funcname = row[2]
                apps[appname].append(funcname)
                duration_weights[funcname] = [int(p) for p in row[7:]]
    
    with open(tracedir+'/'+memory, newline='') as f:
        reader = csv.reader(f)
        reader.__next__()
        for row in reader:
            appname = row[1]
            if appname in apps:
                for funcname in apps[appname]:
                    funcmemory = float(row[-1]) * 0.6
                    # mem_divides = [164.0, 187.0, 196.0, 203.0, 210.0, 218.0, 227.0, 244.0, 271.0, 301.0, 327.9409919024838, 352.0, 405.5391876390531, 515.613886704472, 1227.0]
                    mem_cdf = [0.5780382403995048, 0.7416713777498449, 0.8198813085929253, 0.8662031637362969, 0.8970593241345316, 0.9191992717389691, 0.9359212081993655, 0.9490341650164773, 0.9596161522536051, 0.9683510247579895, 0.9756943524444083, 0.9819618081836383, 0.9873793205181407, 0.9921130262392304, 0.9962879371744671]
                    funcmemory = np.searchsorted(mem_cdf, common.gen.random()) + 1
                    memory_demands[funcname] = funcmemory

    for func in duration_weights:
        if func in memory_demands:
            fns[func] = Function(new_fnid(), memory_demands[func], func)
    
    def get_dur(funcname):
        weights = duration_weights[funcname]
        x = common.gen.random()
        if  x >= 0.25 and x < 0.5:
            return round(weights[2] + (x-0.25)*(weights[3]-weights[2])/0.25) # return milliseconds
        elif x >= 0.5 and x < 0.75:
            return round(weights[3] + (x-0.5)*(weights[4]-weights[3])/0.25)
        elif x >= 0.01 and x < 0.25:
            return round(weights[1] + (x-0.01)*(weights[2]-weights[1])/0.24)
        elif x >= 0.75 and x < 0.99:
            return round(weights[4] + (x-0.75)*(weights[5]-weights[4])/0.24)
        elif x < 0.01:
            return round(weights[0] + x*(weights[1]-weights[0])/0.01)
        else: #  x >= 0.99
            return round(weights[5] + (x-0.99)*(weights[6]-weights[5])/0.01)

    with open(tracedir+'/'+invocations, newline='') as f:
        reader = csv.reader(f)
        reader.__next__()
        for row in reader:
            if row[1] in apps:
                funcname = row[2]
                if funcname not in fns:
                    print('function', funcname, 'not in durations or memory', file=sys.stderr)
                    continue
                start_idx = 4 + start_minute
                counts = [int(c) for c in row[start_idx:start_idx+length]]
                current = common.gen.randrange(60)
                for i in range(len(counts)):
                    c = counts[i]
                    if c == 0:
                        current = (current + 1) % 60
                        continue
                    else:
                        for _ in range(c):
                            second = int(current) + 60*i
                            invocs[second] = invocs.get(second, [])
                            invocs[second].append(Invocation(fns[funcname], min(get_dur(funcname), 1000*299)))
                            current = (current + 60/c) % 60

    for k in invocs.keys():
        common.gen.shuffle(invocs[k])
    slowed = slowstart(invocs, length, start_window, start_load)
    downsampled = downsample(slowed, downsample_factor)
    
    fns_to_invoke = set()
    for invocs in downsampled.values():
        for invoc in invocs:
            fns_to_invoke.add(invoc.function)
    return downsampled, list(fns_to_invoke)

def azure_trace_eval(app_predicate, invocations, durations, memory, start_minute, length):
    invocs, fns = azure_trace(app_predicate, invocations, durations, memory, start_minute, length, 0.0, 1.0, 1.0)
    

def slowstart(invocs, length, start_window, start_load):
    for i in range(int(length*60*start_window)):
        if i >= len(invocs): break
        prob = start_load + (1-start_load) * i / (length * 60 * start_window)
        before = len(invocs[i])
        invocs[i] = [invoc for invoc in invocs[i] if common.gen.random() < prob]
        if False: print(i, before, '->', len(invocs[i]))
    return invocs

def downsample(invocs, factor):
    for i in range(len(invocs)):
        invocs[i] = [invoc for invoc in invocs[i] if common.gen.random() < factor]
    return invocs

def extend_workload(wl1, wl2):
    for k, v in wl2.items():
        if k in wl1:
            wl1[k].extend(v)
        else:
            wl1[k] = v
    return wl1
