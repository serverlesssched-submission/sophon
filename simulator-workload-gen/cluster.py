#!/usr/bin/env python3

import os, sys
import math
import statistics
from collections import OrderedDict, deque
import common

# logs include epochs of events
logs = {'request':[],
        'start':[],
        'finish':[],
        'cold-start':[], 
        'evict':[], 
        'non-home':[]}
# metrics include numbers at every epoch
metrics = { 'load':[],
            'sb-load':[],
            'sys-load':[],
            'inqueue':[],
            'delay':[],
            'distance':[]}
# temporary metrics to be averaged at every epoch
temp_metrics = {'delay':[],
                'distance':[]}

class Function(object):
    def __init__(self, function_id, demand, function_name=None):
        self.function_id = function_id
        self.demand = demand
        self.function_name = function_name

class Invocation(object):
    def __init__(self, function, duration):
        self.function = function
        self.duration = duration
        self.requested = 0
        self.started = 0

class Sandbox(object):
    def __init__(self, host, function, install_time):
        self.host = host
        self.function = function
        self.time_to_install = install_time
        self.state = 'installing' # installing, idle, active
        self.invocation = None
        self.idle_since = None

class Host(object):
    def __init__(self, host_id, configs):
        self.host_id = host_id
        self.capacity = configs['capacity']
        self.invocation_per_host_cap = configs['invocation_per_host_cap']
        self.install_time = configs['install_time']
        self.protect_time = configs['protect_time']
        self.cluster = None
        self.load = 0 # load includes user load, sandbox cold start load, but not idle sandbox load
        self.sb_load = 0 # idle sandbox load
        self.sandboxes = list() # LRU
        self.invocations = set()
        self.request_queue = deque()
    
    def tick(self):
        for sb in list(self.sandboxes): # be sure to use a copy of list because self.sandboxes changes in iteration
            if sb.state == 'active':
                sb.invocation.duration -= 1
                if sb.invocation.duration <= 0:
                    self.finish(sb)

            if sb.time_to_install > 0:
                sb.time_to_install -= 1
                if sb.time_to_install == 0:
                    self.start(sb)
        
        if self.cluster.host_local_queue:
            if self.load == self.capacity:
                return
            while len(self.request_queue) > 0:
                req = self.request_queue.popleft()
                if not self.full(req.function):
                    self.invoke(req)
                else:
                    self.request_queue.appendleft(req)
                    break

    def install(self, invocation):
        sb = Sandbox(self, invocation.function, self.install_time)
        sb.invocation = invocation
        self.sandboxes.append(sb)
        logs['cold-start'].append(self.cluster.epoch)

    def evict(self, load2evict):
        i = 0
        while i < len(self.sandboxes):
            if self.sandboxes[i].state == 'idle':
                released_load = self.sandboxes.pop(i).function.demand
                self.sb_load -= released_load
                load2evict -= released_load
                logs['evict'].append(self.cluster.epoch)
                if load2evict <= 0:
                    break
            else:
                i += 1
        else:
            raise RuntimeError('cannot evict enough sandbox load')

    def start(self, sb):
        invocation = sb.invocation
        invocation.started = self.cluster.epoch
        sb.state = 'active'
        sb.idle_since = None
        self.sandboxes.remove(sb)
        self.sandboxes.append(sb) # update LRU
        temp_metrics['delay'].append(invocation.started - invocation.requested)
        logs['start'].append(self.cluster.epoch)

    def invoke(self, invocation):
        function = invocation.function
        function_id = function.function_id
        self.invocations.add(invocation)
        self.load += function.demand
        self.cluster.load += function.demand

        available_sb = [sb for sb in self.sandboxes if sb.function.function_id == function_id and sb.state == 'idle']
        if len(available_sb) > 0: # warm start
            chosen_sb = common.gen.choice(available_sb) # OW currently randomly chooses warm sb
            chosen_sb.invocation = invocation
            self.start(chosen_sb)
            self.sb_load -= function.demand

        else: # cold start
            if self.load + self.sb_load > self.capacity:
                self.evict(self.load + self.sb_load - self.capacity) # evict
            self.install(invocation)

    def finish(self, sb):
        invocation = sb.invocation
        self.load -= invocation.function.demand
        self.cluster.load -= invocation.function.demand
        self.sb_load += invocation.function.demand
        sb.invocation = None
        sb.state = 'idle'
        sb.idle_since = self.cluster.epoch
        self.invocations.remove(invocation)
        logs['finish'].append(self.cluster.epoch)
    
    def full(self, function):
        if self.load + function.demand > self.capacity: # overload
            return True
        if function.function_id not in [sb.function.function_id for sb in self.sandboxes if sb.state == 'idle']: # cold start
            if self.load + self.sb_load + function.demand > self.capacity: # need to evict
                required = self.load + self.sb_load + function.demand - self.capacity
                for sb in [sbox for sbox in self.sandboxes if (sbox.state == 'idle' and self.cluster.epoch >= sbox.idle_since + self.protect_time)]:
                    required -= sb.function.demand
                    if required <= 0:
                        break
                else:
                    return True
        return len([sb for sb in self.sandboxes if sb.function.function_id == function.function_id and
            sb.state == 'active']) >= self.invocation_per_host_cap # consider full for the function if too many invocations on host already
    
    def describe(self):
        print('host: ', self.host_id, 'running:', len(self.invocations),'utilization:', self.load, 'sandboxes:', len(self.sandboxes))

class Cluster(object):
    def __init__(self, hosts, configs):
        self.capacity = 0
        self.load = 0
        for h in hosts:
            h.cluster = self
            self.capacity += h.capacity
        self.hosts = hosts
        n = len(hosts)
        self.co_primes = [k for k in range(2, n) if math.gcd(k, n) == 1]
        self.host_local_queue = configs['host_local_queue']
        self.request_queue = deque()
        self.epoch = 0

    def request(self, invocation):
        invocation.requested = self.epoch
        self.request_queue.append(invocation)
        logs['request'].append(self.epoch)

    def tick(self):
        for h in self.hosts:
            h.tick()

        if self.host_local_queue:
            # process request queue
            for req in self.request_queue:
                self.schedule(req) # here schedule() should always return True
            self.request_queue.clear()
            # metrics
            metrics['inqueue'].append(sum([len(h.request_queue) for h in self.hosts]))

        else: # global queue
            # process request queue
            remaining_queue = deque()
            while len(self.request_queue) > 0:
                req = self.request_queue.popleft()
                if not self.schedule(req):
                    remaining_queue.append(req)
            self.request_queue = remaining_queue
            # metrics
            metrics['inqueue'].append(len(remaining_queue))

        # collect metrics
        # load = sum([sb.function.demand for h in self.hosts for sb in h.sandboxes if sb.state != 'idle'])
        # assert(load == self.load)
        metrics['load'].append(self.load / self.capacity)
        sb_load = sum([sb.function.demand for h in self.hosts for sb in h.sandboxes if sb.state == 'idle'])
        metrics['sb-load'].append(sb_load / self.capacity)
        sys_load = sum([sb.function.demand for h in self.hosts for sb in h.sandboxes if sb.state == 'installing'])
        metrics['sys-load'].append(sys_load / self.capacity)
        for k, v in temp_metrics.items():
            if len(v) == 0:
                metrics[k].append(0.0)
            else:
                metrics[k].append(sum(v) / len(v))
            temp_metrics[k] = list()

        self.epoch += 1 # tick

    def is_idle(self):
        if self.host_local_queue:
            if not all([len(h.request_queue) == 0 for h in self.hosts]):
                return False
        else:
            if not len(self.request_queue) == 0:
                return False
        return all([len(h.invocations) == 0 for h in self.hosts])

    def schedule(self, invocation):
        def fallback_to_random(invoc):
            common.gen.choice(self.hosts).request_queue.append(invoc)
            temp_metrics['distance'].append(len(self.hosts)/2)
            logs['non-home'].append(self.epoch)
            
        function = invocation.function
        # overload fast path
        if self.load + function.demand > self.capacity:
            if self.host_local_queue:
                fallback_to_random(invocation)
                return True
            else:
                return False
        # choose target
        chosen = function.function_id % len(self.hosts)
        stride = self.co_primes[function.function_id % len(self.co_primes)]
        remaining = len(self.hosts)
        while remaining > 0:
            if not self.hosts[chosen].full(function):
                break
            remaining -= 1
            chosen = (chosen + stride) % len(self.hosts)
        else:
            if self.host_local_queue:
                fallback_to_random(invocation)
                return True
            else:
                return False

        target = self.hosts[chosen]
        target.invoke(invocation)
        temp_metrics['distance'].append(len(self.hosts) - remaining)
        if remaining < len(self.hosts):
            logs['non-home'].append(self.epoch)
        return True

    def describe(self):
        print('======== epoch', self.epoch, '========')
        print('cluster load :\t\t', self.load/self.capacity)
        print('waiting: \t\t', len(self.request_queue))
        for h in self.hosts:
            h.describe()

    def dashboard(self):
        ret = '======== DASHBOARD ========\n'
        ret += 'finish epoch  \t\t {0}\n'.format(self.epoch)
        for k, l in logs.items():
            ret += '{0}  \t\t {1}\n'.format(k, len(l))
        ret += 'average load: \t\t {0}\n'.format(statistics.mean(metrics['load']))
        return ret
