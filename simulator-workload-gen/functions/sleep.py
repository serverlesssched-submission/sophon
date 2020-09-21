#!/usr/bin/env python3

import time
#import common

def main(dict):
    duration = float(dict['duration'])
    time.sleep(max(0, duration - 0.05)) # assume an inherent python overhead of 0.05s
    return {'result': 'success'}

