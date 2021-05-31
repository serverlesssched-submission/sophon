import random
import numpy as np

gen = None

def init_gen(seed):
    global gen
    gen = random.Random(seed)

'''
Create a random sequence that meets distribution of a histogram. Borrowed from https://stackoverflow.com/a/17822210
'''
def random_from_histogram(hist, bins, n):
    bin_midpoints = bins[:-1] + np.diff(bins)/2 # bins should be the midpoints
    cdf = np.cumsum(hist)
    cdf = cdf / cdf[-1]
    values = [gen.random() for _ in range(n)]
    value_bins = np.searchsorted(cdf, values)
    return bin_midpoints[value_bins]

'''
Choose n indices from histogram
'''
def choose_from_histogram(hist, n):
    cdf = np.cumsum(hist)
    cdf = cdf / cdf[-1]
    values = [gen.random() for _ in range(n)]
    value_bins = np.searchsorted(cdf, values)
    return value_bins
