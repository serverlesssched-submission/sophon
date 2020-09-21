from statistics import mean, stdev
import matplotlib.pyplot as plt
import numpy as np


def plot(epoch, logs, metrics, filename):
    nrow = 5
    ncol = 2
    fig, axs = plt.subplots(nrow, ncol, sharex=True, sharey=False, figsize=(12, 10))
    def axi(i):
        return axs[i//ncol][i%ncol]
    x = np.arange(0.5, epoch)

    current = 0
    # request
    axi(current).hist(logs['request'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    axi(current).title.set_text('request: total '+str(len(logs['request'])))
    # start
    current += 1
    axi(current).title.set_text('start')
    axi(current).hist(logs['start'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # finish
    current += 1
    axi(current).title.set_text('finish')
    axi(current).hist(logs['finish'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # load
    current += 1
    def fmt(y):
        return ('%.3f' % mean(y))+'±'+('%.3f' % stdev(y))
    # axi(current).bar(x, metrics['load'], 1, facecolor='k', alpha=0.75)    
    userload = [(load - sys) for load, sys in zip(metrics['load'], metrics['sys-load'])]
    axi(current).bar(x, metrics['sys-load'], 1, facecolor='r', alpha=0.75) # cold start (system) load
    axi(current).bar(x, userload, 1, bottom=metrics['sys-load'], facecolor='g', alpha=0.75) # user load
    axi(current).bar(x, metrics['sb-load'], 1, bottom=metrics['load'], facecolor='y', alpha=0.75) # sandbox load
    axi(current).title.set_text('load: sys ' + fmt(metrics['sys-load']) + ', user ' + fmt(userload) + 
        ', sandbox ' + fmt(metrics['sb-load']))
    # inqueues
    current += 1
    axi(current).title.set_text('inqueue')
    axi(current).bar(x, metrics['inqueue'], 1, facecolor='k', alpha=0.75)
    # cold start
    current += 1
    axi(current).hist(logs['cold-start'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)    
    axi(current).title.set_text('cold-start: total '+str(len(logs['cold-start'])))
    # evict
    current += 1
    axi(current).hist(logs['evict'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    axi(current).title.set_text('evict: total '+str(len(logs['evict'])))
    # delay
    current += 1
    axi(current).bar(x, metrics['delay'], 1, facecolor='k', alpha=0.75)
    axi(current).title.set_text('average delay: '+('%.3f'%mean(metrics['delay']))+'±'+('%.3f'%stdev(metrics['delay'])))
    # non-home
    current += 1
    axi(current).title.set_text('non-home')
    axi(current).hist(logs['non-home'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # distance
    current += 1
    axi(current).bar(x, metrics['distance'], 1, facecolor='k', alpha=0.75)
    axi(current).title.set_text('average distance: '+('%.2f'%mean(metrics['distance']))+'±'+('%.2f'%stdev(metrics['distance'])))

    fig.tight_layout()
    # plt.show()
    fig.savefig(filename)
