import concurrent.futures
import dask.array as da
import dask.local
import numpy as np
from queue import Empty, Queue
import threading

x = da.random.random((10000, 1000), chunks=(1000, 1000))

print(x.shape)

y = np.sum(x, axis=1)

queue = Queue()
executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
batch_size = 3
timeout = 5 # seconds to wait to process batch

# Idea is to have a queue of function calls to be processed in batches by an
# executor.
def queue_worker():
    batch = []
    while True:
        try:
            item = queue.get(timeout=timeout)
            if item is None: # shutdown
                batch = process(batch)
                break
            batch.append(item)
            if len(batch) == batch_size:
                batch = process(batch)
        except Empty:
            # timed out, so go ahead and process the batch
            batch = process(batch)

def process(items):
    if len(items) == 0:
        return []
    if len(set([item[0] for item in items])) == 1: # same func
        func = items[0][0]
        asc = apply_sync_curry(func)
        print("one func!", func)
        args_iterable = [(item[1], item[2], item[3]) for item in items]
        apply_sync_curry_exec_map(asc, args_iterable)
    else: # mixture of funcs (TODO: sub-batches)
        for item in items:
            apply_exec(item[0], item[1], item[2], item[3])
    return []

def apply_sync(func, args=(), kwds={}, callback=None):
    """ A naive synchronous version of apply_async """
    print(func)
    res = func(*args, **kwds)
    if callback is not None:
        callback(res)

def apply_sync_curry(func):
    def asc(args=(), kwds={}, callback=None):
        """ A naive synchronous version of apply_async """
        print(func)
        res = func(*args, **kwds)
        if callback is not None:
            callback(res)
    return asc

def apply_sync_curry_exec(asc, args):
    def call():
        asc(*args)
        queue.task_done()
    executor.submit(call)

def apply_sync_curry_exec_map(asc, args_iterable):
    def call(args):
        asc(*args)
        queue.task_done()
    for _ in executor.map(call, args_iterable):
        pass

def apply_exec(func, args=(), kwds={}, callback=None):
    # Get the executor to submit the call
    def call():
        apply_sync(func, args, kwds, callback)
        queue.task_done()
    executor.submit(call)

def apply_batched(func, args=(), kwds={}, callback=None):
    # Just put the call onto the queue
    print("put")
    queue.put((func, args, kwds, callback))

def get_batched(dsk, keys, **kwargs):
    """A batched version of get_async"""
    kwargs.pop('num_workers', None)    # if num_workers present, remove it
    # num_workers is number of active tasks present at any one time
    # set to more than the batch size so the work queue is not starved
    return dask.local.get_async(apply_batched, batch_size + 1, dsk, keys, **kwargs)

def executor_scheduler(dsk, keys, **kwargs):
    """A scheduler that uses an executor to process tasks in batches"""
    return get_batched(dsk, keys, **kwargs)

if __name__ == '__main__':

    thread = threading.Thread(target=queue_worker)
    thread.start()

    z = y.compute(scheduler=executor_scheduler)
    print(z)
    print(z.shape)

    queue.join()
    queue.put(None) # stop worker
    thread.join()