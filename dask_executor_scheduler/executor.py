import concurrent.futures
import dask.array as da
import dask.local
import numpy as np
from queue import Empty, Queue
import threading

import pywren_ibm_cloud as pywren

# Idea is to have a queue of function calls to be processed in batches by an
# concurrent.futures.Executor.

class ShutdownSentinel():
    """Put an instance of this class on the queue to shut it down"""
    pass

def _make_queue_worker(queue, executor, batch_size, timeout):
    def queue_worker():
        batch = []
        prev_func = None
        while True:
            try:
                item = queue.get(timeout=timeout)
                if isinstance(item, ShutdownSentinel): # shutdown
                    batch = _process(queue, executor, batch)
                    prev_func = None
                    break
                else:
                    func = item[0]
                    if func == prev_func: # same func so keep accumulating
                        batch.append(item)
                        if len(batch) == batch_size:
                            batch = _process(queue, executor, batch)
                            prev_func = None
                    else: # different func so process previous batch
                        batch = _process(queue, executor, batch)
                        batch.append(item)
                        prev_func = func
            except Empty:
                # timed out, so go ahead and process the batch
                batch = _process(queue, executor, batch)
                prev_func = None
    return queue_worker

def _process(queue, executor, items):
    """Use the given executor to process all the items in a single batch"""
    if len(items) == 0:
        return []
    func = items[0][0] # same func for all items in a batch
    args_iterable = [(item[1], item[2]) for item in items]
    callbacks = [item[3] for item in items]
    _apply_func_map(queue, executor, func, args_iterable, callbacks=callbacks)
    return []

def _apply_func_map(queue, executor, func, args_iterable, callbacks):
    def call(args): # this is the call that is serialized by pywren
        return func(*args[0], **args[1])
    for res, callback in zip(executor.map(call, args_iterable), callbacks):
        if callback is not None:
            callback(res)
        queue.task_done()


def executor_scheduler(dsk, keys, executor=None, batch_size=4, timeout=1, **kwargs):
    """ A scheduler that uses an executor to process tasks in batches

    Parameters
    ----------
    dsk : dict
        dask graph
    keys : object or list
        Desired results from graph
    executor : concurrent.futures.Executor
        Executor used to process tasks
    batch_size : int
        Number of items to be processed by a single function call map
    timeout : int
        Seconds to wait to process batch if it doesn't reach batch_size
    """

    if executor is None:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    # queue and thread are local to this function
    queue = Queue()
    thread = threading.Thread(target=_make_queue_worker(queue, executor, batch_size, timeout))
    thread.start()

    def apply_batched(func, args=(), kwds={}, callback=None):
        # Just put the call onto the queue
        queue.put((func, args, kwds, callback))

    kwargs.pop('num_workers', None)    # if num_workers present, remove it
    # num_workers is number of active tasks present at any one time
    # set to more than the batch size so the work queue is not starved
    res = dask.local.get_async(apply_batched, batch_size + 1, dsk, keys, **kwargs)

    # At this point compute() has returned, so queue can be shut down
    queue.join()
    queue.put(ShutdownSentinel()) # stop worker
    thread.join()

    return res

class PywrenExecutor(object):
    """ A wrapper to make a Pywren executor behave like a concurrent.futures.Executor.
    
    Parameters
    ----------
    pywren_executor : pywren_ibm_cloud.FunctionExecutor
        the Pywren executor    
    """

    def __init__(self, pywren_executor):
        self.pywren_executor = pywren_executor

    def map(self, func, iterables):
        futures = self.pywren_executor.map(func, iterables)
        results = self.pywren_executor.get_result()
        return results
