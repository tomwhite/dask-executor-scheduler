# Use the Dask executor scheduler with a threadpool executor

import dask.array as da
import numpy as np

from dask_executor_scheduler import executor_scheduler

if __name__ == '__main__':

    x = da.random.random((10000, 1000), chunks=(1000, 1000))
    y = np.sum(x, axis=1)
    z = y.compute(scheduler=executor_scheduler)
    print(z)
    print(z.shape)
