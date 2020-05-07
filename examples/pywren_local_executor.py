# Use the Dask executor scheduler with a Pywren local_executor

import dask.array as da
import numpy as np
import pywren_ibm_cloud as pywren

from dask_executor_scheduler import executor_scheduler, PywrenExecutor

if __name__ == '__main__':

    x = da.random.random((10000, 1000), chunks=(1000, 1000))
    y = np.sum(x, axis=1)
    z = y.compute(scheduler=executor_scheduler, executor=PywrenExecutor(pywren.local_executor()), batch_size=10)
    print(z)
    print(z.shape)
