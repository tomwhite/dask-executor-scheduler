import dask.array as da
from dask_executor_scheduler import executor_scheduler
import numpy as np

def test():
    x = da.random.random((10000, 1000), chunks=(1000, 1000))
    y = np.sum(x, axis=1)
    z1 = y.compute(scheduler=executor_scheduler)
    assert z1.shape == (10000,)
    z2 = y.compute()
    assert np.allclose(z1, z2)
