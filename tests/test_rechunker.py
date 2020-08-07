import fsspec
from numpy import prod

import zarr
import dask.array as da
import pywren_ibm_cloud as pywren
from dask_executor_scheduler import executor_scheduler, PywrenExecutor
from rechunker import rechunk

def delete(url):
    fs = fsspec.open(url).fs
    if fs.exists(url):
        fs.rm(url, recursive=True)
    

def test_rechunker_local():
    source_url = "data/source.zarr"
    target_url = "data/target.zarr"
    temp_url = "data/temp.zarr"

    # delete
    for url in (source_url, target_url, temp_url):
        delete(url)

    source_store = fsspec.get_mapper(source_url)
    target_store = fsspec.get_mapper(target_url)
    temp_store = fsspec.get_mapper(temp_url)

    shape = (4000, 4000)
    source_chunks = (400, 4000)
    target_chunks = (4000, 400)

    executor = PywrenExecutor(pywren.local_executor())

    # create

    # dask_chunks determine the unit of work - they are bigger than the chunks
    # of the zarr file we are writing to
    dask_chunks = (800, 4000)
    zarr_chunks = source_chunks

    arr = da.random.random(size=shape, chunks=dask_chunks)
    itemsize = arr.dtype.itemsize
    max_mem = str(itemsize * prod(dask_chunks))
    plan = rechunk(arr, zarr_chunks, max_mem, source_store)

    plan.execute(
        scheduler=executor_scheduler,
        executor=executor,
        batch_size=100
    )

    z = zarr.open_array(source_store, mode="r")
    assert z.shape == shape
    assert z.chunks == source_chunks
    assert z.nchunks == 10
    assert z.dtype == float

    # rechunk

    max_mem = 25_600_000

    source_array = zarr.open_array(source_store, mode="r")

    plan = rechunk(
        source_array, target_chunks, max_mem, target_store, temp_store=temp_store
    )

    plan.execute(
        scheduler=executor_scheduler,
        executor=executor,
        batch_size=100
    )

    z = zarr.open_array(target_store, mode="r")
    assert z.shape == shape
    assert z.chunks == target_chunks
    assert z.nchunks == 10
    assert z.dtype == float
