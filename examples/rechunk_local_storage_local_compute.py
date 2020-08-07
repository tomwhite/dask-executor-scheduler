# Rechunk a local Zarr file running local Dask with Pywren

import fsspec
from numpy import prod
import sys

import zarr
import dask.array as da
import pywren_ibm_cloud as pywren
from dask_executor_scheduler import executor_scheduler, PywrenExecutor
from rechunker import rechunk

if __name__ == '__main__':

    args = sys.argv[1:]
    if len(args) != 1:
        print("Usage: python rechunk_local_storage_local_compute.py <command>")
        sys.exit(1)

    command = args[0]

    source_url = "data/source.zarr"
    target_url = "data/target.zarr"
    temp_url = "data/temp.zarr"

    source_store = fsspec.get_mapper(source_url)
    target_store = fsspec.get_mapper(target_url)
    temp_store = fsspec.get_mapper(temp_url)

    shape = (4000, 4000)
    source_chunks = (400, 4000)
    target_chunks = (4000, 400)

    if command == "delete":
        # delete everything

        for url in (source_url, target_url, temp_url):
            fs = fsspec.open(url).fs
            if fs.exists(url):
                fs.rm(url, recursive=True)

    elif command == "create":
        # create a random Zarr array for the source

        # dask_chunks determine the unit of work - they are bigger than the chunks
        # of the zarr file we are writing to
        dask_chunks = (800, 4000)
        zarr_chunks = source_chunks

        executor = PywrenExecutor(pywren.local_executor())

        arr = da.random.random(size=shape, chunks=dask_chunks)
        itemsize = arr.dtype.itemsize
        max_mem = str(itemsize * prod(dask_chunks))
        plan = rechunk(arr, zarr_chunks, max_mem, source_store)

        plan.execute(
            scheduler=executor_scheduler,
            executor=executor,
            batch_size=100
        )

    elif command == "rechunk":
        # rechunk the source to the target

        max_mem = 25_600_000

        executor = PywrenExecutor(pywren.local_executor())

        source_array = zarr.open_array(source_store, mode="r")

        plan = rechunk(
            source_array, target_chunks, max_mem, target_store, temp_store=temp_store
        )

        plan.execute(
            scheduler=executor_scheduler,
            executor=executor,
            batch_size=100
        )
