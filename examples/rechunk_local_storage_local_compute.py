# Rechunk a local Zarr file running local Dask

from pathlib import Path
import tempfile

import zarr
import dask.array as da

from rechunker import api

if __name__ == '__main__':
    with tempfile.TemporaryDirectory() as tmpdirname:
        #tmpdir = Path("tmp")
        tmpdir = Path(tmpdirname)

        source_store = str(tmpdir / "source.zarr")
        shape = (4000, 4000)
        source_chunks = (200, 4000)
        dtype = "f4"

        a_source = zarr.ones(shape, chunks=source_chunks, dtype=dtype, store=source_store)

        target_store = str(tmpdir / "target.zarr")
        temp_store = str(tmpdir / "temp.zarr")
        max_mem = 25_600_000
        target_chunks = (4000, 200)
        delayed = api.rechunk_zarr2zarr_w_dask(
            a_source, target_chunks, max_mem, target_store, temp_store=temp_store
        )

        delayed.compute()
