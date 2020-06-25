# Read/write Zarr data using cloud storage

import sys

import gcsfs
import zarr

if __name__ == "__main__":

    args = sys.argv[1:]
    if len(args) != 2:
        print("Usage: python zarr_cloud_util.py <project_id> <bucket>")
        sys.exit(1)

    project_id = args[0]
    bucket = args[1]
    fs = gcsfs.GCSFileSystem(project=project_id)

    # Create data
    # store = gcsfs.mapping.GCSMap(f"{bucket}/test.zarr", gcs=fs)
    # z = zarr.create(store=store, shape=(1000, 1000), chunks=(500, 500), dtype='i4', overwrite=True)
    # z[:] = 1

    # Delete recursively
    fs.rm(f"{bucket}/source.zarr", recursive=True)
    fs.rm(f"{bucket}/target.zarr", recursive=True)
    fs.rm(f"{bucket}/temp.zarr", recursive=True)
    fs.rm(f"{bucket}/test.zarr", recursive=True)
