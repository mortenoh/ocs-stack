# Scheduler and worker image.
#
# The base tag pins the exact dask/distributed/Python versions this project's
# client uses. Client and cluster must agree: distributed refuses to run, or
# warns loudly, when the versions on either side of the wire differ. Bump this
# tag and the pyproject floors together, never one alone.
FROM ghcr.io/dask/dask:2026.7.1-py3.13

# Workers deserialize whatever the client sends them, so every library used
# inside a submitted function must exist here too. A worker missing xarray
# fails at unpickle time with a ModuleNotFoundError that surfaces on the
# client as a confusing task error.
# Pinned to the exact versions in uv.lock on the client side. numpy and
# tornado are pinned even though the base image ships them: its versions are a
# patch behind the client's, which is enough for distributed to raise a
# VersionMismatchWarning on every connect. Run `make versions` to compare.
RUN pip install --no-cache-dir \
    "xarray==2026.7.0" \
    "zarr==3.3.0" \
    "pandas==3.0.5" \
    "numpy==2.5.2" \
    "tornado==6.5.8" \
    "msgpack==1.2.1" \
    "cloudpickle==3.1.2" \
    "toolz==1.1.0"

# Shared scratch space; compose mounts a volume here so every container and
# the host see the same files. See examples/0302_shared_storage.py for why the
# path must be valid on the WORKER, not on the client.
RUN mkdir -p /data
