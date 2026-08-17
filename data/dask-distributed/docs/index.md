# playground-data-dask-distributed

Learning `dask.distributed` against a real cluster: a scheduler and three
worker containers run with Docker Compose, driven by examples from the host.

This is where dask stops being a library and becomes a system. The local
schedulers covered in the `data/dask` project hide the things that matter most
in production — serialization, data locality, worker memory limits, worker
death, and observability. All of them are visible here.

## Quick start

```bash
make up                       # scheduler + 3 workers
make run EXAMPLE=0101_connect
make dashboard                # http://127.0.0.1:8787/status
make down
```

Examples fall back to an in-process `LocalCluster` (with a printed note) when
the Compose cluster is not running, so they work on any machine.

## Why it matters

An openEO process graph submitted to a deployed
[open-climate-service](https://github.com/dhis2/open-climate-service) instance
runs on workers that share neither memory nor filesystem with the API process.
Every constraint in this project shapes how such a service gets deployed.

See the [API Reference](api-reference.md) for the connection helpers.
