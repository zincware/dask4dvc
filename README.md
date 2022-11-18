[![Coverage Status](https://coveralls.io/repos/github/zincware/dask4dvc/badge.svg?branch=main)](https://coveralls.io/github/zincware/dask4dvc?branch=main)
![PyTest](https://github.com/zincware/dask4dvc/actions/workflows/pytest.yaml/badge.svg)

# Dask4DVC - Distributed Node Exectuion
[DVC](dvc.org) provides tools for building and executing the computational graph locally through various methods. 
The `dask4dvc` package combines [Dask Distributed](https://distributed.dask.org/) with DVC to make it easier to use with HPC managers like [Slurm](https://github.com/SchedMD/slurm).

## Usage
Dask4DVC provides a CLI similar to DVC.

- `dvc repro` becomes `dask4dvc repro`

### SLURM Cluster

You can use `dask4dvc` easily with a slurm cluster.
This requires a running dask scheduler:
```python
from dask_jobqueue import SLURMCluster

cluster = SLURMCluster(
    cores=1, memory='128GB',
    queue="gpu",
    processes=1,
    walltime='8:00:00',
    job_cpu=1,
    job_extra=['-N 1', '--cpus-per-task=1', '--tasks-per-node=64', "--gres=gpu:1"],
    scheduler_options={"port": 31415}
)
cluster.adapt()
```

with this setup you can then run `dask4dvc repro --address 127.0.0.1:31415` on the example port `31415`.