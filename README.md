[![Coverage Status](https://coveralls.io/repos/github/zincware/dask4dvc/badge.svg?branch=main)](https://coveralls.io/github/zincware/dask4dvc?branch=main)
![PyTest](https://github.com/zincware/dask4dvc/actions/workflows/pytest.yaml/badge.svg)
[![PyPI version](https://badge.fury.io/py/dask4dvc.svg)](https://badge.fury.io/py/dask4dvc)
[![zincware](https://img.shields.io/badge/Powered%20by-zincware-darkcyan)](https://github.com/zincware)

# Dask4DVC - Distributed Node Exectuion
[DVC](dvc.org) provides tools for building and executing the computational graph locally through various methods. 
The `dask4dvc` package combines [Dask Distributed](https://distributed.dask.org/) with DVC to make it easier to use with HPC managers like [Slurm](https://github.com/SchedMD/slurm).

The `dask4dvc` package will try to run the DVC graph in parallel.

> :warning: This is an experimental package **not** affiliated in any way with iterative or DVC. ``dask4dvc`` will disbale a few of the checks that DVC implements. Do not make changes to your workspace during the runtime of `dask4dvc`.

## Usage
Dask4DVC provides a CLI similar to DVC.

- `dvc repro` becomes `dask4dvc repro`.
- `dvc queue start --jobs 1` becomes `dask4dvc run`

You can follow the progress using `dask4dvc <cmd> --dashboard`.

> `dask4dvc run --parallel` is available for `dvc queue start --jobs <max-workers>` but it currently leads to the failure of some experiments.

> The `dask4dvc` error messages are currently really sparse. For better error messages please use the DVC commands.

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

You can also use config files with `dask4dvc repro --config myconfig.yaml`.

```yaml
default:
  SGECluster:
    queue: regular
    cores: 10
    memory: 16 GB
```
