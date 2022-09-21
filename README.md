# Dask4DVC - Distributed Node Exectuion
[DVC](dvc.org) provides tools for building and executing the computiational graph locally through variouse methods. Currently DVC graphs can be challenging to deply on HPC resources, e.g. using [Slurm](https://github.com/SchedMD/slurm) or deploy the graph fully in parallel (see https://dvc.org/doc/command-reference/repro#parallel-stage-execution). 
Dask4DVC uses [Dask Distributed](https://distributed.dask.org/) to 
- fully parallelize Graph execution, even through multiple experiments.
- allow for easy deployment on HPC resources.

## Usage
Dask4DVC provides a CLI similar to DVC. The core functionality is build around the `dvc repro` and `dvc exp run` commands.

- `dvc repro` becomes `dask4dvc repro`
- `dvc exp run` becomes `dask4dvc run`

Both commands will launch a Dask Server which can be accessed through `127.0.0.1:8787` 

Further more there is: 
- ``dask4dvc clone <source> <target>`` to make a clone of a DVC repository with a shared cache.
## Custom Workers
You can start a scheduler via `dask-scheduler` and then a worker (in the cwd of your project!) via `dask-worker tcp://127.0.0.1:8786` and use `dask4dvc repro --adress 127.0.0.1:8786`
