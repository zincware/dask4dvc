# DASK4DVC - Use DASK to run the DVC graph

Dask4DVC allows you to run you DVC graph through DASK.
Two interfaces are available:
1. CLI for easy, local parallization
2. Python Interface for defining a Cluster and deploying the calculations on Nodes.

The CLI can also read a client configuration from a config file.

To reproduce your current graph like `dvc repro` you can call `dask4dvc repro`.
