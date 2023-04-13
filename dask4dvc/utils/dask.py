"""Utils that are related to 'dask'."""
import logging
import pathlib
import typing

import dask_jobqueue
import yaml
from dask.distributed import Future

log = logging.getLogger(__name__)


def wait_for_futures(futures: typing.Union[Future, typing.Dict[str, Future]]) -> None:
    """Wait for all given future objects to complete.

    Raises
    ------
    This will fail if one of them fails.
    """
    if isinstance(futures, Future):
        futures = {"main": futures}

    for future in futures.values():
        try:
            _ = future.result()
        except Exception as err:
            log.critical(f"Waiting for result from '{future}' failed with {err}")


def get_cluster_from_config(file: str) -> dask_jobqueue.core.JobQueueCluster:
    """Read 'dask4dvc' config file and create a cluster."""
    data = yaml.safe_load(pathlib.Path(file).read_text())
    default = data["default"]
    cluster_name = next(iter(default))
    cluster = getattr(dask_jobqueue, cluster_name)(**default[cluster_name])
    cluster.adapt()
    return cluster
