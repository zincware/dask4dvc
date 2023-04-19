"""Some general 'dask4dvc' methods."""

import typing
import logging

import dask.distributed
import dvc.lock
import dvc.exceptions
import dvc.repo
import dvc.utils.strictyaml
import dvc.stage
from dvc.stage.cache import RunCacheNotFoundError
import random
import time
import subprocess

from dask4dvc import utils

log = logging.getLogger(__name__)


def _run_locked_cmd(
    repo: dvc.repo.Repo, func: typing.Callable, *args: tuple, **kwargs: dict
) -> typing.Any:
    """Retry running a DVC command until the lock is released.

    Parameters
    ----------
    repo: dvc.repo.Repo
        The DVC repository.
    func: callable
        The DVC command to run. e.g. 'repo.reproduce'
    *args: list
        The positional arguments for the command.
    **kwargs: dict
        The keyword arguments for the command.


    Returns
    -------
    typing.Any: the return value of the command.
    """
    err = ValueError("Something went wrong")
    for _ in range(utils.CONFIG.retries):
        try:
            while repo.lock.is_locked:
                time.sleep(random.random() * 5)  # between 0 and 5 seconds
            return func(*args, **kwargs)
        except (dvc.lock.LockError, dvc.utils.strictyaml.YAMLValidationError) as err:
            log.debug(err)
    raise err


def _load_run_cache(repo: dvc.repo.Repo, stage: dvc.stage.Stage) -> None:
    """Load the run cache for the given stage.

    Raises
    ------
    RunCacheNotFoundError:
        if the stage is not cached.
    """
    with dvc.repo.lock_repo(repo):
        with repo.scm_context():
            repo.stage_cache.restore(stage=stage)
            log.info(
                f"Stage '{stage.addressing}' is cached - skipping run, checking out"
                " outputs "
            )


def submit_stage(name: str, successors: list) -> str:
    """Submit a stage to the Dask cluster."""
    repo = dvc.repo.Repo()

    # dvc reproduce returns the stages that are not checked out
    stages = _run_locked_cmd(repo, repo.reproduce, name, dry=True, single_item=True)

    if len(stages) == 0:
        # if the stage is already checked out, we don't need to run it
        log.info(f"Stage '{name}' didn't change, skipping")

    else:
        if len(stages) > 1:
            # we use single-item, so it should never be more than 1
            raise ValueError("Something went wrong")

        for stage in stages:
            try:
                # check if the stage is already in the run cache
                _run_locked_cmd(repo, _load_run_cache, repo, stages[0])
            except RunCacheNotFoundError:
                # if not, run the stage
                log.info(f"Running stage '{name}': \n > {stage.cmd}")
                subprocess.check_call(stage.cmd, shell=True)
                # add the stage to the run cache
                _run_locked_cmd(repo, repo.commit, name, force=True)

    return name


def parallel_submit(
    client: dask.distributed.Client,
) -> typing.Dict[str, dask.distributed.Future]:
    """Submit all stages to the Dask cluster."""
    mapping = {}
    repo = dvc.repo.Repo()

    for node in repo.index.graph.nodes:
        successors = [
            mapping[successor] for successor in repo.index.graph.successors(node)
        ]

        mapping[node] = client.submit(
            submit_stage, node.name, successors=successors, pure=False
        )

    return mapping
