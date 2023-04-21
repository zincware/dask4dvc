"""Some general 'dask4dvc' methods."""


import contextlib
import typing
import logging

import dask.distributed
import dvc.lock
import dvc.exceptions
import dvc.repo
from dvc.repo.reproduce import _get_steps
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


def submit_stage(name: str, force: bool, successors: list) -> str:
    """Submit a stage to the Dask cluster."""
    repo = dvc.repo.Repo()

    if force:
        stages = [repo.stage.get_target(name)]
    else:
        # dvc reproduce returns the stages that are not checked out
        stages = _run_locked_cmd(repo, repo.reproduce, name, dry=True, single_item=True)

    if len(stages) == 0 and not force:
        # if the stage is already checked out, we don't need to run it
        log.info(f"Stage '{name}' didn't change, skipping")

    else:
        if len(stages) > 1:
            # we use single-item, so it should never be more than 1
            raise ValueError("Something went wrong")

        for stage in stages:
            if not force:
                with contextlib.suppress(RunCacheNotFoundError):
                    # check if the stage is already in the run cache
                    _run_locked_cmd(repo, _load_run_cache, repo, stages[0])
                    return name
            # if not, run the stage
            log.info(f"Running stage '{name}': \n > {stage.cmd}")
            subprocess.check_call(stage.cmd, shell=True)
            # add the stage to the run cache
            _run_locked_cmd(repo, repo.commit, name, force=True)

    return name


def parallel_submit(
    client: dask.distributed.Client, targets: list[str], force: bool
) -> typing.Dict[str, dask.distributed.Future]:
    """Submit all stages to the Dask cluster."""
    mapping = {}
    repo = dvc.repo.Repo()

    if len(targets) == 0:
        targets = repo.index.graph.nodes
    else:
        targets = [repo.stage.get_target(x) for x in targets]

    nodes = _get_steps(repo.index.graph, targets, downstream=False, single_item=False)

    for node in nodes:
        if node.cmd is None:
            # if the stage doesn't have a command, e.g. a dvc tracked file
            # we don't need to run it
            mapping[node] = None
            continue
        successors = [
            mapping[successor] for successor in repo.index.graph.successors(node)
        ]

        mapping[node] = client.submit(
            submit_stage,
            node.addressing,
            force=force,
            successors=successors,
            pure=False,
            key=node.addressing,
        )

    mapping = {
        node.addressing: future for node, future in mapping.items() if future is not None
    }

    return mapping
