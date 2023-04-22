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
import os
import uuid

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
                f"Stage '{stage.name}' is cached - skipping run, checking out outputs "
            )


def submit_stage(
    name: str, force: bool, successors: list, root_dir: str
) -> typing.List[str]:
    """Submit a stage to the Dask cluster."""
    os.chdir(root_dir)
    repo = dvc.repo.Repo(root_dir)

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

    return [x.name for x in stages]


def reproduce(
    repo: dvc.repo.Repo,
    targets=None,
    recursive=False,
    pipeline=False,
    all_pipelines=False,
    after_repro_callback=None,  # TODO
    client: dask.distributed.Client = None,
    **kwargs,
) -> typing.List[dvc.stage.PipelineStage]:
    """Parallel Exectuion drop-in replacement for 'dvc.repo.Repo.reproduce'.

    Use with functools.partial to set the client
    """
    print("GOT HERE!")
    print(kwargs)

    if targets is None:
        targets = []
    mapping = parallel_submit(
        client, targets, force=kwargs.get("force", False), root_dir=repo.root_dir
    )
    results: typing.Dict[str, list] = utils.dask.wait_for_futures(mapping)
    result = []
    for stages in results.values():
        if isinstance(stages, str):  # TODO this should always be a list
            stages = [stages]
        result.extend([repo.stage.get_target(x) for x in stages])
    return result


def parallel_submit(
    client: dask.distributed.Client, targets: list[str], force: bool, root_dir: str = None
) -> typing.Dict[str, dask.distributed.Future]:
    """Submit all stages to the Dask cluster."""
    mapping = {}
    repo = dvc.repo.Repo(root_dir)

    if len(targets) == 0:
        targets = repo.index.graph.nodes
    else:
        targets = [repo.stage.get_target(x) for x in targets]

    nodes = _get_steps(repo.index.graph, targets, downstream=False, single_item=False)

    experiment_name = uuid.uuid4().hex[:8]  # TODO experiment name instead of uuid

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
            node.name,
            force=force,
            root_dir=repo.root_dir,
            successors=successors,
            pure=False,
            key=f"{experiment_name}_{node.name}",
        )

    mapping = {
        node.name: future for node, future in mapping.items() if future is not None
    }

    # print("Submitted stages:")
    # print(mapping)
    # print("Waiting for stages to finish...")

    return mapping
