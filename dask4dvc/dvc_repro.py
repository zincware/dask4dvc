"""Dask4DVC to DVC repo interface."""
import dataclasses
import logging
import subprocess
import typing
import uuid

import dask.distributed
import dvc.cli
import dvc.repo
from dvc.repo.experiments.queue import tasks
from dvc.repo.experiments.queue.base import QueueEntry
from dvc.repo.reproduce import _get_steps
from dvc.stage import PipelineStage

log = logging.getLogger(__name__)


def queue_consecutive_stages(
    repo: dvc.repo.Repo,
    targets: typing.List[str],
    options: list = None,
) -> typing.Dict[PipelineStage, str]:
    """Create an experiment for each stage in the DAG.

    Parameters
    ----------
    repo : dvc.repo.Repo
        The DVC repo to gather the stages from
    targets : typing.List[str], optional
        The stages to queue, by default it will use all stages in the DAG
    options : list, optional
        Additional options to pass to `dvc exp run`, by default None


    Returns
    -------
    typing.Dict[PipelineStage, str]
        A dictionary mapping each stage to its experiment name
    """
    if len(targets) == 0:
        stages = repo.index.graph.nodes
    else:
        stages = [repo.stage.get_target(x) for x in targets]

    ordered_stages = _get_steps(
        repo.index.graph, stages, downstream=False, single_item=False
    )

    experiment_names = {}

    cmd = ["exp", "run", "--queue"]
    if options is not None:
        cmd.extend(options)
    for stage in ordered_stages:
        try:
            name = f"{stage.name}-dask4dvc-{str(uuid.uuid4())[:8]}"
            experiment_names[stage] = name
            dvc.cli.main(cmd + ["--name", name, stage.name])
        except AttributeError:
            # has no attribute name
            log.warning(f"Skipping stage {stage} because it is not a pipeline stage")

    return experiment_names


def get_all_queue_entries(
    repo: dvc.repo.Repo,
) -> typing.Dict[str, typing.Tuple[QueueEntry, str]]:
    """Get QueueEntry and infofile from the queue.

    We do all at once, because doing it in parallel seems not to work probably.
    """
    queue = repo.experiments.celery_queue
    return {
        entry.name: (entry, queue.get_infofile_path(entry.stash_rev))
        for entry in queue.iter_queued()
    }


def exec_run(infofile: str, successors: list = None) -> None:
    """Execute a queued experiment via its infofile."""
    # ! Do not use dvc.cli.main
    subprocess.check_call(["dvc", "exp", "exec-run", "--infofile", infofile])


def remove_experiments(experiments: typing.List[str] = None) -> None:
    """Remove queued experiments."""
    print(f"Removing experiments: {experiments}")
    repo = dvc.repo.Repo()
    queue = repo.experiments.celery_queue
    for msg in queue.celery.iter_queued():
        if msg.headers.get("task") != tasks.run_exp.name:
            continue
        args, kwargs, _embed = msg.decode()
        entry_dict = kwargs.get("entry_dict", args[0])
        if (experiments is None and "-dask4dvc-" in entry_dict["name"]) or (
            experiments is not None and entry_dict["name"] in experiments
        ):
            queue.celery.reject(msg.delivery_tag)


def collect_and_cleanup(entry_dict, executor, infofile):
    """Collect the results of a finished experiment and clean up."""
    try:
        tasks.collect_exp(proc_dict=None, entry_dict=entry_dict)
    finally:
        executor.cleanup(infofile)


def parallel_submit(
    client: dask.distributed.Client,
    repo: dvc.repo.Repo,
    stages: typing.Dict[PipelineStage, str],
) -> typing.Tuple[typing.Dict[PipelineStage, dask.distributed.Future], typing.List[str]]:
    """Submit experiments in parallel."""
    mapping = {}
    queue_entries = get_all_queue_entries(repo)
    experiments = []

    cleanup_data = []

    for stage in stages:
        log.critical(f"Preparing experiment '{stages[stage]}'")
        entry, infofile = queue_entries[stages[stage]]
        executor = tasks.setup_exp(dataclasses.asdict(entry))

        # we use get here, because some stages won't be queued, such as dependency files
        successors = [
            mapping.get(successor) for successor in repo.index.graph.successors(stage)
        ]
        mapping[stage] = client.submit(
            exec_run,
            infofile=infofile,
            successors=successors,
            pure=False,
            key=entry.name,
        )

        cleanup_data.append(
            {
                "executor": executor,
                "infofile": infofile,
                "entry_dict": dataclasses.asdict(entry),
            }
        )
        # mapping[f"{entry.name}-collect"] = client.submit(
        #     collect_and_cleanup,
        #     executor=executor,
        #     infofile=infofile,
        #     entry_dict=dataclasses.asdict(entry),
        #     job=mapping[stage],
        #     pure=False,
        #     key=f"{entry.name}-collect",
        # )
        experiments.append(entry.name)

    return mapping, experiments, cleanup_data


def experiment_submit(
    client: dask.distributed.Client, repo: dvc.repo.Repo, experiments: typing.List[str]
):
    queue_entries = get_all_queue_entries(repo)
    if experiments is None:
        experiments = list(queue_entries.keys())
    mapping = {}
    print(f"Submitting experiments: {experiments}")
    cleanup_data = []
    for experiment in experiments:
        log.critical(f"Preparing experiment '{experiment}'")
        entry, infofile = queue_entries[experiment]
        executor = tasks.setup_exp(dataclasses.asdict(entry))

        mapping[experiment] = client.submit(
            exec_run,
            infofile=infofile,
            successors=[],
            pure=False,
            key=entry.name,
        )
        mapping[f"{entry.name}-collect"] = client.submit(
            collect_and_cleanup,
            entry_dict=dataclasses.asdict(entry),
            job=mapping[experiment],
            pure=False,
            key=f"{entry.name}-collect",
        )

        cleanup_data.append(
            {
                "executor": executor,
                "infofile": infofile,
                "entry_dict": dataclasses.asdict(entry),
            }
        )
    return mapping, list(mapping.keys()), cleanup_data
