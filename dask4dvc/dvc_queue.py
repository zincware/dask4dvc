"""dask4dvc experiment handling."""
import dataclasses
import functools
import logging
from unittest.mock import patch

import dask.distributed
import dvc.cli
import dvc.repo
from dvc.repo.experiments.executor.base import BaseExecutor, ExecutorInfo
from dvc.repo.experiments.executor.local import TempDirExecutor
from dvc.repo.experiments.queue import tasks
from dvc.repo.experiments.queue.base import BaseStashQueue
from dvc.utils.serialize import load_json

from dask4dvc import dvc_repro

logger = logging.getLogger(__name__)


def _get_entry_info_file(name: str):
    with dvc.repo.Repo() as repo:
        queue = repo.experiments.celery_queue
        items = list(queue.iter_queued())
    for entry in items:
        if entry.name == name:
            break
    else:
        raise KeyError(f"Experiment {name} not found in {items}")

    return entry, queue.get_infofile_path(entry.stash_rev)


def setup_experiment(name: str, entries) -> str:
    entry, infofile = entries
    entry_dict = dataclasses.asdict(entry)
    tasks.setup_exp(entry_dict)

    return name


def run_experiment(name: str, entries) -> str:
    entry, infofile = entries
    # ["exp", "exec-run", "--infofile", infofile]
    client = dask.distributed.get_client()

    info = ExecutorInfo.from_dict(load_json(infofile))
    with patch(
        "dvc.repo.reproduce.reproduce",
        wraps=functools.partial(
            dvc_repro.reproduce, client=client, prefix=name.replace("-", "_")
        ),
    ):
        BaseExecutor.reproduce(
            info=info,
            rev="",
            queue=None,
            log_level=logger.getEffectiveLevel(),
            infofile=infofile,
            copy_paths=None,  # self.args.copy_paths,
        )

    return name


def collect_exp(name, entries) -> str:
    entry, infofile = entries
    tasks.collect_exp(None, dataclasses.asdict(entry))
    return name


def cleanup_exp(name, entries):
    entry, infofile = entries
    with dvc.repo.Repo() as repo:
        queue = repo.experiments.celery_queue
        executor = BaseStashQueue.init_executor(
            repo.experiments,
            entry,
            TempDirExecutor,
            location="dvc-task",
        )
        tasks.cleanup_exp(executor, infofile)

        for msg in queue.celery.iter_queued():
            if msg.headers.get("task") != tasks.run_exp.name:
                continue
            args, kwargs, _embed = msg.decode()
            entry_dict = kwargs.get("entry_dict", args[0])
            if entry_dict["name"] == name:
                queue.celery.reject(msg.delivery_tag)


def run_single_experiment(name: str = None, helper_retries: int = 100) -> None:
    """Run a single experiment from the queue."""
    client = dask.distributed.get_client()
    prefix = name.replace("-", "_")

    entries = client.submit(
        _get_entry_info_file, name, retries=helper_retries, key=f"{prefix}_get_queue"
    )
    a = client.submit(
        setup_experiment,
        name=name,
        entries=entries,
        key=f"{prefix}_setup",
        retries=helper_retries,
    )
    x = dask.distributed.Variable(prefix)
    x.set(a)
    b = client.submit(run_experiment, name=a, entries=entries, key=f"{prefix}_run")
    c = client.submit(
        collect_exp,
        name=b,
        entries=entries,
        key=f"{prefix}_collect",
        retries=helper_retries,
    )
    d = client.submit(
        cleanup_exp,
        name=c,
        entries=entries,
        key=f"{prefix}_cleanup",
        retries=helper_retries,
    )

    return d
