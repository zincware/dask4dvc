"""dask4dvc experiment handling."""
import dataclasses
import functools
import logging
from unittest.mock import patch

import dask.distributed
import dvc.cli
import dvc.repo
from dvc.repo.experiments.executor.base import BaseExecutor, ExecutorInfo
from dvc.repo.experiments.queue import tasks
from dvc.utils.serialize import load_json

from dask4dvc import dvc_repro

logger = logging.getLogger(__name__)


def run_single_experiment(name: str = None) -> None:
    """Run a single experiment from the queue."""
    client = dask.distributed.get_client()
    repo = dvc.repo.Repo()  # use with?
    queue = repo.experiments.celery_queue
    if name is None:
        entry = next(iter(queue.iter_queued()))
    else:
        for entry in queue.iter_queued():
            if entry.name == name:
                break
        else:
            raise ValueError(f"Experiment {name} not found")

    infofile = queue.get_infofile_path(entry.stash_rev)

    entry_dict = dataclasses.asdict(entry)

    executor = tasks.setup_exp(entry_dict)

    # ["exp", "exec-run", "--infofile", infofile]
    with patch(
        "dvc.repo.reproduce.reproduce",
        wraps=functools.partial(dvc_repro.reproduce, client=client),
    ):
        info = ExecutorInfo.from_dict(load_json(infofile))
        BaseExecutor.reproduce(
            info=info,
            rev="",
            queue=None,
            log_level=logger.getEffectiveLevel(),
            infofile=infofile,
            copy_paths=None,  # self.args.copy_paths,
        )

    tasks.collect_exp(None, entry_dict)
    tasks.cleanup_exp(executor, infofile)  # TODO have an option to not clean up!

    # clean up celery queue

    for msg in queue.celery.iter_queued():
        if msg.headers.get("task") != tasks.run_exp.name:
            continue
        args, kwargs, _embed = msg.decode()
        entry_dict = kwargs.get("entry_dict", args[0])
        if entry_dict["name"] == name:
            queue.celery.reject(msg.delivery_tag)
