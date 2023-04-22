"""dask4dvc experiment handling."""
import dataclasses
import functools
import logging
import typing
from unittest.mock import patch

import dask.distributed
import dvc.cli
import dvc.repo
from dvc.repo.experiments.executor.base import BaseExecutor, ExecutorInfo
from dvc.repo.experiments.queue import tasks
from dvc.utils.serialize import load_json

from dask4dvc import dvc_repro  # , utils

logger = logging.getLogger(__name__)

# def _reproduce(infofile):
#     client = dask.distributed.get_client()
#     with patch(
#         "dvc.repo.reproduce.reproduce",
#         wraps=functools.partial(dvc_repro.reproduce, client=client),
#         ):
#             info = ExecutorInfo.from_dict(load_json(infofile))
#             result = BaseExecutor.reproduce(
#                 info=info,
#                 rev="",
#                 queue=None,
#                 log_level=logging.CRITICAL, #logger.getEffectiveLevel(),
#                 infofile=infofile,
#                 copy_paths=None,  # self.args.copy_paths,
#             )
#     return result


def run_multiple_experiments(
    client: dask.distributed.Client, targets: typing.List[str]
) -> typing.List[str]:
    repo = dvc.repo.Repo()
    queue = repo.experiments.celery_queue
    entries = []
    for entry in queue.iter_queued():
        if entry.name in targets:
            entries.append(entry)

    infofiles = {}
    executors = {}
    for entry in entries:
        executors[entry.name] = tasks.setup_exp(dataclasses.asdict(entry))

    results = {}
    for entry in entries:
        # results[infofile] = client.submit(_reproduce, infofile)
        # TODO submit to dask
        infofiles[entry.name] = queue.get_infofile_path(entry.stash_rev)

        with patch(
            "dvc.repo.reproduce.reproduce",
            wraps=functools.partial(
                dvc_repro.reproduce, client=client, prefix=entry.name.replace("-", "_")
            ),
        ):
            info = ExecutorInfo.from_dict(load_json(infofiles[entry.name]))
            results[entry.name] = BaseExecutor.reproduce(
                info=info,
                rev="",
                queue=None,
                log_level=logger.getEffectiveLevel(),
                infofile=infofiles[entry.name],
                copy_paths=None,  # self.args.copy_paths,
            )

    # TODO handle successful und failed runs differently!
    # utils.dask.wait_for_futures(client, results)

    for entry in entries:
        tasks.collect_exp(None, dataclasses.asdict(entry))
    for executor, infofile in zip(executors.values(), infofiles.values()):
        tasks.cleanup_exp(executor, infofile)
    # clean up celery queue

    for entry in entries:
        for msg in queue.celery.iter_queued():
            if msg.headers.get("task") != tasks.run_exp.name:
                continue
            args, kwargs, _embed = msg.decode()
            entry_dict = kwargs.get("entry_dict", args[0])
            if entry_dict["name"] == entry.name:
                queue.celery.reject(msg.delivery_tag)


def run_single_experiment(client, name: str = None) -> None:
    """Run a single experiment from the queue."""
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

        # result = dvc.cli.main(["exp", "exec-run", "--infofile", infofile])
        # if result != 0:
        #     raise RuntimeError("Experiment failed")

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


# repo.reproduce -> list[dvc.stage.PipelineStage]
# dvc.cli.main(["exp", "exec-run", "--infofile", infofile])
# calls
# info = ExecutorInfo.from_dict(load_json(self.args.infofile))
#         BaseExecutor.reproduce(
#             info=info,
#             rev="",
#             queue=None,
#             log_level=logger.getEffectiveLevel(),
#             infofile=self.args.infofile,
#             copy_paths=self.args.copy_paths,
#         )
# which calls
# dvc.repo.reproduce <- replace with our function
# no idea what copy_paths does
