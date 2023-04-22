"""dask4dvc experiment handling."""
from dvc.repo.experiments.queue import tasks
import dvc.repo
import dataclasses
import dvc.cli
import functools
import logging
from unittest.mock import patch
from dask4dvc import dvc_repro
from dvc.repo.experiments.executor.base import ExecutorInfo, BaseExecutor
from dvc.utils.serialize import load_json


logger = logging.getLogger(__name__)


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
