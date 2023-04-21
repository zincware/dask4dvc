"""dask4dvc experiment handling."""
from dvc.repo.experiments.queue import tasks
import dvc.repo
import dataclasses
import dvc.cli


def run_single_experiment(name: str = None) -> None:
    """Run a single experiment from the queue."""
    with dvc.repo.Repo() as repo:
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

    result = dvc.cli.main(["exp", "exec-run", "--infofile", infofile])
    if result != 0:
        raise RuntimeError("Experiment failed")

    result = tasks.collect_exp(None, entry_dict)
    tasks.cleanup_exp(executor, infofile)


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
