"""The code that is called by the CLI"""
import logging
import pathlib
import shutil

import dask.distributed

import dask4dvc
import dask4dvc.typehints

log = logging.getLogger(__name__)


def repro(
    client: dask.distributed.Client,
    cwd=None,
    cleanup: bool = True,
    repro_options: list = None,
) -> dask4dvc.typehints.FUTURE_DICT:
    """replicate dvc repro with a given client"""
    # TODO what if the CWD is not where the repo is. E.g. if the worker is launchend in a different directory?
    graph = dask4dvc.dvc_handling.get_dvc_graph(cwd=cwd)  # should work correctly in cwd
    node_pairs = dask4dvc.utils.iterate_over_nodes(graph)  # this only gives names

    return dask4dvc.utils.submit_to_dask(
        client,
        node_pairs=node_pairs,
        cmd=dask4dvc.dvc_handling.submit_dvc_stage,
        cwd=cwd,
        cleanup=cleanup,
        repro_options=repro_options,
    )


def run_all(client: dask.distributed.Client, n_jobs):
    if n_jobs is None:
        n_jobs = len(dask4dvc.dvc_handling.get_queued_exp_names())
    dask_job = client.submit(dask4dvc.dvc_handling.run_all, n_jobs=str(n_jobs))
    # wait for the result
    _ = dask_job.result()


def prepare_experiment_dirs() -> dict:
    working_directory = dask4dvc.utils.make_dask4dvc_working_directory()
    # the directory where all experiments are executed
    cwd = pathlib.Path.cwd()

    queued_exp = dask4dvc.dvc_handling.get_queued_exp_names()

    tmp_dirs = {}
    for exp_name in queued_exp:
        log.debug(f"Preparing directory for experiment '{exp_name}'")
        exp_dir_name = working_directory / exp_name[:8]
        if exp_dir_name.exists():
            shutil.rmtree(exp_dir_name)
        dask4dvc.dvc_handling.load_exp_into_workspace(exp_name, cwd=cwd.as_posix())
        source_repo, target_repo = dask4dvc.dvc_handling.clone(cwd, exp_dir_name)
        dask4dvc.dvc_handling.apply_git_diff(source_repo, target_repo, commit=True)

        tmp_dirs[exp_name] = exp_dir_name

    log.debug(f"Experiments: {tmp_dirs}")

    return tmp_dirs
