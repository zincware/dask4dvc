"""Some general 'dask4dvc' methods."""

import contextlib
import pathlib
import typing

import dask.distributed
import dvc.lock
import dvc.exceptions
import dvc.repo
import dvc.utils.strictyaml
import git
import tqdm
import typer
import znflow
import random
import time
import subprocess

from dask4dvc import utils


@utils.main.timeit
def _exp_branch(queued_experiments: dict) -> list:
    """Create a branch for every experiment."""
    repo_names = []
    for exp, name in tqdm.tqdm(
        queued_experiments.items(),
        ncols=100,
        disable=len(queued_experiments) < utils.CONFIG.tqdm_threshold,
        desc="dvc exp branch",
    ):
        name = f"tmp_{exp[:7]}" if name is None else f"tmp_{name}"
        repo_names.append(name)
        utils.dvc.exp_branch(experiment=exp, branch=name)
    return repo_names


@utils.main.timeit
def _clone_branch(repo_names: list) -> typing.Dict[str, git.Repo]:
    """Make a clone of every branch to a temporary directory."""
    temp_dir = pathlib.Path(".dask4dvc")
    return {
        name: git.Repo.clone_from(url=".", to_path=temp_dir / name, branch=name)
        for name in repo_names
    }


@utils.main.timeit
def _update_run_cache(repos: typing.List[git.Repo]) -> None:
    """Update the run cache for the given repos.

    Because it is using '--local' in a new repository,
    we replace 'dvc cache dir --local <path>' with writing it directly.
    """
    config_file = pathlib.Path(".dvc/config.local")

    for repo in repos:
        cache_dir = pathlib.Path.cwd().resolve() / ".dvc" / "cache"
        with open(repo.working_dir / config_file, "a") as file:
            file.write("[cache]\n")
            file.write(f"\t dir = {cache_dir}\n")


@contextlib.contextmanager
def get_experiment_repos(delete: list) -> typing.Dict[str, git.Repo]:
    """Prepare DVC experiments for parallel execution.

    This contextmanager does:
        1. Get the queued experiments.
        2. Promote them to branches.
        3. Create clones.
        4. Set dvc cache.
    and then finishes by:
        1. removing the temporary branches.
        2. removing the temporary clones.

    Parameters
    ----------
    delete: list[str]
        remove the "branches" and "temp" afterwards.

    Yields
    ------
    dict[str, Repo]: temporary repositories set up for running 'dvc repro' with
        a shared run cache.

    Raises
    ------
    typer.Exit: if no queued experiments are available.

    """
    if utils.git.update_gitignore(ignore=".dask4dvc/"):
        raise ValueError("'.gitignore' file was updated. Please commit changes.")

    queued_experiments = utils.main.timeit(utils.dvc.exp_show_queued)()

    if len(queued_experiments) == 0:
        typer.echo("Skipping: no experiments were found in the queue.")
        raise typer.Exit()

    repo_names = _exp_branch(queued_experiments)
    repos = _clone_branch(repo_names)
    _update_run_cache(list(repos.values()))

    try:
        yield repos
    finally:
        if "branches" in delete:
            git.Repo(".").delete_head(*list(repos), force=True)
        if "temp" in delete:
            utils.main.remove_paths([clone.working_dir for clone in repos.values()])


def _run_locked_cmd(repo, func, *args, **kwargs):
    while repo.lock.is_locked:
        time.sleep(random.random())
    while True:
        with contextlib.suppress(
            dvc.lock.LockError, dvc.utils.strictyaml.YAMLValidationError
        ):
            func(*args, **kwargs)
            break


@znflow.nodify
def submit_stage(name: str, successors: list) -> str:
    """Submit a stage to the Dask cluster."""
    repo = dvc.repo.Repo()
    with utils.dvc.capture_dvc_logging_output() as output:
        _run_locked_cmd(repo, repo.reproduce, name, dry=True)
    if f"Stage '{name}' didn't change, skipping" in output.getvalue():
        _run_locked_cmd(repo, repo.checkout, name)
    else:
        stage = repo.stage.get_target(name)
        # TODO DVCFileSystem can raise "YAMLValidationError: './dvc.lock'"
        subprocess.check_call(stage.cmd, shell=True)
        _run_locked_cmd(repo, repo.commit, name, force=True)

    return name


def parallel_submit(
    client: dask.distributed.Client,
) -> typing.Dict[str, dask.distributed.Future]:
    """Submit all stages to the Dask cluster."""
    graph = znflow.DiGraph()
    mapping = {}
    repo = dvc.repo.Repo()

    for node in repo.index.graph.nodes:
        successors = [
            mapping[successor] for successor in repo.index.graph.successors(node)
        ]
        with graph:
            mapping[node] = submit_stage(
                node.name,
                successors=successors,
            )
    deployment = znflow.deployment.Deployment(graph=graph, client=client)
    deployment.submit_graph()

    return deployment.results
