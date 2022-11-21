"""Some general 'dask4dvc' methods."""

import contextlib
import pathlib
import typing

import git

from dask4dvc import utils


@utils.main.timeit
def _exp_branch(queued_experiments: dict) -> list:
    """Create a branch for every experiment."""
    repo_names = []
    for exp, name in queued_experiments.items():
        name = f"tmp_{exp[:7]}" if name is None else f"tmp_{name}"
        repo_names.append(name)
        utils.dvc.exp_branch(experiment=exp, branch=name)
    return repo_names


@utils.main.timeit
def _clone_branch(repo_names: list) -> typing.Dict[str, git.Repo]:
    """Make a clone of every branch to a temporary directory."""
    repos = {}
    for name in repo_names:
        new_repo = pathlib.Path(".dask4dvc", name)
        repos[name] = git.Repo.clone_from(url=".", to_path=new_repo, branch=name)
    return repos


@utils.main.timeit
def _update_run_cache(repos: typing.List[git.Repo]) -> None:
    """Update the run cache for the given repos."""
    for repo in repos:
        repo.git.execute(
            [
                "dvc",
                "cache",
                "dir",
                "--local",
                str(pathlib.Path.cwd().resolve() / ".dvc" / "cache"),
            ]
        )


@contextlib.contextmanager
def get_experiment_repos() -> typing.Dict[str, git.Repo]:
    """Prepare DVC experiments for parallel execution.

    This contextmanager does:
        1. Get the queued experiments.
        2. Promote them to branches.
        3. Create clones.
        4. Set dvc cache.
    and then finishes by:
        1. removing the temporary branches.
        2. removing the temporary clones.

    Yields
    ------
    dict[str, Repo]: temporary repositories set up for running 'dvc repro' with
        a shared run cache.
    """
    if utils.git.update_gitignore(ignore=".dask4dvc/"):
        raise ValueError("'.gitignore' file was updated. Please commit changes.")

    queued_experiments = utils.main.timeit(utils.dvc.exp_show_queued)()

    repo_names = _exp_branch(queued_experiments)
    repos = _clone_branch(repo_names)
    _update_run_cache(list(repos.values()))

    try:
        yield repos
    finally:
        utils.git.remove_branch(list(repos))
        utils.main.remove_paths([clone.working_dir for clone in repos.values()])
