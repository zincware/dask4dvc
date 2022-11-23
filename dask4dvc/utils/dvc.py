"""Utils that are related to 'DVC'."""
import json
import logging
import re
import subprocess
import typing

import dvc.exceptions
import dvc.repo
import dvc.repo.experiments.queue.celery
import git

from dask4dvc.utils.config import CONFIG

log = logging.getLogger(__name__)


def repro(
    targets: typing.Union[str, list] = None,
    options: typing.Union[str, list] = None,
    cwd: str = None,
    **kwargs: dict,
) -> None:
    """Run 'dvc repro'.

    Parameters
    ----------
    targets: list|str
        Names of the stage to reproduce
    options: list|str, default = None
        A list of additional 'dvc repro' arguments such as '--force' that will be
        passed to the subprocess call.
    cwd: str
        working directory
    kwargs: dict
        required for DASK graph to be built. Typically, these would be some
        dependencies from 'dask.distributed.Future'

    Raises
    ------
    subprocess.CalledProcessError: if dvc cmd fails
    """
    if targets is None:
        targets = []
    elif isinstance(targets, str):
        targets = [targets]
    if options is None:
        options = []
    elif isinstance(options, str):
        options = [options]

    subprocess.run(["dvc", "checkout", "--quiet"], cwd=cwd)

    cmd = ["dvc", "repro"] + targets + options
    subprocess.check_call(cmd, cwd=cwd)


def exp_show(cwd: str = None) -> dict:
    """Convert 'dvc exp show' to a python dict.

    Parameters
    ----------
    cwd: str
        The directory to get the experiments from

    Returns
    -------
    dict:
        A dictionary with keys [workspace, commit1, ..., commitN]
        For commit1...N there are keys for 'baseline' and every queued experiment

    """
    subprocess_out = subprocess.run(
        ["dvc", "exp", "show", "--json"], capture_output=True, check=True, cwd=cwd
    )

    json_str = subprocess_out.stdout.decode("utf-8")
    # we match everything before the last }}}} closes the json string and is
    # followed by some unwanted characters
    json_str = re.findall(r".*\}\}\}\}", json_str)[0]

    return json.loads(json_str)


def _exp_show_queued(cwd: str = None) -> dict:
    """Get all currently queued experiments.

    Try to get the name that was used to queue, otherwise use the hash

    Returns
    -------
    exp_names: dict
        A dictionary with {rev: exp_name}. If 'exp_name' was not set, it is None.

    """
    exp_dict = exp_show(cwd=cwd)
    # I don't understand why they separate this into workspace and some hash?
    base_key = [x for x in exp_dict if x != "workspace"][0]

    exp_names = {}
    for exp_name in exp_dict[base_key]:
        if exp_name == "baseline":
            continue
        if "queued" in exp_dict[base_key][exp_name]["data"]:
            if exp_dict[base_key][exp_name]["data"]["queued"]:
                exp_names[exp_name] = exp_dict[base_key][exp_name]["data"].get("name")
        elif "status" in exp_dict[base_key][exp_name]["data"]:
            if exp_dict[base_key][exp_name]["data"]["status"] == "Queued":
                exp_names[exp_name] = exp_dict[base_key][exp_name]["data"].get("name")
        else:
            raise KeyError(
                f"Could not find information about queued stages for {exp_name}"
            )

    return exp_names


def exp_show_queued(*args: tuple, **kwargs: dict) -> dict:
    """Select the correct method based on CONFIG."""
    if CONFIG.use_dvc_api:
        return collect_queued_experiment(*args, **kwargs)
    return _exp_show_queued(*args, **kwargs)


def collect_queued_experiment(cwd: str = None) -> dict:
    """Show queued experiments using DVC API.

    This uses some internal DVC methods which are not made officially public.
    Therefore, this method can potentially break with any new DVC release.

    Returns
    -------
    exp_names: dict
        A dictionary with {rev: exp_name}. If 'exp_name' was not set, it is None.
    """
    repo = dvc.repo.Repo(root_dir=cwd)

    celery_queue = dvc.repo.experiments.queue.celery.LocalCeleryQueue(repo=repo, ref=None)
    experiments = list(celery_queue.iter_queued())

    return {x.stash_rev: x.name for x in experiments}


def exp_branch(experiment: str, branch: str = None) -> None:
    """Promote an experiment to a branch.

    In comparison to 'dvc exp branch' this also works with queue experiments.

    Parameters
    ----------
    experiment: str
        the name of the experiment
    branch: str
        the name of the branch it will be applied to.
    """
    if branch is None:
        branch = experiment

    repo = git.Repo()

    active_branch = repo.active_branch

    if repo.is_dirty():
        # TODO stash changes?
        raise ValueError("Repo can not be dirty")

    if repo.untracked_files:
        raise ValueError(f"Can not have untracked_files {repo.untracked_files}")

    if branch in repo.references:
        raise ValueError(
            f"Can not create a new branch {branch} because it already exists."
        )

    if CONFIG.use_dvc_api:
        dvc_repo = dvc.repo.Repo()
        dvc_repo.experiments.apply(experiment)
    else:
        repo.git.execute(["dvc", "exp", "apply", experiment])

    repo_branch = repo.create_head(branch)
    repo_branch.checkout()

    if repo.is_dirty():
        repo.git.add(all=True)
        repo.index.commit(f"Applied {experiment}")
    else:
        raise ValueError("The experiment was identical to the working directory.")

    repo.git.checkout(active_branch)


def exp_run_all(jobs: int = 1, **kwargs: dict) -> None:
    """Run 'dvc exp run --run-all' to load experiments."""
    # raises 'daemonic processes are not allowed to have children'
    # dvc.repo.Repo().experiments.reproduce_celery(jobs=jobs)
    subprocess.check_call(["dvc", "exp", "run", "--run-all", "--jobs", str(jobs)])
