"""Everything related to DVC"""
import json
import pathlib
import shutil
import subprocess
import typing

import networkx as nx
import pydot

import dask4dvc.utils


def get_dvc_graph(cwd=None) -> nx.DiGraph:
    """Use the dvc dag command to get the graph into networkx format

    Running 'dvc dag' can be slow on bigger graphs.
    """
    dot_string = subprocess.run(
        ["dvc", "dag", "--dot"], capture_output=True, check=True, cwd=cwd
    )
    dot_string = dot_string.stdout.decode()
    dot_graph = pydot.graph_from_dot_data(dot_string)[0]
    return nx.DiGraph(nx.nx_pydot.from_pydot(dot_graph))


def run_dvc_repro_in_cwd(node_name: str, cwd=None, deps=None) -> None:
    """Run the dvc repro cmd for a selected stage in a given cwd

    Parameters
    ----------
    node_name: str
        Name of the stage to be run
    cwd: str
        working directory

    deps: list[dask.distributed.Future]|Future
        Any dependencies for the dask graph

    Raises
    -------
    subprocess.CalledProcessError: if dvc cmd fails

    """
    if node_name is not None:
        subprocess.check_call(["dvc", "repro", node_name], cwd=cwd)
    else:
        subprocess.check_call(["dvc", "repro"], cwd=cwd)


def prepare_dvc_workspace(
    name: str = None, cwd=None, commit: bool = False, cleanup: bool = True
) -> pathlib.Path:
    """Prepare a DVC workspace copy in a temporary directory

    Attributes
    ----------
    cwd: pathlib.Path, (optional)
        The working directory to start from.
    commit: bool, (default=False)
        Apply the patch and commit the changes.
    cleanup: bool, (default=True)
        Remove the patch file

    Returns
    -------
    cwd: pathlib.Path
        A directory which contains a clone of the cwd repository.
        The DVC cache is set to the cwd cache. # TODO what if the cache was moved

    """
    # TODO run in single temporary directory which is git added to avoid crowding the CWD with potentially many Nodes?
    if cwd is None:
        cwd = pathlib.Path().cwd()
    if name is None:
        tmp_dir = cwd / dask4dvc.utils.get_tmp_dir()
    else:
        tmp_dir = cwd / pathlib.Path(f"tmp_{name}")

    tmp_dir.mkdir(exist_ok=False)

    subprocess.check_call(["git", "clone", cwd, tmp_dir.resolve()])
    subprocess.check_call(
        ["dvc", "cache", "dir", pathlib.Path().cwd() / ".dvc" / "cache"], cwd=tmp_dir
    )
    # !! this has to be set to the true cwd work directory instead of the cwd that is
    #   given, which may not be to true cwd

    # check if git diff is None
    git_diff = subprocess.run(["git", "diff"], capture_output=True, cwd=cwd)
    if git_diff.stdout == b"":
        return tmp_dir
    patch_file = tmp_dir / "patch"
    patch_file.write_text(git_diff.stdout.decode("utf-8"))
    subprocess.check_call(["git", "apply", "patch"], cwd=tmp_dir)
    if cleanup:
        patch_file.unlink()
    if commit:
        subprocess.check_call(["git", "add", "."], cwd=tmp_dir)
        subprocess.check_call(["git", "commit", "-m", "apply patch"], cwd=tmp_dir)

    return tmp_dir


def submit_dvc_stage(
    name: str, deps=None, cwd: pathlib.Path = None, cleanup: bool = True
):
    """Run a DVC stage

    1. prepare a temporary workspace
    2. run 'dvc repro'
    3. if requested, cleanup the workspace

    Parameters
    ----------
    name: str
        Name of the Node
    deps: list[dask.distributed.Future]|Future
        any dependencies for Dask to build the graph
    cwd: pathlib.Path
        The working directory for the repro command.
        Will be None for 'dvc repro' and set to a custom directory for e.g. 'dvc exp run'
    cleanup: bool, default=True
        Remove the workspace after execution. Will also remove if an error occurs.
    """
    # cwd is None if repro, cwd is set when using exp
    tmp_dir = prepare_dvc_workspace(cwd=cwd, cleanup=cleanup)  # dask4dvc repro
    try:
        run_dvc_repro_in_cwd(node_name=name, cwd=tmp_dir.as_posix())
    except subprocess.CalledProcessError as err:
        # remove the tmp directory even if failed
        if cleanup:
            shutil.rmtree(tmp_dir)
        raise subprocess.CalledProcessError(
            returncode=err.returncode, cmd=err.cmd
        ) from err
    if cleanup:
        shutil.rmtree(tmp_dir)


def load_all_exp_to_tmp_dir() -> typing.Dict[str, pathlib.Path]:
    """Load all queued experiments into temporary directories each.

    Returns
    -------
    A dictionary of the created directories with the experiment names as keys.
    """
    queued_exp = get_queued_exp_names()
    tmp_dirs = {}
    for exp_name in queued_exp:
        load_exp_into_workspace(exp_name)
        tmp_dirs[exp_name] = prepare_dvc_workspace(
            name=exp_name[:8], commit=True  # limit exp name to 8 digits
        )

    return tmp_dirs


def load_exp_into_workspace(name: str):
    """Load the given experiment into the workspace"""
    subprocess.check_call(["dvc", "exp", "apply", name])


def run_all_exp() -> None:
    # TODO because this should never actually compute something we can use arbitrary cores
    subprocess.check_call(["dvc", "exp", "run", "--run-all", "--jobs", "8"])


def run_single_exp(queue_id: str) -> None:
    """Run a single experiment. This will modify your workspace"""
    # see https://github.com/iterative/dvc/issues/8121
    subprocess.check_call(["dvc", "exp", "apply", queue_id])
    subprocess.check_call(["dvc", "exp", "run"])
    subprocess.check_call(["dvc", "queue", "remove", queue_id])


def load_exp_to_dict() -> dict:
    """Convert 'dvc exp show' to a python dict"""
    json_dict = subprocess.run(
        ["dvc", "exp", "show", "--json"], capture_output=True, check=True
    )

    return json.loads(json_dict.stdout.decode("utf-8"))


def get_queued_exp_names() -> list:
    """Get all currently queued experiments (names)"""
    exp_dict = load_exp_to_dict()
    # I don't understand why they separate this into workspace and some hash?
    base_key = [x for x in exp_dict if x != "workspace"][0]

    exp_names = []
    for exp_name in exp_dict[base_key]:
        if exp_name == "baseline":
            continue

        if exp_dict[base_key][exp_name]["data"]["queued"]:
            exp_names.append(exp_name)

    return exp_names
