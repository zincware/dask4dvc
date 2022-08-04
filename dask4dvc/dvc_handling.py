"""Everything related to DVC"""
import pathlib
import shutil
import subprocess

import networkx as nx
import pydot

import dask4dvc.utils


def get_dvc_graph() -> nx.DiGraph:
    """Use the dvc dag command to get the graph into networkx format"""
    dot_string = subprocess.run(["dvc", "dag", "--dot"], capture_output=True, check=True)
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

    Raises
    -------
    subprocess.CalledProcessError: if dvc cmd fails

    """
    if node_name is not None:
        subprocess.check_call(["dvc", "repro", node_name], cwd=cwd)
    else:
        subprocess.check_call(["dvc", "repro"], cwd=cwd)


def prepare_dvc_workspace() -> pathlib.Path:
    """Prepare a DVC workspace copy in a temporary directory

    Returns
    -------
    tmp_dir: pathlib.Path
        A directory which contains a clone of the cwd repository.
        The DVC cache is set to the cwd cache. # TODO what if the cache was moved

    """
    # !! commit all changes
    cwd = pathlib.Path().cwd()
    tmp_dir = dask4dvc.utils.get_tmp_dir()

    tmp_dir.mkdir(exist_ok=False)

    subprocess.check_call(["git", "clone", cwd, tmp_dir.resolve()])
    subprocess.check_call(["dvc", "cache", "dir", cwd / ".dvc" / "cache"], cwd=tmp_dir)

    return tmp_dir


def submit_dvc_stage(node_name, deps=None):
    tmp_dir = prepare_dvc_workspace()
    try:
        run_dvc_repro_in_cwd(node_name=node_name, cwd=tmp_dir.as_posix())
    except subprocess.CalledProcessError as err:
        # remove the tmp directory even if failed
        shutil.rmtree(tmp_dir)
        raise subprocess.CalledProcessError(
            returncode=err.returncode, cmd=err.cmd
        ) from err
    shutil.rmtree(tmp_dir)
