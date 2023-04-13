"""global pytest fixtures."""
import os
import pathlib
import subprocess

import dvc.cli
import git
import pytest
import znlib
import zntrack

import dask4dvc.utils.git


def create_dvc_repo() -> None:
    """Git and DVC init."""
    git.Repo.init()
    assert dvc.cli.main(["init"]) == 0


@pytest.fixture
def empty_repo(tmp_path: pathlib.Path) -> pathlib.Path:
    """Empty DVC repo in tmp_path."""
    os.chdir(tmp_path)
    create_dvc_repo()

    return tmp_path


@pytest.fixture
def single_node_repo(empty_repo: pathlib.Path) -> (pathlib.Path, zntrack.Node):
    """DVC repro with a single node."""
    node = znlib.examples.InputToOutput(inputs=3.1415)
    node.write_graph()

    return empty_repo, node


@pytest.fixture
def multi_experiments_repo(empty_repo: pathlib.Path) -> (pathlib.Path, zntrack.Node):
    """Create multiple queued experiments."""
    n_exp = 3

    node = znlib.examples.InputToOutput(inputs=3.1415)
    node.write_graph(run=True)
    dask4dvc.utils.git.update_gitignore(ignore=".dask4dvc")

    repo = git.Repo()
    repo.git.add(all=True)
    repo.index.commit("Initial commit.")

    subprocess.check_call(
        ["dvc", "exp", "run", "--queue", "-S", f"InputToOutput.inputs=range({n_exp})"]
    )

    return empty_repo, node
