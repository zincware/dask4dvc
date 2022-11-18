"""global pytest fixtures."""
import os
import pathlib
import subprocess

import pytest
import znlib
import zntrack


def create_dvc_repo() -> None:
    """Git and DVC init."""
    subprocess.check_call(["git", "init"])
    subprocess.check_call(["dvc", "init"])


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
