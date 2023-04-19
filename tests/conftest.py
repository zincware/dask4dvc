"""global pytest fixtures."""
import os
import pathlib
import typing
import dvc.cli
import git
import pytest
import znlib
import zntrack


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
def single_node_repo(
    empty_repo: pathlib.Path,
) -> typing.Tuple[pathlib.Path, typing.List[zntrack.Node]]:
    """DVC repro with a single node."""
    node = znlib.examples.InputToOutput(inputs=3.1415)
    node.write_graph()

    return empty_repo, [node]


@pytest.fixture
def multi_node_repo(
    empty_repo: pathlib.Path,
) -> typing.Tuple[pathlib.Path, typing.List[zntrack.Node]]:
    """DVC repro with a single node."""
    node = znlib.examples.InputToOutput(inputs=3.1415)
    node.write_graph()

    node2 = znlib.examples.InputToOutput(inputs=node @ "outputs", name="node2")
    node2.write_graph()

    node3 = znlib.examples.InputToOutput(inputs=node2 @ "outputs", name="node3")
    node3.write_graph()

    return empty_repo, [node, node2, node3]
