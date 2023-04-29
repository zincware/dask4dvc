"""Test the 'dask4dvc' CLI."""
import pathlib
import random

import dvc.cli
import git
import pytest
import zntrack
from typer.testing import CliRunner

from dask4dvc.cli.main import app

runner = CliRunner()


class ReadFile(zntrack.Node):
    """Read a file."""

    file: str = zntrack.dvc.deps()
    output: str = zntrack.zn.outs()

    def run(self) -> None:
        """ZnTrack run method."""
        with open(self.file, "r") as f:
            self.output = f.read()


class CreateData(zntrack.Node):
    """Create some data."""

    inputs = zntrack.zn.params()
    output = zntrack.zn.outs()

    def run(self) -> None:
        """ZnTrack run method."""
        self.output = self.inputs


class InputsToOutputs(zntrack.Node):
    """Create some data."""

    inputs = zntrack.zn.deps()
    output = zntrack.zn.outs()

    def run(self) -> None:
        """ZnTrack run method."""
        self.output = self.inputs


class RandomData(zntrack.Node):
    """Create some random."""

    output = zntrack.zn.outs()

    def run(self) -> None:
        """ZnTrack run method."""
        self.output = random.random()


def test_single_node_repro(repo_path: pathlib.Path) -> None:
    """Test repro of a single node."""
    with zntrack.Project() as project:
        node = CreateData(inputs=3.1415)
    project.run(repro=False)

    repo = git.Repo()
    repo.git.add(all=True)
    repo.index.commit("Initial Commit")

    result = runner.invoke(app, ["repro"])
    assert result.exit_code == 0
    node.load()
    # DVCFileSystem can load from cache ?!? WOW!
    # import dvc.cli
    # dvc.cli.main(["repro"])
    # assert pathlib.Path("nodes", "CreateData", "output.json").exists()
    node.output == 3.1415


def test_multi_node_repro(repo_path: pathlib.Path) -> None:
    """Test repro of multiple nodes."""
    with zntrack.Project(automatic_node_names=True) as project:
        data1 = CreateData(inputs=3.1415)
        data2 = CreateData(inputs=2.7182)

        node1 = InputsToOutputs(inputs=data1.output)
        node2 = InputsToOutputs(inputs=data2.output)

    project.run(repro=False)

    repo = git.Repo()
    repo.git.add(all=True)
    repo.index.commit("Initial Commit")

    result = runner.invoke(app, ["repro"])
    assert result.exit_code == 0

    node1.load()
    node2.load()

    assert node1.output == 3.1415
    assert node2.output == 2.7182


def test_multi_node_repro_targets(repo_path: pathlib.Path) -> None:
    """Test repro of selected nodes."""
    with zntrack.Project(automatic_node_names=True) as project:
        data1 = CreateData(inputs=3.1415)
        data2 = CreateData(inputs=2.7182)

        node1 = InputsToOutputs(inputs=data1.output)
        node2 = InputsToOutputs(inputs=data2.output)

    project.run(repro=False)

    repo = git.Repo()
    repo.git.add(all=True)
    repo.index.commit("Initial Commit")

    result = runner.invoke(app, ["repro", node1.name])
    assert result.exit_code == 0

    node1.load()
    with pytest.raises(AttributeError):
        node2.load(lazy=False)
        # TODO ZnTrack: should not require lazy for the error upton access

    assert node1.output == 3.1415


# def test_single_node_repro_force(repo_path: pathlib.Path) -> None:
#     """Test repro of a single node."""
#     with zntrack.Project() as project:
#         node = RandomData()
#     project.run(repro=False)

#     repo = git.Repo()
#     repo.git.add(all=True)
#     repo.index.commit("Initial Commit")

#     result = runner.invoke(app, ["repro"])
#     assert result.exit_code == 0
#     node.load(lazy=False)

#     result = runner.invoke(app, ["repro"])
#     assert result.exit_code == 0

#     node2 = RandomData.from_rev(lazy=False)
#     assert node2.output == pytest.approx(node.output)

#     result = runner.invoke(app, ["repro", "--option=--force"])
#     assert result.exit_code == 0

#     node2 = RandomData.from_rev()
#     assert node2.output != pytest.approx(node.output)


def test_single_node_file_deps(repo_path: pathlib.Path) -> None:
    """Test repro of a single node with file deps."""
    with open("test.txt", "w") as f:
        f.write("Hello World")
    assert dvc.cli.main(["add", "test.txt"]) == 0

    with zntrack.Project() as project:
        node = ReadFile(file="test.txt")
    project.run(repro=False)

    repo = git.Repo()
    repo.git.add(all=True)
    repo.index.commit("Initial Commit")

    result = runner.invoke(app, ["repro"])
    assert result.exit_code == 0

    node.load(lazy=False)
    assert node.output == "Hello World"

    # # Now force and target
    # result = runner.invoke(app, ["repro", "ReadFile", "--option=--force"])
    # assert result.exit_code == 0

    # node.load(lazy=False)
    # assert node.output == "Hello World"


def test_multi_complex_graph(repo_path: pathlib.Path) -> None:
    """Test a more complex path that failed before."""
    with zntrack.Project(automatic_node_names=True) as project:
        data1 = CreateData(inputs=3.1415)
        data2 = CreateData(inputs=2.7182)

        node1 = InputsToOutputs(inputs=[data1.output, data2.output])
        node2 = InputsToOutputs(inputs=node1.output)
        node3 = InputsToOutputs(inputs=node1.output)

        node4 = InputsToOutputs(inputs=[node2.output, node3.output])

        InputsToOutputs(inputs=[node4.output, node2.output])

    project.run(repro=False)

    repo = git.Repo()
    repo.git.add(all=True)
    repo.index.commit("Initial Commit")

    result = runner.invoke(app, ["repro"])
    assert result.exit_code == 0


@pytest.mark.skip(reason="very slow and no additional coverage")
def test_massiv_parallel_graph(repo_path: pathlib.Path) -> None:
    """Test a larger graph."""
    with zntrack.Project(automatic_node_names=True) as project:
        data = [CreateData(inputs=i) for i in range(50)]
        io = [InputsToOutputs(inputs=data[i].output) for i in range(50)]

    project.run(repro=False)

    repo = git.Repo()
    repo.git.add(all=True)
    repo.index.commit("Initial Commit")

    result = runner.invoke(app, ["repro"])
    assert result.exit_code == 0

    for i in range(50):
        io[i].load()
        assert io[i].output == i
