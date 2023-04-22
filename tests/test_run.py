"""Test the 'dask4dvc' CLI."""
import pathlib
import random
import typing

import git
import pytest
import zntrack
from typer.testing import CliRunner
from zntrack.project.zntrack_project import Experiment

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


@pytest.fixture
def queued_experiments_repo(repo_path: pathlib.Path) -> typing.List[Experiment]:
    """Prepare a repo with queued experiments."""
    with zntrack.Project(automatic_node_names=True) as project:
        data1 = CreateData(inputs=1)
        data2 = CreateData(inputs=2)

        InputsToOutputs(inputs=data1.output)
        InputsToOutputs(inputs=data2.output)

    project.run()

    repo = git.Repo()
    repo.git.add(all=True)
    repo.index.commit("Initial Commit")

    with project.create_experiment() as exp1:
        data1.inputs = 3
        data2.inputs = 4

    with project.create_experiment() as exp2:
        data1.inputs = 5
        data2.inputs = 6

    return [exp1, exp2]


def test_run_single_experiment(queued_experiments_repo: typing.List[Experiment]) -> None:
    """Test the 'dask4dvc run-single-experiment' command."""
    exp1, exp2 = queued_experiments_repo
    result = runner.invoke(app, ["run", exp1.name])
    assert result.exit_code == 0

    exp1["InputsToOutputs"].output == 3
    exp1["InputsToOutputs_1"].output == 4

    with pytest.raises(Exception):  # dvc.scm.RevError
        exp2["InputsToOutputs"].output == 5
        exp2["InputsToOutputs_1"].output == 6


def test_run_multiple_experiments(
    queued_experiments_repo: typing.List[Experiment],
) -> None:
    """Test the 'dask4dvc run-multiple-experiments' command."""
    exp1, exp2 = queued_experiments_repo
    result = runner.invoke(app, ["run", exp1.name, exp2.name])
    assert result.exit_code == 0

    exp1["InputsToOutputs"].output == 3
    exp1["InputsToOutputs_1"].output == 4

    exp2["InputsToOutputs"].output == 5
    exp2["InputsToOutputs_1"].output == 6


def test_run_all_experiments(
    queued_experiments_repo: typing.List[Experiment],
) -> None:
    """Test the 'dask4dvc run-multiple-experiments' command."""
    exp1, exp2 = queued_experiments_repo
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 0

    exp1["InputsToOutputs"].output == 3
    exp1["InputsToOutputs_1"].output == 4

    exp2["InputsToOutputs"].output == 5
    exp2["InputsToOutputs_1"].output == 6
