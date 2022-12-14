"""Test the 'dask4dvc' CLI."""
import pytest
from dask.distributed import LocalCluster
from typer.testing import CliRunner

import dask4dvc.utils.dvc
from dask4dvc.cli.main import app

runner = CliRunner()


@pytest.mark.parametrize(
    "cmd",
    (
        ["repro"],
        ["repro", "--option", "--force"],
        ["repro", "--target", "InputToOutput"],
        ["repro", "--target", "InputToOutput", "--option", "--force"],
    ),
)
def test_repro(single_node_repo: tuple, cmd: list) -> None:
    """Test 'dask4dvc repro'."""
    _, node = single_node_repo

    result = runner.invoke(app, cmd)

    assert result.exit_code == 0
    node = node.load()
    assert node.outputs == 3.1415


def test_repro_with_address(single_node_repo: tuple) -> None:
    """The with a given dask LocalCluster."""
    _, node = single_node_repo

    cluster = LocalCluster(scheduler_port=31415)
    cluster.adapt()

    result = runner.invoke(app, ["repro", "--address", "127.0.0.1:31415"])

    assert result.exit_code == 0
    node = node.load()
    assert node.outputs == 3.1415


def test_version() -> None:
    """Test 'dask4dvc --version'."""
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0


def test_run(multi_experiments_repo: tuple) -> None:
    """Test 'dask4dvc run'."""
    assert len(dask4dvc.utils.dvc.exp_show_queued()) == 3
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 0
    assert len(dask4dvc.utils.dvc.exp_show_queued()) == 0

    # Try to run again
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 0
    assert result.stdout.startswith("Skipping: no experiments were found in the queue.")
