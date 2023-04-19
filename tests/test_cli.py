"""Test the 'dask4dvc' CLI."""
import pytest
from dask.distributed import LocalCluster
from typer.testing import CliRunner

from dask4dvc.cli.main import app
import random

runner = CliRunner()


@pytest.mark.parametrize("repo_fixture", ("single_node_repo", "multi_node_repo"))
@pytest.mark.parametrize(
    "cmd",
    (["repro"],),
)
def test_repro(request: pytest.FixtureRequest, cmd: list, repo_fixture: str) -> None:
    """Test 'dask4dvc repro'."""
    _, nodes = request.getfixturevalue(repo_fixture)

    result = runner.invoke(app, cmd)

    assert result.exit_code == 0
    for node in nodes:
        node = node.load()
        assert node.outputs == 3.1415


@pytest.mark.parametrize("repo_fixture", ("single_node_repo", "multi_node_repo"))
def test_repro_with_address(request: pytest.FixtureRequest, repo_fixture: str) -> None:
    """The with a given dask LocalCluster."""
    _, nodes = request.getfixturevalue(repo_fixture)

    port = random.randrange(start=5000, stop=5500)

    cluster = LocalCluster(scheduler_port=port)
    cluster.adapt()

    result = runner.invoke(app, ["repro", "--address", f"127.0.0.1:{port}"])

    assert result.exit_code == 0
    for node in nodes:
        node = node.load()
        assert node.outputs == 3.1415


def test_version() -> None:
    """Test 'dask4dvc --version'."""
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
