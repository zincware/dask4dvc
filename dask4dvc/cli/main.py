"""All methods that come directly from 'dask4dvc' CLI interace."""

import importlib.metadata
import logging
import typing

import dask.distributed
import typer

from dask4dvc import utils

app = typer.Typer()

log = logging.getLogger(__name__)


class Help:
    """Collect typer help strings that are used multiple times."""

    address: str = (
        "This can be the address of a DASK Scheduler server like '127.0.0.1:31415'. If"
        " 'None' Dask will launch a new Server."
    )
    cleanup: str = "Remove the temporary directories"
    parallel: str = (
        "Split the DVC Graph into individual Nodes and run them in parallel if possible."
    )
    wait: str = "Ask before stopping the client"
    option: str = (
        "Additional options to pass to 'dvc repro'. E.g. '--option=--force"
        " --option=--downstream'. Notice that some options like '--force' might show"
        " unexpected behavior."
    )
    targets: str = "Names of the stage to reproduce"


@app.command()
def repro(
    address: str = typer.Option(None, help=Help.address),
    option: typing.List[str] = typer.Option(None, help=Help.option),
    target: typing.List[str] = typer.Option(None),
) -> None:
    """Replicate 'dvc repro' command using dask."""
    with dask.distributed.Client(address) as client:
        result = client.submit(
            utils.dvc.dvc_repro, targets=target, options=option, pure=False
        )

        utils.dask.wait_for_futures(result)


def version_callback(value: bool) -> None:
    """Get the installed dask4dvc version."""
    if value:
        typer.echo(f"dask4dvc {importlib.metadata.version('dask4dvc')}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        None, "--version", callback=version_callback, is_eager=True
    ),
) -> None:
    """Dask4DVC CLI callback.

    Run the DVC graph or DVC experiments in parallel using dask.

    """
    _ = version  # this would be greyed out otherwise
