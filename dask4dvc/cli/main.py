"""All methods that come directly from 'dask4dvc' CLI interace."""

import importlib.metadata
import logging
import subprocess
import typing
import webbrowser

import dask.distributed
import dvc.cli
import dvc.repo
import typer

from dask4dvc import dvc_repro
from dask4dvc.utils.dask import get_cluster_from_config, wait_for_futures

app = typer.Typer()

log = logging.getLogger(__name__)


class Help:
    """Collect typer help strings that are used multiple times."""

    address: str = (
        "This can be the address of a DASK Scheduler server like '127.0.0.1:31415'. If"
        " 'None' Dask will launch a new Server."
    )
    leave: str = "Ask before stopping the client"
    config: str = "path to config file, e.g. 'dask4dvc.yaml'"
    max_workers: str = (
        "Maximum number of workers to use. Using '1' will be the same as 'dvc repro' but"
        " slower."
    )
    dashboard: str = "Open Dask Dashboard in Browser"


@app.command()
def clean() -> None:
    """Remove all dask4dvc experiments from the queue."""
    dvc_repro.remove_experiments()


@app.command()
def repro(
    targets: typing.List[str] = typer.Argument(
        None, help="Name of stages to reproduce. Leave emtpy to run the full graph."
    ),
    address: str = typer.Option(None, help=Help.address),
    leave: bool = typer.Option(True, help=Help.leave),
    config: str = typer.Option(None, help=Help.config),
    max_workers: int = typer.Option(None, help=Help.max_workers),
    dashboard: bool = typer.Option(False, help=Help.dashboard),
    option: typing.List[str] = typer.Option(
        None, "-o", "--option", help="Additional dvc repro options"
    ),
    cleanup: bool = typer.Option(True, help="Remove temporary experiments when done"),
) -> None:
    """Replicate 'dvc repro' command using dask."""
    if len(option) != 0:
        typer.echo("Additional dvc repro options are not implemented yet")
        raise typer.Exit(1)

    repo = dvc.repo.Repo()
    stages = dvc_repro.queue_consecutive_stages(repo, targets, option)

    if config is not None:
        assert address is None, "Can not use address and config file"
        address = get_cluster_from_config(config)

    with dask.distributed.Client(address) as client:
        if dashboard:
            webbrowser.open(client.dashboard_link)
        if max_workers is not None:
            client.cluster.adapt(minimum=1, maximum=max_workers)
        log.info(client)

        mapping, experiments = dvc_repro.parallel_submit(client, repo, stages)

        wait_for_futures(client, mapping)
        if all(x.status == "finished" for x in mapping.values()):
            log.info("All stages finished successfully")
            # dvc.cli.main(["exp", "apply", experiments[-1]])
            dask.distributed.wait(
                client.submit(subprocess.check_call, ["dvc", "repro", *targets])
            )
        if cleanup:
            dvc_repro.remove_experiments(experiments)

        if not leave:
            _ = input("Press Enter to close the client")


@app.command()
def run(
    targets: typing.List[str] = typer.Argument(
        None, help="Name of the DVC experiments to reproduce. Leave emtpy to run all."
    ),
    address: str = typer.Option(None, help=Help.address),
    leave: bool = typer.Option(True, help=Help.leave),
    config: str = typer.Option(None, help=Help.config),
    max_workers: int = typer.Option(None, help=Help.max_workers),
    dashboard: bool = typer.Option(False, help=Help.dashboard),
) -> None:
    """Replicate 'dvc queue start' using dask."""
    if len(targets) == 0:
        targets = None

    repo = dvc.repo.Repo()

    if config is not None:
        assert address is None, "Can not use address and config file"
        address = get_cluster_from_config(config)

    with dask.distributed.Client(address) as client:
        if dashboard:
            webbrowser.open(client.dashboard_link)
        if max_workers is not None:
            client.cluster.adapt(minimum=1, maximum=max_workers)
        log.info(client)

        mapping, _ = dvc_repro.experiment_submit(client, repo, targets)

        wait_for_futures(client, mapping)
        # dvc_repro.remove_experiments(experiments)

        if not leave:
            _ = input("Press Enter to close the client")


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
