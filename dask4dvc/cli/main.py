"""All methods that come directly from 'dask4dvc' CLI interace."""

import importlib.metadata
import logging
import typing
import webbrowser

import dask.distributed
import dvc.repo
import typer

from dask4dvc import dvc_queue, dvc_repro, utils

app = typer.Typer()

log = logging.getLogger(__name__)


class Help:
    """Collect typer help strings that are used multiple times."""

    address: str = (
        "This can be the address of a DASK Scheduler server like '127.0.0.1:31415'. If"
        " 'None' Dask will launch a new Server."
    )
    parallel: str = (
        "Split the DVC Graph into individual Nodes and run them in parallel if possible."
    )
    leave: str = "Ask before stopping the client"
    config: str = "path to config file, e.g. 'dask4dvc.yaml'"
    retries: str = "Number of retries to acquire dvc lock."
    max_workers: str = (
        "Maximum number of workers to use. Using '1' will be the same as 'dvc repro' but"
        " slower."
    )
    dashboard: str = "Open Dask Dashboard in Browser"


@app.command()
def repro(
    targets: typing.List[str] = typer.Argument(None),
    address: str = typer.Option(None, help=Help.address),
    leave: bool = typer.Option(True, help=Help.leave),
    config: str = typer.Option(None, help=Help.config),
    max_workers: int = typer.Option(None, help=Help.max_workers),
    retries: int = typer.Option(10, help=Help.retries),
    force: bool = typer.Option(False, "--force/", "-f/", help="use `dvc repro --force`"),
    dashboard: bool = typer.Option(False, help=Help.dashboard),
) -> None:
    """Replicate 'dvc repro' command using dask."""
    utils.CONFIG.retries = retries

    if config is not None:
        assert address is None, "Can not use address and config file"
        address = utils.dask.get_cluster_from_config(config)

    with dask.distributed.Client(address) as client:
        if dashboard:
            webbrowser.open(client.dashboard_link)
        if max_workers is not None:
            client.cluster.adapt(minimum=1, maximum=max_workers)
        log.info(client)
        results = dvc_repro.parallel_submit(client, targets=targets, force=force)

        utils.dask.wait_for_futures(client, results)
        if not leave:
            utils.main.wait()


@app.command()
def run(
    targets: typing.List[str] = typer.Argument(None),
    address: str = typer.Option(None, help=Help.address),
    config: str = typer.Option(None, help=Help.config),
    dashboard: bool = typer.Option(False, help=Help.dashboard),
    leave: bool = typer.Option(True, help=Help.leave),
    max_workers: int = typer.Option(None, help=Help.max_workers),
    parallel: bool = typer.Option(
        False, help="Run experiments in parallel. Some experiments might fail!"
    ),
) -> None:
    """Run DVC experiments in parallel using dask."""
    # TODO do not wait for results and then submit next, but do all in parallel
    if len(targets) == 0:
        with dvc.repo.Repo() as repo:
            targets = [x.name for x in repo.experiments.celery_queue.iter_queued()]

    if len(targets) == 0:
        typer.echo("No experiments to run.")
        raise typer.Exit()

    if config is not None:
        assert address is None, "Can not use address and config file"
        address = utils.dask.get_cluster_from_config(config)

    typer.echo(f"Running {targets}.")
    with dask.distributed.Client(address) as client:
        if dashboard:
            webbrowser.open(client.dashboard_link)
        if max_workers is not None:
            client.cluster.adapt(minimum=1, maximum=max_workers)
        results = {}

        for target in targets:
            results[target] = dvc_queue.run_single_experiment(target)
            if not parallel:
                utils.dask.wait_for_futures(client, results[target])
        if parallel:
            utils.dask.wait_for_futures(client, results)
        if not leave:
            utils.main.wait()


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
