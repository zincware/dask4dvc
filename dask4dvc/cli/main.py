"""All methods that come directly from 'dask4dvc' CLI interace."""

import importlib.metadata
import logging
import shutil
import subprocess
import typing

import dask.distributed
import typer

from dask4dvc import methods, utils

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
    option: str = (
        "Additional options to pass to 'dvc repro'. E.g. '--option=--force"
        " --option=--downstream'. Notice that some options like '--force' might show"
        " unexpected behavior."
    )
    target: str = "Names of the stage to reproduce. Leave empty to run all stages."
    detach: str = (
        "Run the process in detached mode (Ctrl + C will not close 'dask4dvc' in the"
        " background)."
    )
    config: str = "path to config file, e.g. 'dask4dvc.yaml'"


@app.command()
def repro(
    address: str = typer.Option(None, help=Help.address),
    option: typing.List[str] = typer.Option(None, help=Help.option),
    leave: bool = typer.Option(True, help=Help.leave),
    detach: bool = typer.Option(False, "--detach", "-d", help=Help.detach),
    config: str = typer.Option(None, help=Help.config),
    target: list[str] = typer.Argument(None, help=Help.target, show_default=False),
    parallel: bool = typer.Option(True, help=Help.parallel),
    max_workers: int = typer.Option(None, help="Maximum number of workers to use."),
) -> None:
    """Replicate 'dvc repro' command using dask."""
    if detach:
        cmd = ["dask4dvc", "repro"]
        if address is not None:
            cmd += ["--address", address]
        if config is not None:
            cmd += ["--config", config]
        _ = subprocess.Popen(cmd, start_new_session=True)
        # TODO add all kwargs!
        return

    if config is not None:
        assert address is None, "Can not use address and config file"
        address = utils.dask.get_cluster_from_config(config)

    with dask.distributed.Client(address) as client:
        if max_workers is not None:
            client.cluster.adapt(minimum=1, maximum=max_workers)
        log.info(client)
        if parallel:
            result = methods.parallel_submit(client)
        else:
            result = client.submit(
                utils.dvc.repro, targets=target, options=option, pure=False
            )

        utils.dask.wait_for_futures(result)
        if not leave:
            utils.main.wait()


@app.command()
def run(
    address: str = typer.Option(None, help=Help.address),
    option: typing.List[str] = typer.Option(None, help=Help.option),
    leave: bool = typer.Option(True, help=Help.leave),
    load: bool = typer.Option(
        True,
        help=(
            "Use 'dvc exp run' to load the experiments from run cache. If this option is"
            " not selected, the experiments will only be available through the run cache"
            " and the queue will not be cleared. Do not use with 'always_changed = True'."
        ),
    ),
    delete: typing.List[str] = typer.Option(
        ["branches", "temp"],
        "-D",
        "--delete",
        help="Remove the temporary branches and directories",
    ),
    detach: bool = typer.Option(False, "--detach", "-d", help=Help.detach),
    config: str = typer.Option(None, help=Help.config),
) -> None:
    """Replicate 'dvc exp run --run-all' command using dask.

    This will run the available experiments in parallel using dask.
    When finished, it will load the experiments using 'dvc exp run --run-all'.
    """
    if config is not None:
        assert address is None, "Can not use address and config file"
        address = utils.dask.get_cluster_from_config(config)

    if detach:
        cmd = ["dask4dvc", "run"]
        if address is not None:
            cmd += ["--address", address]
        if config is not None:
            cmd += ["--config", config]
        # TODO add all kwargs!
        _ = subprocess.Popen(cmd, start_new_session=True)
        return

    with methods.get_experiment_repos(delete=delete) as repos:
        with dask.distributed.Client(address) as client:
            log.info(client)
            results = {
                name: client.submit(
                    utils.dvc.repro,
                    options=option,
                    cwd=repo.working_dir,
                    pure=False,
                    key=f"repro_{name[4:]}",  # cut the 'tmp_' in front
                )
                for name, repo in repos.items()
            }

            utils.dask.wait_for_futures(results)
            if load:
                run_all = client.submit(
                    utils.dvc.exp_run_all, n_jobs=len(results), pure=False, deps=results
                )
                utils.dask.wait_for_futures(run_all)

            if not leave:
                utils.main.wait()


@app.command()
def clean(
    branches: bool = typer.Option(
        False,
        help=(
            "Remove all branches created by 'dask4dvc' / all branches starting with"
            " 'tmp_'."
        ),
    ),
    temp: bool = typer.Option(
        False, help="Remove all temporary clones by removing the '.dask4dvc' directory."
    ),
) -> None:
    """Helpers to clean up 'dask4dvc' if something went wrong."""
    if branches:
        utils.git.remove_tmp_branches()
    if temp:
        shutil.rmtree(".dask4dvc/", ignore_errors=True)


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
