"""Typer based CLI interface of dask4dvc"""
import logging
import pathlib
import subprocess
import typing
import urllib.request

import dask.distributed
import typer

import dask4dvc
import dask4dvc.cli.methods as methods
import dask4dvc.typehints

app = typer.Typer()

log = logging.getLogger(__name__)


class Help:
    """Collect typer help strings that are used multiple times."""

    address: str = (
        "This can be the address of a DASK Scheduler server like '127.0.0.1:8786'. If"
        " None Dask will launch a new Server."
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


@app.command()
def repro(
    tmp: bool = typer.Option(
        False,
        help="Only run experiment in temporary directory and don't load into workspace",
    ),
    wait: bool = typer.Option(False, help=Help.wait),
    address: str = typer.Option(
        None,
        help=Help.address,
    ),
    cleanup: bool = typer.Option(True, help=Help.cleanup),
    option: typing.List[str] = typer.Option(None, help=Help.option),
    parallel: bool = typer.Option(False, help=Help.parallel),
):
    """Replicate 'dvc repro' command

    Run 'dvc repro' with parallel execution through Dask distributed.
    """
    log.debug("Running dvc repro")
    dask4dvc.utils.update_gitignore(ignore=".dask4dvc/")
    # TODO If the files are not git tracked, they won't be in the git diff! so make
    #  sure all relevant files are git tracked

    # TODO add something like force to be only applied to 'initial nodes' so all will be reproduced automatically.
    with dask.distributed.Client(address) as client:
        log.info("Dask server initialized")
        if parallel:
            output = methods.repro(client, cleanup=cleanup, repro_options=option)

            dask4dvc.utils.wait_for_futures(output)
            if not tmp:
                log.info("Loading into workspace")
                result = client.submit(
                    dask4dvc.dvc_handling.run_dvc_repro_in_cwd,
                    deps=output,
                    pure=False,
                    options=option,
                )
                _ = (
                    result.result()
                )  # this should only run if all outputs are gathered successfully
        else:
            result = client.submit(
                dask4dvc.dvc_handling.run_dvc_repro_in_cwd,
                pure=False,
                options=option,
            )
            _ = result.result()

        if wait:
            _ = input("Press Enter to close the client")


@app.command()
def run(
    address: str = typer.Option(
        None,
        help=Help.address,
    ),
    cleanup: bool = typer.Option(True, help=Help.cleanup),
    parallel: bool = typer.Option(False, help=Help.parallel),
    wait: bool = typer.Option(False, help=Help.wait),
    load: bool = typer.Option(
        True, help="Load experiments from cache into 'dvc exp show' "
    ),
    option: typing.List[str] = typer.Option(None, help=Help.option),
) -> None:
    with dask.distributed.Client(address) as client:
        log.info("Starting dask server")

        # TODO why isn't this part of running the experiment? Why do it before?
        tmp_dirs = client.submit(
            methods.prepare_experiment_dirs,
        ).result()

        output = {}
        for tmp_dir in tmp_dirs.values():
            if parallel:
                output.update(
                    methods.repro(
                        client, cwd=tmp_dir, cleanup=cleanup, repro_options=option
                    )
                )
            else:
                output[tmp_dir] = client.submit(
                    dask4dvc.dvc_handling.run_dvc_repro_in_cwd,
                    pure=False,
                    cwd=tmp_dir,
                    checkout=True,
                    options=option,
                )

        dask4dvc.utils.wait_for_futures(output)
        if load:
            methods.run_all(client, n_jobs=len(tmp_dirs))
        if wait:
            _ = input("Press Enter to close the client")
        return


@app.command()
def run_all(
    address: str = typer.Option(
        None,
        help=Help.address,
    ),
    jobs: int = typer.Option(None, help="Run this number of jobs in parallel."),
) -> None:
    """Replace queued experiments by running them.

    This will not parallelize them via DASK! Please use 'dask4dvc repro' or 'dask4dvc run'
    instead. This method will use the `dvc queue` instead.
    """
    with dask.distributed.Client(address) as client:
        methods.run_all(client, jobs)


@app.command()
def clone(
    source: str = typer.Argument(
        ..., help="Repository to clone from", show_default=False
    ),
    target: str = typer.Argument(..., help="target directory / path", show_default=False),
    branch: bool = typer.Option(
        False, "--branch", help="Create a new branch after cloning"
    ),
    branch_name: str = typer.Option(
        None,
        help=(
            "The name of the new branch. If using `--branch` without providing a branch"
            " name the 'target' name is used as branch name. This will imply usage"
            " `--branch`"
        ),
        show_default=False,
    ),
    checkout: bool = typer.Option(
        False, "--checkout", help="Run dvc checkout after cloning"
    ),
    apply_diff: bool = typer.Option(
        False,
        "--apply-diff",
        help=(
            "Apply a patch git diff <src> after cloning to copy all changed files as well"
        ),
    ),
) -> None:
    """Make a copy the source DVC repository at the target destination

    This method will
    - clone your repository \n
    - direct the run-cache on the remote repository to the one on the source \n
    - apply all changes of the current workspace to the target repository \n

    Optionally, it will create a new branch on the target repository based on the name
    of the directory.
    """
    source = pathlib.Path(source)
    source_repo, target_repo = dask4dvc.dvc_handling.clone(
        pathlib.Path(source), pathlib.Path(target)
    )
    if branch or branch_name:
        if branch_name is None:
            branch_name = pathlib.Path(target).name
        target_branch = target_repo.create_head(branch_name)
        target_branch.checkout()
    if apply_diff:
        dask4dvc.dvc_handling.apply_git_diff(source_repo, target_repo)
    if checkout:
        target_repo.git.execute(["dvc", "checkout"])

    # TODO remove repository if anything fails


def version_callback(value: bool):
    """Get the installed dask4dvc version"""
    if value:
        typer.echo(f"dask4dvc {dask4dvc.__version__}")
        raise typer.Exit()


@app.command()
def init(add_ignore: bool = True):
    """Create an empty GIT/DVC Repository"""
    subprocess.check_call(["git", "init"])
    subprocess.check_call(["dvc", "init"])
    if add_ignore:
        gitignore = pathlib.Path(".gitignore")
        with urllib.request.urlopen(
            "https://raw.githubusercontent.com/github/gitignore/main/Python.gitignore"
        ) as url:
            gitignore.write_text(url.read().decode("utf-8"))

    subprocess.check_call(["git", "add", "."])
    subprocess.check_call(["git", "commit", "-m", "initial commit (by dask4dvc)"])


@app.callback()
def main(
    version: bool = typer.Option(
        None, "--version", callback=version_callback, is_eager=True
    ),
):
    """Dask4DVC

    Run the DVC graph or DVC experiments in parallel using dask.
    """
    _ = version  # this would be greyed out otherwise
