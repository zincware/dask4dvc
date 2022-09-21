"""Typer based CLI interface of dask4dvc"""
import logging
import pathlib
import shutil

import dask.distributed
import typer

import dask4dvc
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


@app.command()
def repro(
    tmp: bool = typer.Option(
        False,
        help="Only run experiment in temporary directory and don't load into workspace",
    ),
    wait: bool = typer.Option(True, help="Ask before stopping the client"),
    address: str = typer.Option(
        None,
        help=Help.address,
    ),
    cleanup: bool = typer.Option(True, help=Help.cleanup),
):
    """Replicate 'dvc repro' command

    Run 'dvc repro' with parallel execution through Dask distributed.
    """
    log.debug("Running dvc repro")
    # TODO If the files are not git tracked, they won't be in the git diff! so make
    #  sure all relevant files are git tracked
    with dask.distributed.Client(address) as client:
        log.info("Dask server initialized")
        output = _repro(client, cleanup=cleanup)

        dask4dvc.utils.wait_for_futures(output)
        if not tmp:
            log.info("Loading into workspace")
            result = client.submit(
                dask4dvc.dvc_handling.run_dvc_repro_in_cwd,
                node_name=None,
                deps=output,
                pure=False,
            )
            _ = (
                result.result()
            )  # this should only run if all outputs are gathered successfully

        if wait:
            _ = input("Press Enter to close the client")


def _repro(
    client: dask.distributed.Client, cwd=None, cleanup: bool = True
) -> dask4dvc.typehints.FUTURE_DICT:
    """replicate dvc repro with a given client"""
    # TODO what if the CWD is not where the repo is. E.g. if the worker is launchend in a different directory?
    graph = dask4dvc.dvc_handling.get_dvc_graph(cwd=cwd)  # should work correctly in cwd
    node_pairs = dask4dvc.utils.iterate_over_nodes(graph)  # this only gives names

    return dask4dvc.utils.submit_to_dask(
        client,
        node_pairs=node_pairs,
        cmd=dask4dvc.dvc_handling.submit_dvc_stage,
        cwd=cwd,
        cleanup=cleanup,
    )


@app.command()
def run(
    name: str = None,
    address: str = typer.Option(
        None,
        help=Help.address,
    ),
    cleanup: bool = typer.Option(True, help=Help.cleanup),
) -> None:
    """Replicate 'dvc exp run --run-all'

    Run 'dvc exp run --run-all' with full parallel execution using Dask distributed.
    """
    if name is None:
        tmp_dirs = dask4dvc.dvc_handling.load_all_exp_to_tmp_dir()

        with dask.distributed.Client(address) as client:
            log.info("Starting dask server")
            output = {}
            for tmp_dir in tmp_dirs.values():
                output.update(_repro(client, cwd=tmp_dir, cleanup=cleanup))

            dask4dvc.utils.wait_for_futures(output)
            # clean up exp temp tmp_dirs
            if cleanup:
                for tmp_dir in tmp_dirs.values():
                    shutil.rmtree(tmp_dir)
                answer = input(
                    "Press Enter to load experiments and close dask client (yes/no) "
                )

        if answer == "yes":
            for exp_name in tmp_dirs:
                log.info(f"Loading experiment {exp_name}")
                # TODO I think this should probably be done on a client submit as well
                # TODO you can use dvc status to check if it only has to be loaded?
                dask4dvc.dvc_handling.run_single_exp(exp_name)
        return
    raise ValueError("reproducing single experiments is currently not possible")
    # log.warning(f"Running experiment {name}")
    # TODO consider stashing the current workspace, then load the exp and run it in
    #  tmp_dir then load the stash again to not modify the workspace.
    # dask4dvc.dvc_handling.load_exp_into_workspace(name)
    # repro(tmp=True)
    # after the experiment finished we can load it into the workspace from runcache
    # this is currently not supported, see https://github.com/iterative/dvc/issues/8121
    # dask4dvc.dvc_handling.run_exp(name)


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
) -> None:
    """Make a copy the source DVC repository at the target destination

    This method will
    - clone your repository \n
    - direct the run-cache on the remote repository to the one on the source \n
    - apply all changes of the current workspace to the target repository \n

    Optionally, it will create a new branch on the target repository based on the name
    of the directory.
    """
    target_repo = dask4dvc.dvc_handling.clone(pathlib.Path(source), pathlib.Path(target))
    if branch or branch_name:
        if branch_name is None:
            branch_name = pathlib.Path(target).name
        target_branch = target_repo.create_head(branch_name)
        target_branch.checkout()
    if checkout:
        target_repo.git.execute(["dvc", "checkout"])

    # TODO remove repository if anything fails


def version_callback(value: bool):
    """Get the installed dask4dvc version"""
    if value:
        typer.echo(f"dask4dvc {dask4dvc.__version__}")
        raise typer.Exit()


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
