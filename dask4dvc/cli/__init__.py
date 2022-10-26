"""Typer based CLI interface of dask4dvc"""
import logging
import pathlib
import shutil
import typing

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
    parallel: str = (
        "Split the DVC Graph into individual Nodes and run them in parallel if possible."
    )
    wait: str = "Ask before stopping the client"


@app.command()
def repro(
    tmp: bool = typer.Option(
        False,
        help="Only run experiment in temporary directory and don't load into workspace",
    ),
    wait: bool = typer.Option(True, help=Help.wait),
    address: str = typer.Option(
        None,
        help=Help.address,
    ),
    cleanup: bool = typer.Option(True, help=Help.cleanup),
    option: typing.List[str] = typer.Option(
        None,
        help=(
            "Additional options to pass to 'dvc repro'. E.g. '--option=--force"
            " --option=--downstream'. Notice that some options like '--force' might"
            " show unexpected behavior."
        ),
    ),
    parallel: bool = typer.Option(True, help=Help.parallel),
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
            output = _repro(client, cleanup=cleanup, repro_options=option)

            dask4dvc.utils.wait_for_futures(output)
            if not tmp:
                log.info("Loading into workspace")
                result = client.submit(
                    dask4dvc.dvc_handling.run_dvc_repro_in_cwd,
                    node_name=None,
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
                node_name=None,
                pure=False,
                options=option,
            )
            _ = result.result()

        if wait:
            _ = input("Press Enter to close the client")


def _repro(
    client: dask.distributed.Client,
    cwd=None,
    cleanup: bool = True,
    repro_options: list = None,
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
        repro_options=repro_options,
    )


@app.command()
def run(
    address: str = typer.Option(
        None,
        help=Help.address,
    ),
    cleanup: bool = typer.Option(True, help=Help.cleanup),
    parallel: bool = typer.Option(True, help=Help.parallel),
    wait: bool = typer.Option(True, help=Help.wait),
    load: bool = typer.Option(
        True, help="Load experiments from cache into 'dvc exp show' "
    ),
    option: typing.List[str] = typer.Option(
        None,
        help=(
            "Additional options to pass to 'dvc repro'. E.g. '--option=--force"
            " --option=--downstream'. Notice that some options like '--force' might"
            " show unexpected behavior."
        ),
    ),
) -> None:
    working_directory = dask4dvc.utils.make_dask4dvc_working_directory()
    # the directory where all experiments are executed
    cwd = pathlib.Path.cwd()


    queued_exp = dask4dvc.dvc_handling.get_queued_exp_names()

    tmp_dirs = {}
    for exp_name in queued_exp:
        log.debug(f"Preparing directory for experiment '{exp_name}'")
        exp_dir_name = working_directory / exp_name[:8]
        if exp_dir_name.exists():
            shutil.rmtree(exp_dir_name)
        dask4dvc.dvc_handling.load_exp_into_workspace(exp_name, cwd=cwd.as_posix())
        source_repo, target_repo = dask4dvc.dvc_handling.clone(cwd, exp_dir_name)
        dask4dvc.dvc_handling.apply_git_diff(source_repo, target_repo, commit=True)

        tmp_dirs[exp_name] = exp_dir_name

    log.debug(f"Experiments: {tmp_dirs}")

    with dask.distributed.Client(address) as client:
        log.info("Starting dask server")
        output = {}
        for tmp_dir in tmp_dirs.values():
            if parallel:
                # raise ValueError("Parallel currently not supported")
                output.update(_repro(client, cwd=tmp_dir, cleanup=cleanup))
            else:
                output[tmp_dir] = client.submit(
                    dask4dvc.dvc_handling.run_dvc_repro_in_cwd,
                    node_name=None,
                    pure=False,
                    cwd=tmp_dir,
                    checkout=True,
                    options=option,
                )

        dask4dvc.utils.wait_for_futures(output)
        if load:
            log.info("Loading Experiments to be available via 'dvc exp show'")
            for exp_id, exp_name in queued_exp.items():
                log.info(f"Loading experiment {exp_name}")
                # TODO I think this should probably be done on a client submit as well
                # TODO you can use dvc status to check if it only has to be loaded?
                dask4dvc.dvc_handling.run_single_exp(exp_id, name=exp_name)
        if wait:
            _ = input("Press Enter to close the client")
        return


# @app.command()
# def run(
#     name: str = None,
#     address: str = typer.Option(
#         None,
#         help=Help.address,
#     ),
#     cleanup: bool = typer.Option(True, help=Help.cleanup),
#     wait: bool = typer.Option(True, help=Help.wait),
#     load: bool = typer.Option(True, help="Load experiments from cache into 'dvc exp show' ")
# ) -> None:
#     """Replicate 'dvc exp run --run-all'
#
#     Run 'dvc exp run --run-all' with full parallel execution using Dask distributed.
#     """
#     if name is None:
#         tmp_dirs = dask4dvc.dvc_handling.load_all_exp_to_tmp_dir()

#         with dask.distributed.Client(address) as client:
#             log.info("Starting dask server")
#             output = {}
#             for tmp_dir in tmp_dirs.values():
#                 output.update(_repro(client, cwd=tmp_dir, cleanup=cleanup))
#
#             dask4dvc.utils.wait_for_futures(output)
#             # clean up exp temp tmp_dirs
#             if cleanup:
#                 for tmp_dir in tmp_dirs.values():
#                     shutil.rmtree(tmp_dir)
#         if load:
#             for exp_name in tmp_dirs:
#                 log.info(f"Loading experiment {exp_name}")
#                 # TODO I think this should probably be done on a client submit as well
#                 # TODO you can use dvc status to check if it only has to be loaded?
#                 dask4dvc.dvc_handling.run_single_exp(exp_name)
#         if wait:
#             _ = input("Press Enter to close the client")
#         return
#     raise ValueError("reproducing single experiments is currently not possible")
#     # log.warning(f"Running experiment {name}")
#     # TODO consider stashing the current workspace, then load the exp and run it in
#     #  tmp_dir then load the stash again to not modify the workspace.
#     # dask4dvc.dvc_handling.load_exp_into_workspace(name)
#     # repro(tmp=True)
#     # after the experiment finished we can load it into the workspace from runcache
#     # this is currently not supported, see https://github.com/iterative/dvc/issues/8121
#     # dask4dvc.dvc_handling.run_exp(name)


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
