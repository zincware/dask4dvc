import logging
import shutil
import subprocess
import typing

import dask.distributed
import typer

import dask4dvc
import dask4dvc.typehints

app = typer.Typer()

log = logging.getLogger(__name__)


@app.command()
def repro(
    tmp: bool = typer.Option(
        False,
        help="Only run experiment in temporary directory and don't load into workspace",
    ),
    wait: bool = typer.Option(True, help="Ask before stopping the client"),
):
    print("Running dvc repro")
    with dask.distributed.Client() as client:
        log.warning("Dask server initialized")
        output = _repro(client)

        dask4dvc.utils.wait_for_futures(output)
        if not tmp:
            log.warning("Loading into workspace")
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
def run(name: str = None) -> None:
    # TODO rename to exp run
    cleanup = True

    if name is None:
        tmp_dirs = dask4dvc.dvc_handling.load_all_exp_to_tmp_dir()

        with dask.distributed.Client() as client:
            log.warning("Starting dask server")
            output = {}
            for tmp_dir in tmp_dirs.values():
                output.update(_repro(client, cwd=tmp_dir, cleanup=cleanup))

            dask4dvc.utils.wait_for_futures(output)
            # clean up exp temp tmp_dirs
            if cleanup:
                for tmp_dir in tmp_dirs.values():
                    shutil.rmtree(tmp_dir)
            answer = input(
                "Press Enter to load experiments and close dask client (yes/no)"
            )

        if answer == "yes":
            for exp_name in tmp_dirs:
                print(50 * "#-")
                print(f"Loading experiment {exp_name}")
                print(50 * "#-")
                dask4dvc.dvc_handling.run_single_exp(exp_name)
            # dask4dvc.dvc_handling.run_all_exp()

        return
    else:
        raise ValueError("reproducing single experiments is currently not possible")
    # log.warning(f"Running experiment {name}")
    # TODO consider stashing the current workspace, then load the exp and run it in
    #  tmp_dir then load the stash again to not modify the workspace.
    # dask4dvc.dvc_handling.load_exp_into_workspace(name)
    # repro(tmp=True)
    # after the experiment finished we can load it into the workspace from runcache
    # this is currently not supported, see https://github.com/iterative/dvc/issues/8121
    # dask4dvc.dvc_handling.run_exp(name)


def version_callback(value: bool):
    """Get the installed zntrack version"""
    if value:
        typer.echo(f"dask4dvc {dask4dvc.__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        None, "--version", callback=version_callback, is_eager=True
    ),
):
    """
    <update me>
    """
    _ = version  # this would be greyed out otherwise
