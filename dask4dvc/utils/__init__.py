import pathlib
import typing
import uuid

from dask.distributed import Future

from dask4dvc.utils.graph import get_starting_nodes, iterate_over_nodes, submit_to_dask

__all__ = ["get_starting_nodes", "iterate_over_nodes", "submit_to_dask"]


def wait_for_futures(futures: typing.Dict[str, Future]):
    """Wait for all given future objects to complete

    Raises
    ------
    This will fail if one of them fails.
    """
    for future in futures.values():
        _ = future.result()


def get_tmp_dir(uuid_length: int = 8) -> pathlib.Path:
    """Get a random path"""
    return pathlib.Path(f"tmp_{str(uuid.uuid4())[:uuid_length]}")


def update_gitignore(
    ignore: str, gitignore: typing.Union[str, pathlib.Path] = ".gitignore"
):
    """Add 'ignore' to the gitignore file, if not already there"""
    gitignore = pathlib.Path(gitignore)
    if gitignore.exists():
        with gitignore.open("r") as f:
            for line in f.readlines():
                if line == ignore:
                    return
    with gitignore.open("a") as f:
        f.write(ignore)


def make_dask4dvc_working_directory() -> pathlib.Path:
    """Create a '.dask4dvc' working directory"""
    working_dir = pathlib.Path(".dask4dvc")
    working_dir.mkdir(exist_ok=True)

    update_gitignore(ignore=".dask4dvc/")
    return working_dir
