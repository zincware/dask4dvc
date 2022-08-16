import pathlib
import typing
import uuid

from dask.distributed import Future

from dask4dvc.utils.graph import get_starting_nodes, iterate_over_nodes, submit_to_dask

__all__ = ["get_starting_nodes", "iterate_over_nodes", "submit_to_dask"]


def wait_for_futures(futures: typing.Dict[str, Future]):
    for future in futures.values():
        _ = future.result()


def get_tmp_dir(uuid_length: int = 8) -> pathlib.Path:
    """Get a random path"""
    return pathlib.Path(f"tmp_{str(uuid.uuid4())[:uuid_length]}")
