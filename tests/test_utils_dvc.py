"""Test the 'dask4dvc' CLI."""
import pathlib

import dask_jobqueue
import pytest
import yaml

import dask4dvc


@pytest.mark.parametrize(
    ("targets", "options"),
    (
        (None, None),
        ("InputToOutput", None),
        (None, "--force"),
        ("InputToOutput", "--force"),
        (["InputToOutput"], ["--force"]),
    ),
)
def test_repro(single_node_repo: tuple, targets: list, options: list) -> None:
    """Test 'dask4dvc.utils.dvc.dvc_repro'."""
    _, node = single_node_repo

    dask4dvc.utils.dvc.repro(targets=targets, options=options)

    node = node.load()
    assert node.outputs == 3.1415


def test_dask_get_cluster_from_config(tmp_path: pathlib.Path) -> None:
    """Test 'dask4dvc.utils.dask.get_cluster_from_config'."""
    file = pathlib.Path("dask4dvc.yaml")
    config = {"default": {"PBSCluster": {"cores": 1, "memory": "1GB"}}}
    file.write_text(yaml.safe_dump(config))

    cluster = dask4dvc.utils.dask.get_cluster_from_config(file)
    assert isinstance(cluster, dask_jobqueue.PBSCluster)
