"""Test the 'dask4dvc' CLI."""
import pytest

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
