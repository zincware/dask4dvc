"""Test the 'dask4dvc' package."""
import dask4dvc


def test_version() -> None:
    """Test Version."""
    assert dask4dvc.__version__ == "0.1.0"
