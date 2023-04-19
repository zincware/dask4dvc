"""General 'dask4dvc' utils."""


def wait() -> None:
    """Wait until user input."""
    _ = input("Press Enter to close the client")
