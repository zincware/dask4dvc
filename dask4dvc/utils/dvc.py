"""Utils that are related to 'DVC'."""
import subprocess
import typing


def dvc_repro(
    targets: typing.Union[str, list] = None,
    options: typing.Union[str, list] = None,
    cwd: str = None,
    **kwargs: dict
) -> None:
    """Run 'dvc repro'.

    Parameters
    ----------
    targets: list|str
        Names of the stage to reproduce
    options: list|str, default = None
        A list of additional 'dvc repro' arguments such as '--force' that will be
        passed to the subprocess call.
    cwd: str
        working directory
    kwargs: dict
        required for DASK graph to be built. Typically, these would be some
        dependencies from 'dask.distributed.Future'

    Raises
    ------
    subprocess.CalledProcessError: if dvc cmd fails
    """
    if targets is None:
        targets = []
    elif isinstance(targets, str):
        targets = [targets]
    if options is None:
        options = []
    elif isinstance(options, str):
        options = [options]

    cmd = ["dvc", "repro"] + targets + options
    subprocess.check_call(cmd, cwd=cwd)
