"""Utils that are related to GIT and DVC.

This includes cloning, setting up the cache, managing temporary directories.
"""

import pathlib
import typing

import git


def update_gitignore(
    ignore: str, gitignore: typing.Union[str, pathlib.Path] = ".gitignore"
) -> bool:
    """Add 'ignore' to the gitignore file, if not already there.

    Returns
    -------
    bool, true if the gitignore was updated.
    """
    repo = git.Repo(".")
    if repo.ignored(ignore):
        return False

    gitignore = pathlib.Path(gitignore)

    with gitignore.open("a", encoding="utf-8") as file:
        file.write("\n# dask4dvc temporary directory \n")
        file.write(ignore)
        file.write("\n")
    return True


def remove_tmp_branches() -> None:
    """Remove all branches starting with 'tmp_'."""
    repo = git.Repo()
    for branch in repo.heads:
        if branch.name.startswith("tmp_"):
            repo.delete_head(branch, force=True)
