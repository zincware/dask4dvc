import pathlib

import git
import pytest

FILE = pathlib.Path(__file__).resolve()


@pytest.fixture()
def dvc_repository(tmp_path) -> git.Repo:
    repo = git.Repo.init(tmp_path / "main", mkdir=True)
    repo_path = pathlib.Path(repo.working_dir)

    repo.git.execute(["dvc", "init"])
    repo.git.execute(
        [
            "dvc",
            "stage",
            "add",
            "-n",
            "write_file",
            "-d",
            "script",
            "-o",
            "file.txt",
            "echo Hello World > file.txt",
        ]
    )
    (repo_path / "script").write_text("Lorem Ipsum")
    repo.index.add(["dvc.yaml", ".dvcignore", "script"])
    repo.index.commit("Initial commit")
    return repo


@pytest.fixture()
def examples() -> pathlib.Path:
    """Path to a File with simple test nodes"""
    return FILE.parent / "examples.py"
