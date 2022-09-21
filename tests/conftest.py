import git
import pytest


@pytest.fixture()
def dvc_repository(tmp_path) -> git.Repo:
    repo_path = tmp_path / "main"

    repo = git.Repo.init(repo_path)
    repo.init()
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
