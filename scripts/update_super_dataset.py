import collections
import os
import pathlib
import platform
import shutil
import subprocess

import dandi.dandiapi
import requests

LIMIT_DANDISETS = 5

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", None)
if GITHUB_TOKEN is None:
    message = "GITHUB_TOKEN environment variable not set"
    raise ValueError(message)

BASE_GITHUB_URL = f"https://{GITHUB_TOKEN}@github.com"
BASE_GITHUB_API_URL = "https://api.github.com/repos"
HEADER = {"Authorization": f"token {GITHUB_TOKEN}"}

SYSTEM = platform.system()
if SYSTEM == "Windows":
    # For Cody's local running
    BASE_DIRECTORY = pathlib.Path("E:/GitHub/bids-dandisets")
else:
    # For CI
    BASE_DIRECTORY = pathlib.Path.cwd() / "bids-dandisets"
    BASE_DIRECTORY.mkdir(exist_ok=True)


def run(limit: int | None = None) -> None:
    super_dataset_repo_directory = BASE_DIRECTORY / "super-dataset"
    if not super_dataset_repo_directory.exists():
        git_clone_command = f"git clone {BASE_GITHUB_URL}/bids-dandisets/super-dataset.git"
        _deploy_subprocess(command=git_clone_command, cwd=BASE_DIRECTORY)

    _update_draft(repo_directory=super_dataset_repo_directory)

    # Start from clean slate
    submodule_directories = [
        path for path in super_dataset_repo_directory.iterdir() if path.is_dir() and not path.name.startswith(".")
    ]
    collections.deque((shutil.rmtree(path) for path in submodule_directories), maxlen=0)

    client = dandi.dandiapi.DandiAPIClient()
    dandisets = client.get_dandisets()

    for counter, dandiset in enumerate(dandisets):
        if limit is not None and counter >= limit:
            break

        dandiset_id = dandiset.identifier

        print(f"Creating submodule for Dandiset {dandiset_id}...")

        repo_name = f"bids-dandisets/{dandiset_id}"
        repo_api_url = f"{BASE_GITHUB_API_URL}/{repo_name}"
        response = requests.get(url=repo_api_url, headers=HEADER)
        if response.status_code != 200:
            print(f"Status code {response.status_code}: {response.json()["message"]}")

            if response.status_code == 403:  # TODO: Not sure how to handle this yet
                continue

        datalad_command = f"datalad install https://github.com/{repo_name}"
        _deploy_subprocess(command=datalad_command, cwd=super_dataset_repo_directory)

        print(f"Process complete for Dandiset {dandiset_id}!\n\n")

    _configure_git_repo(repo_directory=super_dataset_repo_directory)
    _push_changes(repo_directory=super_dataset_repo_directory, branch_name="latest")


def _deploy_subprocess(
    *,
    command: str | list[str],
    cwd: str | pathlib.Path | None = None,
    environment_variables: dict[str, str] | None = None,
    error_message: str | None = None,
    ignore_errors: bool = False,
) -> str | None:
    error_message = error_message or "An error occurred while executing the command."

    result = subprocess.run(
        args=command,
        cwd=cwd,
        shell=True,
        env=environment_variables,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0 and ignore_errors is False:
        message = (
            f"\n\nError code {result.returncode}\n"
            f"{error_message}\n\n"
            f"stdout: {result.stdout}\n\n"
            f"stderr: {result.stderr}\n\n"
        )
        raise RuntimeError(message)
    if result.returncode != 0 and ignore_errors is True:
        return None

    return result.stdout


def _configure_git_repo(repo_directory: pathlib.Path) -> None:
    _deploy_subprocess(
        command='git config --local user.email "github-actions[bot]@users.noreply.github.com"', cwd=repo_directory
    )
    _deploy_subprocess(command='git config --local user.name "github-actions[bot]"', cwd=repo_directory)


def _update_draft(repo_directory: pathlib.Path) -> None:
    _deploy_subprocess(command="git checkout draft", cwd=repo_directory)
    _deploy_subprocess(command="git pull", cwd=repo_directory)


def _push_changes(repo_directory: pathlib.Path, branch_name: str) -> None:
    _deploy_subprocess(command="git add .", cwd=repo_directory)
    _deploy_subprocess(command='git commit --message "update"', cwd=repo_directory, ignore_errors=True)

    push_command = "git push" if branch_name == "latest" else f"git push --set-upstream origin {branch_name}"
    _deploy_subprocess(command=push_command, cwd=repo_directory)


if __name__ == "__main__":
    run(limit=LIMIT_DANDISETS)
