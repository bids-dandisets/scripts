import os
import pathlib
import subprocess

import dandi.dandiapi
import requests

GITHUB_TOKEN = os.environ.get("_GITHUB_API_KEY", None)
if GITHUB_TOKEN is None:
    message = "`_GITHUB_API_KEY` environment variable not set"
    raise ValueError(message)
os.environ["GITHUB_TOKEN"] = GITHUB_TOKEN

BASE_DIRECTORY = pathlib.Path("E:/GitHub/bids-dandisets/work")

BASE_GITHUB_API_URL = "https://api.github.com/repos"
BASE_GITHUB_URL = f"https://{GITHUB_TOKEN}@github.com"
AUTHENTICATION_HEADER = {"Authorization": f"token {GITHUB_TOKEN}"}


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


def reset_excess_branches() -> None:
    client = dandi.dandiapi.DandiAPIClient()
    dandisets = client.get_dandisets()

    for dandiset in dandisets:
        dandiset_id = dandiset.identifier

        repo_name = f"bids-dandisets/{dandiset_id}"
        repo_api_url = f"{BASE_GITHUB_API_URL}/{repo_name}"
        response = requests.get(url=repo_api_url, headers=AUTHENTICATION_HEADER)
        if response.status_code != 200:
            print(f"Status code {response.status_code}: {response.json()["message"]}")
            continue

        repo_directory = BASE_DIRECTORY / dandiset_id
        if not repo_directory.exists():
            print(f"Cloning Dandiset {dandiset_id}...")
            repo_name = f"bids-dandisets/{dandiset_id}"
            repo_url = f"{BASE_GITHUB_URL}/{repo_name}"
            _deploy_subprocess(command=f"git clone {repo_url}", cwd=BASE_DIRECTORY, ignore_errors=True)

        print(f"Cleaning excess branches on Dandiset {dandiset_id}...")

        branch_list = _deploy_subprocess(command="git branch -r", cwd=repo_directory)
        branches = {line.strip() for line in branch_list.splitlines()}
        skip_branches = {"origin/HEAD -> origin/draft", "origin/draft", "origin/git-annex"}
        branches_to_delete = branches - skip_branches
        print("\tCleaning excess branches...")
        for branch in branches_to_delete:
            branch_name = branch.removeprefix("origin/")
            _deploy_subprocess(command=f"git push origin --delete {branch_name}", cwd=repo_directory)
            _deploy_subprocess(command=f"git branch -D {branch_name}", cwd=repo_directory, ignore_errors=True)
            _deploy_subprocess(command=f"git branch -D {branch_name}", cwd=repo_directory, ignore_errors=True)

        print("Cleaning complete!\n\n")


if __name__ == "__main__":
    reset_excess_branches()
