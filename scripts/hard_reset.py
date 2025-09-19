import os
import pathlib
import shutil
import subprocess

import dandi.dandiapi
import requests

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", None)
if GITHUB_TOKEN is None:
    message = "GITHUB_TOKEN environment variable not set"
    raise ValueError(message)

BASE_GITHUB_API_URL = "https://api.github.com/repos"
BASE_DIRECTORY = pathlib.Path("E:/GitHub/bids-dandisets")
authentication_header = {"Authorization": f"token {GITHUB_TOKEN}"}


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


def reset_github_repos() -> None:
    client = dandi.dandiapi.DandiAPIClient()
    dandisets = client.get_dandisets()

    for dandiset in dandisets:
        dandiset_id = dandiset.identifier
        repo_directory = BASE_DIRECTORY / dandiset_id

        print(f"Cleaning Dandiset {dandiset_id}...")
        if repo_directory.exists():
            print("Cleaning local directory...")
            shutil.rmtree(path=repo_directory)

        repo_name = f"bids-dandisets/{dandiset_id}"
        repo_api_url = f"{BASE_GITHUB_API_URL}/{repo_name}"
        response = requests.get(url=repo_api_url, headers=authentication_header)
        if response.status_code == 200:
            print("Cleaning repository...")
            headers = {"Authorization": f"token {GITHUB_TOKEN}"}
            requests.delete(url=repo_api_url, headers=headers)
        print("Cleaning complete!\n\n")


if __name__ == "__main__":
    reset_github_repos()
